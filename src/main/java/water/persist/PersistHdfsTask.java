package water.persist;

import java.io.IOException;

import org.apache.hadoop.fs.*;

import water.*;
import water.util.Log;
import water.util.Log.Tag.Sys;

/**
 * Distributed task to store key on HDFS.
 *
 * If it is a simple value, it is simply stored on hdfs. For arraylets, chunks are stored in order
 * by their home nodes. Each node continues storing chunks until the next to be stored has different
 * home in which case the task is passed to the home node of that chunk.
 *
 * @author tomasnykodym, cliffc
 */
public class PersistHdfsTask extends DTask<PersistHdfsTask> {
  String _path;
  Key _arykey;                  // Base array key
  long _indexFrom;              // Chunk number
  String _err;                  // Error reporting string

  public static String store2Hdfs(String path, Key key) {
    assert key._kb[0] != Key.ARRAYLET_CHUNK;
    Value v = DKV.get(key);
    if( v == null ) return "Key " + key + " not found";
    if( !v.isArray() ) {        // Simple chunk?
      v.setHdfs();              // Set to HDFS and be done
      return null;              // Success
    }

    // For ValueArrays, make the .hex header
    ValueArray ary = v.get();
    String err = freeze(path, ary);
    if( err != null ) return err;

    // The task managing which chunks to write next,
    // store in a known key
    PersistHdfsTask ts = new PersistHdfsTask();
    ts._path = path;
    ts._arykey = ary._key;
    Key selfKey = ts.selfKey();
    UKV.put(selfKey, ts);

    // Then start writing chunks in-order with the zero chunk
    RPC.call(ts.chunkHome(), ts);

    // Watch the progress key until it gets removed or an error appears
    long idx = 0;
    while( (ts = UKV.get(selfKey, PersistHdfsTask.class)) != null ) {
      if( ts._indexFrom != idx ) {
        Log.debug(Sys.HDFS_, idx, "/", ary.chunks());
        idx = ts._indexFrom;
      }
      if( ts._err != null ) {   // Found an error?
        UKV.remove(selfKey);    // Cleanup & report
        return ts._err;
      }
      try {
        Thread.sleep(100);
      } catch( InterruptedException e ) {}
    }
    Log.debug(Sys.HDFS_, ary.chunks(), "/", ary.chunks());
    //PersistHdfs.refreshHDFSKeys();
    return null;
  }

  @Override public void compute2() {
    ValueArray ary = DKV.get(_arykey).get();
    Key self = selfKey();

    while( _indexFrom < ary.chunks() ) {
      Key ckey = ary.getChunkKey(_indexFrom++);
      if( !ckey.home() ) {      // Next chunk not At Home?
        RPC.call(chunkHome(), this); // Hand the baton off to the next node/chunk
        return;
      }
      Value val = DKV.get(ckey); // It IS home, so get the data
      _err = appendChunk(_path, val.memOrLoad());
      if( _err != null ) return;
      UKV.put(self, this);       // Update the progress/self key
    }
    // We did the last chunk.  Removing the selfKey is the signal to the web
    // thread that All Done.
    UKV.remove(self);
  }

  private Key selfKey() {
    return Key.make("Store2HDFS" + _arykey);
  }

  private H2ONode chunkHome() {
    return ValueArray.getChunkKey(_indexFrom, _arykey).home_node();
  }

  // Write this freezable to the file specified by the key.
  // Return null on success, and a failure string otherwise.
  private static String freeze(String path, Freezable f) {
    String res = null;
    FSDataOutputStream s = null;
    try {
      Path p = new Path(path);
      FileSystem fs = FileSystem.get(p.toUri(), PersistHdfs.CONF);
      fs.mkdirs(p.getParent());
      s = fs.create(p);
      byte[] b = f.write(new AutoBuffer()).buf();
      s.write(b);
    } catch( IOException e ) {
      res = e.getMessage(); // Just the exception message, throwing the stack trace away
    } finally {
      try {
        if( s != null ) s.close();
      } catch( IOException e ) {}
    }
    return res;
  }

  private static String appendChunk(String path, byte[] data) {
    String res = null;
    FSDataOutputStream s = null;
    try {
      Path p = new Path(path);
      FileSystem fs = FileSystem.get(p.toUri(), PersistHdfs.CONF);
      s = fs.append(p);
      Log.debug(Sys.HDFS_, "append=" + data.length);
      s.write(data);
    } catch( IOException e ) {
      res = e.getMessage(); // Just the exception message, throwing the stack trace away
    } finally {
      try {
        if( s != null ) s.close();
      } catch( IOException e ) {}
    }
    return res;
  }
}
