package water.hdfs;

import water.*;
import water.DTask;

/**
 * Distributed task to store key on HDFS.
 *
 * If it is a simple value, it is simply stored on hdfs.  For arraylets, chunks
 * are stored in order by their home nodes.  Each node continues storing chunks
 * until the next to be stored has different home in which case the task is
 * passed to the home node of that chunk.
 *
 * @author tomasnykodym, cliffc
 *
 */
public class TaskStore2HDFS extends DTask<TaskStore2HDFS> {
  Key _arykey;                  // Base array key
  long _indexFrom;              // Chunk number
  String _err;                  // Error reporting string

  public static String store2Hdfs(Key srcKey) {
    assert srcKey._kb[0] != Key.ARRAYLET_CHUNK;
    assert PersistHdfs.getPathForKey(srcKey) != null; // Validate key name
    Value v = DKV.get(srcKey);
    if( v == null ) return "Key "+srcKey+" not found";
    if( !v.isArray() ) {        // Simple chunk?
      v.setHdfs();              // Set to HDFS and be done
      return null;              // Success
    }

    // For ValueArrays, make the .hex header
    ValueArray ary = v.get();
    String err = PersistHdfs.freeze(srcKey,ary);
    if( err != null ) return err;

    // The task managing which chunks to write next,
    // store in a known key
    TaskStore2HDFS ts = new TaskStore2HDFS(srcKey);
    Key selfKey = ts.selfKey();
    UKV.put(selfKey,ts);

    // Then start writing chunks in-order with the zero chunk
    H2ONode chk0_home = ValueArray.getChunkKey(0,srcKey).home_node();
    RPC.call(ts.chunkHome(),ts);

    // Watch the progress key until it gets removed or an error appears
    long idx = 0;
    while( (ts=UKV.get(selfKey,TaskStore2HDFS.class)) != null ) {
      if( ts._indexFrom != idx ) {
        System.out.print(" "+idx+"/"+ary.chunks());
        idx = ts._indexFrom;
      }
      if( ts._err != null ) {   // Found an error?
        UKV.remove(selfKey);    // Cleanup & report
        return ts._err;
      }
      try { Thread.sleep(100); } catch( InterruptedException e ) { }
    }
    System.out.println(" "+ary.chunks()+"/"+ary.chunks());

    //PersistHdfs.refreshHDFSKeys();
    return null;
  }

  public TaskStore2HDFS(Key srcKey) { _arykey = srcKey; }

  @Override
  public final TaskStore2HDFS invoke(H2ONode sender) {
    compute();
    return this;
  }

  @Override
  public void compute2() {
    String path = null;// getPathFromValue(val);
    ValueArray ary = DKV.get(_arykey).get();
    Key self = selfKey();

    while( _indexFrom < ary.chunks() ) {
      Key ckey = ary.getChunkKey(_indexFrom++);
      if( !ckey.home() ) {      // Next chunk not At Home?
        RPC.call(chunkHome(),this); // Hand the baton off to the next node/chunk
        return;
      }
      Value val = DKV.get(ckey); // It IS home, so get the data
      _err = PersistHdfs.appendChunk(_arykey,val);
      if( _err != null ) return;
      UKV.put(self,this);       // Update the progress/self key
    }
    // We did the last chunk.  Removing the selfKey is the signal to the web
    // thread that All Done.
    UKV.remove(self);
  }

  private Key selfKey() {
    return Key.make("Store2HDFS"+_arykey);
  }
  private H2ONode chunkHome() {
    return ValueArray.getChunkKey(_indexFrom,_arykey).home_node();
  }
}
