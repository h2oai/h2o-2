package water.hdfs;

import water.*;
import java.io.*;
import jsr166y.ForkJoinWorkerThread;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import com.google.common.io.ByteStreams;
import com.google.common.io.Closeables;

/** Persistence backend for HDFS */
public abstract class PersistHdfs {
  static         final String KEY_PREFIX="hdfs:";
  static         final int    KEY_PREFIX_LEN = KEY_PREFIX.length();
  static         final int    HDFS_LEN;
  static private final Configuration _conf;
  static private       FileSystem _fs;
  static         final String ROOT;

  static void initialize() {}
  static {
    if( H2O.OPT_ARGS.hdfs_config!=null ) {
      _conf = new Configuration();
      File p = new File(H2O.OPT_ARGS.hdfs_config);
      if (!p.exists())
        Log.die("[h2o,hdfs] Unable to open hdfs configuration file "+p.getAbsolutePath());
      _conf.addResource(p.getAbsolutePath());
      System.out.println("[h2o,hdfs] resource " + p.getAbsolutePath() + " added to the hadoop configuration");
    } else {
      if( H2O.OPT_ARGS.hdfs != null && !H2O.OPT_ARGS.hdfs.isEmpty() ) {
        _conf = new Configuration();
        _conf.set("fs.defaultFS",H2O.OPT_ARGS.hdfs);
      } else {
        _conf = null;
      }
    }
    ROOT = H2O.OPT_ARGS.hdfs_root == null ? "ice" : H2O.OPT_ARGS.hdfs_root;
    if( H2O.OPT_ARGS.hdfs_config != null || (H2O.OPT_ARGS.hdfs != null && !H2O.OPT_ARGS.hdfs.isEmpty()) ) {
      HDFS_LEN = H2O.OPT_ARGS.hdfs.length();
      try {
        _fs = FileSystem.get(_conf);
        if (H2O.OPT_ARGS.hdfs_nopreload==null) {
          // This code blocks alot, and does not have FJBlock support coded in
          assert !(Thread.currentThread() instanceof ForkJoinWorkerThread);
          _fs.listStatus(new Path(ROOT)); // Initial touch of top-level path
          int num = addFolder(new Path(ROOT));
          System.out.println("[h2o,hdfs] " + H2O.OPT_ARGS.hdfs+ROOT+" loaded " + num + " keys");
        }
      } catch( IOException e ) {
        System.out.println(e.getMessage());
        Log.die("[h2o,hdfs] Unable to initialize persistency store home at " + H2O.OPT_ARGS.hdfs+ROOT);
      }
    } else {
      HDFS_LEN = 0;
    }
  }

  private static int addFolder(Path p) {
    int num=0;
    try {
      for( FileStatus fs : _fs.listStatus(p) ) {
        Path pfs = fs.getPath();
        if( fs.isDir() ) {
          num += addFolder(pfs);
        } else {
          num++;
          Key k = getKeyForPathString(pfs.toString());
          long size = fs.getLen();
          Value val = null;
          if( pfs.getName().endsWith(".hex") ) { // Hex file?
            FSDataInputStream s = _fs.open(pfs);
            int sz = (int)Math.min(1L<<20,size); // Read up to the 1st meg
            byte [] mem = MemoryManager.malloc1(sz);
            s.readFully(mem);
            // Convert to a ValueArray (hope it fits in 1Meg!)
            ValueArray ary = new ValueArray(k,size,Value.HDFS).read(new AutoBuffer(mem));
            ary._persist = Value.HDFS|Value.ON_dsk;
            val = ary.value();
          } else if( size >= 2*ValueArray.CHUNK_SZ ) {
            val = new ValueArray(k,size,Value.HDFS).value(); // ValueArray byte wrapper over a large file
          } else {
            val = new Value(k,(int)size,Value.HDFS); // Plain Value
          }
          val.setdsk();
          H2O.putIfAbsent_raw(k,val);
        }
      }
    } catch( IOException e ) {
      System.err.println("[hdfs] Unable to list the folder " + p.toString()+" : "+e);
    }
    return num;
  }

  // file name implementation -------------------------------------------------
  // Convert Keys to Path Strings and vice-versa.  Assert this is a bijection.
  static Key getKeyForPathString(String str) {
    Key key = getKeyForPathString_impl(str);
    assert getPathStringForKey_impl(key).equals(str)
      : "hdfs name bijection: '"+str+"' makes key "+key+" makes '"+getPathStringForKey_impl(key)+"'";
    return key;
  }
  static private String getPathStringForKey(Key key) {
    String str = getPathStringForKey_impl(key);
    assert getKeyForPathString_impl(str).equals(key)
      : "hdfs name bijection: key "+key+" makes '"+str+"' makes key "+getKeyForPathString_impl(str);
    return str;
  }
  // Actually we typically want a Path not a String
  public static Path getPathForKey(Key k) {  return new Path(getPathStringForKey(k)); }

  // The actual conversions; str->key and key->str
  // Convert string 'hdfs://192.168.1.151/datasets/3G_poker_shuffle'
  // into    key                    'hdfs:datasets/3G_poker_shuffle'
  static Key getKeyForPathString_impl(String str) {
    assert str.indexOf(H2O.OPT_ARGS.hdfs)==0 : str;
    return Key.make(KEY_PREFIX+str.substring(HDFS_LEN));
  }
  private static String getPathStringForKey_impl(Key k) {
    return H2O.OPT_ARGS.hdfs+new String(k._kb,KEY_PREFIX_LEN,k._kb.length-KEY_PREFIX_LEN);
  }

  // file implementation -------------------------------------------------------

  // Read up to 'len' bytes of Value.  Value should already be persisted to
  // disk.  A racing delete can trigger a failure where we get a null return,
  // but no crash (although one could argue that a racing load&delete is a bug
  // no matter what).
  public static byte[] fileLoad(Value v) {
    byte[] b = MemoryManager.malloc1(v._max);
    FSDataInputStream s = null;
    try {
      long skip = 0;
      Key k = v._key;
      // Convert an arraylet chunk into a long-offset from the base file.
      if( k._kb[0] == Key.ARRAYLET_CHUNK ) {
        skip = ValueArray.getChunkOffset(k); // The offset
        k = ValueArray.getArrayKey(k);       // From the base file key
        if( k.toString().endsWith(".hex") ) { // Hex file?
          int value_len = DKV.get(k).get().length;  // How long is the ValueArray header?
          skip += value_len;
        }
      }
      s = _fs.open(getPathForKey(k));
      ByteStreams.skipFully(s, skip);
      ByteStreams.readFully(s, b);
      assert v.isPersisted();
      return b;
    } catch( IOException e ) { // Broken disk / short-file???
      System.err.println(e);
      return null;
    } finally {
      Closeables.closeQuietly(s);
    }
  }

  // Store Value v to disk.
  public static void fileStore(Value v) {
    // Only the home node does persistence on NFS
    if( !v._key.home() ) return;
    assert !v.isPersisted();

    // Never store arraylets on NFS, instead we'll store the entire array.
    assert v._isArray==0;
    throw H2O.unimpl();
    //try {
    //  Path p = getPathForKey(v._key);
    //  f.mkdirs();
    //  FSDataOutputStream s = new FSDataOutputStream(f);
    //  try {
    //    byte[] m = v.mem();
    //    assert (m == null || m.length == v._max); // Assert not saving partial files
    //    if( m!=null )
    //      s.write(m);
    //    v.setdsk(); // Set as write-complete to disk
    //  } finally {
    //    s.close();
    //  }
    //} catch( IOException e ) {
    //}
  }

  static public void fileDelete(Value v) {
    assert !v.isPersisted(); // Upper layers already cleared out
    throw H2O.unimpl();
    //File f = getFileForKey(v._key);
    //f.delete();
  }

  static public Value lazyArrayChunk( Key key ) {
    Key arykey = ValueArray.getArrayKey(key);  // From the base file key
    long off = ValueArray.getChunkOffset(key); // The offset
    long size = 0;
    try {
      size = _fs.getFileStatus(getPathForKey(arykey)).getLen();
    } catch( IOException e ) {
      System.err.println(e);
      return null;
    }
    long rem = size-off;        // Remainder to be read
    if( arykey.toString().endsWith(".hex") ) { // Hex file?
      int value_len = DKV.get(arykey).get().length;  // How long is the ValueArray header?
      rem -= value_len;
    }
    // the last chunk can be fat, so it got packed into the earlier chunk
    if( rem < ValueArray.CHUNK_SZ && off > 0 ) return null;
    int sz = (rem >= ValueArray.CHUNK_SZ*2) ? (int)ValueArray.CHUNK_SZ : (int)rem;
    Value val = new Value(key,sz,Value.HDFS);
    val.setdsk(); // But its already on disk.
    return val;
  }

  // Write this freezable to the file specified by the key.
  // Return null on success, and a failure string otherwise.
  static public String freeze( Key key, Freezable f ) {
    String res = null;
    FSDataOutputStream s = null;
    try {
      Path p = getPathForKey(key);
      _fs.mkdirs(p.getParent());
      s = _fs.create(p);
      byte[] b = f.write(new AutoBuffer()).buf();
      s.write(b);
    } catch( IOException e ) {
      res = e.getMessage(); // Just the exception message, throwing the stack trace away
    } finally {
      if( s != null )
        try { s.close(); } catch( IOException e ) { }
    }
    return res;
  }

  // Append the chunk Value to the file specified by the key.
  // Return null on success, and a failure string otherwise.
  static public String appendChunk( Key key, Value val ) {
    String res = null;
    FSDataOutputStream s = null;
    try {
      s = _fs.append(getPathForKey(key));
      System.err.println("[hdfs] append="+val.get().length);
      s.write(val.get());
    } catch( IOException e ) {
      res = e.getMessage(); // Just the exception message, throwing the stack trace away
    } finally {
      if( s != null )
        try { s.close(); } catch( IOException e ) { }
    }
    return res;
  }
}
