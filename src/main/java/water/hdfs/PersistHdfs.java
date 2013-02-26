package water.hdfs;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import water.*;
import water.api.Constants;

import com.google.common.base.Strings;
import com.google.common.io.ByteStreams;
import com.google.common.io.Closeables;
import com.google.gson.*;

/** Persistence backend for HDFS */
public abstract class PersistHdfs {
  static         final String KEY_PREFIX="hdfs:";
  static         final int    KEY_PREFIX_LEN = KEY_PREFIX.length();
  static private final Configuration CONF;

  public static void initialize() { }
  static {
    Configuration conf = null;
    if( H2O.OPT_ARGS.hdfs_config != null ) {
      conf = new Configuration();
      File p = new File(H2O.OPT_ARGS.hdfs_config);
      if (!p.exists())
        Log.die("[h2o,hdfs] Unable to open hdfs configuration file "+p.getAbsolutePath());
      conf.addResource(new Path(p.getAbsolutePath()));
      System.out.println("[h2o,hdfs] resource " + p.getAbsolutePath() + " added to the hadoop configuration");
    } else {
      conf = new Configuration();
      if( !Strings.isNullOrEmpty(H2O.OPT_ARGS.hdfs) ) {
        // setup default remote Filesystem - for version 0.21 and higher
        conf.set("fs.defaultFS",H2O.OPT_ARGS.hdfs);
        // To provide compatibility with version 0.20.0 it is necessary to setup the property
        // fs.default.name which was in newer version renamed to 'fs.defaultFS'
        conf.set("fs.default.name",H2O.OPT_ARGS.hdfs);
      }
    }
    CONF = conf;
  }

  public static Configuration getConf() { return CONF; }

  public static void addFolder(Path p, JsonArray succeeded, JsonArray failed) throws IOException {
    FileSystem fs = FileSystem.get(p.toUri(), CONF);
    addFolder(fs, p, succeeded, failed);
  }

  @SuppressWarnings("deprecation")
  private static void addFolder(FileSystem fs, Path p, JsonArray succeeded, JsonArray failed) {
    try {
      for( FileStatus file : fs.listStatus(p) ) {
        Path pfs = file.getPath();
        if( file.isDir() ) {
          addFolder(fs, pfs, succeeded, failed);
        } else {
          Key k = getKeyForPathString(pfs.toString());
          long size = file.getLen();
          Value val = null;
          if( pfs.getName().endsWith(".hex") ) { // Hex file?
            FSDataInputStream s = fs.open(pfs);
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
          DKV.put(k, val);

          JsonObject o = new JsonObject();
          o.addProperty(Constants.KEY, k.toString());
          o.addProperty(Constants.FILE, p.toString());
          succeeded.add(o);
        }
      }
    } catch( IOException e ) {
      JsonObject o = new JsonObject();
      o.addProperty(Constants.FILE, p.toString());
      o.addProperty(Constants.ERROR, e.getMessage());
      failed.add(o);
    }
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

  static Key getKeyForPathString_impl(String str) {
    return Key.make(KEY_PREFIX+str);
  }
  private static String getPathStringForKey_impl(Key k) {
    return new String(k._kb, KEY_PREFIX_LEN, k._kb.length-KEY_PREFIX_LEN);
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
          int value_len = DKV.get(k).memOrLoad().length;  // How long is the ValueArray header?
          skip += value_len;
        }
      }
      Path p = getPathForKey(k);
      FileSystem fs = FileSystem.get(p.toUri(), CONF);
      s = fs.open(p);
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
    assert !v.isArray();
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

  static public Value lazyArrayChunk( Key key ) {
    Key arykey = ValueArray.getArrayKey(key);  // From the base file key
    long off = ValueArray.getChunkOffset(key); // The offset
    long size = 0;
    try {
      Path p = getPathForKey(arykey);
      FileSystem fs = FileSystem.get(p.toUri(), CONF);
      size = fs.getFileStatus(p).getLen();
    } catch( IOException e ) {
      System.err.println(e);
      return null;
    }
    long rem = size-off;        // Remainder to be read
    if( arykey.toString().endsWith(".hex") ) { // Hex file?
      int value_len = DKV.get(arykey).memOrLoad().length;  // How long is the ValueArray header?
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
      FileSystem fs = FileSystem.get(p.toUri(), CONF);
      fs.mkdirs(p.getParent());
      s = fs.create(p);
      byte[] b = f.write(new AutoBuffer()).buf();
      s.write(b);
    } catch( IOException e ) {
      res = e.getMessage(); // Just the exception message, throwing the stack trace away
    } finally {
      Closeables.closeQuietly(s);
    }
    return res;
  }

  // Append the chunk Value to the file specified by the key.
  // Return null on success, and a failure string otherwise.
  static public String appendChunk( Key key, Value val ) {
    String res = null;
    FSDataOutputStream s = null;
    try {
      Path p = getPathForKey(key);
      FileSystem fs = FileSystem.get(p.toUri(), CONF);
      s = fs.append(p);
      System.err.println("[hdfs] append="+val.memOrLoad().length);
      s.write(val.memOrLoad());
    } catch( IOException e ) {
      res = e.getMessage(); // Just the exception message, throwing the stack trace away
    } finally {
      Closeables.closeQuietly(s);
    }
    return res;
  }
}
