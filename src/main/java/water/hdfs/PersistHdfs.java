package water.hdfs;

import java.io.*;
import java.net.SocketTimeoutException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.s3.S3Exception;

import water.*;
import water.Job.ProgressMonitor;
import water.api.Constants;
import water.util.*;
import water.util.Log.Tag.Sys;

import com.google.common.base.Strings;
import com.google.common.io.ByteStreams;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

/** Persistence backend for HDFS */
public abstract class PersistHdfs {
  static         final String KEY_PREFIX="";
  static         final int    KEY_PREFIX_LEN = KEY_PREFIX.length();
  static private final Configuration CONF;

  public static void initialize() { }
  static {
    Configuration conf = null;
    if( H2O.OPT_ARGS.hdfs_config != null ) {
      conf = new Configuration();
      File p = new File(H2O.OPT_ARGS.hdfs_config);
      if (!p.exists())
        Log.die("Unable to open hdfs configuration file "+p.getAbsolutePath());
      conf.addResource(new Path(p.getAbsolutePath()));
      Log.debug(Sys.HDFS_,"resource ", p.getAbsolutePath(), " added to the hadoop configuration");
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


  public static class H2OHdfsInputStream extends RIStream {
    final FileSystem _fs;
    final Path _path;

    public H2OHdfsInputStream(Path p, long offset,ProgressMonitor pmon) throws IOException{
      super(offset,pmon);
      _path = p;
      _fs = FileSystem.get(p.toUri(), CONF);
      setExpectedSz(_fs.getFileStatus(p).getLen());
      open();
    }

    @Override protected InputStream open(long offset) throws IOException {
      FSDataInputStream is = _fs.open(_path);
      is.seek(offset);
      return is;
    }

  }
  public static InputStream openStream(Key k,ProgressMonitor pmon) throws IOException{
    H2OHdfsInputStream res = null;
    try{
      res =  new H2OHdfsInputStream(getPathForKey(k),0,pmon);
    } catch(IOException e){
      try{Thread.sleep(1000);} catch(Exception ex){}
      Log.warn("Error while opening HDFS key " + k.toString() + ", will wait and retry.");
      res = new H2OHdfsInputStream(getPathForKey(k),0,pmon);
    }
    return res;
  }

  public static void addFolder(Path p, JsonArray succeeded, JsonArray failed) throws IOException {
    FileSystem fs = FileSystem.get(p.toUri(), CONF);
    addFolder(fs, p, succeeded, failed);
  }

  @SuppressWarnings("deprecation")
  private static void addFolder(FileSystem fs, Path p, JsonArray succeeded, JsonArray failed) {
    try {
      if( fs == null ) return;
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
            ValueArray ary = new ValueArray(k,0).read(new AutoBuffer(mem));
            val = new Value(k,ary,Value.HDFS);
          } else if( size >= 2*ValueArray.CHUNK_SZ ) {
            val = new Value(k,new ValueArray(k,size),Value.HDFS); // ValueArray byte wrapper over a large file
          } else {
            val = new Value(k,(int)size,Value.HDFS); // Plain Value
          }
          val.setdsk();
          DKV.put(k, val);

          JsonObject o = new JsonObject();
          o.addProperty(Constants.KEY, k.toString());
          o.addProperty(Constants.FILE, pfs.toString());
          o.addProperty(Constants.VALUE_SIZE, file.getLen());
          succeeded.add(o);
        }
      }
    } catch( IOException e ) {
      Log.err(e);
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
    // Count all i/o time from here, including all retry overheads
    long start_io_ms = System.currentTimeMillis();
    byte[] b = MemoryManager.malloc1(v._max);
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
    FSDataInputStream s = null;
    while(true) {
      try {
        long start_ns = System.nanoTime(); // Blocking i/o call timing - without counting repeats
        FileSystem fs = FileSystem.get(p.toUri(), CONF);
        s = fs.open(p);
        // NOTE:
        // The following line degrades performance of HDFS load from S3 API: s.readFully(skip,b,0,b.length);
        // Google API's simple seek has better performance
        // Load of 300MB file via Google API ~ 14sec, via s.readFully ~ 5min (under the same condition)
        ByteStreams.skipFully(s, skip);
        ByteStreams.readFully(s, b);
        assert v.isPersisted();
        TimeLine.record_IOclose(start_ns,start_io_ms,1/*read*/,v._max,Value.HDFS);
        return b;
      // Explicitly ignore the following exceptions but
      // fail on the rest IOExceptions
      } catch (EOFException e)           { ignoreAndWait(e,false);
      } catch (SocketTimeoutException e) { ignoreAndWait(e,false);
      } catch (S3Exception e)            { ignoreAndWait(e,false);
      } catch (IOException e)            { ignoreAndWait(e,true);
      } finally {
        try { if( s != null ) s.close(); } catch( IOException e ) { }
      }
    }
  }

  private static void ignoreAndWait(final Exception e, boolean printException) {
    H2O.ignore(e, "Hit HDFS reset problem, retrying...", printException);
    try { Thread.sleep(500); } catch (InterruptedException ie) {}
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
    while (true) {
      try {
        Path p = getPathForKey(arykey);
        FileSystem fs = FileSystem.get(p.toUri(), CONF);
        size = fs.getFileStatus(p).getLen();
        break;
      } catch (EOFException e)           { ignoreAndWait(e,false);
      } catch (SocketTimeoutException e) { ignoreAndWait(e,false);
      } catch (S3Exception e)            { ignoreAndWait(e,false);
      } catch (IOException e)            { ignoreAndWait(e,true);
      }
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
      try { if( s != null ) s.close(); } catch( IOException e ) { }
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
      Log.debug(Sys.HDFS_,"append="+val.memOrLoad().length);
      s.write(val.memOrLoad());
    } catch( IOException e ) {
      res = e.getMessage(); // Just the exception message, throwing the stack trace away
    } finally {
      try { if( s != null ) s.close(); } catch( IOException e ) { }
    }
    return res;
  }
}
