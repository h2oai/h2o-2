package water.persist;

import java.io.*;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.util.ArrayList;
import java.util.concurrent.Callable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;

import water.*;
import water.Job.ProgressMonitor;
import water.api.Constants;
import water.api.Constants.Extensions;
import water.fvec.*;
import water.util.*;
import water.util.Log.Tag.Sys;

import com.google.common.base.Strings;
import com.google.common.io.ByteStreams;
import dontweave.gson.*;


public final class PersistHdfs extends Persist {
  public static final Configuration CONF;
  private final Path _iceRoot;

  // Returns String with path for given key.
  private static String getPathForKey(Key k) {
    final int off = k._kb[0]==Key.DVEC ? Vec.KEY_PREFIX_LEN : 0;
    return new String(k._kb,off,k._kb.length-off);
  }

  static {
    Log.POST(4001, "");
    Configuration conf = null;
    Log.POST(4002, "");
    if( H2O.OPT_ARGS.hdfs_config != null ) {
      Log.POST(4003, "");
      conf = new Configuration();
      File p = new File(H2O.OPT_ARGS.hdfs_config);
      if( !p.exists() ) Log.die("Unable to open hdfs configuration file " + p.getAbsolutePath());
      conf.addResource(new Path(p.getAbsolutePath()));
      Log.debug(Sys.HDFS_, "resource ", p.getAbsolutePath(), " added to the hadoop configuration");
      Log.info(Sys.HDFS_, "resource ", p.getAbsolutePath(), " added to the hadoop configuration");
      Log.POST(4004, "");
    } else {
      Log.POST(4005, "");
      conf = new Configuration();
      Log.POST(4006, "");
      if( !Strings.isNullOrEmpty(H2O.OPT_ARGS.hdfs) ) {
        // setup default remote Filesystem - for version 0.21 and higher
        Log.POST(4007, "");
        conf.set("fs.defaultFS", H2O.OPT_ARGS.hdfs);
        // To provide compatibility with version 0.20.0 it is necessary to setup the property
        // fs.default.name which was in newer version renamed to 'fs.defaultFS'
        Log.POST(4008, "");
        conf.set("fs.default.name", H2O.OPT_ARGS.hdfs);
      }
    }
    Log.POST(4009, "");
    CONF = conf;
    Log.POST(4010, "");
  }

  // Loading HDFS files
  PersistHdfs() {
    Log.POST(4000, "");
    _iceRoot = null;
  }

  // Loading/Writing ice to HDFS
  PersistHdfs(URI uri) {
    try {
      _iceRoot = new Path(uri + "/ice" + H2O.SELF_ADDRESS.getHostAddress() + "-" + H2O.API_PORT);
      // Make the directory as-needed
      FileSystem fs = FileSystem.get(_iceRoot.toUri(), CONF);
      fs.mkdirs(_iceRoot);
    } catch( Exception e ) {
      throw Log.errRTExcept(e);
    }
  }

  @Override public String getPath() {
    return _iceRoot != null ? _iceRoot.toString() : null;
  }

  @Override public void loadExisting() {
    // TODO?
    throw new UnsupportedOperationException();
  }

  @Override public void clear() {
    assert this == getIce();
    run(new Callable() {
      @Override public Object call() throws Exception {
        FileSystem fs = FileSystem.get(_iceRoot.toUri(), CONF);
        fs.delete(_iceRoot, true);
        return null;
      }
    }, false, 0);
  }

  private static class H2OHdfsInputStream extends RIStream {
    final FileSystem _fs;
    final Path _path;

    public H2OHdfsInputStream(Path p, long offset, ProgressMonitor pmon) throws IOException {
      super(offset, pmon);
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

  public static InputStream openStream(Key k, ProgressMonitor pmon) throws IOException {
    H2OHdfsInputStream res = null;
    Path p = new Path(k.toString());
    try {
      res = new H2OHdfsInputStream(p, 0, pmon);
    } catch( IOException e ) {
      try {
        Thread.sleep(1000);
      } catch( Exception ex ) {}
      Log.warn("Error while opening HDFS key " + k.toString() + ", will wait and retry.");
      res = new H2OHdfsInputStream(p, 0, pmon);
    }
    return res;
  }


  @Override public byte[] load(final Value v) {
    final byte[] b = MemoryManager.malloc1(v._max);
    long skip = 0;
    Key k = v._key;
    if(k._kb[0] == Key.DVEC)
      skip = FileVec.chunkOffset(k); // The offset
    final Path p = _iceRoot == null?new Path(getPathForKey(k)):new Path(_iceRoot, getIceName(v));
    final long skip_ = skip;
    run(new Callable() {
      @Override public Object call() throws Exception {
        FileSystem fs = FileSystem.get(p.toUri(), CONF);
        FSDataInputStream s = null;
        try {
          s = fs.open(p);
          // NOTE:
          // The following line degrades performance of HDFS load from S3 API: s.readFully(skip,b,0,b.length);
          // Google API's simple seek has better performance
          // Load of 300MB file via Google API ~ 14sec, via s.readFully ~ 5min (under the same condition)
          ByteStreams.skipFully(s, skip_);
          ByteStreams.readFully(s, b);
          assert v.isPersisted();
        } finally {
          Utils.close(s);
        }
        return null;
      }
    }, true, v._max);
    return b;
  }

  @Override public void store(Value v) {
    // Should be used only if ice goes to HDFS
    assert this == getIce();
    assert !v.isPersisted();
    byte[] m = v.memOrLoad();
    assert (m == null || m.length == v._max); // Assert not saving partial files
    store(new Path(_iceRoot, getIceName(v)), m);
    v.setdsk(); // Set as write-complete to disk
  }

  public static void store(final Path path, final byte[] data) {
    run(new Callable() {
      @Override public Object call() throws Exception {
        FileSystem fs = FileSystem.get(path.toUri(), CONF);
        fs.mkdirs(path.getParent());
        FSDataOutputStream s = fs.create(path);
        try {
          s.write(data);
        } finally {
          s.close();
        }
        return null;
      }
    }, false, data.length);
  }

  @Override public void delete(final Value v) {
    assert this == getIce();
    assert !v.isPersisted();   // Upper layers already cleared out
    run(new Callable() {
      @Override public Object call() throws Exception {
        Path p = new Path(_iceRoot, getIceName(v));
        FileSystem fs = FileSystem.get(p.toUri(), CONF);
        fs.delete(p, true);
        return null;
      }
    }, false, 0);
  }

  private static class Size {
    int _value;
  }

  private static void run(Callable c, boolean read, int size) {
    // Count all i/o time from here, including all retry overheads
    long start_io_ms = System.currentTimeMillis();
    while( true ) {
      try {
        long start_ns = System.nanoTime(); // Blocking i/o call timing - without counting repeats
        c.call();
        TimeLine.record_IOclose(start_ns, start_io_ms, read ? 1 : 0, size, Value.HDFS);
        break;
        // Explicitly ignore the following exceptions but
        // fail on the rest IOExceptions
      } catch( EOFException e ) {
        ignoreAndWait(e, false);
      } catch( SocketTimeoutException e ) {
        ignoreAndWait(e, false);
      } catch( IOException e ) {
        // Newer versions of Hadoop derive S3Exception from IOException
        if (e.getClass().getName().contains("S3Exception")) {
          ignoreAndWait(e, false);
        } else {
          ignoreAndWait(e, true);
        }
      } catch( RuntimeException e ) {
        // Older versions of Hadoop derive S3Exception from RuntimeException
        if (e.getClass().getName().contains("S3Exception")) {
          ignoreAndWait(e, false);
        } else {
          throw Log.errRTExcept(e);
        }
      } catch( Exception e ) {
        throw Log.errRTExcept(e);
      }
    }
  }

  private static void ignoreAndWait(final Exception e, boolean printException) {
    H2O.ignore(e, "Hit HDFS reset problem, retrying...", printException);
    try {
      Thread.sleep(500);
    } catch( InterruptedException ie ) {}
  }

  /*
   * Load all files in a folder.
   */

  public static void addFolder(Path p, JsonArray succeeded, JsonArray failed) throws IOException {
    FileSystem fs = FileSystem.get(p.toUri(), PersistHdfs.CONF);
    if(!fs.exists(p)){
      JsonObject o = new JsonObject();
      o.addProperty(Constants.FILE, p.toString());
      o.addProperty(Constants.ERROR, "Path does not exist!");
      failed.add(o);
      return;
    }
    addFolder(fs, p, succeeded, failed);
  }

  public static void addFolder2(Path p, ArrayList<String> keys,ArrayList<String> failed) throws IOException {
    FileSystem fs = FileSystem.get(p.toUri(), PersistHdfs.CONF);
    if(!fs.exists(p)){
      failed.add("Path does not exist: '" + p.toString() + "'");
      return;
    }
    addFolder2(fs, p, keys, failed);
  }

  private static void addFolder2(FileSystem fs, Path p, ArrayList<String> keys, ArrayList<String> failed) {
    try {
      if( fs == null ) return;

      Futures futures = new Futures();
      for( FileStatus file : fs.listStatus(p) ) {
        Path pfs = file.getPath();
        if( file.isDir() ) {
          addFolder2(fs, pfs, keys, failed);
        } else {
          long size = file.getLen();
          Key res;
          if( pfs.getName().endsWith(Extensions.JSON) ) {
            throw H2O.unimpl();
          } else if( pfs.getName().endsWith(Extensions.HEX) ) { // Hex file?
            throw H2O.unimpl();
          } else {
            Key k = null;
            keys.add((k = HdfsFileVec.make(file, futures)).toString());
            Log.info("PersistHdfs: DKV.put(" + k + ")");
          }
        }
      }
    } catch( Exception e ) {
      Log.err(e);
      failed.add(p.toString());
    }
  }

  private static void addFolder(FileSystem fs, Path p, JsonArray succeeded, JsonArray failed) {
    try {
      if( fs == null ) return;
      for( FileStatus file : fs.listStatus(p) ) {
        Path pfs = file.getPath();
        if( file.isDir() ) {
          addFolder(fs, pfs, succeeded, failed);
        } else {
          Key k = Key.make(pfs.toString());
          long size = file.getLen();
          Value val = new Value(k, (int) size, Value.HDFS); // Plain Value
          val.setdsk();
          DKV.put(k, val);
          Log.info("PersistHdfs: DKV.put(" + k + ")");
          JsonObject o = new JsonObject();
          o.addProperty(Constants.KEY, k.toString());
          o.addProperty(Constants.FILE, pfs.toString());
          o.addProperty(Constants.VALUE_SIZE, file.getLen());
          succeeded.add(o);
        }
      }
    } catch( Exception e ) {
      Log.err(e);
      JsonObject o = new JsonObject();
      o.addProperty(Constants.FILE, p.toString());
      o.addProperty(Constants.ERROR, e.getMessage());
      failed.add(o);
    }
  }
}
