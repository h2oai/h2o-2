package water.persist;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

import com.google.common.io.ByteStreams;

import tachyon.client.ReadType;
import tachyon.client.TachyonFS;
import water.*;
import water.Job.ProgressMonitor;
import water.fvec.FileVec;
import water.fvec.Vec;
import water.util.*;

public class PersistTachyon extends Persist<TachyonFS> {

  public static final String PREFIX = "tachyon://";
  public static final String DEFAULT_CLIENT_URI = "tachyon://localhost:19998";

  private final String _defaultUri;

  PersistTachyon() { this(DEFAULT_CLIENT_URI); }
  PersistTachyon(String uri) { _defaultUri = uri; }

  public static InputStream openStream(Key k, ProgressMonitor pmon) {
    return new H2OTachyonInputStream(k, pmon);
  }

  @Override public byte[] load(Value v) {
    Key k = v._key; // key for value
    if (k._kb[0] != Key.DVEC) throw H2O.unimpl(); // Load only from values stored in vector
    long skip = FileVec.chunkOffset(k); // Compute skip for this value
    long start_io_ms = System.currentTimeMillis();
    final byte[] b = MemoryManager.malloc1(v._max);
    String[] keyComp = decodeKey(k);
    String clientUri = keyComp[0];
    String fpath = keyComp[1];
    TachyonFS tfs = null;
    InputStream is = null;
    try {
      tfs = (TachyonFS) (Persist.I[Value.TACHYON].createClient(clientUri));
      long start_ns = System.nanoTime(); // Blocking i/o call timing - without counting repeats
      is = tfs.getFile(fpath).getInStream(ReadType.NO_CACHE);
      ByteStreams.skipFully(is, skip);
      ByteStreams.readFully(is, b);
      TimeLine.record_IOclose(start_ns, start_io_ms, 1/* read */, v._max, Value.TACHYON);
      return b;
    } catch (IOException e) {
      throw new RuntimeException(Log.err("File load failed: ", e));
    } finally {
      if (is!=null) Utils.close(is);
    }
  }

  public static final class H2OTachyonInputStream extends RIStream {
    final private Key key;
    final private String clientURI;
    final private String fpath;
    protected H2OTachyonInputStream(Key k, long from, ProgressMonitor pmon) {
      super(from, pmon);
      key = k;
      String[] c = decodeKey(k);
      clientURI = c[0];
      fpath = c[1];
    }
    public H2OTachyonInputStream(Key k, ProgressMonitor pmon) {
      this(k, 0, pmon);
    }
    @Override protected InputStream open(long offset) throws IOException {
      TachyonFS tfs = (TachyonFS) (Persist.I[Value.TACHYON].createClient(clientURI));
      InputStream is = tfs.getFile(fpath).getInStream(ReadType.NO_CACHE);
      is.skip(offset);
      return is;
    }
  }

  /** Split key name composed of tachyon://<client-uri>/filename into two parts:
   *  - client-uri without tachyon:// prefix
   *  - filename
   *  And returns both components.
   */
  private static String[] decodeKey(Key k) {
    String s = new String((k.isChunkKey()) ? Arrays.copyOfRange(k._kb, Vec.KEY_PREFIX_LEN, k._kb.length):k._kb);
    return decode(s);
  }

  public static String[] decode(String s) {
    assert s.startsWith(PREFIX) : "Unsupported key name for tachyon: " + s;
    s = s.substring(PREFIX.length());
    int nextSlash = s.indexOf('/');
    String clientUri, filename;
    if (nextSlash!=-1) {
      clientUri = s.substring(0, nextSlash);
      filename  = s.substring(nextSlash);
    } else {
      clientUri = s;
      filename = "/";
    }
    return new String[] { clientUri, filename };
  }

  //
  // Un-implemented methods
  //
  @Override public String getPath() {
    throw new UnsupportedOperationException();
  }

  @Override public void clear() {
    throw new UnsupportedOperationException();
  }

  @Override public void loadExisting() {
    throw new UnsupportedOperationException();
  }

  @Override public void store(Value v) {
    throw new UnsupportedOperationException();
  }

  @Override public void delete(Value v) {
    throw new UnsupportedOperationException();
  }

  //
  // Methods providing low-level client implementation
  //
  @Override public String getDefaultURI() {
    return _defaultUri;
  }
  @Override public TachyonFS createClient(String uri) throws IOException {
    if (!uri.startsWith(PREFIX)) uri = PREFIX + uri;
    return TachyonFS.get(uri);
  }
}
