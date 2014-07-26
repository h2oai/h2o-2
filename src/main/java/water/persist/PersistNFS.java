package water.persist;

import java.io.*;
import java.nio.channels.FileChannel;

import water.*;

// Persistence backend for network file system.
// Just for loading or storing files.
//
// @author cliffc
public final class PersistNFS extends Persist {

  static final String KEY_PREFIX = "nfs:";
  public static final int KEY_PREFIX_LENGTH = KEY_PREFIX.length();

  // file implementation -------------------------------------------------------
  public static Key decodeFile(File f) {
    String kname = KEY_PREFIX + File.separator + f.toString();
    assert (kname.length() <= 512);
    // all NFS keys are NFS-kind keys
    return Key.make(kname.getBytes());
  }

  // Returns the file for given key.
  private static File getFileForKey(Key k) {
    final int len = KEY_PREFIX_LENGTH+1; // Strip key prefix & leading slash
    final int off = k._kb[0]==Key.DVEC ? water.fvec.Vec.KEY_PREFIX_LEN : 0;
    String s = new String(k._kb,len+off,k._kb.length-(len+off));
    return new File(s);
  }

  public static InputStream openStream(Key k) throws IOException {
    return new FileInputStream(getFileForKey(k));
  }

  // Read up to 'len' bytes of Value. Value should already be persisted to
  // disk.  A racing delete can trigger a failure where we get a null return,
  // but no crash (although one could argue that a racing load&delete is a bug
  // no matter what).
  @Override public byte[] load(Value v) {
    long skip = 0;
    Key k = v._key;
    // Convert a chunk into a long-offset from the base file.
    if( k._kb[0] == Key.DVEC )
      skip = water.fvec.NFSFileVec.chunkOffset(k); // The offset
    try {
      FileInputStream s = null;
      try {
        s = new FileInputStream(getFileForKey(k));
        FileChannel fc = s.getChannel();
        fc.position(skip);
        AutoBuffer ab = new AutoBuffer(fc, true, Value.NFS);
        byte[] b = ab.getA1(v._max);
        ab.close();
        assert v.isPersisted();
        return b;
      } finally {
        if( s != null ) s.close();
      }
    } catch( IOException e ) { // Broken disk / short-file???
      H2O.ignore(e);
      return null;
    }
  }

  // Store Value v to disk.
  @Override public void store(Value v) {
    // Only the home node does persistence on NFS
    if( !v._key.home() ) return;
    // A perhaps useless cutout: the upper layers should test this first.
    if( v.isPersisted() ) return;
    try {
      File f = getFileForKey(v._key);
      f.mkdirs();
      FileOutputStream s = new FileOutputStream(f);
      try {
        byte[] m = v.memOrLoad();
        assert (m == null || m.length == v._max); // Assert not saving partial files
        if( m != null ) new AutoBuffer(s.getChannel(), false, Value.NFS).putA1(m, m.length).close();
        v.setdsk(); // Set as write-complete to disk
      } finally {
        s.close();
      }
    } catch( IOException e ) {
      H2O.ignore(e);
    }
  }

  // TODO needed if storing ice to S3

  @Override public String getPath() {
    throw new UnsupportedOperationException();
  }

  @Override public void clear() {
    throw new UnsupportedOperationException();
  }

  @Override public void loadExisting() {
    throw new UnsupportedOperationException();
  }

  @Override public void delete(Value v) {
    throw new UnsupportedOperationException();
  }
}
