package water.fvec;

import tachyon.thrift.ClientFileInfo;
import water.*;
import water.persist.PersistTachyon;

public class TachyonFileVec extends FileVec {
  public static Key make(String serverUri, ClientFileInfo tf) {
    Futures fs = new Futures();
    Key key = make(serverUri, tf, fs);
    fs.blockForPending();
    return key;
  }
  public static Key make(String serverUri, ClientFileInfo tf, Futures fs) {
    String fname = tf.getPath(); // Always return absolute path /dir/filename
    Key k = Key.make(PersistTachyon.PREFIX + serverUri + fname);
    long size = tf.getLength();
    Key k2 = Vec.newKey(k);
    // Insert the top-level FileVec key into the store
    Vec v = new TachyonFileVec(k2,size);
    DKV.put(k2, v, fs);
    new Frame(k,new String[]{fname},new Vec[]{v}).delete_and_lock(null).unlock(null);
    return k;
  }
  private TachyonFileVec(Key key, long len) {super(key,len,Value.TACHYON);}
}
