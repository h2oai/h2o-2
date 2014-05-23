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
    long size = tf.getLength();
    Key k = Key.make(PersistTachyon.PREFIX + serverUri + fname);
    Key k2 = Vec.newKey(k);
    new Frame(k).delete_and_lock(null);
    // Insert the top-level FileVec key into the store
    Vec v = new TachyonFileVec(k2,size);
    DKV.put(k2, v, fs);
    Frame fr = new Frame(k,new String[] {fname}, new Vec[] {v});
    fr.update(null);
    fr.unlock(null);
    return k;
  }
  private TachyonFileVec(Key key, long len) {super(key,len,Value.TACHYON);}
}
