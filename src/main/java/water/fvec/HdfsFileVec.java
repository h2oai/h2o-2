package water.fvec;

import org.apache.hadoop.fs.FileStatus;

import water.*;

// A distributed file-backed Vector
//
public class HdfsFileVec extends FileVec {
  // Make a new NFSFileVec key which holds the filename implicitly.
  // This name is used by the DVecs to load data on-demand.
  public static Key make(FileStatus f) {
    Futures fs = new Futures();
    Key key = make(f, fs);
    fs.blockForPending();
    return key;
  }
  public static Key make(FileStatus f, Futures fs) {
    long size = f.getLen();
    Key k = Key.make(f.getPath().toString());
    Key k2 = Vec.newKey(k);
    // Insert the top-level FileVec key into the store
    Vec v = new HdfsFileVec(k2,size);
    DKV.put(k2, v, fs);
    UKV.put(k, new Frame(new String[]{"0"},new Vec[]{v}));
    return k;
  }
  private HdfsFileVec(Key key, long len) {super(key,len,Value.HDFS);}
}
