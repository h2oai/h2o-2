package water.fvec;

import water.*;

import com.amazonaws.services.s3.model.S3ObjectSummary;

// A distributed file-backed Vector
//
public class S3FileVec extends FileVec {
  // Make a new NFSFileVec key which holds the filename implicitly.
  // This name is used by the DVecs to load data on-demand.
  public static Key make(S3ObjectSummary obj) {
    Futures fs = new Futures();
    Key key = make(obj, fs);
    fs.blockForPending();
    return key;
  }
  public static Key make(S3ObjectSummary obj, Futures fs) {
    Key k = Key.make("s3://" + obj.getBucketName() + "/" + obj.getKey());
    long size = obj.getSize();
    Key k2 = Vec.newKey(k);
    // Insert the top-level FileVec key into the store
    Vec v = new S3FileVec(k2,size);
    DKV.put(k2, v, fs);
    UKV.put(k, new Frame(new String[]{"0"},new Vec[]{v}));
    return k;
  }
  private S3FileVec(Key key, long len) {super(key,len,Value.S3);}
}
