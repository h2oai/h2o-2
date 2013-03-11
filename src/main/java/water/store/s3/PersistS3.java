package water.store.s3;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import water.*;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;
import com.google.common.base.Objects;
import com.google.common.io.ByteStreams;
import com.google.common.io.Closeables;

/** Persistence backend for S3 */
public abstract class PersistS3 {
  private static void log(String printf, Object... args) {
    System.err.printf("[s3] " + printf + "\n", args);
  }
  private static final String   HELP = "You can specify a credentials properties file with the -aws_credentials command line switch.";

  // Default location of the S3 credentials file
  private static final String  DEFAULT_CREDENTIALS_LOCATION = "AwsCredentials.properties";
  private static final String  KEY_PREFIX                   = "s3:";
  private static final int     KEY_PREFIX_LEN               = KEY_PREFIX.length();

  private static final AmazonS3 S3;
  static {
    File credentials = new File(Objects.firstNonNull(H2O.OPT_ARGS.aws_credentials, DEFAULT_CREDENTIALS_LOCATION));
    AmazonS3 s3 = null;
    try {
      s3 = new AmazonS3Client(new PropertiesCredentials(credentials));
    } catch( Throwable e ) {
      e.printStackTrace();
      log("Unable to create S3 backend.");
      if( H2O.OPT_ARGS.aws_credentials == null )
        log(HELP);
    }
    S3 = s3;
  }

  public static AmazonS3 getClient() {
    if( S3 == null ) {
      StringBuilder msg = new StringBuilder();
      msg.append("Unable to load S3 credentials.");
      if( H2O.OPT_ARGS.aws_credentials == null )
        msg.append(HELP);
      throw new IllegalArgumentException(msg.toString());
    }
    return S3;
  }

  public static Key loadKey(S3ObjectSummary obj) throws IOException {
    Key k = encodeKey(obj.getBucketName(), obj.getKey());
    long size = obj.getSize();
    Value val = null;
    if( obj.getKey().endsWith(".hex") ) { // Hex file?
      S3Object s3obj = getObjectForKey(k, 0, ValueArray.CHUNK_SZ);
      S3ObjectInputStream is = s3obj.getObjectContent();

      int sz = (int) Math.min(ValueArray.CHUNK_SZ, size);
      byte[] mem = MemoryManager.malloc1(sz);
      ByteStreams.readFully(is, mem);
      ValueArray ary = new ValueArray(k, sz, Value.S3).read(new AutoBuffer(mem));
      ary._persist = Value.S3 | Value.ON_dsk;
      val = ary.value();
    } else if( size >= 2 * ValueArray.CHUNK_SZ ) {
      // ValueArray byte wrapper over a large file
      val = new ValueArray(k, size, Value.S3).value();
    } else {
      val = new Value(k, (int) size, Value.S3); // Plain Value
    }
    val.setdsk();
    DKV.put(k, val);
    return k;
  }

  // file implementation -------------------------------------------------------

  // Read up to 'len' bytes of Value. Value should already be persisted to
  // disk. A racing delete can trigger a failure where we get a null return,
  // but no crash (although one could argue that a racing load&delete is a bug
  // no matter what).
  public static byte[] fileLoad(Value v) {
    byte[] b = MemoryManager.malloc1(v._max);
    S3ObjectInputStream s = null;
    try {
      long skip = 0;
      Key k = v._key;
      // Convert an arraylet chunk into a long-offset from the base file.
      if( k._kb[0] == Key.ARRAYLET_CHUNK ) {
        skip = ValueArray.getChunkOffset(k); // The offset
        k = ValueArray.getArrayKey(k); // From the base file key
        if( k.toString().endsWith(".hex") ) { // Hex file?
          int value_len = DKV.get(k).memOrLoad().length; // How long is the ValueArray
                                                   // header?
          skip += value_len;
        }
      }
      S3Object s3obj = getObjectForKey(k, skip, v._max);
      s = s3obj.getObjectContent();
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
    if( !v._key.home() )
      return;
    // Never store arraylets on S3, instead we'll store the entire array.
    assert !v.isArray();

    Key dest = MultipartUpload.init(v);
    MultipartUpload.run(dest, v, null, null);
  }

  static public Value lazyArrayChunk(Key key) {
    Key arykey = ValueArray.getArrayKey(key); // From the base file key
    long off = ValueArray.getChunkOffset(key); // The offset
    long size = getObjectMetadataForKey(arykey).getContentLength();

    long rem = size - off; // Remainder to be read
    if( arykey.toString().endsWith(".hex") ) { // Hex file?
      int value_len = DKV.get(arykey).memOrLoad().length; // How long is the
                                                    // ValueArray header?
      rem -= value_len;
    }
    // the last chunk can be fat, so it got packed into the earlier chunk
    if( rem < ValueArray.CHUNK_SZ && off > 0 )
      return null;
    int sz = (rem >= ValueArray.CHUNK_SZ * 2) ? (int) ValueArray.CHUNK_SZ : (int) rem;
    Value val = new Value(key, sz, Value.S3);
    val.setdsk(); // But its already on disk.
    return val;
  }

  /**
   * Creates the key for given S3 bucket and key.
   *
   * Returns the H2O key, or null if the key cannot be created.
   *
   * @param bucket
   *          Bucket name
   * @param key
   *          Key name (S3)
   * @return H2O key pointing to the given bucket and key.
   */
  public static Key encodeKey(String bucket, String key) {
    Key res = encodeKeyImpl(bucket, key);
    assert checkBijection(res, bucket, key);
    return res;
  }

  /**
   * Decodes the given H2O key to the S3 bucket and key name.
   *
   * Returns the array of two strings, first one is the bucket name and second
   * one is the key name.
   *
   * @param k
   *          Key to be decoded.
   * @return Pair (array) of bucket name and key name.
   */
  public static String[] decodeKey(Key k) {
    String[] res = decodeKeyImpl(k);
    assert checkBijection(k, res[0], res[1]);
    return res;
  }

  private static boolean checkBijection(Key k, String bucket, String key) {
    Key en = encodeKeyImpl(bucket, key);
    String[] de = decodeKeyImpl(k);
    boolean res = Arrays.equals(k._kb, en._kb) && bucket.equals(de[0]) && key.equals(de[1]);
    assert res : "Bijection failure:" + "\n\tKey 1:" + k + "\n\tKey 2:" + en + "\n\tBkt 1:" + bucket + "\n\tBkt 2:"
        + de[0] + "\n\tStr 1:" + key + "\n\tStr 2:" + de[1] + "";
    return res;
  }

  private static Key encodeKeyImpl(String bucket, String key) {
    return Key.make(KEY_PREFIX + bucket + '/' + key);
  }

  private static String[] decodeKeyImpl(Key k) {
    String s = new String(k._kb);
    assert s.startsWith(KEY_PREFIX) && s.indexOf('/') >= 0 : "Attempting to decode non s3 key: " + k;
    s = s.substring(KEY_PREFIX_LEN);
    int dlm = s.indexOf('/');
    String bucket = s.substring(0, dlm);
    String key = s.substring(dlm + 1);
    return new String[] { bucket, key };
  }

  // Gets the S3 object associated with the key that can read length bytes from
  // offset
  private static S3Object getObjectForKey(Key k, long offset, long length) {
    try {
      String[] bk = decodeKey(k);
      GetObjectRequest r = new GetObjectRequest(bk[0], bk[1]);
      r.setRange(offset, offset + length);
      return S3.getObject(r);
    } catch( AmazonClientException e ) {
      return null;
    }
  }

  // Gets the object metadata associated with given key.
  private static ObjectMetadata getObjectMetadataForKey(Key k) {
    String[] bk = decodeKey(k);
    assert (bk.length == 2);
    return S3.getObjectMetadata(bk[0], bk[1]);
  }
}
