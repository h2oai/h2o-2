package water.store.s3;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import water.*;
import water.util.ByteBufferInputStream;

import com.amazonaws.services.s3.model.*;

/**
 * Map/reduce upload to S3.
 */
public class MultipartUpload extends MRTask {
  private static final boolean DEBUG = false;
  private static final int     BATCH = 10;

  public static class Progress extends Iced {
    public int     _todo, _done;
    public String  _error;
    public boolean _confirmed;
  }

  private Key      _dest, _key;
  private String   _bucket, _object, _uploadId;

  // Reduced
  private int[]    _parts;
  private String[] _etags;

  public static Key init(Value value) {
    Key dest = Key.make(value._key + ".s3progress");
    Progress progress = new Progress();
    ValueArray va = va(value);
    int todo = (int) va.chunks(); // cast int OK, S3 cannot do more than 10K
    todo *= 2; // Step at beginning and end of each block to show progress
    todo /= BATCH; // Batch chunks: S3 requires >5MB and rows are aligned
    progress._todo = todo;
    UKV.put(dest, progress);
    return dest;
  }

  public static void run(Key dest, Value value, String bucket, String object) {
    if( object == null || object.length() == 0 )
      object = value._key.toString();

    String uploadId = null;

    try {
      InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(bucket, object);
      InitiateMultipartUploadResult initResponse = PersistS3.S3.initiateMultipartUpload(initRequest);
      uploadId = initResponse.getUploadId();

      MultipartUpload task = new MultipartUpload();
      task._dest = dest;
      task._key = value._key;
      task._bucket = bucket;
      task._object = object;
      task._uploadId = uploadId;
      task.invoke(value._key);

      List<PartETag> partETags = new ArrayList<PartETag>();
      for( int i = 0; i < task._parts.length; i++ )
        partETags.add(new PartETag(task._parts[i], task._etags[i]));

      CompleteMultipartUploadRequest compRequest = new CompleteMultipartUploadRequest( //
          bucket, object, uploadId, partETags);
      PersistS3.S3.completeMultipartUpload(compRequest);
      Progress progress = new Progress();
      progress._confirmed = true;
      UKV.put(dest, progress);
    } catch( Exception e ) {
      try {
        PersistS3.S3.abortMultipartUpload(new AbortMultipartUploadRequest(bucket, object, uploadId));
      } catch( Exception _ ) {
      }

      Progress progress = new Progress();
      progress._error = e.toString();
      UKV.put(dest, progress);
    }
  }

  @Override
  public void map(Key key) {
    if( DEBUG )
      System.out.println("s3 map " + key + ": " + this);

    assert key.home();
    long chunk = ValueArray.getChunkIndex(key);

    if( chunk % BATCH == 0 ) {
      step(_dest);

      Value value = DKV.get(_key);
      ValueArray va = va(value);
      ArrayList<ByteBuffer> buffers = new ArrayList<ByteBuffer>();
      long limit = Math.min(chunk + 10, va.chunks());
      int length = 0;

      for( long c = chunk; c < limit; c++ ) {
        AutoBuffer bits = va.getChunk(c);
        buffers.add(bits._bb);
        length += bits.remaining();
      }

      UploadPartRequest uploadRequest = new UploadPartRequest() //
          .withBucketName(_bucket) //
          .withKey(_object) //
          .withUploadId(_uploadId) //
          .withPartNumber(1 + (int) ValueArray.getChunkIndex(key)) //
          .withFileOffset(ValueArray.getChunkOffset(key)) //
          .withPartSize(length) //
          .withInputStream(new ByteBufferInputStream(buffers));

      PartETag result = PersistS3.S3.uploadPart(uploadRequest).getPartETag();
      _parts = new int[1];
      _parts[0] = result.getPartNumber();
      _etags = new String[1];
      _etags[0] = result.getETag();

      step(_dest);
    }
  }

  @Override
  public void reduce(DRemoteTask rt) {
    if( DEBUG )
      System.out.println("s3 reduce: " + this);

    MultipartUpload task = (MultipartUpload) rt;

    if( task._parts != null ) {
      if( _parts == null ) {
        _parts = task._parts;
        _etags = task._etags;
      } else {
        _parts = merge(_parts, task._parts);
        _etags = merge(_etags, task._etags);
      }
    }
  }

  private static ValueArray va(Value value) {
    if( value._isArray != 0 )
      return ValueArray.value(value);

    return new ValueArray(value._key, value.length(), Value.S3);
  }

  private static void step(Key key) {
    new TAtomic<Progress>() {
      @Override
      public Progress atomic(Progress old) {
        Progress update = new Progress();
        update._todo = old._todo;
        update._done = old._done + 1;

        if( DEBUG )
          System.out.println("s3 step " + update._done + " of " + update._todo);

        return update;
      }

      @Override
      public Progress alloc() {
        return new Progress();
      }
    }.invoke(key);
  }

}