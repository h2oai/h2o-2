package water.api;

import java.io.IOException;

import water.DKV;
import water.Key;
import water.store.s3.PersistS3;

import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.gson.*;

public class ImportS3 extends Request {
  public class BucketArg extends TypeaheadInputText<String> {
    public BucketArg(String name) {
      super(TypeaheadS3BucketRequest.class, name, true);
    }

    @Override
    protected String parse(String input) throws IllegalArgumentException {
      PersistS3.checkCredentials();
      if( !PersistS3.S3.doesBucketExist(input) )
        throw new IllegalArgumentException("S3 Bucket " + input + " not found!");
      return input;
    }

    @Override
    protected String queryDescription() {
      return "existing S3 Bucket";
    }

    @Override
    protected String defaultValue() {
      return null;
    }
  }

  protected final BucketArg _bucket = new BucketArg(BUCKET);

  public ImportS3() {
    _requestHelp = "Imports the given Amazon S3 Bucket.  All nodes in the "
        + "cloud must have permission to access the Amazon bucket.";
    _bucket._requestHelp = "Amazon S3 Bucket to import.";
  }

  @Override
  protected Response serve() {
    JsonObject json = new JsonObject();

    JsonArray succ = new JsonArray();
    JsonArray fail = new JsonArray();
    String bucket = _bucket.value();
    for( S3ObjectSummary obj : PersistS3.S3.listObjects(bucket).getObjectSummaries() ) {
      try {
        Key k = PersistS3.loadKey(obj);
        JsonObject o = new JsonObject();
        o.addProperty(KEY, k.toString());
        o.addProperty(FILE, obj.getKey());
        succ.add(o);
      } catch( IOException e ) {
        fail.add(new JsonPrimitive(obj.getKey()));
      }
    }
    json.add(SUCCEEDED, succ);
    json.add(FAILED, fail);
    DKV.write_barrier();

    Response r = Response.done(json);
    r.setBuilder(SUCCEEDED + "." + KEY, new KeyCellBuilder());
    return r;
  }
}
