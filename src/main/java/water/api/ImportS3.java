package water.api;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import water.DKV;
import water.Key;
import water.persist.PersistS3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import dontweave.gson.*;
import water.util.Log;

public class ImportS3 extends Request {
  public class BucketArg extends TypeaheadInputText<String> {
    public BucketArg(String name) {
      super(TypeaheadS3BucketRequest.class, name, true);
    }

    @Override
    protected String parse(String input) throws IllegalArgumentException {
      AmazonS3 s3 = PersistS3.getClient();
      if( !s3.doesBucketExist(input) )
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

  public void processListing(ObjectListing listing, JsonArray succ, JsonArray fail){
    for( S3ObjectSummary obj : listing.getObjectSummaries() ) {
      try {
        Key k = PersistS3.loadKey(obj);
        JsonObject o = new JsonObject();
        o.addProperty(KEY, k.toString());
        o.addProperty(FILE, obj.getKey());
        o.addProperty(VALUE_SIZE, obj.getSize());
        succ.add(o);
      } catch( IOException e ) {
        JsonObject o = new JsonObject();
        o.addProperty(FILE, obj.getKey());
        o.addProperty(ERROR, e.getMessage());
        fail.add(o);
      }
    }
  }

  @Override
  protected Response serve() {
    String bucket = _bucket.value();
    Log.info("ImportS3 processing (" + bucket + ")");
    JsonObject json = new JsonObject();
    JsonArray succ = new JsonArray();
    JsonArray fail = new JsonArray();
    AmazonS3 s3 = PersistS3.getClient();
    ObjectListing currentList = s3.listObjects(bucket);
    processListing(currentList, succ, fail);
    while(currentList.isTruncated()){
      currentList = s3.listNextBatchOfObjects(currentList);
      processListing(currentList, succ, fail);
    }
    json.add(NUM_SUCCEEDED, new JsonPrimitive(succ.size()));
    json.add(SUCCEEDED, succ);
    json.add(NUM_FAILED, new JsonPrimitive(fail.size()));
    json.add(FAILED, fail);
    DKV.write_barrier();
    Response r = Response.done(json);
    r.setBuilder(SUCCEEDED + "." + KEY, new KeyCellBuilder());
    return r;
  }
}
