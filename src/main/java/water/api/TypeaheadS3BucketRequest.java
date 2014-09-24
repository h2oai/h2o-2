
package water.api;

import water.persist.PersistS3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.Bucket;
import com.google.common.base.Strings;
import dontweave.gson.JsonArray;
import dontweave.gson.JsonPrimitive;

public class TypeaheadS3BucketRequest extends TypeaheadRequest {

  public TypeaheadS3BucketRequest() {
    super("Provides a simple JSON array of S3 paths.","");
  }

  @Override
  protected JsonArray serve(String filter, int limit) {
    JsonArray array = new JsonArray();
    try {
      AmazonS3 s3 = PersistS3.getClient();
      filter = Strings.nullToEmpty(filter);
      for( Bucket b : s3.listBuckets() ) {
        if( b.getName().startsWith(filter) )
          array.add(new JsonPrimitive(b.getName()));
        if( array.size() == limit) break;
      }
    } catch( IllegalArgumentException xe ) { }
    return array;
  }
}
