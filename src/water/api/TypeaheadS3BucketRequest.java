
package water.api;

import water.store.s3.PersistS3;

import com.amazonaws.services.s3.model.Bucket;
import com.google.common.base.Strings;
import com.google.gson.JsonArray;
import com.google.gson.JsonPrimitive;

public class TypeaheadS3BucketRequest extends TypeaheadRequest {

  public TypeaheadS3BucketRequest() {
    super("Provides a simple JSON array of S3 Buckets.","");
  }

  @Override
  protected JsonArray serve(String filter, int limit) {
    JsonArray array = new JsonArray();
    if( PersistS3.S3 == null ) return array;

    filter = Strings.nullToEmpty(filter);
    for( Bucket b : PersistS3.S3.listBuckets() ) {
      if( b.getName().startsWith(filter) )
        array.add(new JsonPrimitive(b.getName()));
      if( array.size() == limit) break;
    }
    return array;
  }
}
