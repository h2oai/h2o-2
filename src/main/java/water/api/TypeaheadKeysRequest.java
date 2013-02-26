package water.api;

import hex.DGLM.GLMModel;
import hex.rf.RFModel;

import java.util.Arrays;

import water.H2O;
import water.Key;

import com.google.gson.JsonArray;
import com.google.gson.JsonPrimitive;

public class TypeaheadKeysRequest extends TypeaheadRequest {
  public TypeaheadKeysRequest(String msg, String filter) { super(msg, filter); }

  @Override
  protected JsonArray serve(String filter, int limit) {
    JsonArray array = new JsonArray();
    Key[] keys = new Key[limit];
    int len = 0;
    // Gather some keys that pass all filters
    for( Key key : H2O.keySet() ) {
      if( filter != null &&     // Have a filter?
          key.toString().indexOf(filter) == -1 )
        continue;               // Ignore this filtered-out key
      if( !key.user_allowed() ) // Also filter out for user-keys
        continue;
      if( !shouldIncludeKey(key) ) continue;
      keys[len++] = key;        // Capture the key
      if( len == keys.length ) break;
    }
    // sort the keys, for pretty display & reliable ordering
    Arrays.sort(keys,0,len);
    for( int i = 0; i < len; ++i) array.add(new JsonPrimitive(keys[i].toString()));
    return array;
  }

  // By default, all keys passing filters
  protected boolean shouldIncludeKey(Key k) { return true; }
}


class TypeaheadModelKeyRequest extends TypeaheadKeysRequest {
  public TypeaheadModelKeyRequest() {
    super("Provides a simple JSON array of filtered keys known to the "+
          "current node that are Models at the time of calling.",
          "Model_");
  }
}

class TypeaheadGLMModelKeyRequest extends TypeaheadKeysRequest {
  public TypeaheadGLMModelKeyRequest() {
    super("Provides a simple JSON array of filtered keys known to the "+
          "current node that are GLMModels at the time of calling.",
          GLMModel.KEY_PREFIX);
  }
}

class TypeaheadRFModelKeyRequest extends TypeaheadKeysRequest {
  public TypeaheadRFModelKeyRequest() {
    super("Provides a simple JSON array of filtered keys known to the "+
          "current node that are RFModels at the time of calling.",
          RFModel.KEY_PREFIX);
  }
}
