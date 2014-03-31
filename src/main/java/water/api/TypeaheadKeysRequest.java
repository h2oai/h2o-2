package water.api;

import hex.DGLM.GLMModel;
import hex.nb.NBModel;
import hex.pca.PCAModel;
import hex.*;
import hex.rf.RFModel;

import java.util.Arrays;

import water.*;

import com.google.gson.JsonArray;
import com.google.gson.JsonPrimitive;

public class TypeaheadKeysRequest extends TypeaheadRequest {
  final String _cname;
  int _typeid;                  // Also filter for Keys of this type
  public TypeaheadKeysRequest(String msg, String filter, Class C) {
    super(msg, filter);
    _cname = C == null ? null : C.getName();
  }

  @Override
  protected JsonArray serve(String filter, int limit) {
    JsonArray array = new JsonArray();
    Key[] keys = new Key[limit];
    int len = 0;
    // Gather some keys that pass all filters
    for( Key key : H2O.globalKeySet(null) ) {
      if( filter != null &&     // Have a filter?
          key.toString().indexOf(filter) == -1 )
        continue;               // Ignore this filtered-out key
      if( !key.user_allowed() ) // Also filter out for user-keys
        continue;
      Value val = DKV.get(key);
      if( val == null ) continue; // Deleted key?
      if( !matchesType(val) ) continue; // Wrong type?
      if( !shouldIncludeKey(key) ) continue; // Generic override
      keys[len++] = key;        // Capture the key
      if( len == keys.length ) break;
    }
    // sort the keys, for pretty display & reliable ordering
    Arrays.sort(keys,0,len);
    for( int i = 0; i < len; ++i) array.add(new JsonPrimitive(keys[i].toString()));
    return array;
  }

  protected boolean matchesType(Value val) {
    // One-shot monotonic racey update from 0 to the known fixed typeid.
    // Since all writers write the same typeid, there is no race.
    if( _typeid == 0 && _cname != null ) _typeid = TypeMap.onIce(_cname);
    return _typeid == 0 || val.type() == _typeid;
  }

  // By default, all keys passing filters
  protected boolean shouldIncludeKey(Key k) { return true; }
}


class TypeaheadModelKeyRequest extends TypeaheadKeysRequest {
  public TypeaheadModelKeyRequest() {
    super("Provides a simple JSON array of filtered keys known to the "+
          "current node that are Models at the time of calling.",
          "Model_",null);
  }
}

class TypeaheadGLMModelKeyRequest extends TypeaheadKeysRequest {
  public TypeaheadGLMModelKeyRequest() {
    super("Provides a simple JSON array of filtered keys known to the "+
          "current node that are GLMModels at the time of calling.",
          null,GLMModel.class);
  }
}

class TypeaheadRFModelKeyRequest extends TypeaheadKeysRequest {
  public TypeaheadRFModelKeyRequest() {
    super("Provides a simple JSON array of filtered keys known to the "+
          "current node that are RFModels at the time of calling.",
          null,RFModel.class);
  }
}

class TypeaheadKMeansModelKeyRequest extends TypeaheadKeysRequest {
  public TypeaheadKMeansModelKeyRequest() {
    super("Provides a simple JSON array of filtered keys known to the "+
          "current node that are KMeansModels at the time of calling.",
          null,KMeansModel.class);
  }
}

class TypeaheadPCAModelKeyRequest extends TypeaheadKeysRequest {
  public TypeaheadPCAModelKeyRequest() {
    super("Provides a simple JSON array of filtered keys known to the "+
          "current node that are PCAModels at the time of calling.",
          null,PCAModel.class);
  }
}

class TypeaheadNBModelKeyRequest extends TypeaheadKeysRequest {
  public TypeaheadNBModelKeyRequest() {
    super("Provides a simple JSON array of filtered keys known to the "+
          "current node that are NBModels at the time of calling.",
          null,NBModel.class);
  }
}

class TypeaheadHexKeyRequest extends TypeaheadKeysRequest {
  public TypeaheadHexKeyRequest() {
    super("Provides a simple JSON array of filtered keys known to the "+
          "current node that are ValueArrays at the time of calling.",
          null,ValueArray.class);
  }

  @Override protected boolean matchesType(Value val) {
    if( val.type() == TypeMap.VALUE_ARRAY )
      return val.isHex();
    return val.type() == TypeMap.FRAME;
  }
}
