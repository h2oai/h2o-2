package water.api;

import hex.DGLM.GLMModel;
import hex.nb.NBModel;
import hex.pca.PCAModel;
import hex.*;
import hex.rf.RFModel;

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
    return serve(filter, limit, 2000);
  }
  protected JsonArray serve(String filter, int limit, long timetolerance) {
    JsonArray array = new JsonArray();
    int len = 0;
    // Gather some keys that pass all filters
    for( H2O.KeyInfo kinfo : H2O.KeySnapshot.globalSnapshot(2000)._keyInfos) {
      if( filter != null &&     // Have a filter?
          kinfo._key.toString().indexOf(filter) == -1 )
        continue;               // Ignore this filtered-out key
      if( !matchesType(kinfo) ) continue; // Wrong type?
      if( !shouldIncludeKey(kinfo) ) continue; // Generic override
      array.add(new JsonPrimitive(kinfo._key.toString()));
      if(array.size() == limit)break;
    }
    return array;
  }

  protected boolean matchesType(H2O.KeyInfo ki) {
    // One-shot monotonic racey update from 0 to the known fixed typeid.
    // Since all writers write the same typeid, there is no race.
    if( _typeid == 0 && _cname != null ) _typeid = TypeMap.onIce(_cname);
    return _typeid == 0 || ki._type == _typeid;
  }

  // By default, all keys passing filters
  protected boolean shouldIncludeKey(H2O.KeyInfo k) { return true; }
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

  @Override protected boolean matchesType(H2O.KeyInfo kinfo) {
    return !kinfo._rawData && (kinfo._type == TypeMap.FRAME || kinfo._type == TypeMap.VALUE_ARRAY);
  }
}
