package water.api;

import hex.nb.NBModel;
import hex.pca.PCAModel;
import hex.*;

import water.*;
import water.fvec.Frame;

import dontweave.gson.JsonArray;
import dontweave.gson.JsonPrimitive;

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
    // No type filtering
    if( _typeid == 0 && _cname == null ) return true;
    // One-shot monotonic racey update from 0 to the known fixed typeid.
    // Since all writers write the same typeid, there is no race.
    if( _typeid == 0 ) _typeid = TypeMap.onIce(_cname);
    if( ki._type == _typeid ) return true;
    // Class Model is abstract, and TypeMap clazz() does not handle that well.
    // Also, want to allow both OldModel & Model.
    // Hack: check for water.Model and name the class directly.
    Class kclz = TypeMap.clazz(ki._type);
    if( TypeMap.className(_typeid).equals("water.Model") )
      return Model.class.isAssignableFrom(kclz);
    return TypeMap.clazz(_typeid).isAssignableFrom(kclz);
  }

  // By default, all keys passing filters
  protected boolean shouldIncludeKey(H2O.KeyInfo k) { return true; }
}


class TypeaheadModelKeyRequest extends TypeaheadKeysRequest {
  public TypeaheadModelKeyRequest() {
    super("Provides a simple JSON array of filtered keys known to the "+
          "current node that are Models at the time of calling.",
          null,Model.class);
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
          "current node that are Frames at the time of calling.",
          null,Frame.class);
  }

  @Override protected boolean matchesType(H2O.KeyInfo kinfo) {
    return !kinfo._rawData && (kinfo._type == TypeMap.FRAME);
  }
}
