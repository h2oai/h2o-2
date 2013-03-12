package water.api;

import water.*;

public class TypeaheadHexKeyRequest extends TypeaheadKeysRequest {

  public TypeaheadHexKeyRequest() {
    super("Provides a simple JSON array of filtered keys known to the "+
          "current node that are ValueArrays at the time of calling.", "");
  }

  @Override
  protected boolean shouldIncludeKey(Key k) {
    Value v = H2O.get(k);
    if( v == null ) return false;
    if( !v.isArray() ) return false;
    ValueArray va = ValueArray.value(v);
    return va._cols != null && va._cols.length > 0;
  }
}
