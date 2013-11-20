package water.api;

import com.google.gson.JsonObject;
import water.H2O;
import water.Key;
import water.UKV;
import water.util.Log;

public class RemoveAll extends JSONOnlyRequest {
  @Override
  protected Response serve() {
    try {
      Log.info("Removing all keys!!!");
      Log.info("Removing "+H2O.keySet().size()+" keys");
      final int numKeys = H2O.keySet().size();
      Key[] keys = new Key[numKeys];
      int len = 0;
      //Loop over keys
      for( Key key : H2O.keySet() ) {
        if( H2O.get(key) == null ) continue;
        keys[len++] = key;
        if( len == keys.length ) break;
      }
      //remove keys
      UKV.removeAll(keys);
      assert H2O.keySet().size() <= 1; //1 null key left over
    } catch( Exception e ) {
      return Response.error(e.getMessage());
    }

    JsonObject response = new JsonObject();
    return Response.done(response);
  }
}
