package water.api;

import water.UKV;
import water.Value;
import water.util.Log;

import com.google.gson.JsonObject;

public class Remove extends Request {
  protected final H2OExistingKey _key = new H2OExistingKey(KEY);

  @Override
  protected Response serve() {
    Value v = _key.value();

    try {
      UKV.remove(v._key);
    } catch( Exception e ) {
      return Response.error(e.getMessage());
    }

    JsonObject response = new JsonObject();
    response.addProperty(KEY, _key.toString());
    return Response.redirect(response, StoreView.class, null);
  }
}
