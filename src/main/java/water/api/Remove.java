package water.api;

import water.UKV;
import water.Value;
import water.Futures;

import com.google.gson.JsonObject;

public class Remove extends Request {
  @Override public RequestServer.API_VERSION[] supportedVersions() { return SUPPORTS_V1_V2; }

  protected final H2OExistingKey _key = new H2OExistingKey(KEY);

  @Override
  protected Response serve() {
    Value v = _key.value();

    try {
      Futures fs = new Futures();
      UKV.remove(v._key, fs);
      fs.blockForPending();
    } catch( Exception e ) {
      return Response.error(e.getMessage());
    }

    JsonObject response = new JsonObject();
    response.addProperty(KEY, _key.toString());
    return Response.redirect(response, StoreView.class, null);
  }
}
