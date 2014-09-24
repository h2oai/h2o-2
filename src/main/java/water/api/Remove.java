package water.api;

import water.Futures;
import water.Lockable;
import water.UKV;
import water.Value;

import dontweave.gson.JsonObject;

public class Remove extends Request {
  @Override public RequestServer.API_VERSION[] supportedVersions() { return SUPPORTS_V1_V2; }

  protected final H2OExistingKey _key = new H2OExistingKey(KEY);

  @Override
  protected Response serve() {
    try { Lockable.delete(_key.value()._key); } 
    catch( Throwable e ) { return Response.error(e); }

    JsonObject response = new JsonObject();
    response.addProperty(KEY, _key.toString());
    return Response.redirect(response, StoreView.class, null);
  }
}
