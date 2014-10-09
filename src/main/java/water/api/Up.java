package water.api;

import dontweave.gson.JsonObject;
import water.util.Log;

public class Up extends Request {
  @Override public RequestServer.API_VERSION[] supportedVersions() { return SUPPORTS_V1_V2; }

  @Override
  protected Response serve() {
    JsonObject response = new JsonObject();
    return Response.done(response);
  }
}
