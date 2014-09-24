package water.api;

import dontweave.gson.JsonObject;
import water.util.Log;

public class LogAndEcho extends Request {
  @Override public RequestServer.API_VERSION[] supportedVersions() { return SUPPORTS_V1_V2; }

  protected final Str _message = new Str("message", "");

  @Override
  protected Response serve() {
    String s = _message.value();
    Log.info(s);

    JsonObject response = new JsonObject();
    response.addProperty("message", s);
    return Response.done(response);
  }
}
