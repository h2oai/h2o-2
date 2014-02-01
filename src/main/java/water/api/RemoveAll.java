package water.api;

import com.google.gson.JsonObject;
import water.util.Log;
import water.util.RemoveAllKeysTask;

public class RemoveAll extends JSONOnlyRequest {
  @Override public RequestServer.API_VERSION[] supportedVersions() { return SUPPORTS_ONLY_V2; }
  @Override protected void registered(RequestServer.API_VERSION version) { super.registered(version); }

  @Override
  protected Response serve() {
    try {
      Log.info("Removing all keys for the cluster");
      RemoveAllKeysTask collector = new RemoveAllKeysTask();
      collector.invokeOnAllNodes();
    } catch( Throwable e ) {
      return Response.error(e);
    }

    JsonObject response = new JsonObject();
    return Response.done(response);
  }
}
