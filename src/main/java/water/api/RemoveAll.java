package water.api;

import com.google.gson.JsonObject;
import water.util.Log;
import water.util.RemoveAllKeysTask;

public class RemoveAll extends JSONOnlyRequest {
  @Override
  protected Response serve() {
    try {
      Log.info("Removing all keys for the cluster");
      RemoveAllKeysTask collector = new RemoveAllKeysTask();
      collector.invokeOnAllNodes();
    } catch( Exception e ) {
      return Response.error(e.getMessage());
    }

    JsonObject response = new JsonObject();
    return Response.done(response);
  }
}
