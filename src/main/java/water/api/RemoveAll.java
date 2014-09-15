package water.api;

import dontweave.gson.JsonObject;
import water.Job;
import water.util.Log;
import water.util.RemoveAllKeysTask;

public class RemoveAll extends JSONOnlyRequest {
  @Override public RequestServer.API_VERSION[] supportedVersions() { return SUPPORTS_ONLY_V2; }
  @Override protected void registered(RequestServer.API_VERSION version) { super.registered(version); }

  @Override
  protected Response serve() {
    try {
      Log.info("Removing all keys for the cluster");

      // First cancel all jobs and wait for them to be done.
      Log.info("Cancelling all jobs...");
      for (Job job : Job.all()) {
        job.cancel();
        Job.waitUntilJobEnded(job.self());
      }
      Log.info("Finished cancelling all jobs");

      RemoveAllKeysTask collector = new RemoveAllKeysTask();
      collector.invokeOnAllNodes();

      Log.info("Finished removing keys");
    } catch( Throwable e ) {
      return Response.error(e);
    }

    JsonObject response = new JsonObject();
    return Response.done(response);
  }
}
