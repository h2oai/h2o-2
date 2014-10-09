
package water.api;

import java.util.TimerTask;

import water.H2O;
import water.UDPRebooted;

import dontweave.gson.JsonObject;

public class Shutdown extends Request {

  public Shutdown() {
    _requestHelp = "Shutdown the cloud.";
  }

  @Override public Response serve() {
    java.util.Timer t = new java.util.Timer("Shutdown Timer");
    t.schedule(new TimerTask() {
      @Override
      public void run() {
        UDPRebooted.T.shutdown.send(H2O.SELF);
        H2O.exit(0);
      }
    }, 100);

    JsonObject json = new JsonObject();
    json.addProperty(STATUS, "shutting down");
    return Response.done(json);
  }
}
