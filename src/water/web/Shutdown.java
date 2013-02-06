package water.web;

import java.util.Properties;
import java.util.TimerTask;

import water.H2O;
import water.UDPRebooted;

import com.google.gson.JsonObject;

public class Shutdown extends H2OPage {
  @Override public JsonObject serverJson(Server server, Properties parms, String sessionID) throws PageError {
    java.util.Timer t = new java.util.Timer("Shutdown Timer");
    t.schedule(new TimerTask() {
      @Override
      public void run() {
        UDPRebooted.T.shutdown.send(H2O.SELF);
      }
    }, 100);


    JsonObject json = new JsonObject();
    json.addProperty("Status", "Shutting down");
    return json;
  }

  @Override public String serveImpl(Server server, Properties args, String sessionID) throws PageError {
    serverJson(server, args, sessionID);
    return "Shutting down";
  }
}
