package water.api;

import dontweave.gson.*;
import water.util.LinuxProcFileReader;

/**
 * Redirect to water meter page.
 */
public class WaterMeter extends HTMLOnlyRequest {
  protected String build(Response response) {
    return "<meta http-equiv=\"refresh\" content=\"0; url=watermeter/index.html\">";
  }

  public static class WaterMeterCpuTicks extends JSONOnlyRequest {
    @Override
    public RequestServer.API_VERSION[] supportedVersions() { return SUPPORTS_ONLY_V2; }

    /**
     * Iterates over fields and their annotations, and creates argument handlers.
     */
    @Override protected void registered(RequestServer.API_VERSION version) {
      super.registered(version);
    }

    @Override protected Response serve() {
      long[][] cpuTicks;
      LinuxProcFileReader lpfr = new LinuxProcFileReader();
      lpfr.read();
      if (lpfr.valid()) {
        cpuTicks = lpfr.getCpuTicks();
      }
      else {
        cpuTicks = new long[0][0];
      }

      JsonArray j = new JsonArray();
      for (long[] arr : cpuTicks) {
        JsonArray j2 = new JsonArray();
        j2.add(new JsonPrimitive(arr[0]));
        j2.add(new JsonPrimitive(arr[1]));
        j2.add(new JsonPrimitive(arr[2]));
        j2.add(new JsonPrimitive(arr[3]));
        j.add(j2);
      }
      JsonObject o = new JsonObject();
      o.add("cpuTicks", j);
      return Response.done(o);
    }
  }
}

