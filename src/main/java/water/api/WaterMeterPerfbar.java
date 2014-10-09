package water.api;

import dontweave.gson.*;
import water.*;
import water.util.LinuxProcFileReader;
import water.util.Log;

/**
 * Redirect to water meter page.
 */
public class WaterMeterPerfbar extends HTMLOnlyRequest {
  protected String build(Response response) {
    return "" +
            "<div class='container' id='perfbarContainer'>" +
            "<script>" +
            "var PB_LINEOFTEXT_BACKGROUND_COLOR = \"#fff\";" +
            "</script>" +
            "<script src=\"watermeter/perfbar.js\"></script>" +
            "</div>";
  }

  public static class WaterMeterCpuTicks extends JSONOnlyRequest {
    Int node_idx = new Int("node_idx", -1);

    @Override
    public RequestServer.API_VERSION[] supportedVersions() { return SUPPORTS_ONLY_V2; }

    /**
     * Iterates over fields and their annotations, and creates argument handlers.
     */
    @Override protected void registered(RequestServer.API_VERSION version) {
      super.registered(version);
    }

    private static class GetTicksTask extends DTask<GetTicksTask> {
      private long[][] _cpuTicks;

      public GetTicksTask() {
        _cpuTicks = null;
      }

      @Override public void compute2() {
        LinuxProcFileReader lpfr = new LinuxProcFileReader();
        lpfr.read();
        if (lpfr.valid()) {
          _cpuTicks = lpfr.getCpuTicks();
        }
        else {
          // In the case where there isn't any tick information, the client receives a json
          // response object containing an array of length 0.
          //
          // e.g.
          // { cpuTicks: [] }
          _cpuTicks = new long[0][0];
        }

        tryComplete();
      }

      @Override public byte priority() {
        return H2O.MIN_HI_PRIORITY;
      }
    }

    @Override protected Response serve() {
      if ((node_idx.value() < 0) || (node_idx.value() >= H2O.CLOUD.size())) {
        throw new IllegalArgumentException("Illegal node_idx for this H2O cluster (must be from 0 to " + H2O.CLOUD.size() + ")");
      }

      H2ONode node = H2O.CLOUD._memary[node_idx.value()];
      GetTicksTask ppt = new GetTicksTask();
      Log.trace("GetTicksTask starting to node " + node_idx.value() + "...");
      // Synchronous RPC call to get ticks from remote (possibly this) node.
      new RPC<GetTicksTask>(node, ppt).call().get();
      Log.trace("GetTicksTask completed to node " + node_idx.value());
      long[][] cpuTicks = ppt._cpuTicks;

      // Stuff tick information into json response.
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
