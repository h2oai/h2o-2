package water.api;

import java.io.ByteArrayInputStream;
import java.util.Properties;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import water.*;
import water.api.RequestBuilders.Response;
import water.util.LogCollectorTask;

public class LogView extends Request {
  @Override protected Response serve() {
    water.Log.LogStr logstr = UKV.get(water.Log.LOG_KEY);
    JsonArray ary = new JsonArray();
    if( logstr != null ) {
      for( int i=0; i<water.Log.LogStr.MAX; i++ ) {
        int x = (i+logstr._idx+1)&(water.Log.LogStr.MAX-1);
        if( logstr._dates[x] == null ) continue;
        JsonObject obj = new JsonObject();
        obj.addProperty("date", logstr._dates[x]);
        obj.addProperty("h2o" , logstr._h2os [x].toString());
        obj.addProperty("pid" , logstr._pids [x]);
        obj.addProperty("thr" , logstr._thrs [x]);
        obj.addProperty("msg" , logstr._msgs [x]);
        ary.add(obj);
      }
    }
    JsonObject result = new JsonObject();
    result.add("log",ary);

    Response response = Response.done(result);
    response.addHeader("<a class='btn btn-primary' href='LogDownload.html'>Download all logs</a>");
    return response;
  }

  static class LogDownload extends Request {

    @Override public water.NanoHTTPD.Response serve(NanoHTTPD server, Properties args, RequestType type) {
      // collect nodes' logs
      LogCollectorTask collector = new LogCollectorTask();
      collector.invokeOnAllNodes();

      // FIXME put here zip for each file.
      String outputFile = "h2o.log";
      byte[] result = pack(collector._result);
      NanoHTTPD.Response res = server.new Response(NanoHTTPD.HTTP_OK,NanoHTTPD.MIME_DEFAULT_BINARY, new ByteArrayInputStream(result));
      res.addHeader("Content-Length", Long.toString(result.length));
      res.addHeader("Content-Disposition", "attachment; filename="+outputFile);
      return res;
    }

    @Override protected Response serve() {
      throw new Error("Get should not be called from this context");
    }

    private byte[] pack(byte[][] results) {
      int l = 0;
      for (int i = 0; i<results.length;i++) l+=results[i].length;
      l += 2*results.length; // delimiter
      byte[] pack = new byte[l];
      l = 0;
      for (int i = 0; i<results.length;i++) {
        System.arraycopy(results[i], 0, pack, l, results[i].length);
        l += results[i].length;
        pack[l++] = (byte) '-';
        pack[l++] = (byte) '\n';
      }
      return pack;
    }
  }
}
