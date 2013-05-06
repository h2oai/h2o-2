package water.api;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import water.*;
import water.util.*;
import water.util.Log.LogStr;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

public class LogView extends Request {
  @Override protected Response serve() {
    LogStr logstr = UKV.get(Log.LOG_KEY);
    JsonArray ary = new JsonArray();
    if( logstr != null ) {
      for( int i=0; i<LogStr.MAX; i++ ) {
        int x = (i+logstr._idx+1)&(LogStr.MAX-1);
        if( logstr._dates[x] == null ) continue;
        JsonObject obj = new JsonObject();
        obj.addProperty("date", logstr._dates[x]);
        obj.addProperty("h2o" , logstr._h2os [x].toString());
        obj.addProperty("pid" , logstr._pids [x]);
        obj.addProperty("thr" , logstr._thrs [x]);
        obj.addProperty("kind", Log.KINDS[logstr._kinds[x]].name());
        obj.addProperty("sys", Log.SYSS[logstr._syss[x]].name());
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

  @Override protected boolean log() {
    return false;
  }

  static class LogDownload extends Request {

    @Override public water.NanoHTTPD.Response serve(NanoHTTPD server, Properties args, RequestType type) {
      // collect nodes' logs
      LogCollectorTask collector = new LogCollectorTask();
      collector.invokeOnAllNodes();

      // FIXME put here zip for each file.
      String outputFile = getOutputLogName();
      byte[] result = null;
      try {
        result = zipLogs(collector._result);
      } catch (IOException e) {
        // put the exception into output log
        result = e.toString().getBytes();
      }
      NanoHTTPD.Response res = server.new Response(NanoHTTPD.HTTP_OK,NanoHTTPD.MIME_DEFAULT_BINARY, new ByteArrayInputStream(result));
      res.addHeader("Content-Length", Long.toString(result.length));
      res.addHeader("Content-Disposition", "attachment; filename="+outputFile);
      return res;
    }

    @Override protected Response serve() {
      throw new RuntimeException("Get should not be called from this context");
    }

    private String getOutputLogName() {
      String pattern = "yyMMdd-hhmmss";
      SimpleDateFormat formatter = new SimpleDateFormat(pattern);
      String now = formatter.format(new Date());

      return "h2o-" + now + ".zip";
    }

    private byte[] zipLogs(byte[][] results) throws IOException {
      int l = 0;
      assert H2O.CLOUD._memary.length == results.length : "Unexpected change in the cloud!";
      for (int i = 0; i<results.length;l+=results[i++].length);
      ByteArrayOutputStream baos = new ByteArrayOutputStream(l);
      ZipOutputStream zos = new ZipOutputStream(baos);
      try {
        for (int i =0; i<results.length; i++) {
          String filename = "node"+i+H2O.CLOUD._memary[i].toString().replace(':', '_').replace('/', '_') + ".log";
          ZipEntry ze = new ZipEntry(filename);
          zos.putNextEntry(ze);
          zos.write(results[i]);
          zos.closeEntry();
        }
      } finally { zos.close(); }

      return baos.toByteArray();
    }
  }
}
