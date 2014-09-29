package water.api;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import water.*;
import water.util.*;

import dontweave.gson.JsonObject;

public class LogView extends Request {
  @Override protected Response serve() {
    String s = water.util.Log.getLogPathFileName();
    JsonObject result = new JsonObject();
    File f = new File (s);
    String contents = Utils.readFile(f);
    if (contents == null) {
      contents = "Not yet initialized, please refresh...";
    }
    result.addProperty("log", "<pre>" + contents + "</pre>");

    Response response = Response.done(result);
    response.addHeader("<a class='btn btn-primary' href='LogDownload.html'>Download all logs</a>");
    return response;
  }

  @Override protected boolean log() {
    return false;
  }

  static class LogDownload extends Request {

    @Override public water.NanoHTTPD.Response serve(NanoHTTPD server, Properties args, RequestType type) {
      Log.info("\nCollecting logs.");

      // collect nodes' logs
      LogCollectorTask collector = new LogCollectorTask();
      collector.invokeOnAllNodes();

      // FIXME put here zip for each file.
      String outputFileStem = getOutputLogStem();
      byte[] result = null;
      try {
        result = zipLogs(collector._result, outputFileStem);
      } catch (IOException e) {
        // put the exception into output log
        result = e.toString().getBytes();
      }
      NanoHTTPD.Response res = server.new Response(NanoHTTPD.HTTP_OK,NanoHTTPD.MIME_DEFAULT_BINARY, new ByteArrayInputStream(result));
      res.addHeader("Content-Length", Long.toString(result.length));
      res.addHeader("Content-Disposition", "attachment; filename="+outputFileStem + ".zip");
      return res;
    }

    @Override protected Response serve() {
      throw new RuntimeException("Get should not be called from this context");
    }

    private String getOutputLogStem() {
      String pattern = "yyyyMMdd_hhmmss";
      SimpleDateFormat formatter = new SimpleDateFormat(pattern);
      String now = formatter.format(new Date());

      return "h2ologs_" + now;
    }

    private byte[] zipLogs(byte[][] results, String topDir) throws IOException {
      int l = 0;
      assert H2O.CLOUD._memary.length == results.length : "Unexpected change in the cloud!";
      for (int i = 0; i<results.length;l+=results[i++].length);
      ByteArrayOutputStream baos = new ByteArrayOutputStream(l);

      // Add top-level directory.
      ZipOutputStream zos = new ZipOutputStream(baos);
      {
        ZipEntry zde = new ZipEntry (topDir + File.separator);
        zos.putNextEntry(zde);
      }

      try {
        // Add zip directory from each cloud member.
        for (int i =0; i<results.length; i++) {
          String filename =
                  topDir + File.separator +
                  "node" + i +
                  H2O.CLOUD._memary[i].toString().replace(':', '_').replace('/', '_') +
                  ".zip";
          ZipEntry ze = new ZipEntry(filename);
          zos.putNextEntry(ze);
          zos.write(results[i]);
          zos.closeEntry();
        }

        // Close the top-level directory.
        zos.closeEntry();
      } finally {
        // Close the full zip file.
        zos.close();
      }

      return baos.toByteArray();
    }
  }
}
