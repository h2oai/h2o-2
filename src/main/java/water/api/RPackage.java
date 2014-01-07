package water.api;

import java.io.*;
import java.util.Properties;

import org.apache.poi.util.IOUtils;

import water.*;

import com.google.gson.*;

public class RPackage extends Request {
  @Override protected Response serve() {
    JsonObject result = new JsonObject();
    result.addProperty(VERSION, H2O.VERSION);

    String info = Boot._init.loadContent("/R/info.txt");
    String info_split[] = info.split("\\r?\\n");
    result.addProperty("filename", info_split[0]);
    result.addProperty("md5_hash", info_split[1]);

    Response response = Response.done(result);
    response.addHeader("<a class='btn btn-primary' href='RDownload.html'>Download h2oRClient</a>");
    return response;
  }

  static class RDownload extends Request {
    @Override public water.NanoHTTPD.Response serve(NanoHTTPD server, Properties args, RequestType type) {
      String info = Boot._init.loadContent("/R/info.txt");
      String fname = info.split("\\r?\\n")[0];
      InputStream is = Boot._init.getResource2("/R/" + fname);
      byte[] result;
      try {
        result = IOUtils.toByteArray(is);
      } catch( IOException e ) {
        // result = e.toString().getBytes();
        throw new RuntimeException(e);
      }

      NanoHTTPD.Response res = new NanoHTTPD.Response(NanoHTTPD.Response.Status.OK, RequestServer.MIME_DEFAULT_BINARY, new ByteArrayInputStream(result));
      res.addHeader("Content-Length", Long.toString(result.length));
      res.addHeader("Content-Disposition", "attachment; filename=" + fname);
      return res;
    }

    @Override protected Response serve() {
      throw new RuntimeException("Get should not be called from this context");
    }
  }
}