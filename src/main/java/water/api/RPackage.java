package water.api;

import water.*;
import com.google.gson.*;

public class RPackage extends Request {
  @Override protected Response serve() {
    JsonObject result = new JsonObject();
    result.addProperty(VERSION, H2O.VERSION);
    // InputStream is_file = getClass().getResourceAsStream("/R/h2o_1.7.0.99999.tar.gz");
    result.addProperty("filename", "h2o_1.7.0.99999.tar.gz");
    String md5 = Boot._init.loadContent("/R/md5.txt");
    result.addProperty("md5_hash", md5);
    Response response = Response.done(result);
    return response;
  }
}