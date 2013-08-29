package water.api;

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
    return response;
  }
}