package water.api;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import water.util.Log;
import water.*;
import water.exec.*;
import water.fvec.*;

import java.util.Properties;


/**
 * Executes an AST passed as JSON from R.
 */
public class Exec3 extends Request2 {
  static final int API_WEAVER=1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  @API(help="String to execute", required=true, filter=Default.class)
  String ast;

  @Override public Response serve() {
    System.out.println(ast);
    JsonElement el = (new Gson()).fromJson(ast, JsonElement.class);
    System.out.println(ast);
    JsonObject response = el.getAsJsonObject();

    response.addProperty("ast", ast);
    return Response.done(response);
  }

  @Override protected NanoHTTPD.Response serveGrid(NanoHTTPD server, Properties parms, RequestType type) {
    return superServeGrid(server, parms, type);
  }
}
