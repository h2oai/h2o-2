package water.exec3;

import com.google.gson.*;
import water.NanoHTTPD;
import water.Request2;
import water.api.DocGen;
import water.exec.*;

import java.util.Properties;



public class AST2IR extends Request2 {

  static final int API_WEAVER=1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  @API(help="String to execute", required=true, filter=Default.class)
  String ast;

  @Override public Response serve() {
    JsonElement el = (new Gson()).fromJson(ast, JsonElement.class);
    System.out.println(ast);
    JsonObject response = el.getAsJsonObject();
    String op_id = response.get("astop").getAsJsonObject().get("operator").getAsString();

    if (response.get("astop").getAsJsonObject().get("type").getAsString().equals("BinaryOperator")) {
      ASTOp op = ASTOp.BIN_INFIX_OPS.get(op_id);
      System.out.println(op);

//      op.make();
    }

    return Response.done(response);
  }


  @Override protected NanoHTTPD.Response serveGrid(NanoHTTPD server, Properties parms, RequestType type) {
    return superServeGrid(server, parms, type);
  }
}
