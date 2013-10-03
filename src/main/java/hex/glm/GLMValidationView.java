package hex.glm;

import water.*;
import water.api.DocGen;
import water.api.Request;

public class GLMValidationView extends Request2 {
  static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  @API(help="GLM Validation Key", required=true, filter=GLMValKeyFilter.class)
  Key _valKey;
  class GLMValKeyFilter extends H2OKey { public GLMValKeyFilter() { super("",true); } }

  @API(help="GLM Val")
  GLMValidation glm_val;

  public static String link(String txt, Key val) {
    return "<a href='GLMValidationView.html?_valKey=" + val + "'>" + txt + "</a>";
  }

  public static Response redirect(Request req, Key valKey) {
    return new Response(Response.Status.redirect, req, -1, -1, "GLMValidationView", "_valKey", valKey);
  }

  @Override public boolean toHTML(StringBuilder sb){
    glm_val.generateHTML("", sb);
    return true;
  }

  @Override protected Response serve() {
    glm_val = DKV.get(_valKey).get();
    return new Response(Response.Status.done,this,-1,-1,null);
  }

}