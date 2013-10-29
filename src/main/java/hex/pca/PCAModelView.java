package hex.pca;

import water.*;
import water.api.DocGen;
import water.api.Request;

public class PCAModelView extends Request2 {
  static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  @API(help="PCA Model Key", required = true, filter = PCAModelKeyFilter.class)
  Key _modelKey;
  class PCAModelKeyFilter extends H2OKey { public PCAModelKeyFilter() { super("",true); } }

  @API(help="PCA Model")
  PCAModel pca_model;

  public static String link(String txt, Key model) {
    return "<a href='/2/PCAModelView.html?_modelKey=" + model + "'>" + txt + "</a>";
  }

  public static Response redirect(Request req, Key modelKey) {
    return new Response(Response.Status.redirect, req, -1, -1, "/2/PCAModelView", "_modelKey", modelKey);
  }

  @Override public boolean toHTML(StringBuilder sb){
    pca_model.generateHTML("", sb);
    return true;
  }

  @Override protected Response serve() {
    pca_model = DKV.get(_modelKey).get();
    return new Response(Response.Status.done, this,-1, -1, null);
  }
}
