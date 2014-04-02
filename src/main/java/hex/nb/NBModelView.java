package hex.nb;

import water.*;
import water.api.DocGen;
import water.api.Request;

public class NBModelView extends Request2 {
  static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  @API(help="Naive Bayes Model Key", required = true, filter = NBModelKeyFilter.class)
  Key _modelKey;
  class NBModelKeyFilter extends H2OKey { public NBModelKeyFilter() { super("",true); } }

  @API(help="Naive Bayes Model")
  NBModel nb_model;

  public static String link(String txt, Key model) {
    return "<a href='/2/NBModelView.html?_modelKey=" + model + "'>" + txt + "</a>";
  }

  public static Response redirect(Request req, Key modelKey) {
    return Response.redirect(req, "/2/NBModelView", "_modelKey", modelKey);
  }

  @Override public boolean toHTML(StringBuilder sb){
    nb_model.generateHTML("", sb);
    return true;
  }

  @Override protected Response serve() {
    nb_model = DKV.get(_modelKey).get();
    return Response.done(this);
  }
}
