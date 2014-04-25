package water.api;

import hex.gbm.GBM.GBMModel;
import water.*;

public class GBMModelView extends Request2 {
  static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  @API(help="GBM Model Key", required=true, filter=GBMModelKeyFilter.class)
  Key _modelKey;
  class GBMModelKeyFilter extends H2OKey { public GBMModelKeyFilter() { super("model_key",true); } }

  @API(help="GBM Model")
  public GBMModel gbm_model;

  public static String link(String txt, Key model) {
    return "<a href='GBMModelView.html?_modelKey=" + model + "'>" + txt + "</a>";
  }

  public static Response redirect(Request req, Key modelKey) {
    return Response.redirect(req, "/2/GBMModelView", "_modelKey", modelKey);
  }

  @Override public boolean toHTML(StringBuilder sb){
    gbm_model.get_params().makeJsonBox(sb);
    gbm_model.generateHTML("GBM Model", sb);
    return true;
  }

  @Override protected Response serve() {
    gbm_model = UKV.get(_modelKey);
    if (gbm_model == null) return Response.error("Model '" + _modelKey + "' not found!");
    else return Response.done(this);
  }

  @Override public void toJava(StringBuilder sb) { gbm_model.toJavaHtml(sb); }
  @Override protected String serveJava() {
    GBMModel m = UKV.get(_modelKey);
    if (m!=null)
      return m.toJava();
    else
      return "";
  }
}
