package water.api;

import hex.CoxPH.CoxPHModel;
import water.Key;
import water.Request2;
import water.UKV;

public class CoxPHModelView extends Request2 {
  static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  @API(help="Cox Proportional Hazards Model Key", required=true, filter=CoxPHModelKeyFilter.class)
  Key _modelKey;
  class CoxPHModelKeyFilter extends H2OKey { public CoxPHModelKeyFilter() { super("model_key",true); } }

  @API(help="Cox Proportional Hazards Model")
  public CoxPHModel coxph_model;

  public static String link(String txt, Key model) {
    return "<a href='CoxPHModelView.html?_modelKey=" + model + "'>" + txt + "</a>";
  }

  public static Response redirect(Request req, Key modelKey) {
    return Response.redirect(req, "/2/CoxPHModelView", "_modelKey", modelKey);
  }

  @Override public boolean toHTML(StringBuilder sb){
    coxph_model.get_params().makeJsonBox(sb);
    coxph_model.generateHTML("Cox Proportional Hazards Model", sb);
    return true;
  }

  @Override protected Response serve() {
    coxph_model = UKV.get(_modelKey);
    if (coxph_model == null)
      return Response.error("Model '" + _modelKey + "' not found!");
    else
      return Response.done(this);
  }

  @Override public void toJava(StringBuilder sb) {
    coxph_model.toJavaHtml(sb);
  }

  @Override protected String serveJava() {
    CoxPHModel m = UKV.get(_modelKey);
    if (m != null)
      return m.toJava();
    else
      return "";
  }
}
