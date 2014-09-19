package hex.singlenoderf;

import water.DKV;
import water.UKV;
import water.Key;
import water.Request2;
import water.api.DocGen;
import water.api.Request;


public class SpeeDRFModelView extends Request2 {
  static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  @API(help="SpeeDRF Model Key", required = true, filter = SpeeDRFKeyFilter.class)
  Key _modelKey;
  class SpeeDRFKeyFilter extends H2OKey { public SpeeDRFKeyFilter() { super("",true); } }

  @API(help="SpeeDRF Model")
  SpeeDRFModel speedrf_model;

  public static String link(String txt, Key model) {
    return "<a href='/2/SpeeDRFModelView.html?_modelKey=" + model + "'>" + txt + "</a>";
  }

  public static Response redirect(Request req, Key modelKey) {
    return Response.redirect(req, "/2/SpeeDRFModelView", "_modelKey", modelKey);
  }

  @Override public void toJava(StringBuilder sb) { speedrf_model.transform2DTreeModel().toJavaHtml(sb); }

  @Override public boolean toHTML(StringBuilder sb){
    speedrf_model.generateHTML("", sb);
    return true;
  }

  @Override public String serveJava() {
    SpeeDRFModel m = UKV.get(_modelKey);
      if (m!=null) {
        return m.transform2DTreeModel().toJava();
      } else {
        return "";
      }
  }

  @Override protected Response serve() {
    speedrf_model = DKV.get(_modelKey).get();
    return Response.done(this);
  }
}
