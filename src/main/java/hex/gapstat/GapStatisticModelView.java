package hex.gapstat;

import water.DKV;
import water.Key;
import water.Request2;
import water.api.DocGen;
import water.api.Request;


public class GapStatisticModelView extends Request2 {
  static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  @API(help="Gap Statistic Model Key", required = true, filter = GSKeyFilter.class)
  Key _modelKey;
  class GSKeyFilter extends H2OKey { public GSKeyFilter() { super("",true); } }

  @API(help="Gap Statistic Model")
  GapStatisticModel gap_model;

  public static String link(String txt, Key model) {
    return "<a href='/2/GapStatisticModelView.html?_modelKey=" + model + "'>" + txt + "</a>";
  }

  public static Response redirect(Request req, Key modelKey) {
    return Response.redirect(req, "/2/GapStatisticModelView", "_modelKey", modelKey);
  }

  @Override public boolean toHTML(StringBuilder sb){
    gap_model.generateHTML("", sb);
    return true;
  }

  @Override protected Response serve() {
    gap_model = DKV.get(_modelKey).get();
    return Response.done(this);
  }
}