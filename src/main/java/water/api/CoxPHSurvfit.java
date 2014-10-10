package water.api;

import hex.CoxPH.CoxPHModel;
import water.*;
import water.fvec.Frame;
import water.fvec.Vec;
import water.util.RString;

public class CoxPHSurvfit extends Request2 {
  static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  @API(help = "Model", required = true, filter = Default.class)
  public Key model;

  @API(help="New X Value",   required=false,  filter=Default.class)
  double x_new = Double.NaN;

  @API(help = "Survival Curve", filter = Default.class)
  public Key survfit;

  public static String link(Key k, double x_new, String content) {
    RString rs = new RString("<a href='CoxPHSurvfit.query?model=%$key&x_new=%x_new'>%content</a>");
    rs.replace("key", k.toString());
    rs.replace("x_new", x_new);
    rs.replace("content", content);
    return rs.toString();
  }

  @Override protected Response serve() {
    try {
      if (model == null)
        throw new IllegalArgumentException("Model is required to perform validation!");
      CoxPHModel m = DKV.get(model).get();
      if (survfit == null)
        survfit = Key.make("__Survfit_" + Key.make());
      m.makeSurvfit(survfit, x_new);
      return Inspect2.redirect(this, survfit.toString());
    } catch (Throwable t) {
      return Response.error(t);
    }
  }
}
