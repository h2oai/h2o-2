package water.api;

import hex.Quantiles;
import water.Key;
import water.Request2;
//import water.api.Request.API;
//import water.api.Request.VecSelect;
//import water.api.Request.*;
//import water.api.RequestArguments.FrameKey;
//import water.api.RequestBuilders.Response;
import water.fvec.*;
import water.util.RString;

public class QuantilesPage extends Request2 {
  static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  // This Request supports the HTML 'GET' command, and this is the help text
  // for GET.
  static final String DOC_GET = "Quantiles on column";

  @API(help="Data Frame", required=true, filter=FrameKey.class)
  Frame frm;

  // BUG: API will not accept plain VecSelect here; must subclass VecSelect
  @API(help="Column", required=true, filter=QVecSelect.class)
  Vec vec;
  class QVecSelect extends VecSelect { QVecSelect() { super("frm"); } }

  // API can't handle declaring these as double[]. Could parse double[] from String
  @API(help="Quantile_a", required=true, filter=Default.class)  // BUG: filter=Real.class doesn't appear
  double quantile_a = .05;
  @API(help="Quantile_b", required=true, filter=Default.class)
  double quantile_b = .10;
  @API(help="Quantile_c", required=true, filter=Default.class)
  double quantile_c = .15;
  @API(help="Quantile_d", required=true, filter=Default.class)
  double quantile_d = .85;
  @API(help="Quantile_e", required=true, filter=Default.class)
  double quantile_e = .90;
  @API(help="Quantile_f", required=true, filter=Default.class)
  double quantile_f = .95;

  @API(help="Pass 1 msec")     long pass1time;
  @API(help="Pass 2 msec")     long pass2time;
  //@API(help="nrows")           long nrows;
  @API(help="qvalue_a")        double qvalue_a;
  @API(help="qvalue_b")        double qvalue_b;
  @API(help="qvalue_c")        double qvalue_c;
  @API(help="qvalue_d")        double qvalue_d;
  @API(help="qvalue_e")        double qvalue_e;
  @API(help="qvalue_f")        double qvalue_f;

  @Override public Response serve() {

    Quantiles qq = new Quantiles(vec, quantile_a,quantile_b,quantile_c,quantile_d,quantile_e,quantile_f,
        pass1time,pass2time);

    qvalue_a = qq.qval[0];
    qvalue_b = qq.qval[1];
    qvalue_c = qq.qval[2];
    qvalue_d = qq.qval[3];
    qvalue_e = qq.qval[4];
    qvalue_f = qq.qval[5];

    return new Response(Response.Status.done, this, -1, -1, null);
  }

  /** Return the HTML query link to this page */
  public static String link(Key k, String content) {
    RString rs = new RString("<a href='Quantiles.query?data_key=%$key'>%content</a>");
    rs.replace("key", k.toString());
    rs.replace("content", content);
    return rs.toString();
  }

}
