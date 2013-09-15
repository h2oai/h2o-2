package water.api;

import hex.Quantiles;
import water.Key;
import water.Request2;
import water.fvec.*;
import water.util.RString;

public class QuantilesPage extends Request2 {
  static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  // This Request supports the HTML 'GET' command, and this is the help text
  // for GET.
  static final String DOC_GET = "Quantiles (epsilon-approximate) on arbitrary column";

  @API(help="Data Frame", required=true, filter=FrameKey.class)
  Frame frm;

  /* BUG: API will not accept plain VecSelect here:
   * java.lang.Exception: Class water.api.Request$VecSelect must have an empty constructor
   *@API(help="Column", required=true, filter=VecSelect.class)
   *Vec vec; */
  @API(help="Column", required=true, filter=QVecSelect.class)
  Vec vec;
  // Cliff says could add _vec here, initialize it from every map() call
  class QVecSelect extends VecSelect { QVecSelect() { super("source"); } }

  // API can't handle declaring these as double[] // KMeansGrid: parse double[] from String
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

  @API(help="Quantiles") 
  double[] quantiles = new double[]{quantile_a, quantile_b, quantile_c, quantile_d, quantile_e, quantile_f};

  @API(help="Pass 1 msec")     long pass1time;
  @API(help="Pass 2 msec")     long pass2time;
  @API(help="Pass 3 msec")     long pass3time;
  @API(help="nrows (N)")       long nrows;

  @Override public Response serve() {
    //locals

    Quantiles qq = new Quantiles(frm, vec, quantiles);

    //passes

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
