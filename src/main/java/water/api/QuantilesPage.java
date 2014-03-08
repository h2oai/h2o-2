package water.api;

import hex.Quantiles;
import water.*;
import water.util.RString;
import water.fvec.*;

public class QuantilesPage extends Request2 {
  static final int API_WEAVER=1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  // This Request supports the HTML 'GET' command, and this is the help text
  // for GET.
  static final String DOC_GET = "Returns a summary of a fluid-vec frame";

  @API(help="An existing H2O Frame key.", required=true, filter=Default.class)
  public Frame source_key;

  @API(help="Column to calculate quantile for", required=true, filter=responseFilter.class)
  public Vec column;
  class responseFilter extends VecClassSelect { responseFilter() { super("source_key"); } }

  @API(help = "Quantile to calculate (0.0-1.0)", filter = Default.class, dmin = 0, dmax = 1)
  public double quantile = 0.0;

  @API(help = "Number of bins for quantile (1-1000000)", filter = Default.class, lmin = 1, lmax = 1000000)
  public int max_qbins = 1000;

  @API(help = "Type 2 (discont.) and type 7 (cont.) are supported (like R)", filter = Default.class)
  public int interpolation_type = 7;

  @API(help = "Column name.")
  String column_name;

  @API(help = "Quantile requested.")
  double quantile_requested;

  @API(help = "Interpolation type.")
  int interpolation_type_requested;

  @API(help = "False if an exact result is provided, True if the answer is interpolated.")
  boolean interpolated;

  @API(help = "Number of iterations actually performed.")
  int iterations;

  @API(help = "Result.")
  double result;

  public static String link(Key k, String content) {
    RString rs = new RString("<a href='QuantilesPage.query?source=%$key'>"+content+"</a>");
    rs.replace("key", k.toString());
    return rs.toString();
  }

  @Override protected Response serve() {
    if( source_key == null ) return RequestServer._http404.serve();
    if( column == null ) return RequestServer._http404.serve();
    if (column.isEnum()) {
      throw new IllegalArgumentException("Column is an enum");
    }
    if (! ((interpolation_type == 2) || (interpolation_type == 7))) {
      throw new IllegalArgumentException("Unsupported interpolation type");
    }

    Vec[] vecs = new Vec[1];
    String[] names = new String[1];
    vecs[0] = column;
    names[0] = source_key.names()[source_key.find(column)];
    Frame fr = new Frame(names, vecs);

    Futures fs = new Futures();
    for( Vec vec : vecs) {
        vec.rollupStats(fs);
        // just to see, move to using these rather than the min/max/mean from basicStats
        double vmax = vec.max();
        double vmin = vec.min();
        double vmean = vec.mean();
        double vsigma = vec.sigma();
        long vnaCnt = vec.naCnt();
        boolean visInt = vec.isInt();
    }
    fs.blockForPending();

    Quantiles[] qbins;
    // not used on the single pass approx. will use on multipass iterations
    double valStart = vecs[0].min();
    double valEnd = vecs[0].max();
    qbins = new Quantiles.BinTask2(quantile, max_qbins, valStart, valEnd).doAll(fr)._qbins;

    if (qbins != null) {
      qbins[0].finishUp(vecs[0], max_qbins);
      column_name = qbins[0].colname;
      quantile_requested = qbins[0].QUANTILES_TO_DO[0];
      interpolation_type_requested = interpolation_type;
      result = qbins[0]._pctile[0];
    }

    // just see we can do another iteration with same values
    if (false) {
        qbins = new Quantiles.BinTask2(quantile, max_qbins, valStart, valEnd).doAll(fr)._qbins;
    }

    return Response.done(this);
  }
}
