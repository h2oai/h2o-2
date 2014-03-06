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

  @API(help = "Quantile to calculate", filter = Default.class, dmin = 0, dmax = 1)
  public double quantile = 0.0;

  @API(help = "Number of bins for quantile (1-1000000)", filter = Default.class, lmin = 1, lmax = 1000000)
  public int max_qbins = 1000;

  @API(help = "Column name.")
  String column_name;

  @API(help = "Quantile requested.")
  double quantile_requested;

  @API(help = "Result.")
  double result;

  public static String link(Key k, String content) {
    RString rs = new RString("<a href='SummaryPage2.query?source=%$key'>"+content+"</a>");
    rs.replace("key", k.toString());
    return rs.toString();
  }

  @Override protected Response serve() {
    if( source_key == null ) return RequestServer._http404.serve();
    if( column == null ) return RequestServer._http404.serve();
    if (column.isEnum()) {
      throw new IllegalArgumentException("Column is an enum");
    }

    Vec[] vecs = new Vec[1];
    String[] names = new String[1];
    vecs[0] = column;
    names[0] = source_key.names()[source_key.find(column)];
    Frame fr = new Frame(names, vecs);

    Futures fs = new Futures();
    for( Vec vec : vecs) vec.rollupStats(fs);
    fs.blockForPending();

    Quantiles.BasicStat basicStats[] = new Quantiles.PrePass().doAll(fr).finishUp()._basicStats;
    Quantiles[] summaries;
    summaries = new Quantiles.SummaryTask2(basicStats, quantile, max_qbins).doAll(fr)._summaries;
    if (summaries != null) {
      summaries[0].finishUp(vecs[0]);
      column_name = summaries[0].colname;
      quantile_requested = summaries[0].DEFAULT_PERCENTILES[0];
      result = summaries[0]._pctile[0];
    }

    return Response.done(this);
  }
}
