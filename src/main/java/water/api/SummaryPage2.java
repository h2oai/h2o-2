package water.api;

import hex.Summary2;
import water.*;
import water.util.Log;
import water.util.RString;
import water.fvec.*;
import water.util.Utils;

import java.util.Iterator;

/**
 *
 */
public class SummaryPage2 extends Request2 {
  static final int API_WEAVER=1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  // This Request supports the HTML 'GET' command, and this is the help text
  // for GET.
  static final String DOC_GET = "Returns a summary of a fluid-vec frame";

  @API(help="An existing H2O Frame key.", required=true, filter=Default.class)
  Frame source;

  class colsFilter1 extends MultiVecSelect { public colsFilter1() { super("source");} }
  @API(help = "Select columns", filter=colsFilter1.class)
  int[] cols;

  @API(help = "Maximum columns to show summaries of", filter = Default.class, lmin = 1)
  int max_ncols = 1000;

  @API(help = "Number of bins for quantile (1-10000000)", filter = Default.class, lmin = 1, lmax = 1000000)
  int max_qbins = 1000;

  @API(help = "Column summaries.")
  Summary2[] summaries;

  public static String link(Key k, String content) {
    RString rs = new RString("<a href='SummaryPage2.query?source=%$key'>"+content+"</a>");
    rs.replace("key", k.toString());
    return rs.toString();
  }

  @Override protected Response serve() {
    if( source == null ) return RequestServer._http404.serve();
    // select all columns by default
    if( cols == null ) {
      cols = new int[Math.min(source.vecs().length,max_ncols)];
      for(int i = 0; i < cols.length; i++) cols[i] = i;
    }
    Vec[] vecs = new Vec[cols.length];
    String[] names = new String[cols.length];
    for (int i = 0; i < cols.length; i++) {
      vecs[i] = source.vecs()[cols[i]];
      names[i] = source._names[cols[i]];
    }
    Frame fr = new Frame(names, vecs);
    if(fr.numRows() == 0)
      throw new IllegalArgumentException("Data frame has zero rows!");

    Futures fs = new Futures();
    for( Vec vec : vecs) vec.rollupStats(fs);
    fs.blockForPending();

    Summary2.BasicStat basicStats[] = new Summary2.PrePass().doAll(fr).finishUp()._basicStats;
    summaries = new Summary2.SummaryTask2(basicStats, max_qbins).doAll(fr)._summaries;
    if (summaries != null)
      for (int i = 0; i < cols.length; i++)
        summaries[i].finishUp(vecs[i]);
    return Response.done(this);
  }

  @Override public boolean toHTML( StringBuilder sb ) {

    sb.append("<div class=container-fluid'>");
    sb.append("<div class='row-fluid'>");
    sb.append("<div class='span2' style='overflow-y:scroll;height:100%;left:0;position:fixed;text-align:right;overflow-x:scroll;'><h5>Columns</h5>");
    if (summaries != null && summaries.length > max_ncols)
      sb.append("<div class='alert'>Too many columns were selected. "+max_ncols+" of them are shown!</div>");

    StringBuilder innerPageBdr = null;
    if (summaries != null) {
      innerPageBdr = new StringBuilder("<div class='span10' style='float:right;height:90%;overflow-y:scroll'>");
      for( int i = 0; i < Math.min(summaries.length,max_ncols); i++) {
        String cname = source._names[cols[i]];
        Summary2 s2 = summaries[i];
        s2.toHTML(source.vecs()[cols[i]],cname,innerPageBdr);
        sb.append("<div><a href='#col_" + cname + "'>" + cname + "</a></div>");
      }
      innerPageBdr.append("</div>");
    }
    sb.append("</div>");
    sb.append("</div>");
    if (summaries != null) sb.append(innerPageBdr);
    sb.append("</div>");
    return true;
  }
}
