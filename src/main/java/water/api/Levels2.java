package water.api;

import water.*;
import water.fvec.Frame;
import water.fvec.Vec;
import water.util.RString;

public class Levels2 extends Request2 {
  static final int API_WEAVER=1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  // This Request supports the HTML 'GET' command, and this is the help text
  // for GET.
  static final String DOC_GET = "Returns the factor levels of each column in a frame";

  @API(help="An existing H2O Frame key.", required=true, filter=Default.class)
  Frame source;

  class colsFilter1 extends MultiVecSelect { public colsFilter1() { super("source");} }
  @API(help = "Select columns", filter=colsFilter1.class)
  int[] cols;

  @API(help = "Maximum columns to show summaries of", filter = Default.class, lmin = 1)
  int max_ncols = 1000;

  @API(help = "Factor levels of each column")
  String[][] levels;

  public static String link(Key k, String content) {
    RString rs = new RString("<a href='Levels2.query?source=%$key'>"+content+"</a>");
    rs.replace("key", k.toString());
    return rs.toString();
  }

  @Override protected Response serve() {
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

    levels = new String[cols.length][];
    for(int i = 0; i < cols.length; i++)
      levels[i] = vecs[i].domain() == null ? null : vecs[i].domain().clone();
    return Response.done(this);
  }
}
