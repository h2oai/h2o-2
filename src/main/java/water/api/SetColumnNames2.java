package water.api;

import water.*;
import water.fvec.Frame;
import water.util.RString;

public class SetColumnNames2 extends Request2 {
  static final int API_WEAVER=1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  // This Request supports the HTML 'GET' command, and this is the help text
  // for GET.
  static final String DOC_GET = "Sets the column names of a frame.";

  @API(help="An existing H2O Frame key.", required=true, filter=Default.class)
  Frame source;

  class colsFilter1 extends MultiVecSelect { public colsFilter1() { super("source");} }
  @API(help = "Select columns", filter=colsFilter1.class)
  int[] cols;

  @API(help="Column names as a parsed frame.", filter=Default.class)
  Frame copy_from;

  @API(help="Column names as a vector of strings.", gridable=false, filter=Default.class)
  String comma_separated_list;

  public static String link(Key k, String content) {
    RString rs = new RString("<a href='SetColumnNames2.query?source=%$key'>"+content+"</a>");
    rs.replace("key", k.toString());
    return rs.toString();
  }

  @Override protected Response serve() {
    if( source == null ) return RequestServer._http404.serve();
    // select all columns by default
    if( cols == null ) {
      cols = new int[source.vecs().length];
      for(int i = 0; i < cols.length; i++) cols[i] = i;
    }

    if(comma_separated_list == null && copy_from == null)
      throw new IllegalArgumentException("No column names given");
    else if(comma_separated_list != null && copy_from != null)
      throw new IllegalArgumentException("Cannot specify column names as both frame and list.");

    String[] names_str = comma_separated_list == null ? copy_from.names() : comma_separated_list.split(",");
    if(cols.length != names_str.length)
      throw new IllegalArgumentException("number of columns don't match!");

    for(int i = 0; i < cols.length; i++)
      source._names[cols[i]] = names_str[i];
    Futures fs = new Futures();
    DKV.put(source._key, source, fs);
    fs.blockForPending();
    return Inspect2.redirect(this, source._key.toString());
  }
}
