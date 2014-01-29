package water.api;

import com.google.gson.JsonObject;

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
  Frame target;

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
    if(target == null) return RequestServer._http404.serve();
    if(comma_separated_list == null && copy_from == null)
      throw new IllegalArgumentException("No column names given");
    if(comma_separated_list != null && copy_from != null)
      throw new IllegalArgumentException("Cannot specify column names as both frame and list.");

    String[] names_str = comma_separated_list == null ? copy_from.names() : comma_separated_list.split(",");
    if(target.vecs().length != names_str.length)
      throw new IllegalArgumentException("number of columns don't match!");

    for(int i = 0; i < target.vecs().length; i++)
      target._names[i] = names_str[i];
    Futures fs = new Futures();
    DKV.put(target._key, target, fs);
    fs.blockForPending();
    return Inspect2.redirect(this, target._key.toString());
  }
}
