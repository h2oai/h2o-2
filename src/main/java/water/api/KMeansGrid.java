package water.api;

import water.Key;
import water.util.RString;

import com.google.gson.JsonObject;

public class KMeansGrid extends KMeansShared {
  static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  // This Request supports the HTML 'GET' command, and this is the help text
  // for GET.
  static final String DOC_GET = "Grid search for k-means parameters";

  @API(help = "Number of clusters to try")
  final RSeq k = new RSeq("k", true, new NumberSequence("2:10:1", false, 1), false);

  @API(help = "Maximum number of iterations before stopping")
  final RSeq max_iter = new RSeq("max_iter", true, new NumberSequence("10:100:10", true, 10), true);

  public KMeansGrid() {
    // Reorder arguments
    _arguments.remove(k);
    _arguments.add(1, k);
    _arguments.remove(max_iter);
    _arguments.add(2, max_iter);
  }

  public static String link(Key k, String content) {
    RString rs = new RString("<a href='KMeansGrid.query?%key_param=%$key'>%content</a>");
    rs.replace("key_param", SOURCE_KEY);
    rs.replace("key", k.toString());
    rs.replace("content", content);
    return rs.toString();
  }

  // ---
  // Make a new Grid Search object.
  @Override protected Response serve() {
  }
}
