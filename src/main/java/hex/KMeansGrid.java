package hex;

import water.*;
import water.api.DocGen;
import water.util.RString;

public class KMeansGrid extends KMeansShared {
  static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  // This Request supports the HTML 'GET' command, and this is the help text
  // for GET.
  static final String DOC_GET = "Grid search for k-means parameters";

//@formatter:off
  @API(help = "Number of clusters", required = true, filter = kFilter.class)
  int[] k;
  class kFilter extends RSeq { public kFilter() { super("2:10:1", false); } }

  @API(help = "Maximum number of iterations before stopping", required = true, filter = max_iterFilter.class)
  int[] max_iter;
  class max_iterFilter extends RSeq { public max_iterFilter() { super("10:100:10", true); } }

  @API(help = "Columns to use as input")
  int[] cols;
  class colsFilter extends ColumnSelect { public colsFilter() { super("source_key"); } }

  @API(help = "Square error for each parameter combination")
  double[][] errors;
//@formatter:on

  public static String link(Key k, String content) {
    RString rs = new RString("<a href='KMeansGrid.query?%key_param=%$key'>%content</a>");
    rs.replace("key_param", SOURCE_KEY);
    rs.replace("key", k.toString());
    rs.replace("content", content);
    return rs.toString();
  }

  @Override protected void onArgumentsParsed() {
    if( source_key != null && destination_key == null ) {
      String n = source_key.toString();
      int dot = n.lastIndexOf('.');
      if( dot > 0 )
        n = n.substring(0, dot);
      String res = n + Extensions.KMEANS_GRID;
      destination_key = Key.make(res);
    }
  }

  @Override protected void run() {
    ValueArray va = DKV.get(source_key).get();
    hex.KMeans first = hex.KMeans.start(Key.make(), va, k[0], 0, max_iter[0], seed, normalize, cols);
    KMeansModel model = first.get();
    errors = new double[k.length][max_iter.length];
    for( int ki = 0; ki < k.length; ki++ ) {
      for( int mi = 0; mi < max_iter.length; mi++ ) {
        if( ki != 0 || mi != 0 ) {
          KMeans job = KMeans.start(first.dest(), va, k[mi], 0, max_iter[mi], seed, normalize, cols);
          model = job.get();
        }
        errors[ki][mi] = model._error;
      }
      remove();
    }
  }
}