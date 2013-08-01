package hex;

import water.*;
import water.util.RString;

/**
 * Grid search for k-means parameters.
 */
public class KMeansGrid extends KMeansShared {
  @API(help = "Number of clusters")
  @Input(required = true)
  @Sequence(pattern = "2:10:1")
  int[] k;

  @API(help = "Maximum number of iterations before stopping")
  @Input(required = true)
  @Sequence(pattern = "10:100:10", mult = true)
  int[] max_iter;

  @API(help = "Columns to use as input")
  @Input
  @ColumnSelect(key = "source_key")
  int[] cols;

  @API(help = "Square error for each parameter combination")
  double[][] errors;

  public static String link(Key k, String content) {
    RString rs = new RString("<a href='KMeansGrid.query?%key_param=%$key'>%content</a>");
    rs.replace("key_param", SOURCE_KEY);
    rs.replace("key", k.toString());
    rs.replace("content", content);
    return rs.toString();
  }

  @Override protected void run() {
    if( destination_key == null ) {
      String n = source_key.toString();
      int dot = n.lastIndexOf('.');
      if( dot > 0 )
        n = n.substring(0, dot);
      String res = n + Extensions.KMEANS_GRID;
      destination_key = Key.make(res);
    }

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