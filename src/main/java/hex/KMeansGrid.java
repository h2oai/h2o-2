package hex;

import water.Key;
import water.api.Constants;
import water.api.GLMGridProgress;

import com.google.gson.JsonObject;

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

  @API(help = "Square error for each parameter combination")
  double[][] errors;

  private KMeansGrid() {
    _description = "KMeansGrid";
    destination_key = dest;
  }

  @Override protected void run() {
    hex.KMeans first = start(Key.make(), ks[0], ms[0]);
    hex.KMeansGrid grid = hex.KMeansGrid.start(destination_key.value(), first, ks, ms, cols);
    // Redirect to the grid-search status page
    JsonObject j = new JsonObject();
    j.addProperty(Constants.DEST_KEY, grid.dest().toString());
    Response r = GLMGridProgress.redirect(j, grid.self(), grid.dest());
    r.setBuilder(Constants.DEST_KEY, new KeyElementBuilder());
    return r;

    KMeansModel model = first.get();
    errors = new double[k.length][maxIter.length];
    for( int ki = 0; ki < k.length; ki++ ) {
      for( int mi = 0; mi < maxIter.length; mi++ ) {
        if( ki != 0 || mi != 0 ) {
          KMeans job = KMeans.start(first.dest(), model._va, k[mi], model._epsilon, maxIter[mi], //
              model._randSeed, model._normalized, cols);
          model = job.get();
        }
        errors[ki][mi] = model._error;
      }
      remove();
    }
  }
}