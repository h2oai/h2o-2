package hex;

import hex.KMeans.Initialization;

import java.util.Random;

import water.Job.ColumnsJob;
import water.UKV;
import water.api.DocGen;
import water.util.Log;

/**
 * Scalable K-Means++ (KMeans||)<br>
 * http://theory.stanford.edu/~sergei/papers/vldb12-kmpar.pdf<br>
 * http://www.youtube.com/watch?v=cigXAxV3XcY
 */
public class KMeans2 extends ColumnsJob {
  static final int API_WEAVER = 1;
  static public DocGen.FieldDoc[] DOC_FIELDS;
  static final String DOC_GET = "k-means";

  @API(help = "Clusters initialization", filter = Default.class)
  public Initialization initialization = Initialization.None;

  @API(help = "Number of clusters", required = true, filter = Default.class, lmin = 2, lmax = 100000)
  public int k = 2;

  @API(help = "Maximum number of iterations before stopping", required = true, filter = Default.class, lmin = 1, lmax = 100000)
  public int max_iter = 100;

  @API(help = "Whether data should be normalized", filter = Default.class)
  public boolean normalize;

  @API(help = "Seed for the random number generator", filter = Default.class)
  public long seed = new Random().nextLong();

  @API(help = "Iterations the algorithm ran")
  public int iterations;

  public KMeans2() {
    description = DOC_GET;
  }

  @Override protected void exec() {
    Log.info(DOC_GET + source);
    UKV.put(destination_key, source);
  }
}
