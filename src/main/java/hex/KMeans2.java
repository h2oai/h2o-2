package hex;

import hex.KMeans.Initialization;

import java.util.ArrayList;
import java.util.Random;

import water.*;
import water.Job.ColumnsJob;
import water.api.DocGen;
import water.fvec.*;
import water.util.Utils;

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

  public class KMeansModel2 extends Model implements Progress {
    @API(help = "Cluster centers, always denormalized")
    public double[][] clusters;

    @API(help = "Sum of min square distances")
    public double error;

    @API(help = "Clusters initialization")
    public Initialization initialization;

    @API(help = "Iterations the algorithm ran")
    public int iterations;

    @API(help = "Maximum number of iterations")
    public int max_iter;

    @API(help = "Seed for the random number generator")
    public long seed;

    @API(help = "Whether data was normalized")
    public boolean normalized;

    public KMeansModel2(Key self, Key data, Frame source) {
      super(self, data, source);
    }

    @Override public float progress() {
      return Math.min(1f, iterations / (float) max_iter);
    }

    @Override protected float[] score0(Chunk[] chunks, int rowInChunk, double[] tmp, float[] preds) {
      data[data.length - 1] = Double.NaN; // Response variable column not used
      return KMeans.closest(_clusters, data, new ClusterDist())._cluster;

      return out;
    }

    @Override protected float[] score0(double[] data, float[] preds) {
      throw H2O.unimpl();
    }

    @Override public ConfusionMatrix cm() {
      return new ConfusionMatrix(confusion_matrix);
    }
  }

  public KMeans2() {
    description = DOC_GET;
  }

  @Override protected void exec() {
    Vec[] vecs = selectVecs(source);
    final Key sourceKey = Key.make(input("source"));
    KMeansModel2 model = new KMeansModel2(destination_key, sourceKey, source);
    model.initialization = initialization;
    model.max_iter = max_iter;
    model.seed = seed;
    model.normalized = normalize;

    double[] subs = null, muls = null;
    if( normalize ) {
      subs = new double[vecs.length];
      muls = new double[vecs.length];

      for( int i = 0; i < vecs.length; i++ ) {
        subs[i] = (float) vecs[i].mean();
        double sigma = vecs[i].sigma();
        // TODO unify handling of constant columns
        muls[i] = sigma > 1e-6 ? 1 / sigma : 1;
      }
    }

    // -1 to be different from all chunk indexes (C.f. Sampler)
    Random rand = Utils.getRNG(seed - 1);
    double[][] clusters;
    if( initialization == Initialization.None ) {
      // Initialize all clusters to random rows
      clusters = new double[k][vecs.length];
      for( int i = 0; i < clusters.length; i++ )
        randomRow(vecs, rand, clusters[i], subs, muls);
    } else {
      // Initialize first cluster to random row
      clusters = new double[1][];
      clusters[0] = new double[cols.length - 1];
      randomRow(vecs, rand, clusters[0], subs, muls);

      while( model.iterations < 5 ) {
        // Sum squares distances to clusters
        SumSqr sqr = new SumSqr();
        sqr._clusters = clusters;
        sqr._subs = subs;
        sqr._muls = muls;
        sqr.doAll(vecs);

        // Sample with probability inverse to square distance
        Sampler sampler = new Sampler();
        sampler._clusters = clusters;
        sampler._sqr = sqr._sqr;
        sampler._probability = k * 3; // Over-sampling
        sampler._seed = seed;
        sampler._subs = subs;
        sampler._muls = muls;
        sampler.doAll(vecs);
        clusters = Utils.append(clusters, sampler._sampled);

        if( cancelled() ) {
          remove();
          return;
        }
        model.clusters = normalize ? denormalize(clusters, vecs) : clusters;
        model.error = sqr._sqr;
        model.iterations++;
        UKV.put(destination_key, model);
      }

      clusters = recluster(clusters, k, rand, initialization);
    }

    for( ;; ) {
      Lloyds task = new Lloyds();
      task._clusters = clusters;
      task._subs = subs;
      task._muls = muls;
      task.doAll(vecs);
      clusters = task._clusters;

      for( int cluster = 0; cluster < clusters.length; cluster++ ) {
        if( task._counts[cluster] > 0 ) {
          for( int column = 0; column < cols.length - 1; column++ ) {
            double value = task._sums[cluster][column] / task._counts[cluster];
            clusters[cluster][column] = value;
          }
        }
      }
      model.clusters = normalize ? denormalize(clusters, vecs) : clusters;
      model.error = task._sqr;
      model.iterations++;
      UKV.put(destination_key, model);
      if( model.iterations >= max_iter )
        break;
      if( cancelled() )
        break;
    }

    remove();
  }

  public static class SumSqr extends MRTask2<SumSqr> {
    // IN
    double[] _subs, _muls; // Normalization
    double[][] _clusters;

    // OUT
    double _sqr;

    @Override public void map(Chunk[] cs) {
      double[] values = new double[cs.length];
      ClusterDist cd = new ClusterDist();
      for( int row = 0; row < cs[0]._len; row++ ) {
        datad(values, cs, row, _subs, _muls);
        _sqr += minSqr(_clusters, values, cd);
      }
      _subs = _muls = null;
      _clusters = null;
    }

    @Override public void reduce(SumSqr other) {
      _sqr += other._sqr;
    }
  }

  public static class Sampler extends MRTask2<Sampler> {
    // IN
    double[][] _clusters;
    double _sqr;           // Min-square-error
    double _probability;   // Odds to select this point
    long _seed;
    double[] _subs, _muls; // Normalization

    // OUT
    double[][] _sampled;   // New clusters

    @Override public void map(Chunk[] cs) {
      double[] values = new double[cs.length];
      ArrayList<double[]> list = new ArrayList<double[]>();
      Random rand = Utils.getRNG(_seed + cs[0]._start);
      ClusterDist cd = new ClusterDist();

      for( int row = 0; row < cs[0]._len; row++ ) {
        datad(values, cs, row, _subs, _muls);
        double sqr = minSqr(_clusters, values, cd);
        if( _probability * sqr > rand.nextDouble() * _sqr )
          list.add(values.clone());
      }

      _sampled = new double[list.size()][];
      list.toArray(_sampled);
      _clusters = null;
      _subs = _muls = null;
    }

    @Override public void reduce(Sampler other) {
      _sampled = Utils.append(_sampled, other._sampled);
    }
  }

  public static class Lloyds extends MRTask2<Lloyds> {
    // IN
    double[][] _clusters;
    double[] _subs, _muls; // Normalization

    // OUT
    double[][] _sums;      // Sum of (normalized) features in each cluster
    int[] _counts;         // Count of rows in cluster
    double _sqr;           // Total sqr distance

    @Override public void map(Chunk[] cs) {
      double[] values = new double[cs.length];
      _sums = new double[_clusters.length][cs.length];
      _counts = new int[_clusters.length];
      ClusterDist cd = new ClusterDist();

      // Find closest cluster for each row
      for( int row = 0; row < cs[0]._len; row++ ) {
        datad(values, cs, row, _subs, _muls);
        closest(_clusters, values, cd);
        int cluster = cd._cluster;
        _sqr += cd._dist;
        if( cluster == -1 )
          continue; // Ignore broken row

        // Add values and increment counter for chosen cluster
        Utils.add(_sums[cluster], values);
        _counts[cluster]++;
      }
      _clusters = null;
      _subs = _muls = null;
    }

    @Override public void reduce(Lloyds other) {
      Utils.add(_sums, other._sums);
      Utils.add(_counts, other._counts);
      _sqr += other._sqr;
    }
  }

  private static final class ClusterDist {
    int _cluster;
    double _dist;
  }

  private static ClusterDist closest(double[][] clusters, double[] point, ClusterDist cd) {
    return closest(clusters, point, cd, clusters.length);
  }

  private static double minSqr(double[][] clusters, double[] point, ClusterDist cd) {
    return closest(clusters, point, cd, clusters.length)._dist;
  }

  private static double minSqr(double[][] clusters, double[] point, ClusterDist cd, int count) {
    return closest(clusters, point, cd, count)._dist;
  }

  /** Return both nearest of N cluster/centroids, and the square-distance. */
  private static ClusterDist closest(double[][] clusters, double[] point, ClusterDist cd, int count) {
    int min = -1;
    double minSqr = Double.MAX_VALUE;
    for( int cluster = 0; cluster < count; cluster++ ) {
      double sqr = 0;           // Sum of dimensional distances
      int pts = point.length;   // Count of valid points
      for( int column = 0; column < point.length; column++ ) {
        double d = point[column];
        if( Double.isNaN(d) ) { // Bad data?
          pts--;                // Do not count
        } else {
          double delta = d - clusters[cluster][column];
          sqr += delta * delta;
        }
      }
      // Scale distance by ratio of valid dimensions to all dimensions - since
      // we did not add any error term for the missing point, the sum of errors
      // is small - ratio up "as if" the missing error term is equal to the
      // average of other error terms.  Same math another way:
      //   double avg_dist = sqr / pts; // average distance per feature/column/dimension
      //   sqr = sqr * point.length;    // Total dist is average*#dimensions
      if( pts < point.length )
        sqr *= point.length / pts;
      if( sqr < minSqr ) {
        min = cluster;
        minSqr = sqr;
      }
    }
    cd._cluster = min;          // Record nearest cluster
    cd._dist = minSqr;          // Record square-distance
    return cd;                  // Return for flow-coding
  }

  // KMeans++ re-clustering
  public static double[][] recluster(double[][] points, int k, Random rand, Initialization init) {
    double[][] res = new double[k][];
    res[0] = points[0];
    int count = 1;
    ClusterDist cd = new ClusterDist();
    switch( init ) {
      case PlusPlus: { // k-means++
        while( count < res.length ) {
          double sum = 0;
          for( int i = 0; i < points.length; i++ )
            sum += minSqr(res, points[i], cd, count);

          for( int i = 0; i < points.length; i++ ) {
            if( minSqr(res, points[i], cd, count) >= rand.nextDouble() * sum ) {
              res[count++] = points[i];
              break;
            }
          }
        }
        break;
      }
      case Furthest: { // Takes cluster further from any already chosen ones
        while( count < res.length ) {
          double max = 0;
          int index = 0;
          for( int i = 0; i < points.length; i++ ) {
            double sqr = minSqr(res, points[i], cd, count);
            if( sqr > max ) {
              max = sqr;
              index = i;
            }
          }
          res[count++] = points[index];
        }
        break;
      }
      default:
        throw new IllegalStateException();
    }
    return res;
  }

  private void randomRow(Vec[] vecs, Random rand, double[] cluster, double[] subs, double[] muls) {
    long row = Math.max(0, (long) (rand.nextDouble() * vecs[0].length()) - 1);
    datad(cluster, vecs, row, subs, muls);
  }

  private static double[][] denormalize(double[][] clusters, Vec[] vecs) {
    double[][] value = new double[clusters.length][clusters[0].length];
    for( int row = 0; row < value.length; row++ ) {
      for( int col = 0; col < clusters[row].length; col++ ) {
        double d = clusters[row][col];
        d *= vecs[col].sigma();
        d += vecs[col].mean();
        value[row][col] = d;
      }
    }
    return value;
  }

  /**
   * Return a row of normalized values. If missing, use the mean (which we know exists because we
   * filtered out columns with no mean).
   */
  private void datad(double[] values, Vec[] vecs, long row, double[] subs, double[] muls) {
    for( int i = 0; i < vecs.length - 1; i++ ) {
      double d = vecs[i].at(row);
      if( subs != null ) {
        d -= subs[i];
        d *= muls[i];
      }
      values[i] = d;
    }
  }

  private static void datad(double[] values, Chunk[] chks, int row, double[] subs, double[] muls) {
    for( int i = 0; i < chks.length - 1; i++ ) {
      double d = chks[i].at0(row);
      if( subs != null ) {
        d -= subs[i];
        d *= muls[i];
      }
      values[i] = d;
    }
  }
}
