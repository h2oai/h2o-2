package hex;

import java.util.*;

import water.*;
import water.H2O.H2OCountedCompleter;
import water.util.Utils;

/**
 * Scalable K-Means++ (KMeans||)<br>
 * http://theory.stanford.edu/~sergei/papers/vldb12-kmpar.pdf<br>
 * http://www.youtube.com/watch?v=cigXAxV3XcY
 */
public class KMeans extends Job {
  public static final String KEY_PREFIX = "__KMeansModel_";

  public static final Key makeKey() {
    return Key.make(KEY_PREFIX + Key.make());
  }

  private KMeans(Key dest, int k, int... cols) {
    super("KMeans K: " + k + ", Cols: " + cols.length, dest);
  }

  public static KMeans start(Key dest, final ValueArray va, final int k, final double epsilon, final int maxIter,
      long randSeed, boolean normalize, int... cols) {
    final KMeans job = new KMeans(dest, k, cols);

    // k-means is an unsupervised learning algorithm and does not require a
    // response-column to train. This also means the clusters are not classes
    // (although, if a class/response is associated with each
    // row we could count the number of each class in each cluster).
    int cols2[] = Arrays.copyOf(cols, cols.length + 1);
    cols2[cols.length] = -1;  // No response column
    final KMeansModel res = new KMeansModel(job.dest(), cols2, va._key);
    res._normalized = normalize;
    res._randSeed = randSeed;
    res._maxIter = maxIter;
    UKV.put(job.dest(), res);
    // Updated column mapping selection after removing various junk columns
    final int[] filteredCols = res.columnMapping(va.colNames());

    H2OCountedCompleter task = new H2OCountedCompleter() {
      @Override public void compute2() {
        job.run(res, va, k, epsilon, filteredCols);
        tryComplete();
      }
    };
    H2O.submitTask(job.start(task));
    return job;
  }

  private void run(KMeansModel res, ValueArray va, int k, double epsilon, int[] cols) {
    // -1 to be different from all chunk indexes (C.f. Sampler)
    Random rand = Utils.getRNG(res._randSeed - 1);
    // Initialize first cluster to random row
    double[][] clusters = new double[1][];
    clusters[0] = new double[cols.length - 1];
    long row = Math.max(0, (long) (rand.nextDouble() * va._numrows) - 1);
    AutoBuffer bits = va.getChunk(va.chknum(row));
    datad(va, bits, va.rowInChunk(va.chknum(row), row), cols, res._normalized, clusters[0]);

    while( res._iteration < 5 ) {
      // Sum squares distances to clusters
      Sqr sqr = new Sqr();
      sqr._arykey = va._key;
      sqr._cols = cols;
      sqr._clusters = clusters;
      sqr._normalize = res._normalized;
      sqr.invoke(va._key);

      // Sample with probability inverse to square distance
      Sampler sampler = new Sampler();
      sampler._arykey = va._key;
      sampler._cols = cols;
      sampler._clusters = clusters;
      sampler._normalize = res._normalized;
      sampler._sqr = sqr._sqr;
      sampler._probability = k * 3; // Over-sampling
      sampler._seed = res._randSeed;
      sampler.invoke(va._key);
      clusters = DRemoteTask.merge(clusters, sampler._clust2);

      if( cancelled() ) {
        remove();
        return;
      }

      res._iteration++;
      res._clusters = clusters;
      UKV.put(dest(), res);
    }

    clusters = recluster(clusters, k, rand);
    res._clusters = clusters;

    for( ;; ) {
      boolean moved = false;
      Lloyds task = new Lloyds();
      task._arykey = va._key;
      task._cols = cols;
      task._clusters = clusters;
      task._normalize = res._normalized;
      task.invoke(va._key);

      for( int cluster = 0; cluster < clusters.length; cluster++ ) {
        if( task._counts[cluster] > 0 ) {
          for( int column = 0; column < cols.length - 1; column++ ) {
            double value = task._sums[cluster][column] / task._counts[cluster];
            if( Math.abs(value - clusters[cluster][column]) > epsilon ) {
              moved = true;
            }
            clusters[cluster][column] = value;
          }
        }
      }
      res._error = task._error;
      res._iteration++;
      UKV.put(dest(), res);
      // Iterate until no cluster mean moves more than epsilon,
      if( !moved ) break;
      // reached max iterations,
      if( res._maxIter != 0 && res._iteration >= res._maxIter ) break;
      // or job cancelled
      if( cancelled() ) break;
    }

    remove();
  }

  public static class Sqr extends MRTask {
    Key _arykey;          // IN:  Big Table key
    int[] _cols;          // IN:  Columns-in-use mapping
    double[][] _clusters; // IN:  Centroids/clusters
    boolean _normalize;   // IN:  Normalize

    double _sqr;          // OUT: sum-squared-error

    @Override public void map(Key key) {
      assert key.home();
      ValueArray va = DKV.get(_arykey).get();
      AutoBuffer bits = va.getChunk(key);
      int rows = va.rpc(ValueArray.getChunkIndex(key));
      double[] values = new double[_cols.length - 1];
      ClusterDist cd = new ClusterDist();
      for( int row = 0; row < rows; row++ )
        _sqr += minSqr(_clusters, datad(va, bits, row, _cols, _normalize, values), cd);
      _arykey = null;
      _cols = null;
      _clusters = null;
    }

    @Override public void reduce(DRemoteTask rt) {
      _sqr += ((Sqr) rt)._sqr;
    }
  }

  public static class Sampler extends MRTask {
    Key _arykey;          // IN:  Big Table key
    int[] _cols;          // IN:  Columns-in-use mapping
    double[][] _clusters; // IN:  Centroids/clusters
    double _sqr;          // IN:  min-square-error
    double _probability;  // IN:  odds to select this point
    long _seed;           // IN:  random seed
    boolean _normalize;   // IN:  Normalize

    double[][] _clust2;   // OUT: new clusters

    @Override public void map(Key key) {
      assert key.home();
      ValueArray va = DKV.get(_arykey).get();
      AutoBuffer bits = va.getChunk(key);
      long chunk = ValueArray.getChunkIndex(key);
      int rows = va.rpc(chunk);
      double[] values = new double[_cols.length - 1];
      ArrayList<double[]> list = new ArrayList<double[]>();
      Random rand = Utils.getRNG(_seed + chunk);
      ClusterDist cd = new ClusterDist();

      for( int row = 0; row < rows; row++ ) {
        double sqr = minSqr(_clusters, datad(va, bits, row, _cols, _normalize, values), cd);
        if( _probability * sqr > rand.nextDouble() * _sqr ) list.add(values.clone());
      }

      _clust2 = new double[list.size()][];
      list.toArray(_clust2);
      _arykey = null;
      _cols = null;
      _clusters = null;
    }

    @Override public void reduce(DRemoteTask rt) {
      Sampler task = (Sampler) rt;
      _clust2 = _clust2 == null ? task._clust2 : merge(_clust2, task._clust2);
    }
  }

  public static class Lloyds extends MRTask {
    Key _arykey;          // IN:  Big Table key
    int[] _cols;          // IN:  Columns-in-use mapping
    double[][] _clusters; // IN:  Centroids/clusters
    boolean _normalize;   // IN:  Normalize

    double[][] _sums;     // OUT: Sum of (normalized) features in each cluster
    int[] _counts;        // OUT: Count of rows in cluster
    double _error;        // OUT: Total sqr distance

    @Override public void map(Key key) {
      assert key.home();
      ValueArray va = DKV.get(_arykey).get();
      AutoBuffer bits = va.getChunk(key);
      int rows = va.rpc(ValueArray.getChunkIndex(key));
      double[] values = new double[_cols.length - 1];

      // Create result arrays
      _sums = new double[_clusters.length][_cols.length - 1];
      _counts = new int[_clusters.length];
      ClusterDist cd = new ClusterDist();

      // Find closest cluster for each row
      for( int row = 0; row < rows; row++ ) {
        datad(va, bits, row, _cols, _normalize, values);
        closest(_clusters, values, cd);
        int cluster = cd._cluster;
        _error += cd._dist;
        if( cluster == -1 ) continue; // Ignore broken row

        // Add values and increment counter for chosen cluster
        Utils.add(_sums[cluster],values);
        _counts[cluster]++;
      }
      _arykey = null;
      _cols = null;
      _clusters = null;
    }

    @Override public void reduce(DRemoteTask rt) {
      Lloyds task = (Lloyds) rt;
      if( _sums == null ) {
        _sums = task._sums;
        _counts = task._counts;
        _error = task._error;
      } else {
        Utils.add(_sums  ,task._sums  );
        Utils.add(_counts,task._counts);
        _error += task._error;
      }
    }
  }

  // A dumb-ass class for doing multi-value returns
  static final class ClusterDist {
    int _cluster;
    double _dist;
  }

  public static ClusterDist closest(double[][] clusters, double[] point, ClusterDist cd) {
    return closest(clusters, point, cd, clusters.length);
  }

  public static double minSqr(double[][] clusters, double[] point, ClusterDist cd) {
    return closest(clusters, point, cd, clusters.length)._dist;
  }

  public static double minSqr(double[][] clusters, double[] point, ClusterDist cd, int N) {
    return closest(clusters, point, cd, N)._dist;
  }

  // Return both nearest of N cluster/centroids, and the square-distance.
  public static ClusterDist closest(double[][] clusters, double[] point, ClusterDist cd, int N) {
    int min = -1;
    double minSqr = Double.MAX_VALUE;
    for( int cluster = 0; cluster < N; cluster++ ) {
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
      if( pts < point.length ) sqr *= point.length / pts;
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
  public static double[][] recluster(double[][] points, int k, Random rand) {
    double[][] res = new double[k][];
    res[0] = points[0];
    int count = 1;
    ClusterDist cd = new ClusterDist();

    while( count < res.length ) {
//      // Original k-means++, doesn't seem to help in many cases
//      double sum = 0;
//      for( int i = 0; i < points.length; i++ )
//        sum += minSqr(res, points[i], cd, count);
//
//      for( int i = 0; i < points.length; i++ ) {
//        if( minSqr(res, points[i], cd, count) >= rand.nextDouble() * sum ) {
//          res[count++] = points[i];
//          break;
//        }
//      }
      // Takes cluster further from any already chosen ones
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

    return res;
  }

  // Return a row of normalized values.  If missing, use the mean (which we
  // know exists because we filtered out columns with no mean).
  public static double[] datad(ValueArray va, AutoBuffer bits, int row, int[] cols, boolean normalize, double[] res) {
    for( int c = 0; c < cols.length - 1; c++ ) {
      ValueArray.Column C = va._cols[cols[c]];
      // Use the mean if missing data, then center & normalize
      double d = (va.isNA(bits, row, C) ? C._mean : va.datad(bits, row, C));
      if( normalize ) {
        d -= C._mean;
        d = (C._sigma == 0.0 || Double.isNaN(C._sigma)) ? d : d / C._sigma;
      }
      res[c] = d;
    }
    return res;
  }
}
