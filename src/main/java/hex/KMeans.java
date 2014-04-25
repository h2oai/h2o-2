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

  public enum Initialization {
    None, PlusPlus, Furthest
  };

  public static final Key makeKey() {
    return Key.make(KEY_PREFIX + Key.make());
  }

  public static KMeans start(Key dest, final ValueArray va, final int k, final Initialization init, //
      final int maxIter, long randSeed, boolean normalize, int... cols) {

    // k-means is an unsupervised learning algorithm and does not require a
    // response-column to train. This also means the clusters are not classes
    // (although, if a class/response is associated with each
    // row we could count the number of each class in each cluster).
    if( cols == null || cols.length == 0 ) {
      cols = new int[va._cols.length - 1];
      for( int i = 0; i < cols.length; i++ )
        cols[i] = i;
    }
    int cols2[] = Arrays.copyOf(cols, cols.length + 1);
    cols2[cols.length] = -1;  // No response column

    final KMeans job = new KMeans();
    job.destination_key = dest;
    final KMeansModel res = new KMeansModel(job.dest(), cols2, va._key);
    res._normalized = normalize;
    res._randSeed = randSeed;
    res._maxIter = maxIter;
    res._initialization = init;
    res.delete_and_lock(job.self());
    va.read_lock(job.self());
    // Updated column mapping selection after removing various junk columns
    final int[] filteredCols = res.columnMapping(va.colNames());

    H2OCountedCompleter task = new H2OCountedCompleter() {
      @Override public void compute2() {
        job.run(res, va, k, init, filteredCols);
        tryComplete();
      }
    };
    job.start(task);
    H2O.submitTask(task);
    return job;
  }

  private void randomRow(ValueArray va, int[] cols, Random rand, boolean normalize, double[] cluster) {
    long row = Math.max(0, (long) (rand.nextDouble() * va._numrows) - 1);
    AutoBuffer bits = va.getChunk(va.chknum(row));
    datad(va, bits, va.rowInChunk(va.chknum(row), row), cols, normalize, cluster);
  }

  private void run(KMeansModel res, ValueArray va, int k, Initialization init, int[] cols) {
    // -1 to be different from all chunk indexes (C.f. Sampler)
    Random rand = Utils.getRNG(res._randSeed - 1);
    double[][] clusters;

    if( init == Initialization.None ) {
      // Initialize all clusters to random rows
      clusters = new double[k][];
      for( int i = 0; i < clusters.length; i++ ) {
        clusters[i] = new double[cols.length - 1];
        randomRow(va, cols, rand, res._normalized, clusters[i]);
      }
    } else {
      // Initialize first cluster to random row
      clusters = new double[1][];
      clusters[0] = new double[cols.length - 1];
      randomRow(va, cols, rand, res._normalized, clusters[0]);

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
        clusters = Utils.append(clusters, sampler._clust2);

        if( !isRunning(self()) ) {
          remove();
          return;
        }

        res._iteration++;
        res._clusters = clusters;
        res.update(self());
      }

      clusters = recluster(clusters, k, rand, init);
    }
    res._clusters = clusters;

    for( ;; ) {
      Lloyds task = new Lloyds();
      task._arykey = va._key;
      task._cols = cols;
      task._clusters = clusters;
      task._normalize = res._normalized;
      task.invoke(va._key);

      double[] betwnSqrs = new double[clusters.length];
      double[] gm = new double[cols.length - 1];
      int[] validMeans = new int[cols.length - 1];

      for(int cluster = 0; cluster < clusters.length; cluster++) {
        if(task._counts[cluster] > 0) {
          for(int column = 0; column < cols.length - 1; column++) {
            double value = task._sums[cluster][column] / task._counts[cluster];
            clusters[cluster][column] = value;
            gm[column] += value;
            validMeans[column]++;
          }
        }
      }

      for(int column = 0; column < cols.length - 1; column++) {
        if(validMeans[column] != 0)
          gm[column] /= validMeans[column];
      }

      for(int cluster = 0; cluster < clusters.length; cluster++) {
        for(int column = 0; column < cols.length - 1; column++) {
           double mean_delta = clusters[cluster][column] - gm[column];
           betwnSqrs[cluster] += task._counts[cluster] * mean_delta * mean_delta;
        }
      }

      double between_cluster_SS = 0.0;
      for(int clu = 0; clu < betwnSqrs.length; clu++)
          between_cluster_SS += betwnSqrs[clu];

      res._between_cluster_SS = between_cluster_SS;
      res._error = task._error;
      res._total_SS = res._error + res._between_cluster_SS;
      res._iteration++;
      res.update(self());
      if( res._iteration >= res._maxIter )
        break;
      if( !isRunning(self()) )
        break;
    }
    res.unlock(self());
    va.unlock(self());
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
        if( _probability * sqr > rand.nextDouble() * _sqr )
          list.add(values.clone());
      }

      _clust2 = new double[list.size()][];
      list.toArray(_clust2);
      _arykey = null;
      _cols = null;
      _clusters = null;
    }

    @Override public void reduce(DRemoteTask rt) {
      Sampler task = (Sampler) rt;
      _clust2 = _clust2 == null ? task._clust2 : Utils.append(_clust2, task._clust2);
    }
  }

  public static class Lloyds extends MRTask {
    Key _arykey;          // IN:  Big Table key
    int[] _cols;          // IN:  Columns-in-use mapping
    double[][] _clusters; // IN:  Centroids/clusters
    boolean _normalize;   // IN:  Normalize

    double[][] _sums;     // OUT: Sum of (normalized) features in each cluster
    long[] _counts;       // OUT: Count of rows in cluster
    double _error;        // OUT: Total sqr distance

    @Override public void map(Key key) {
      assert key.home();
      ValueArray va = DKV.get(_arykey).get();
      AutoBuffer bits = va.getChunk(key);
      int rows = va.rpc(ValueArray.getChunkIndex(key));
      double[] values = new double[_cols.length - 1];

      // Create result arrays
      _sums = new double[_clusters.length][_cols.length - 1];
      _counts = new long[_clusters.length];
      ClusterDist cd = new ClusterDist();

      // Find closest cluster for each row
      for( int row = 0; row < rows; row++ ) {
        datad(va, bits, row, _cols, _normalize, values);
        closest(_clusters, values, cd);
        int cluster = cd._cluster;
        _error += cd._dist;
        if( cluster == -1 )
          continue; // Ignore broken row

        // Add values and increment counter for chosen cluster
        Utils.add(_sums[cluster], values);
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
        Utils.add(_sums, task._sums);
        Utils.add(_counts, task._counts);
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
