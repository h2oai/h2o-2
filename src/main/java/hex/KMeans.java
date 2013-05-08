package hex;

import java.util.*;

import jsr166y.CountedCompleter;
import water.*;
import water.H2O.H2OCountedCompleter;
import water.Job.ChunkProgressJob;
import water.Job.Progress;
import water.ValueArray.Column;
import water.api.Constants;
import water.util.Log;
import water.util.Log.Tag.Sys;

import com.google.gson.*;

/**
 * Scalable K-Means++ (KMeans||)<br>
 * http://theory.stanford.edu/~sergei/papers/vldb12-kmpar.pdf<br>
 * http://www.youtube.com/watch?v=cigXAxV3XcY
 */
public abstract class KMeans {
  private static final boolean NORMALIZE = false;
  public static long RAND_SEED = 0; // 0x1234567L might be a better seed
  public static final String KEY_PREFIX = "__KMeansModel_";

  public static final Key makeKey() {
    return Key.make(KEY_PREFIX + Key.make());
  }

  public static class KMeansModel extends Model implements Progress {
    public static final String NAME = KMeansModel.class.getSimpleName();
    public double[][] _clusters; // The cluster centers, normalized according to _va
    public int _iteration;

    // Empty constructor for deserialization
    public KMeansModel() {}

    private KMeansModel(Key selfKey, int cols[], Key dataKey) {
      super(selfKey, cols, dataKey);
    }

    // Progress reporting for the job/progress page
    @Override public float progress() {
      return Math.min(1f, _iteration / (float) 20);
    }

    // Accept only columns with a defined mean. Used during the Model.<init> call.
    @Override public boolean columnFilter(ValueArray.Column C) {
      return !Double.isNaN(C._mean);
    }

    public JsonObject toJson() {
      JsonObject res = new JsonObject();
      JsonArray ary = new JsonArray();
      for( double[] dd : clusters() ) {
        JsonArray ary2 = new JsonArray();
        for( double d : dd )
          ary2.add(new JsonPrimitive(d));
        ary.add(ary2);
      }
      res.add(Constants.CLUSTERS, ary);
      return res;
    }

    // Return the clusters, denormalized
    public double[][] clusters() {
      double dd[][] = new double[_clusters.length][_clusters[0].length];
      for( int j = 0; j < dd.length; j++ ) {
        double ds[] = _clusters[j];
        for( int i = 0; i < ds.length; i++ ) {
          ValueArray.Column C = _va._cols[i];
          double d = ds[i];
          if( NORMALIZE ) {
            if( C._sigma != 0.0 && !Double.isNaN(C._sigma) ) d *= C._sigma;
            d += C._mean;
          }
          dd[j][i] = d;
        }
      }
      return dd;
    }

    /**
     * Single row scoring, on properly ordered data. Will return NaN if any data element contains a
     * NaN. Returns the cluster-number, which is mostly an internal value. Last data element refers
     * to the response variable, which is not used for kmeans.
     */
    protected double score0(double[] data) {
      for( int i = 0; i < data.length - 1; i++ ) { // Normalize the data before scoring
        ValueArray.Column C = _va._cols[i];
        double d = data[i];
        if( NORMALIZE ) {
          d -= C._mean;
          if( C._sigma != 0.0 && !Double.isNaN(C._sigma) ) d /= C._sigma;
        }
        data[i] = d;
      }
      data[data.length - 1] = Double.NaN; // Response variable column not used
      return closest(_clusters, data, new ClusterDist())._cluster;
    }

    /** Single row scoring, on a compatible ValueArray (when pushed throw the mapping) */
    protected double score0(ValueArray data, int row, int[] mapping) {
      throw H2O.unimpl();
    }

    /** Bulk scoring API, on a compatible ValueArray (when pushed throw the mapping) */
    protected double score0(ValueArray data, AutoBuffer ab, int row_in_chunk, int[] mapping) {
      throw H2O.unimpl();
    }

    public final void print() {
      StringBuilder sb = new StringBuilder();
      sb.append("I: ").append(_iteration).append("[");
      double[][] c = clusters();
      for( int i = 0; i < c.length; i++ )
        sb.append(c[i][2]).append(",");
      sb.append("]");
      Log.debug(Sys.KMEAN, sb);
    }

  }

  // Compute the cluster members, and the mean-dist to each cluster
  public static class KMeansScore extends MRTask {
    Key _arykey;                // IN:  The dataset
    int _cols[];                // IN:  Cols->Features mapping
    double _clusters[][];       // IN:  The (normalized) Clusters
    public long _rows[];        // OUT: Count of rows per-cluster
    public double _dist[];      // OUT: Normalized sqr-error per-cluster

    public static KMeansScore score(KMeansModel model, ValueArray ary) {
      KMeansScore kms = new KMeansScore();
      kms._arykey = ary._key;
      kms._cols = model.columnMapping(ary.colNames());
      kms._clusters = model._clusters; // Normalized clusters
      kms.invoke(ary._key);            // Do It
      return kms;
    }

    @Override public void map(Key key) {
      _rows = new long[_clusters.length];
      _dist = new double[_clusters.length];
      assert key.home();
      ValueArray va = DKV.get(_arykey).get();
      AutoBuffer bits = va.getChunk(key);
      int rows = va.rpc(ValueArray.getChunkIndex(key));
      double[] values = new double[_cols.length - 1];
      ClusterDist cd = new ClusterDist();
      for( int row = 0; row < rows; row++ ) {
        datad(va, bits, row, _cols, values);
        closest(_clusters, values, cd);
        _rows[cd._cluster]++;
        _dist[cd._cluster] += cd._dist;
      }
      _arykey = null;
      _cols = null;
      _clusters = null;
    }

    @Override public void reduce(DRemoteTask rt) {
      KMeansScore kms = (KMeansScore) rt;
      if( _rows == null ) {
        _rows = kms._rows;
        _dist = kms._dist;
      } else {
        for( int i = 0; i < _rows.length; i++ ) {
          _rows[i] += kms._rows[i];
          _dist[i] += kms._dist[i];
        }
      }
    }

    public JsonObject toJson() {
      JsonObject res = new JsonObject();
      JsonArray rows = new JsonArray();
      for( int i = 0; i < _rows.length; ++i )
        rows.add(new JsonPrimitive(_rows[i]));
      JsonArray dist = new JsonArray();
      for( int i = 0; i < _dist.length; ++i )
        dist.add(new JsonPrimitive(_dist[i]));
      res.add("rows_per_cluster", rows);
      res.add("sqr_error_per_cluster", dist);
      return res;
    }
  }

  // Classify a dataset using a model and generates a list of classes
  public static class KMeansApply extends MRTask {
    ChunkProgressJob _job;  // IN
    Key _arykey;            // IN:  The dataset
    int _cols[];            // IN:  Cols->Features mapping
    double _clusters[][];   // IN:  The (normalized) Clusters

    static final int ROW_SIZE = 4;

    public static Job run(final Key dest, final KMeansModel model, final ValueArray ary) {
      String desc = "KMeans apply model: " + model._selfKey + " to " + ary._key;
      final ChunkProgressJob job = new ChunkProgressJob(desc, dest, ary.chunks());
      final H2OCountedCompleter fjtask = new H2OCountedCompleter() {
        @Override public void compute2() {
          KMeansApply kms = new KMeansApply();
          kms._job = job;
          kms._arykey = ary._key;
          kms._cols = model.columnMapping(ary.colNames());
          kms._clusters = model._clusters;
          kms.invoke(ary._key);

          Column c = new Column();
          c._name = Constants.RESPONSE;
          c._size = ROW_SIZE;
          c._scale = 1;
          c._min = Double.NaN;
          c._max = Double.NaN;
          c._mean = Double.NaN;
          c._sigma = Double.NaN;
          c._domain = null;
          c._n = ary.numRows();
          ValueArray res = new ValueArray(dest, ary.numRows(), c._size, new Column[] { c });
          DKV.put(dest, res);
          DKV.write_barrier();
          job.remove();
          tryComplete();
        }

        @Override public boolean onExceptionalCompletion(Throwable ex, CountedCompleter caller) {
          job.onException(ex);
          return super.onExceptionalCompletion(ex, caller);
        }
      };
      H2O.submitTask(job.start(fjtask));
      return job;
    }

    /**
     * Creates a new ValueArray with classes. New ValueArray is not aligned with source one
     * unfortunately so have to send results to each chunk owner using Atomic.
     */
    @Override public void map(Key key) {
      assert key.home();
      if( !_job.cancelled() ) {
        ValueArray va = DKV.get(_arykey).get();
        AutoBuffer bits = va.getChunk(key);
        long startRow = va.startRow(ValueArray.getChunkIndex(key));
        int rows = va.rpc(ValueArray.getChunkIndex(key));
        int rpc = (int) (ValueArray.CHUNK_SZ / ROW_SIZE);
        long chunk = ValueArray.chknum(startRow, va.numRows(), ROW_SIZE);
        long updatedChk = chunk;
        long updatedRow = startRow;
        double[] values = new double[_cols.length - 1];
        ClusterDist cd = new ClusterDist();
        int[] clusters = new int[rows];
        int count = 0;
        for( int row = 0; row < rows; row++ ) {
          datad(va, bits, row, _cols, values);
          closest(_clusters, values, cd);
          chunk = ValueArray.chknum(startRow + row, va.numRows(), ROW_SIZE);
          if( chunk != updatedChk ) {
            updateClusters(clusters, count, chunk, va.numRows(), rpc, updatedRow);
            updatedChk = chunk;
            updatedRow = startRow + row;
            count = 0;
          }
          clusters[count++] = cd._cluster;
        }
        if( count > 0 ) updateClusters(clusters, count, chunk, va.numRows(), rpc, updatedRow);
        _job.updateProgress(1);
      }
      _job = null;
      _arykey = null;
      _cols = null;
      _clusters = null;
    }

    @Override public void reduce(DRemoteTask rt) {}

    private void updateClusters(int[] clusters, int count, long chunk, long numrows, int rpc, long updatedRow) {
      final int offset = (int) (updatedRow - (rpc * chunk));
      final Key chunkKey = ValueArray.getChunkKey(chunk, _job.dest());
      final int[] message;
      if( count == clusters.length ) message = clusters;
      else {
        message = new int[count];
        System.arraycopy(clusters, 0, message, 0, message.length);
      }
      final int rows = ValueArray.rpc(chunk, rpc, numrows);
      new Atomic() {
        @Override public Value atomic(Value val) {
          assert val == null || val._key.equals(chunkKey);
          AutoBuffer b = new AutoBuffer(rows * ROW_SIZE);
          if( val != null ) b._bb.put(val.memOrLoad());
          for( int i = 0; i < message.length; i++ )
            b.put4((offset + i) * 4, message[i]);
          b.position(b.limit());
          return new Value(chunkKey, b.buf());
        }
      }.invoke(chunkKey);
    }
  }

  // Return a row of normalized values.  If missing, use the mean (which we
  // know exists because we filtered out columns with no mean).
  private static double[] datad(ValueArray va, AutoBuffer bits, int row, int[] cols, double[] res) {
    for( int c = 0; c < cols.length - 1; c++ ) {
      ValueArray.Column C = va._cols[c];
      // Use the mean if missing data, then center & normalize
      double d = (va.isNA(bits, row, C) ? C._mean : va.datad(bits, row, C));
      if( NORMALIZE ) {
        d -= C._mean;
        d = (C._sigma == 0.0 || Double.isNaN(C._sigma)) ? d : d / C._sigma;
      }
      res[c] = d;
    }
    return res;
  }

  public static void run(Key dest, ValueArray va, int k, double epsilon, int... cols) {
    Job job = startJob(dest, va, k, epsilon, cols);
    run(job, va, k, epsilon, cols);
  }

  public static Job startJob(Key dest, ValueArray va, int k, double epsilon, int... cols) {
    Job job = new Job("KMeans K: " + k + ", Cols: " + cols.length, dest);
    job.start(null);
    return job;
  }

  public static void run(Job job, ValueArray va, int k, double epsilon, int... cols) {
    // Unlike other models, k-means is a discovery-only procedure and does
    // not require a response-column to train.  This also means the clusters
    // are not classes (although, if a class/response is associated with each
    // row we could count the number of each class in each cluster).
    int cols2[] = Arrays.copyOf(cols, cols.length + 1);
    cols2[cols.length] = -1;  // No response column
    KMeansModel res = new KMeansModel(job.dest(), cols2, va._key);
    // Updated column mapping selection after removing various junk columns
    cols = res.columnMapping(va.colNames());

    // Initialize first cluster to first row
    double[][] clusters = new double[1][];
    clusters[0] = new double[cols.length - 1];
    AutoBuffer bits = va.getChunk(0);
    datad(va, bits, 0, cols, clusters[0]);

    while( res._iteration < 5 ) {
      // Sum squares distances to clusters
      Sqr sqr = new Sqr();
      sqr._arykey = va._key;
      sqr._cols = cols;
      sqr._clusters = clusters;
      sqr.invoke(va._key);

      // Sample with probability inverse to square distance
      Sampler sampler = new Sampler();
      sampler._arykey = va._key;
      sampler._cols = cols;
      sampler._clusters = clusters;
      sampler._sqr = sqr._sqr;
      sampler._probability = k * 3; // Over-sampling
      sampler.invoke(va._key);
      clusters = DRemoteTask.merge(clusters, sampler._clust2);

      if( job.cancelled() ) {
        job.remove();
        return;
      }

      res._iteration++;
      res._clusters = clusters;
      UKV.put(job.dest(), res);
    }

    clusters = recluster(clusters, k);
    res._clusters = clusters;

    // Iterate until no cluster mean moves more than epsilon
    boolean moved = true;
    while( moved ) {
      moved = false;
      Lloyds task = new Lloyds();
      task._arykey = va._key;
      task._cols = cols;
      task._clusters = clusters;
      task.invoke(va._key);

      for( int cluster = 0; cluster < clusters.length; cluster++ ) {
        for( int column = 0; column < cols.length - 1; column++ ) {
          double value = task._sums[cluster][column] / task._counts[cluster];
          if( Math.abs(value - clusters[cluster][column]) > epsilon ) {
            moved = true;
          }
          clusters[cluster][column] = value;
        }
      }

      res._iteration++;
      UKV.put(job.dest(), res);
      if( job.cancelled() ) break;
    }

    job.remove();
  }

  public static class Sqr extends MRTask {
    Key _arykey;         // IN:  Big Table key
    int[] _cols;           // IN:  Columns-in-use mapping
    double[][] _clusters;       // IN:  Centroids/clusters
    double _sqr;            // OUT: sum-squared-error

    @Override public void map(Key key) {
      assert key.home();
      ValueArray va = DKV.get(_arykey).get();
      AutoBuffer bits = va.getChunk(key);
      int rows = va.rpc(ValueArray.getChunkIndex(key));
      double[] values = new double[_cols.length - 1];
      ClusterDist cd = new ClusterDist();
      for( int row = 0; row < rows; row++ )
        _sqr += minSqr(_clusters, datad(va, bits, row, _cols, values), cd);
      _arykey = null;
      _cols = null;
      _clusters = null;
    }

    @Override public void reduce(DRemoteTask rt) {
      _sqr += ((Sqr) rt)._sqr;
    }
  }

  public static class Sampler extends MRTask {
    Key _arykey;         // IN:  Big Table key
    int[] _cols;           // IN:  Columns-in-use mapping
    double[][] _clusters;       // IN:  Centroids/clusters
    double _sqr;            // IN:  min-square-error
    double _probability;    // IN:  odds to select this point

    // Reduced
    double[][] _clust2;         // OUT: new clusters

    @Override public void map(Key key) {
      assert key.home();
      ValueArray va = DKV.get(_arykey).get();
      AutoBuffer bits = va.getChunk(key);
      int rows = va.rpc(ValueArray.getChunkIndex(key));
      double[] values = new double[_cols.length - 1];
      ArrayList<double[]> list = new ArrayList<double[]>();
      Random rand = RAND_SEED == 0 ? new Random() : new Random(RAND_SEED);
      ClusterDist cd = new ClusterDist();

      for( int row = 0; row < rows; row++ ) {
        double sqr = minSqr(_clusters, datad(va, bits, row, _cols, values), cd);
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
    Key _arykey;         // IN:  Big Table key
    int[] _cols;           // IN:  Columns-in-use mapping
    double[][] _clusters;       // IN:  Centroids/clusters

    // Reduced - sums and counts for each cluster
    double[][] _sums;           // OUT: Sum of (normalized) features in each cluster
    int[] _counts;         // OUT: Count of rows in cluster

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
        datad(va, bits, row, _cols, values);
        int cluster = closest(_clusters, values, cd)._cluster;
        if( cluster == -1 ) continue; // Ignore broken row

        // Add values and increment counter for chosen cluster
        for( int column = 0; column < values.length; column++ )
          _sums[cluster][column] += values[column];
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
      } else {
        for( int cluster = 0; cluster < _counts.length; cluster++ ) {
          for( int column = 0; column < _sums[0].length; column++ )
            _sums[cluster][column] += task._sums[cluster][column];
          _counts[cluster] += task._counts[cluster];
        }
      }
    }
  }

  // A dumb-ass class for doing multi-value returns
  private static final class ClusterDist {
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
      int pts = point.length;     // Count of valid points
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
  public static double[][] recluster(double[][] points, int k) {
    double[][] res = new double[k][];
    res[0] = points[0];
    int count = 1;
    Random rand = RAND_SEED == 0 ? new Random() : new Random(RAND_SEED);
    ClusterDist cd = new ClusterDist();

    while( count < res.length ) {
      // Compute total-square-distance from all points to all other points so-far
      double sum = 0;
      for( int i = 0; i < points.length; i++ )
        sum += minSqr(res, points[i], cd, count);

      for( int i = 0; i < points.length; i++ ) {
        if( minSqr(res, points[i], cd, count) > rand.nextDouble() * sum ) {
          res[count++] = points[i];
          break;
        }
      }
    }

    return res;
  }
}
