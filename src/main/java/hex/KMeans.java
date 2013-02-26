package hex;

import java.util.*;

import water.*;
import water.Job.Progress;

import com.google.gson.*;

/**
 * Scalable K-Means++ (KMeans||)<br>
 * http://theory.stanford.edu/~sergei/papers/vldb12-kmpar.pdf<br>
 * http://www.youtube.com/watch?v=cigXAxV3XcY
 */
public abstract class KMeans {
  private static final boolean DEBUG = false;
  public static Long           RAND_SEED;

  public static class KMeansModel extends Model implements Progress {
    public static final String KEY_PREFIX = "__KMeansModel_";
    public double[][]          _clusters;                    // The cluster centers, normalized according to _va
    public int                 _iteration;

    // Empty constructor for deserialization
    public KMeansModel() {
    }

    KMeansModel(Key selfKey, int cols[], Key dataKey) {
      // Unlike other models, k-means is a discovery-only procedure and does
      // not require a response-column to train. This also means the clusters
      // are not classes (although, if a class/response is associated with each
      // row we could count the number of each class in each cluster).
      super(selfKey, cols, dataKey);
    }

    @Override
    public float progress() {
      return Math.min(1f, _iteration / (float) 20);
    }

    // Accept only columns with a defined mean. Used during the Model.<init> call.
    @Override
    public boolean columnFilter(ValueArray.Column C) {
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
      res.add("clusters", ary);
      return res;
    }

    // Return the clusters, denormalized
    public double[][] clusters() {
      double dd[][] = _clusters.clone();
      for( double ds[] : dd )
        for( int i = 0; i < ds.length; i++ ) {
          ValueArray.Column C = _va._cols[i];
          double d = ds[i];
          if( C._sigma != 0.0 && !Double.isNaN(C._sigma) )
            d *= C._sigma;
          d += C._mean;
          ds[i] = d;
        }
      return dd;
    }

    /**
     * Single row scoring, on properly ordered data. Will return NaN if any data element contains a NaN. Returns the cluster-number, which
     * is mostly an internal value.
     */
    protected double score0(double[] data) {
      for( int i = 0; i < data.length; i++ ) { // Normalize the data before scoring
        ValueArray.Column C = _va._cols[i];
        double d = data[i] - C._mean;
        if( C._sigma != 0.0 && !Double.isNaN(C._sigma) )
          d /= C._sigma;
        data[i] = d;
      }
      return closest(_clusters, data);
    }

    /** Single row scoring, on a compatible ValueArray (when pushed throw the mapping) */
    protected double score0(ValueArray data, int row, int[] mapping) {
      throw H2O.unimpl();
    }

    /** Bulk scoring API, on a compatible ValueArray (when pushed throw the mapping) */
    protected double score0(ValueArray data, AutoBuffer ab, int row_in_chunk, int[] mapping) {
      throw H2O.unimpl();
    }
  }

  // Return a normalized value. If missing, return the mean (which we know
  // exists because we filtered out columns with no mean).
  private static double datad(ValueArray va, AutoBuffer bits, int row, ValueArray.Column C) {
    if( va.isNA(bits, row, C) )
      return C._mean;
    double d = va.datad(bits, row, C) - C._mean;
    return (C._sigma == 0.0 || Double.isNaN(C._sigma)) ? d : d / C._sigma;
  }

  public static void run(Key dest, ValueArray va, int k, double epsilon, int... cols) {
    Job job = startJob(dest, va, k, epsilon, cols);
    run(job, va, k, epsilon, cols);
  }

  public static Job startJob(Key dest, ValueArray va, int k, double epsilon, int... cols) {
    Job job = new Job("KMeans K: " + k + ", Cols: " + cols.length, dest);
    job.start();
    return job;
  }

  public static void run(Job job, ValueArray va, int k, double epsilon, int... cols) {
    KMeansModel res = new KMeansModel(job.dest(), cols, va._key);
    // Updated column mapping selection after removing various junk columns
    cols = res.columnMapping(va.colNames());

    // Initialize first cluster to first row
    double[][] clusters = new double[1][];
    clusters[0] = new double[cols.length];
    AutoBuffer bits = va.getChunk(0);
    for( int c = 0; c < cols.length; c++ )
      clusters[0][c] = datad(va, bits, 0, va._cols[cols[c]]);

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
      clusters = DRemoteTask.merge(clusters, sampler._newClusters);

      if( job.cancelled() ) {
        job.remove();
        return;
      }

      res._iteration++;
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
        for( int column = 0; column < cols.length; column++ ) {
          double value = task._sums[cluster][column] / task._counts[cluster];
          if( Math.abs(value - clusters[cluster][column]) > epsilon )
            moved = true;
          clusters[cluster][column] = value;
        }
      }

      res._iteration++;
      UKV.put(job.dest(), res);
      if( job.cancelled() )
        break;
    }

    job.remove();
  }

  public static class Sqr extends MRTask {
    Key        _arykey;
    int[]      _cols;
    double[][] _clusters;

    // Reduced
    double     _sqr;

    @Override
    public void map(Key key) {
      if( DEBUG )
        System.out.println("sqr map " + key + ": " + this);

      assert key.home();
      ValueArray va = ValueArray.value(DKV.get(_arykey));
      AutoBuffer bits = va.getChunk(key);
      int rows = bits.remaining() / va._rowsize;
      double[] values = new double[_cols.length];

      for( int row = 0; row < rows; row++ ) {
        for( int column = 0; column < _cols.length; column++ )
          values[column] = datad(va, bits, row, va._cols[_cols[column]]);

        _sqr += minSqr(_clusters, _clusters.length, values);
      }
    }

    @Override
    public void reduce(DRemoteTask rt) {
      if( DEBUG )
        System.out.println("sqr reduce " + this);

      Sqr task = (Sqr) rt;
      _sqr += task._sqr;
    }
  }

  public static class Sampler extends MRTask {
    Key        _arykey;
    int[]      _cols;
    double[][] _clusters;
    double     _sqr;
    double     _probability;

    // Reduced
    double[][] _newClusters;

    @Override
    public void map(Key key) {
      if( DEBUG )
        System.out.println("sampler map " + key + ": " + this + ", sqr: " + _sqr);

      assert key.home();
      ValueArray va = ValueArray.value(DKV.get(_arykey));
      AutoBuffer bits = va.getChunk(key);
      int rows = bits.remaining() / va._rowsize;
      double[] values = new double[_cols.length];
      ArrayList<double[]> list = new ArrayList<double[]>();
      Random rand = RAND_SEED == null ? new Random() : new Random(RAND_SEED);

      for( int row = 0; row < rows; row++ ) {
        for( int column = 0; column < _cols.length; column++ )
          values[column] = datad(va, bits, row, va._cols[_cols[column]]);

        double sqr = minSqr(_clusters, _clusters.length, values);

        if( _probability * sqr > rand.nextDouble() * _sqr ) {
          if( DEBUG )
            System.out.println("sampled: " + Arrays.toString(values));

          list.add(values.clone());
        }
      }

      _newClusters = new double[list.size()][];
      list.toArray(_newClusters);
    }

    @Override
    public void reduce(DRemoteTask rt) {
      if( DEBUG )
        System.out.println("sampler reduce " + this);

      Sampler task = (Sampler) rt;

      if( _newClusters != null )
        _newClusters = merge(_newClusters, task._newClusters);
      else
        _newClusters = task._newClusters;
    }
  }

  public static class Lloyds extends MRTask {
    Key        _arykey;
    int[]      _cols;
    double[][] _clusters;

    // Reduced - sums and counts for each cluster
    double[][] _sums;
    int[]      _counts;

    @Override
    public void map(Key key) {
      if( DEBUG )
        System.out.println("KMeans map " + key + ": " + this);

      assert key.home();
      ValueArray va = ValueArray.value(DKV.get(_arykey));
      AutoBuffer bits = va.getChunk(key);
      int rows = bits.remaining() / va._rowsize;
      double[] values = new double[_cols.length];

      // Create result arrays
      _sums = new double[_clusters.length][_cols.length];
      _counts = new int[_clusters.length];

      // Find closest cluster for each row
      for( int row = 0; row < rows; row++ ) {
        for( int column = 0; column < _cols.length; column++ )
          values[column] = datad(va, bits, row, va._cols[_cols[column]]);

        int cluster = closest(_clusters, values);

        // Add values and increment counter for chosen cluster
        for( int column = 0; column < _cols.length; column++ )
          _sums[cluster][column] += values[column];
        _counts[cluster]++;
      }
    }

    @Override
    public void reduce(DRemoteTask rt) {
      if( DEBUG )
        System.out.println("KMeans reduce " + this);
      Lloyds task = (Lloyds) rt;
      if( _sums == null ) {
        _sums = new double[_clusters.length][_cols.length];
        _counts = new int[_clusters.length];
      }

      for( int cluster = 0; cluster < _clusters.length; cluster++ ) {
        for( int column = 0; column < _cols.length; column++ )
          _sums[cluster][column] += task._sums[cluster][column];
        _counts[cluster] += task._counts[cluster];
      }
    }
  }

  public static double minSqr(double[][] clusters, int clusterCount, double[] point) {
    double minSqr = Double.MAX_VALUE;

    for( int cluster = 0; cluster < clusterCount; cluster++ ) {
      double sqr = 0;

      for( int column = 0; column < point.length; column++ ) {
        double delta = point[column] - clusters[cluster][column];
        sqr += delta * delta;
      }

      if( sqr < minSqr )
        minSqr = sqr;
    }

    return minSqr;
  }

  public static int closest(double[][] clusters, double[] point) {
    int min = -1;
    double minSqr = Double.MAX_VALUE;
    for( int cluster = 0; cluster < clusters.length; cluster++ ) {
      double sqr = 0;
      for( int column = 0; column < point.length; column++ ) {
        assert !Double.isNaN(point[column]);
        double delta = point[column] - clusters[cluster][column];
        sqr += delta * delta;
      }
      if( sqr < minSqr ) {
        min = cluster;
        minSqr = sqr;
      }
    }
    return min;
  }

  // KMeans++ re-clustering
  public static double[][] recluster(double[][] points, int k) {
    double[][] res = new double[k][];
    res[0] = points[0];
    int count = 1;
    Random rand = RAND_SEED == null ? new Random() : new Random(RAND_SEED);

    while( count < res.length ) {
      double sum = 0;
      for( int i = 0; i < points.length; i++ )
        sum += minSqr(res, count, points[i]);

      for( int i = 0; i < points.length; i++ ) {
        if( minSqr(res, count, points[i]) > rand.nextDouble() * sum ) {
          res[count++] = points[i];
          break;
        }
      }
    }

    return res;
  }
}
