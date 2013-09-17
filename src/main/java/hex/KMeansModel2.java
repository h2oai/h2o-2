package hex;

import hex.KMeans.ClusterDist;
import jsr166y.CountedCompleter;
import water.*;
import water.H2O.H2OCountedCompleter;
import water.Job.ChunkProgressJob;
import water.ValueArray.Column;
import water.api.Constants;
import water.util.*;
import water.util.Log;
import water.util.Log.Tag.Sys;

import com.google.gson.*;

public class KMeansModel2 extends OldModel {
  public double[][] _clusters; // The cluster centers, normalized according to _va
  public double _error; // Sum of min square distances
  public KMeans2 _job;

  public KMeansModel2(Key selfKey, int cols[], Key dataKey) {
    super(selfKey, cols, dataKey);
  }

  // Accept only columns with a defined mean. Used during the Model.<init> call.
  @Override public boolean columnFilter(ValueArray.Column C) {
    return !Double.isNaN(C._mean);
  }

  @Override public JsonObject toJson() {
    JsonObject res = new JsonObject();
    res.addProperty(Constants.VERSION, H2O.VERSION);
    res.addProperty(Constants.TYPE, KMeansModel2.class.getName());
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
        if( _job.normalize ) {
          if( C._sigma != 0.0 && !Double.isNaN(C._sigma) )
            d *= C._sigma;
          d += C._mean;
        }
        dd[j][i] = d;
      }
    }
    return dd;
  }

  /**
   * Single row scoring, on properly ordered data. Will return NaN if any data element contains a
   * NaN. Returns the cluster-number, which is mostly an internal value. Last data element refers to
   * the response variable, which is not used for k-means.
   */
  protected double score0(double[] data) {
    for( int i = 0; i < data.length - 1; i++ ) { // Normalize the data before scoring
      ValueArray.Column C = _va._cols[i];
      double d = data[i];
      if( _job.normalize ) {
        d -= C._mean;
        if( C._sigma != 0.0 && !Double.isNaN(C._sigma) )
          d /= C._sigma;
      }
      data[i] = d;
    }
    data[data.length - 1] = Double.NaN; // Response variable column not used
    return KMeans.closest(_clusters, data, new ClusterDist())._cluster;
  }

  /** Single row scoring, on a compatible ValueArray (when pushed throw the mapping) */
  protected double score0(ValueArray data, int row) {
    throw H2O.unimpl();
  }

  /** Bulk scoring API, on a compatible ValueArray (when pushed throw the mapping) */
  protected double score0(ValueArray data, AutoBuffer ab, int row_in_chunk) {
    throw H2O.unimpl();
  }

  public final void print() {
    StringBuilder sb = new StringBuilder();
    sb.append("I: ").append(_job.iterations).append("[");
    double[][] c = clusters();
    for( int i = 0; i < c.length; i++ )
      sb.append(c[i][2]).append(",");
    sb.append("]");
    Log.debug(Sys.KMEAN, sb);
  }

  // Compute the cluster members, and the mean-dist to each cluster
  public static class KMeansScore extends MRTask {
    Key _arykey;                // IN:  The dataset
    int _cols[];                // IN:  Cols->Features mapping
    double _clusters[][];       // IN:  The (normalized) Clusters
    boolean _normalized;        // IN

    public long _rows[];        // OUT: Count of rows per-cluster
    public double _dist[];      // OUT: Normalized sqr-error per-cluster

    public static KMeansScore score(KMeansModel2 model, ValueArray ary) {
      KMeansScore kms = new KMeansScore();
      kms._arykey = ary._key;
      kms._cols = model.columnMapping(ary.colNames());
      kms._clusters = model._clusters;
      kms._normalized = model._job.normalize;
      kms.invoke(ary._key);
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
        KMeans.datad(va, bits, row, _cols, _normalized, values);
        KMeans.closest(_clusters, values, cd);
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
        Utils.add(_rows, kms._rows);
        Utils.add(_dist, kms._dist);
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
    boolean _normalized;    // IN

    static final int ROW_SIZE = 4;

    public static Job run(final Key dest, final KMeansModel2 model, final ValueArray ary) {
      UKV.remove(dest); // Delete dest first, or chunk size from previous key can crash job
      String desc = "KMeans apply model: " + model._selfKey + " to " + ary._key;
      final ChunkProgressJob job = new ChunkProgressJob(desc, dest, ary.chunks());
      final H2OCountedCompleter fjtask = new H2OCountedCompleter() {
        @Override public void compute2() {
          KMeansApply kms = new KMeansApply();
          kms._job = job;
          kms._arykey = ary._key;
          kms._cols = model.columnMapping(ary.colNames());
          kms._clusters = model._clusters;
          kms._normalized = model._job.normalize;
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
          KMeans.datad(va, bits, row, _cols, _normalized, values);
          KMeans.closest(_clusters, values, cd);
          chunk = ValueArray.chknum(startRow + row, va.numRows(), ROW_SIZE);
          if( chunk != updatedChk ) {
            updateClusters(clusters, count, updatedChk, va.numRows(), rpc, updatedRow);
            updatedChk = chunk;
            updatedRow = startRow + row;
            count = 0;
          }
          clusters[count++] = cd._cluster;
        }
        if( count > 0 )
          updateClusters(clusters, count, chunk, va.numRows(), rpc, updatedRow);
        _job.updateProgress(1);
      }
      _job = null;
      _arykey = null;
      _cols = null;
      _clusters = null;
    }

    @Override public void reduce(DRemoteTask rt) {
    }

    private void updateClusters(int[] clusters, int count, long chunk, long numrows, int rpc, long updatedRow) {
      final int offset = (int) (updatedRow - (rpc * chunk));
      final Key chunkKey = ValueArray.getChunkKey(chunk, _job.dest());
      final int[] message;
      if( count == clusters.length )
        message = clusters;
      else {
        message = new int[count];
        System.arraycopy(clusters, 0, message, 0, message.length);
      }
      final int rows = ValueArray.rpc(chunk, rpc, numrows);
      new Atomic() {
        @Override public Value atomic(Value val) {
          assert val == null || val._key.equals(chunkKey);
          AutoBuffer b = new AutoBuffer(rows * ROW_SIZE);
          if( val != null )
            b._bb.put(val.memOrLoad());
          for( int i = 0; i < message.length; i++ )
            b.put4((offset + i) * 4, message[i]);
          b.position(b.limit());
          return new Value(chunkKey, b.buf());
        }
      }.invoke(chunkKey);
    }
  }
}
