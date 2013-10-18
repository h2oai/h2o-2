package hex;

import hex.DGLM.Family;
import hex.DGLM.GLMModel.Status;
import hex.DGLM.GLMParams;
import hex.DGLM.Gram;
import hex.DGLM.GramMatrixFunc;
import hex.NewRowVecTask.DataFrame;
import hex.NewRowVecTask.JobCancelledException;

import java.util.Arrays;
import java.util.Comparator;

import jsr166y.CountedCompleter;

import org.apache.commons.lang.ArrayUtils;

import water.*;
import water.H2O.H2OCountedCompleter;
import water.Job.ChunkProgressJob;
import water.api.Constants;
import water.api.PCA;
import Jama.Matrix;
import Jama.SingularValueDecomposition;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

public abstract class DPCA {
  /* Track PCA job progress */
  public static class PCAJob extends ChunkProgressJob {
    public PCAJob(ValueArray data, Key dest) {
      super(data.chunks());
      description = "PCA(" + data._key.toString() + ")";
      destination_key = dest;
    }

    public boolean isDone() {
      return DKV.get(self()) == null;
    }

    @Override public float progress() {
      ChunkProgress progress = UKV.get(progressKey());
      return (progress != null ? progress.progress() : 0);
    }
  }

  /* Store parameters that go into PCA calculation */
  public static class PCAParams extends Iced {
    public int _maxPC = PCA.MAX_COL;
    public double _tol = 0;
    public boolean _standardized = true;

    public PCAParams(double tol, boolean standardized) {
      _tol = tol;
      _standardized = standardized;
    }

    public PCAParams(int maxPC, double tol, boolean standardized) {
      _maxPC = maxPC;
      _tol = tol;
      _standardized = standardized;
    }

    public JsonObject toJson() {
      JsonObject res = new JsonObject();
      res.addProperty("tolerance", _tol);
      res.addProperty("standardized", _standardized);
      return res;
    }
  }

  /* Attributes and functions of the PCA model */
  public static class PCAModel extends water.OldModel {
    String _error;
    Status _status;
    final int[] _colCatMap;
    final int _response;

    public final PCAParams _pcaParams;
    public final double[] _sdev;
    public final double[] _propVar;
    public final double[] _cumVar;
    public final double[][] _eigVec;
    public final int _rank;
    public int _num_pc;

    public Status status() {
      return _status;
    }

    public String error() {
      return _error;
    }

    public static final String NAME = PCAModel.class.getSimpleName();
    public static final String KEY_PREFIX = "__PCAModel_";

    public static final Key makeKey() {
      return Key.make(KEY_PREFIX + Key.make());
    }


    public PCAModel(Status status, float progress, Key k, DataFrame data, double[] sdev, double[] propVar, double[] cumVar,
        double[][] eigVec, int rank, int response, int num_pc, PCAParams pcaps) {
      this(status, progress, k, data._ary, data._modelDataMap, data._colCatMap, sdev, propVar, cumVar, eigVec, rank, response, num_pc, pcaps);
    }

    public PCAModel(Status status, float progress, Key k, ValueArray ary, int[] colIds, int[] colCatMap, double[] sdev,
        double[] propVar, double[] cumVar, double[][] eigVec, int rank, int response, int num_pc, PCAParams pcap) {
      super(k, colIds, ary._key);
      _status = status;
      _colCatMap = colCatMap;
      _sdev = sdev;
      _propVar = propVar;
      _cumVar = cumVar;
      _eigVec = eigVec;
      _response = response;
      _pcaParams = pcap;
      _rank = rank;
      _num_pc = num_pc;
    }

    public void store() { UKV.put(_selfKey, this); }

    @Override public boolean columnFilter(ValueArray.Column C) { return true; }

    @Override protected double score0(double[] data) {
      double[] mapped = new double[_num_pc];
      for(int j = 0; j < _num_pc; j++) {
        for(int i = 0; i < data.length; i++) {
          if(Double.isNaN(data[i])) {
            mapped[j] = Double.NaN;
            break;
          }
          else
            mapped[j] += data[i]*_eigVec[i][j];
        }
      }
      return mapped[0];   // TODO: PCA score is vector of length = number of PCs
    }

    @Override public JsonObject toJson() {
      JsonObject res = new JsonObject();
      res.addProperty(Constants.VERSION, H2O.VERSION);
      res.addProperty(Constants.TYPE, PCAModel.class.getName());
      res.addProperty(Constants.MODEL_KEY, _selfKey.toString());
      res.addProperty("rank", _rank);
      res.add("PCAParams", _pcaParams.toJson());

      // Add standard deviation and proportion of variance to output
      JsonObject sdev = new JsonObject();
      JsonObject prop = new JsonObject();
      JsonObject cum = new JsonObject();
      if(_sdev != null) {
      for(int i = 0; i < _sdev.length; i++) {
        sdev.addProperty("PC" + i, _sdev[i]);
        prop.addProperty("PC" + i, _propVar[i]);
        cum.addProperty("PC" + i, _cumVar[i]);
      } }
      res.add("stdDev", sdev);
      res.add("propVar", prop);
      res.add("cumVar", cum);

      // Add eigenvectors to output
      // Singular values ordered in weakly descending order
      JsonArray eigvec = new JsonArray();
      if(_eigVec != null) {
      for(int j = 0; j < _eigVec[0].length; j++) {
        JsonObject vec = new JsonObject();
        for(int i = 0; i < _eigVec.length; i++)
          vec.addProperty(_va._cols[i]._name, _eigVec[i][j]);
        eigvec.add(vec);
      } }
      res.add("eigenvectors", eigvec);
      return res;
    }
  }

  /* Store eigenvector matrix for later manipulation */
  public static class EigenvectorMatrix extends Iced {
    public double[][] _arr;
    long _numcol;

    public EigenvectorMatrix(int n) {
      _arr = new double[n][n];
    }

    public EigenvectorMatrix(double[][] eigvec) {
      _arr = eigvec;
    }

    public EigenvectorMatrix clone() {
      EigenvectorMatrix res = new EigenvectorMatrix(0);
      res._arr = _arr.clone();
      for(int i = 0; i < _arr.length; ++i)
        res._arr[i] = _arr[i].clone();
      res._numcol = _numcol;
      return res;
    }

    public final int size() { return _arr.length; }

    @Override public String toString() {
      StringBuilder sb = new StringBuilder();
      for(double[] r: _arr)
        sb.append(Arrays.toString(r) + "\n");
      return sb.toString();
    }

    public static Key makeKey(Key dataKey, Key modelKey) {
      return Key.make("Eigenvectors of (" + dataKey + "," + modelKey + ")");
    }
  }

  static class reverseDouble implements Comparator<Double> {
    public int compare(Double a, Double b) {
        return b.compareTo(a);
      }
    }

  public static int getNumPC(double[] sdev, double tol) {
    if(sdev == null) return 0;
    double cutoff = tol*sdev[0];
    int ind = Arrays.binarySearch(ArrayUtils.toObject(sdev), cutoff, new reverseDouble());
    return Math.abs(ind+1);
  }

  public static PCAJob startPCAJob(Key dest, final DataFrame data, final PCAParams params) {
    if(dest == null) dest = PCAModel.makeKey();
    final PCAJob job = new PCAJob(data._ary, dest);
    final double[] sdev = null;
    final double[] propVar = null;
    final double[] cumVar = null;
    final double[][] eigVec = null;

    UKV.put(job.dest(), new PCAModel(Status.ComputingModel, 0.0f, job.dest(), data, sdev, propVar, cumVar, eigVec, 0, 0, 0, params));
    final H2OCountedCompleter fjtask = new H2OCountedCompleter() {
      @Override public void compute2() {
        try {
          buildModel(job, job.dest(), data, params);
          assert !job.cancelled();
          job.remove();
        } catch( JobCancelledException e ) {
          UKV.remove(job.dest());
        }
        tryComplete();
      }

      @Override public boolean onExceptionalCompletion(Throwable ex, CountedCompleter caller) {
        if( job != null ) job.onException(ex);
        return super.onExceptionalCompletion(ex, caller);
      }
    };
    job.start(fjtask);
    H2O.submitTask(fjtask);
    return job;
  }

  public static PCAModel buildModel(Job job, Key resKey, DataFrame data, PCAParams params) throws JobCancelledException {
    if(resKey == null) resKey = PCAModel.makeKey();

    // Run SVD on Gram matrix
    GramMatrixFunc gramF = new GramMatrixFunc(data, new GLMParams(Family.gaussian), null);
    Gram gram = gramF.apply(job, data);
    Matrix myGram = new Matrix(gram.getXX());   // X'X/n where n = num rows
    // int nfeat = myGram.getRowDimension();
    SingularValueDecomposition mySVD = myGram.svd();

    // Extract eigenvalues and eigenvectors
    // Note: Singular values ordered in weakly descending order by algorithm
    double[] Sval = mySVD.getSingularValues();
    double[][] eigVec = mySVD.getV().getArray();  // rows = features, cols = principal components
    // DKV.put(EigenvectorMatrix.makeKey(data._ary._key, resKey), new EigenvectorMatrix(eigVec));

    // Compute standard deviation
    double[] sdev = new double[Sval.length];
    double totVar = 0;
    double dfcorr = data._ary._numrows/(data._ary._numrows - 1.0);
    for(int i = 0; i < Sval.length; i++) {
      // if(params._standardized)
        Sval[i] = dfcorr*Sval[i];   // Correct since degrees of freedom = n-1
      sdev[i] = Math.sqrt(Sval[i]);
      totVar += Sval[i];
    }

    double[] propVar = new double[Sval.length];    // Proportion of total variance
    double[] cumVar = new double[Sval.length];    // Cumulative proportion of total variance
    for(int i = 0; i < Sval.length; i++) {
      // eigVec[i] = eigV.getMatrix(0,nfeat-1,i,i).getColumnPackedCopy();
      propVar[i] = Sval[i]/totVar;
      cumVar[i] = i == 0 ? propVar[0] : cumVar[i-1] + propVar[i];
    }

    int ncomp = Math.min(getNumPC(sdev, params._tol), params._maxPC);
    // int ncomp = getNumPC(sdev, params._tol);
    // int ncomp = Math.min(getNumPC(Sval, params._tol), (int)data._nobs-1);
    // int ncomp = Math.min(params._num_pc, Sval.length);
    PCAModel myModel = new PCAModel(Status.Done, 0.0f, resKey, data, sdev, propVar, cumVar, eigVec, mySVD.rank(), 0, ncomp, params);
    myModel.store();
    return myModel;
  }
}
