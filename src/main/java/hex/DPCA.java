package hex;

import jsr166y.CountedCompleter;
import hex.DGLM.*;
import hex.DGLM.GLMModel.Status;
import hex.NewRowVecTask.DataFrame;
import hex.NewRowVecTask.JobCancelledException;

import water.*;
import water.H2O.H2OCountedCompleter;
import water.Job.ChunkProgressJob;
import water.api.Constants;

import com.google.gson.*;

import Jama.Matrix;
import Jama.SingularValueDecomposition;

public abstract class DPCA {
  public static class PCAJob extends ChunkProgressJob {
    public PCAJob(ValueArray data, Key dest) {
      super("PCA(" + data._key.toString() + ")", dest, data.chunks() * 2);
    }

    public boolean isDone() {
      return DKV.get(self()) == null;
    }

    @Override public float progress() {
      ChunkProgress progress = UKV.get(progressKey());
      return (progress != null ? progress.progress() : 0);
    }
  }

  public static class PCAParams extends Iced {
    public int _num_pc = 5;

    public PCAParams(int num_pc) {
      _num_pc = num_pc;
    }

    public JsonObject toJson() {
      JsonObject res = new JsonObject();
      res.addProperty("numPC", _num_pc);
      return res;
    }
  }

  public static class PCAModel extends water.Model {
    String _error;
    Status _status;
    final int[] _colCatMap;
    final int _response;
    public final PCAParams _pcaParams;
    public final double[] _sdev;
    public final double[] _propVar;
    public final double[][] _eigVec;

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

    public PCAModel() {
      _status = Status.NotStarted;
      _colCatMap = null;
      _sdev = null;
      _propVar = null;
      _eigVec = null;
      _response = 0;
      _pcaParams = null;
    }

    public PCAModel(Status status, float progress, Key k, DataFrame data, double[] sdev, double[] propVar,
        double[][] eigVec, int response, PCAParams pcaps) {
      this(status, progress, k, data._ary, data._modelDataMap, data._colCatMap, sdev, propVar, eigVec, response, pcaps);
    }

    public PCAModel(Status status, float progress, Key k, ValueArray ary, int[] colIds, int[] colCatMap, double[] sdev,
        double[] propVar, double[][] eigVec, int response, PCAParams pcap) {
      super(k, colIds, ary._key);
      _status = status;
      _colCatMap = colCatMap;
      _sdev = sdev;
      _propVar = propVar;
      _eigVec = eigVec;
      _response = response;
      _pcaParams = pcap;
    }

    public void store() {
      UKV.put(_selfKey, this);
    }

    @Override protected double score0(double[] data) {
      throw H2O.unimpl();
    }

    @Override public JsonObject toJson() {
      JsonObject res = new JsonObject();
      res.addProperty(Constants.VERSION, H2O.VERSION);
      res.addProperty(Constants.TYPE, PCAModel.class.getName());
      res.addProperty(Constants.MODEL_KEY, _selfKey.toString());
      res.add("PCAParams", _pcaParams.toJson());

      // Add standard deviation to output
      JsonObject sdev = new JsonObject();
      JsonObject prop = new JsonObject();
      for(int i = 0; i < _sdev.length; i++) {
        sdev.addProperty("PC" + i, _sdev[i]);
        prop.addProperty("PC" + i, _propVar[i]);
      }
      res.add("stdDev", sdev);
      res.add("propVar", prop);

      // Add eigenvectors to output
      // Singular values ordered in weakly descending order
      JsonArray eigvec = new JsonArray();
      for(int i = 0; i < _pcaParams._num_pc; i++) {
        JsonObject vec = new JsonObject();
        for(int j = 0; j < _eigVec[i].length; j++)
          vec.addProperty(_va._cols[j]._name, _eigVec[i][j]);
        eigvec.add(vec);
      }
      res.add("eigenvectors", eigvec);
      return res;
    };
  }

  public static PCAJob startPCAJob(Key dest, final DataFrame data, final PCAParams params) {
    if(dest == null) dest = PCAModel.makeKey();
    final PCAJob job = new PCAJob(data._ary, dest);
    final double[] sdev = null;
    final double[] propVar = null;
    final double[][] eigVec = null;

    UKV.put(job.dest(), new PCAModel(Status.ComputingModel, 0.0f, job.dest(), data, sdev, propVar, eigVec, 0, params));
    final H2OCountedCompleter fjtask = new H2OCountedCompleter() {
      @Override public void compute2() {
        try {
          buildModel(job, job.dest(), data, params._num_pc);
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
    H2O.submitTask(job.start(fjtask));
    return job;
  }

  public static PCAModel buildModel(Job job, Key resKey, DataFrame data, int num_pc) throws JobCancelledException {
    if(resKey == null) resKey = PCAModel.makeKey();

    // Run SVD on Gram matrix
    GramMatrixFunc gramF = new GramMatrixFunc(data, new GLMParams(Family.gaussian), null);
    Gram gram = gramF.apply(job, data);
    Matrix myGram = new Matrix(gram.getXX());
    SingularValueDecomposition mySVD = myGram.svd();

    // Compute standard deviation from eigenvalues
    double[] Sval = mySVD.getSingularValues();
    int ncomp = Math.min(num_pc, Sval.length);
    double[] sdev = new double[ncomp];
    double totVar = 0;
    for(int i = 0; i < ncomp; i++) {
      sdev[i] = Math.sqrt(Sval[i]);
      totVar += Sval[i];
    }

    // Extract eigenvectors
    Matrix eigV = mySVD.getV();
    int nfeat = eigV.getRowDimension();
    double[][] eigVec = new double[ncomp][nfeat];
    double[] propVar = new double[ncomp];    // Proportion of total variance

    // Singular values ordered in weakly descending order
    for(int i = 0; i < ncomp; i++) {
      eigVec[i] = eigV.getMatrix(0,nfeat-1,i,i).getColumnPackedCopy();
      propVar[i] = Sval[i]/totVar;
    }

    PCAParams params = new PCAParams(ncomp);
    PCAModel myModel = new PCAModel(Status.Done, 0.0f, resKey, data, sdev, propVar, eigVec, 0, params);
    myModel.store();
    return myModel;
  }
}