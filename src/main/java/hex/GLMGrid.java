package hex;

import hex.DGLM.Family;
import hex.DGLM.GLMModel;
import hex.DGLM.GLMParams;
import hex.DLSM.ADMMSolver;
import hex.NewRowVecTask.JobCancelledException;

import java.util.*;

import water.*;
import water.H2O.H2OCountedCompleter;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

public class GLMGrid extends Job {
  Key                  _datakey; // Data to work on
  transient ValueArray _ary;    // Expanded VA bits
  int                  _xs[];   // Array of columns to use
  double[]             _lambdas; // Grid search values
  double[]             _ts;     // Thresholds
  double[]             _alphas; // Grid search values
  int                  _xfold;
  boolean              _parallel;
  GLMParams            _glmp;

  public GLMGrid(Key dest, ValueArray va, GLMParams glmp, int[] xs, double[] ls, double[] as, double[] thresholds, int xfold, boolean parallel) {
    super("GLMGrid", dest);
    _ary = va; // VA is large, and already in a Key so make it transient
    _datakey = va._key; // ... and use the data key instead when reloading
    _glmp = glmp;
    _xs = xs;
    _lambdas = ls;
    Arrays.sort(_lambdas);
    _ts = thresholds;
    _alphas = as;
    _xfold = xfold;
    _parallel = parallel;
    _glmp.checkResponseCol(_ary._cols[xs[xs.length-1]], new ArrayList<String>()); // ignore warnings here, they will be shown for each mdoel anyways
  }

  // Update dest for a new model. In a static function, to avoid closing
  // over the 'this' pointer of a GLMGrid and thus serializing it as part
  // of the atomic update.
  private void update(GLMModel m, final int idx, final long runTime, Futures fs) {
    final Model model = m;
    final Key jobKey = self();
    fs.add(new TAtomic<GLMModels>() {
      final Key _job = jobKey;
      boolean done = false;
      @Override
      public GLMModels atomic(GLMModels old) {
        old._ms[idx] = model._selfKey;
        done = ++old._count == old._ms.length;
        old._runTime = Math.max(runTime,old._runTime);
        return old;
      }
      @Override public void onSuccess() {
        // we're done, notify
        if(done) DKV.remove(_job);
      };
    }.fork(dest()));
  }

  private static class GridTask extends DTask<GridTask> {
    final int _aidx;
    GLMGrid   _job;
    boolean   _parallel;
    Key       _aryKey;
    GridTask(GLMGrid job, int aidx, boolean parallel) {
      _aidx = aidx;
      _job = job;
      _parallel = parallel;
      _aryKey = _job._ary._key;
      assert _aryKey != null;
    }
    @Override
    public void compute2() {
      final int N = _job._lambdas.length;
      double [] beta = null;
      Futures fs = new Futures();
      ValueArray ary = DKV.get(_aryKey).get();
      try{
        for( int l1 = 1; l1 <= _job._lambdas.length && !_job.cancelled(); l1++ ) {
          GLMModel m = DGLM.buildModel(_job,GLMModel.makeKey(false),DGLM.getData(ary, _job._xs, null, true), new ADMMSolver(_job._lambdas[N-l1], _job._alphas[_aidx]), _job._glmp,beta,_job._xfold, _parallel);
          beta = m._normBeta.clone();
          _job.update(m, (_job._lambdas.length-l1) + _aidx * _job._lambdas.length, System.currentTimeMillis() - _job._startTime,fs);
        }
        fs.blockForPending();
      }catch(JobCancelledException e){/* do not need to do anything here but stop the execution*/}
    }

    @Override
    public GridTask invoke(H2ONode client){
      compute2();
      // don't send input data back!
      _job = null;
      _aryKey = null;
      return this;
    }
  }
  @Override
  public void start() {
    super.start();
    UKV.put(dest(), new GLMModels(_lambdas.length * _alphas.length));
    if(_parallel) {
      final int cloudsize = H2O.CLOUD._memary.length;
      int myId = H2O.SELF.index();
      for( int a = 0; a < _alphas.length; a++ ) {
        GridTask t = new GridTask(this, a, _parallel);
        int nodeId = (myId+a)%cloudsize;
        if(nodeId == myId)
          H2O.submitTask(t);
        else
          RPC.call(H2O.CLOUD._memary[nodeId],t);
      }
    } else {
      H2O.submitTask(new H2OCountedCompleter() {
        @Override
        public void compute2() {
          for( int a = 0; a < _alphas.length; a++ ) {
            GridTask t = new GridTask(GLMGrid.this, a, _parallel);
            t.compute2();
          }
          remove();
        }
      });
    }
  }

  public static class GLMModels extends Iced implements Progress {
    // The computed GLM models: product of length of lamda1s,lambda2s,rhos,alphas
    Key[] _ms;
    int   _count;
    long _runTime = 0;

    public final long runTime(){return _runTime;}

    GLMModels(int length) { _ms = new Key[length]; }
    GLMModels() { }
    @Override public float progress() { return _count / (float) _ms.length; }

    public Iterable<GLMModel> sorted() {
      // NOTE: deserialized object is now kept in KV, so we can not modify it here.
      // We have to create our own private copy before sort!
      Key [] ms = _ms.clone();
      Arrays.sort(ms, new Comparator<Key>() {
        @Override
        public int compare(Key k1, Key k2) {
          Value v1 = null, v2 = null;
          if( k1 != null )
            v1 = DKV.get(k1);
          if( k2 != null )
            v2 = DKV.get(k2);
          if( v1 == null && v2 == null )
            return 0;
          if( v1 == null )
            return 1; // drive the nulls to the end
          if( v2 == null )
            return -1;
          GLMModel m1 = v1.get();
          GLMModel m2 = v2.get();
          if( m1._glmParams._family == Family.binomial ) {
            double cval1 = m1._vals[0].AUC(), cval2 = m2._vals[0].AUC();
            if( cval1 == cval2 ) {
              if( m1._vals[0].classError() != null ) {
                double[] cerr1 = m1._vals[0].classError(), cerr2 = m2._vals[0].classError();
                assert (cerr2 != null && cerr1.length == cerr2.length);
                for( int i = 0; i < cerr1.length; ++i ) {
                  cval1 += cerr1[i];
                  cval2 += cerr2[i];
                }
              }
              if( cval1 == cval2 ) {
                cval1 = m1._vals[0].err();
                cval2 = m2._vals[0].err();
              }
            }
            return Double.compare(cval2, cval1);
          } else
            return Double.compare(m1._vals[0]._err, m2._vals[0]._err);
        }
      });
      final Key[] keys = ms;
      int lastIdx = ms.length;
      for( int i = 0; i < ms.length; ++i ) {
        if( keys[i] == null || DKV.get(keys[i]) == null ) {
          lastIdx = i;
          break;
        }
      }
      final int N = lastIdx;
      return new Iterable<GLMModel>() {
        @Override
        public Iterator<GLMModel> iterator() {
          return new Iterator<GLMModel>() {
            int _idx = 0;
            @Override public GLMModel next() { return DKV.get(keys[_idx++]).get(); }
            @Override public boolean hasNext() { return _idx < N; }
            @Override public void remove() { throw new UnsupportedOperationException(); }
          };
        }
      };
    }

    // Convert all models to Json (expensive!)
    public JsonObject toJson() {
      JsonObject j = new JsonObject();
      // sort models according to their performance
      JsonArray arr = new JsonArray();
      for( GLMModel m : sorted() )
        arr.add(m.toJson());
      j.add("models", arr);
      return j;
    }
  }
}
