package hex;

import hex.DGLM.Family;
import hex.DGLM.GLMModel;
import hex.DGLM.GLMParams;
import hex.DLSM.ADMMSolver;

import java.util.*;

import jsr166y.CountedCompleter;

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
  GLMParams            _glmp;

  public GLMGrid(Key dest, ValueArray va, GLMParams glmp, int[] xs, double[] ls, double[] as, double[] thresholds, int xfold) {
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
    _glmp.checkResponseCol(_ary._cols[xs[xs.length-1]], new ArrayList<String>()); // ignore warnings here, they will be shown for each mdoel anyways
  }

  private class GridTask extends H2OCountedCompleter {
    final int _aidx;
    GridTask(int aidx) { _aidx = aidx; }

    @Override
    public void compute2() {
      double [] beta = null;
      Futures fs = new Futures();
      try {
        for( int l1 = 1; l1 <= _lambdas.length; l1++ ) {
          if(cancelled())
            break;
          GLMModel m = do_task(beta,_lambdas.length-l1,_aidx); // Do a step; get a model
          beta = m._normBeta.clone();
          update(dest(), m, (_lambdas.length-l1) * _alphas.length + _aidx, System.currentTimeMillis() - _startTime,fs);
        }
      fs.blockForPending();
      }finally {
        tryComplete();
      }
    }

  }
  @Override
  public void start() {
    super.start();
    UKV.put(dest(), new GLMModels(_lambdas.length * _alphas.length));
    final int N = _alphas.length;
    H2O.submitTask(new H2OCountedCompleter() {
      @Override
      public void compute2() {
        setPendingCount(N);
        for( int a = 0; a < _alphas.length; a++ ){
          GridTask t = new GridTask(a);
          t.setCompleter(this);
          H2O.submitTask(t);
        }
        tryComplete(); // This task is done
      }
      @Override public void onCompletion(CountedCompleter caller){remove();}
    });
  }

  // Update dest for a new model. In a static function, to avoid closing
  // over the 'this' pointer of a GLMGrid and thus serializing it as part
  // of the atomic update.
  private static void update(Key dest, GLMModel m, final int idx, final long runTime, Futures fs) {
    final Model model = m;
    fs.add(new TAtomic<GLMModels>() {
      @Override
      public GLMModels atomic(GLMModels old) {
        old._ms[idx] = model._selfKey;
        old._count++;
        old._runTime = Math.max(runTime,old._runTime);
        return old;
      }
    }.fork(dest));
  }

  // ---
  // Do a single step (blocking).
  // In this case, run 1 GLM model.
  private GLMModel do_task(double [] beta, int l, int alpha) {
    GLMModel m = DGLM.buildModel(DGLM.getData(_ary, _xs, null, true), new ADMMSolver(_lambdas[l], _alphas[alpha]), _glmp,beta);
    if( _xfold <= 1 )
      m.validateOn(_ary, null, _ts);
    else
      m.xvalidate(_ary, _xfold, _ts);
    return m;
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
