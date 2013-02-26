package hex;

import hex.DGLM.Family;
import hex.DGLM.GLMModel;
import hex.DGLM.GLMParams;
import hex.DLSM.ADMMSolver;

import java.util.*;

import water.*;

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
    _ts = thresholds;
    _alphas = as;
    _xfold = xfold;
  }

  public GLMGrid() {
  }

  @Override
  public void start() {
    UKV.put(dest(), new GLMModels(_lambdas.length * _alphas.length));

    H2O.FJP_NORM.submit(new DTask() {
      @Override
      public void compute() {
        final int N = _alphas.length;
        GLMModel m = null;
        try {
          OUTER: for( int l1 = 1; l1 <= _lambdas.length; l1++ ) {
            for( int a = 0; a < _alphas.length; a++ ) {
              if( cancelled() )
                break OUTER;
              m = do_task(m,_lambdas.length-l1,a); // Do a step; get a model
              update(dest(), m, (_lambdas.length-l1) * N + a);
            }
          }
        } finally {
          remove();
        }
        tryComplete(); // This task is done
      }

      // Not intended for remote or distributed execution; task control runs on one node.
      public GLMGrid invoke(H2ONode sender) {
        throw H2O.unimpl();
      }
    });
  }

  // Update dest for a new model. In a static function, to avoid closing
  // over the 'this' pointer of a GLMGrid and thus serializing it as part
  // of the atomic update.
  private static void update(Key dest, final GLMModel m, final int idx) {
    new TAtomic<GLMModels>() {
      @Override
      public GLMModels atomic(GLMModels old) {
        old._ms[idx] = m._selfKey;
        old._count++;
        return old;
      }
    }.invoke(dest);
  }

  // ---
  // Do a single step (blocking).
  // In this case, run 1 GLM model.
  private GLMModel do_task(GLMModel m, int l, int alpha) {
    m = DGLM.buildModel(DGLM.getData(_ary, _xs, null, true), new ADMMSolver(_lambdas[l], _alphas[alpha]), _glmp);
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

    GLMModels(int length) {
      _ms = new Key[length];
    }

    GLMModels() {
    }

    @Override
    public float progress() {
      return _count / (float) _ms.length;
    }

    public Iterable<GLMModel> sorted() {
      Arrays.sort(_ms, new Comparator<Key>() {
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
          GLMModel m1 = v1.get(new GLMModel());
          GLMModel m2 = v2.get(new GLMModel());
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
      final Key[] keys = _ms;
      int lastIdx = _ms.length;
      for( int i = 0; i < _ms.length; ++i ) {
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

            @Override
            public void remove() {
              throw new UnsupportedOperationException();
            }

            @Override
            public GLMModel next() {
              return DKV.get(keys[_idx++]).get(new GLMModel());
            }

            @Override
            public boolean hasNext() {
              return _idx < N;
            }
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
