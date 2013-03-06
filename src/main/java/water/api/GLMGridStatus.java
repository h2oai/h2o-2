package water.api;

import hex.*;
import hex.DGLM.Family;
import hex.DGLM.GLMModel;
import hex.DGLM.GLMParams;
import hex.DLSM.ADMMSolver;

import java.util.*;

import water.*;
import water.DTask.DTaskImpl;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;


// A grid-search task.  This task is embedded in a Value and mapped to a Key,
// and only can be updated via Atomic ops on the mapped Value.  So basically,
// all these fields are "final" in this POJO and are modified by atomic update.
class GLMGridStatus extends DTaskImpl<GLMGridStatus> {
  // Self-key - actual data is stored in the K/V store.  This is just a
  // convenient POJO to read the bits.
  Key _taskey;                // Myself key

  // Request any F/J worker thread to stop working on this task
  boolean _stop;
  // Set when a top-level F/J worker thread starts up,
  // and cleared when it shuts down.
  boolean _working = true;

  Key _datakey;               // Datakey to work on
  transient ValueArray _ary;  // Expanded VA bits
  int _xs[];                  // Array of columns to use
  double [] _lambdas;        // Grid search values
  double [] _ts;
  double [] _alphas;          // Grid search values
  int _xfold;

  GLMParams _glmp;

  // Progress: Count of GLM models computed so far.
  int _progress;
  int _max;

  // The computed GLM models: product of length of lamda1s,lambda2s,rhos,alphas
  Key _ms[];

  // Fraction complete
  float progress() { return (float)_progress/_ms.length; }

  public GLMGridStatus(Key taskey, ValueArray va, GLMParams glmp, int[] xs, double[]ls,  double[]as, double[]thresholds, int xfold) {
    _taskey = taskey;           // Capture the args
    _ary = va;                  // VA is large, and already in a Key so make it transient
    _datakey = va._key;         // ... and use the datakey instead when reloading
    _glmp = glmp;
    _xs = xs;
    _lambdas = ls;
    _ts       = thresholds;
    _alphas   = as;
    _max = ls.length*as.length;
    _ms = new Key[_max];
    _xfold = xfold;
  }
  // Void constructor for serialization
  public GLMGridStatus() {}

  // Work on this task.  Only *this* main thread updates '_working' &
  // '_progress' & '_ms' fields.  Only other web threads update the '_stop'
  // field; web threads also atomically-read the progress&ms fields.
  public void compute2() {
    assert _working == true;
    final int N = _alphas.length;
    GLMModel m = null;
    OUTER:
    for( int l1=1; l1<= _lambdas.length; l1++ )
          for( int a=0; a<_alphas.length; a++) {
            if( _stop ) break OUTER;
            m = do_task(m,_lambdas.length-l1,a); // Do a step; get a model
            // Now update this Status.
            update(_taskey,m,(_lambdas.length-l1)*N+a);
            // Fetch over the 'this' all new bits.  Mostly witness updates to
            // _progress and _stop fields.
            UKV.get(_taskey,this);
          }

    // Update _working to 'false' - we have stopped working
    set_working(_taskey,false);
    tryComplete();            // This task is done
  }

  // Update status for a new model.  In a static function, to avoid closing
  // over the 'this' pointer of a GLMGridStatus and thus serializing it as part
  // of the atomic update.
  static void update( Key taskey, final GLMModel m, final int idx) {
    new TAtomic<GLMGridStatus>() {
      @Override public GLMGridStatus atomic(GLMGridStatus old) {
        old._ms[idx] = m._selfKey; old._progress++; return old; }
      @Override public GLMGridStatus alloc() { return new GLMGridStatus(); }
    }.invoke(taskey);
  }

  // Update the _working field atomically.  In a static function, to avoid
  // closing over the 'this' pointer of a GLMGridStatus and thus serializing it
  // as part of the atomic update.
  static void set_working( Key taskey, final boolean working) {
    new TAtomic<GLMGridStatus>() {
      @Override public GLMGridStatus atomic(GLMGridStatus old) {
        old._working = working; return old; }
      @Override public GLMGridStatus alloc() { return new GLMGridStatus(); }
    }.invoke(taskey);
  }

  // ---
  // Do a single step (blocking).
  // In this case, run 1 GLM model.
  private GLMModel do_task(GLMModel m, int l, int alpha) {
    m = DGLM.buildModel(DGLM.getData(_ary, _xs, null, true), new ADMMSolver(_lambdas[l], _alphas[alpha]), _glmp);
    if(_xfold <= 1)
      m.validateOn(_ary, null,_ts);
    else
      m.xvalidate (_ary,_xfold,_ts);
    return m;
  }
  // ---

  String model_name( int step ) {
    return "Model "+step;
  }

  public Iterable<GLMModel> computedModels(){
    Arrays.sort(_ms, new Comparator<Key>() {
      @Override
      public int compare(Key k1, Key k2) {
        Value v1 = null, v2 = null;
        if(k1 != null)v1 = DKV.get(k1);
        if(k2 != null)v2 = DKV.get(k2);
        if(v1 == null && v2 == null)return 0;
        if(v1 == null)return 1; // drive the nulls to the end
        if(v2 == null)return -1;
        GLMModel m1 = v1.get(new GLMModel());
        GLMModel m2 = v2.get(new GLMModel());
        if(m1._glmParams._family == Family.binomial){
          double cval1 = m1._vals[0].AUC(), cval2 = m2._vals[0].AUC();
          if(cval1 == cval2){
            if(m1._vals[0].classError() != null){
              double [] cerr1 = m1._vals[0].classError(), cerr2 = m2._vals[0].classError();
              assert (cerr2 != null && cerr1.length == cerr2.length);
              for(int i = 0; i < cerr1.length; ++i){
                cval1 += cerr1[i];
                cval2 += cerr2[i];
              }
            }
            if(cval1 == cval2){
              cval1 = m1._vals[0].err();
              cval2 = m2._vals[0].err();
            }
          }
          return Double.compare(cval2, cval1);
       } else
         return Double.compare(m1._vals[0]._err,m2._vals[0]._err);
      }
    });
    final Key [] keys = _ms;
    int lastIdx = _ms.length;
    for(int i = 0; i < _ms.length; ++i){
      if(keys[i] == null || DKV.get(keys[i]) == null){
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
    for(GLMModel m:computedModels()){
      arr.add(m.toJson());
    }
    j.add("models", arr);
    return j;
  }
  // Not intended for remote or distributed execution; task control runs on
  // one node.
  public GLMGridStatus invoke( H2ONode sender ) { throw H2O.unimpl(); }
}
