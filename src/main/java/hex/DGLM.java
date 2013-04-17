package hex;
import hex.ConfusionMatrix.ErrMetric;
import hex.DGLM.GLMModel.Status;
import hex.DLSM.ADMMSolver.NonSPDMatrixException;
import hex.DLSM.GeneralizedGradientSolver;
import hex.DLSM.LSMSolver;
import hex.NewRowVecTask.DataFrame;
import hex.NewRowVecTask.JobCancelledException;
import hex.NewRowVecTask.RowFunc;
import hex.RowVecTask.Sampling;

import java.util.*;

import jsr166y.CountedCompleter;
import water.*;
import water.H2O.H2OCountedCompleter;
import water.Job.ChunkProgressJob;
import water.ValueArray.Column;
import water.api.Constants;

import com.google.common.base.Objects;
import com.google.gson.*;



public abstract class DGLM {
  public static final int DEFAULT_MAX_ITER = 50;
  public static final double DEFAULT_BETA_EPS = 1e-4;

  public static class GLMException extends RuntimeException {
    public GLMException(String msg){super(msg);}
  }

  public static class GLMJob extends ChunkProgressJob {
    public GLMJob(ValueArray data, Key dest, int xval, GLMParams params){
      // approximate the total number of computed chunks as 25 per normal model computation + 10 iterations per xval model)
      super("GLM(" + data._key.toString() + ")",dest, (params._family == Family.gaussian)?data.chunks()*(xval+1):data.chunks()*(20+4*xval));
    }
    public boolean isDone(){return DKV.get(self()) == null;}
    @Override
    public float progress() {
      ChunkProgress progress = UKV.get(progressKey());
      return (progress != null ? progress.progress() : 0);
    }

  }

  public static class GLMParams extends Iced {
    public Family _family = Family.gaussian;
    public Link _link;
    public double _betaEps = 1e-4;
    public int _maxIter = 50;
    public double _caseVal;
    public double _caseWeight = 1.0;
    public CaseMode _caseMode = CaseMode.none;
    public boolean _reweightGram = true;

    public GLMParams(Family family){this(family,family.defaultLink);}

    public GLMParams(Family family, Link link){
      _family = family;
      _link = link;
    }

    public void checkResponseCol(Column ycol, ArrayList<String> warnings){
      switch(_family){
      case poisson:
        if(ycol._min < 0)
          throw new GLMException("Invalid response variable " + ycol._name + ", Poisson family requires response to be >= 0. ");
        if(ycol._domain != null && ycol._domain.length > 0)
          throw new GLMException("Invalid response variable " + ycol._name + ", Poisson family requires response to be integer number >= 0. Got categorical.");
        if(ycol.isFloat())
          warnings.add("Running family=Poisson on non-integer response column. Poisson is dicrete distribution, consider using gamma or gaussian instead.");
        break;
      case gamma:
        if(ycol._min <= 0)
          throw new GLMException("Invalid response variable " + ycol._name + ", Gamma family requires response to be > 0. ");
        if(ycol._domain != null && ycol._domain.length > 0)
          throw new GLMException("Invalid response variable " + ycol._name + ", Poisson family requires response to be integer number >= 0. Got categorical.");
        break;
      case binomial:
        if(_caseMode == CaseMode.none && (ycol._min < 0 || ycol._max > 1))
          if(ycol._min <= 0)
            throw new GLMException("Invalid response variable " + ycol._name + ", Binomial family requires response to be from [0,1] or have Case predicate. ");
        break;
      default:
        //pass
      }
    }

    public JsonObject toJson(){
      JsonObject res = new JsonObject();
      res.addProperty("family", _family.toString());
      res.addProperty("link", _link.toString());
      res.addProperty("betaEps", _betaEps);
      res.addProperty("maxIter", _maxIter);
      if(_caseMode != null && _caseMode != CaseMode.none){
        res.addProperty("caseVal",_caseMode.exp(_caseVal));
        res.addProperty("weight",_caseWeight);
      }
      return res;
    }
  }

  public enum CaseMode {
    none("n/a"),
    lt("<"),
    gt(">"),
    lte("<="),
    gte(">="),
    eq("="),
    neq("!="),
    ;
    final String _str;

    CaseMode(String str){
      _str = str;
    }
    public String toString(){
      return _str;
    }

    public String exp(double v){
      switch(this){
      case none:
        return "n/a";
      default:
        return "x" + _str + v;
      }
    }

    public final boolean isCase(double x, double y){
      switch(this){
      case lt:
        return x < y;
      case gt:
        return x > y;
      case lte:
        return x <= y;
      case gte:
        return x >= y;
      case eq:
        return x == y;
      case neq:
        return x != y;
      default:
        assert false;
        return false;
      }
    }
  }
  public static enum Link {
    familyDefault(0),
    identity(0),
    logit(0),
    log(0.1),
//    probit(0),
//    cauchit(0),
//    cloglog(0),
//    sqrt(0),
    inverse(0),
//    oneOverMu2(0);
    ;
    public final double defaultBeta;

    Link(double b){defaultBeta = b;}

    public final double link(double x){
      switch(this){
      case identity:
        return x;
      case logit:
        assert 0 <= x && x <= 1;
        return Math.log(x/(1 - x));
      case log:
        return Math.log(x);
      case inverse:
        double xx = (x < 0)?Math.min(-1e-5,x):Math.max(1e-5, x);
        return 1.0/xx;
      default:
        throw new Error("unsupported link function id  " + this);
      }
    }


    public final double linkInv(double x){
      switch(this){
      case identity:
        return x;
      case logit:
        return 1.0 / (Math.exp(-x) + 1.0);
      case log:
        return Math.exp(x);
      case inverse:
        double xx = (x < 0)?Math.min(-1e-5,x):Math.max(1e-5, x);
        return 1.0/xx;
      default:
        throw new Error("unexpected link function id  " + this);
      }
    }

    public final double linkInvDeriv(double x){
      switch(this){
        case identity:
          return 1;
        case logit:
          double g = Math.exp(-x);
          double gg = (g+1)*(g+1);
          return g /gg;
        case log:
          //return (x == 0)?MAX_SQRT:1/x;
          return Math.max(Math.exp(x), Double.MIN_NORMAL);
        case inverse:
          double xx = (x < 0)?Math.min(-1e-5,x):Math.max(1e-5, x);
          return -1/(xx*xx);
        default:
          throw new Error("unexpected link function id  " + this);
      }
    }
  }
  // helper function
  static final double y_log_y(double y, double mu){
    mu = Math.max(Double.MIN_NORMAL, mu);
    return (y != 0) ? (y * Math.log(y/mu)) : 0;
  }

  // supported families
  public static enum Family {
    gaussian(Link.identity,null),
    binomial(Link.logit,new double[]{Double.NaN,1.0,0.5}),
    poisson(Link.log,null),
    gamma(Link.inverse,null);
    public final Link defaultLink;
    public final double [] defaultArgs;
    Family(Link l, double [] d){defaultLink = l; defaultArgs = d;}

    public double mustart(double y){
      switch(this){
      case gaussian:
        return y;
      case binomial:
        return 0.5;
      case poisson:
        return y + 0.1;
      case gamma:
        return y;
      default:
        throw new Error("unimplemented");
      }
    }
    public double variance(double mu){
      switch(this){
      case gaussian:
        return 1;
      case binomial:
        assert 0 <= mu && mu <= 1:"unexpected mu:" + mu;
        return mu*(1-mu);
      case poisson:
        return mu;
      case gamma:
        return mu*mu;
      default:
        throw new Error("unknown family Id " + this);
      }
    }
  /**
  * Per family deviance computation.
  *
  * @param family
  * @param yr
  * @param ym
  * @return
  */
  public double deviance(double yr, double ym){
    switch(this){
      case gaussian:
        return (yr - ym)*(yr - ym);
      case binomial:
        return 2*((y_log_y(yr, ym)) + y_log_y(1-yr, 1-ym));
      case poisson:
        if(yr == 0)return 2*ym;
        return 2*((yr * Math.log(yr/ym)) - (yr - ym));
      case gamma:
        if(yr == 0)return -2;
        return -2*(Math.log(yr/ym) - (yr - ym)/ym);
      default:
        throw new Error("unknown family Id " + this);
      }
    }
  }

  public abstract GLMModel solve(GLMModel model, ValueArray ary);

  static final class Gram extends Iced {
    double [][] _xx;
    double   [] _xy;
    double      _yy;
    long        _nobs;
    public Gram(){}
    public Gram(int N){
      _xy = MemoryManager.malloc8d(N);
      _xx = new double[N][];
      for(int i = 0; i < N; ++i)
        _xx[i] = MemoryManager.malloc8d(i+1);
    }
    public double [][] getXX(){
      final int N = _xy.length;
      double [][] xx = new double[N][N];
      for( int i = 0; i < N; ++i ) {
        for( int j = 0; j < _xx[i].length; ++j ) {
            xx[i][j] = _xx[i][j];
            xx[j][i] = _xx[i][j];
        }
      }
      return xx;
    }
    public double [] getXY(){
      return _xy;
    }
    public double getYY(){return _yy;}

    public void add(Gram grm){
      final int N = _xy.length;
      assert N > 0;
      _yy += grm._yy;
      _nobs += grm._nobs;
      for(int i = 0; i < N; ++i){
        _xy[i] += grm._xy[i];
        if(_xx != null){
          final int n = _xx[i].length;
          for(int j = 0; j < n; ++j)
            _xx[i][j] += grm._xx[i][j];
        }
      }
    }

    public final boolean hasNaNsOrInfs() {
      for(int i = 0; i < _xy.length; ++i){
        if(Double.isInfinite(_xy[i]) || Double.isNaN(_xy[i]))
          return true;
        for(int j = 0; j < _xx[i].length; ++j)
          if(Double.isInfinite(_xx[i][j]) || Double.isNaN(_xx[i][j]))
            return true;
      }
      return false;
    }
  }

  private static class GLMXvalSetup extends Iced {
    final int _id;
    public GLMXvalSetup(int i){_id = i;}
  }
  private static class GLMXValTask extends MRTask {
    transient ValueArray _ary;
    Key _aryKey;
    Job _job;
    boolean _standardize;
    LSMSolver _lsm;
    GLMParams _glmp;
    double [] _betaStart;
    int [] _cols;
    double [] _thresholds;
    final int _folds;
    Key [] _models;
    boolean _parallel;

    public GLMXValTask(Job job, int folds, ValueArray ary, int [] cols, boolean standardize, LSMSolver lsm, GLMParams glmp, double [] betaStart, double [] thresholds, boolean parallel){
      _job = job;
      _folds = folds;
      _ary = ary; _aryKey = ary._key;
      _cols = cols;
      _standardize = standardize;
      _lsm = lsm;
      _glmp = glmp;
      _betaStart = betaStart;
      _thresholds = thresholds;
      _parallel = parallel;
    }

    @Override
    public void init() {
      super.init();
      _ary = DKV.get(_aryKey).get();
      _models = new Key[_folds];

    }

    @Override
    public void map(Key key) {
      GLMXvalSetup setup = DKV.get(key).get();
      Sampling s = new Sampling(setup._id,_folds,false);
      assert _models[setup._id] == null;
      _models[setup._id] = GLMModel.makeKey(false);
      try{
        DGLM.buildModel(_job, _models[setup._id],DGLM.getData(_ary, _cols, s, _standardize), _lsm, _glmp, _betaStart.clone(), 0, _parallel);
      } catch(JobCancelledException e) {
        UKV.remove(_models[setup._id]);
      }
      // cleanup before sending back
      DKV.remove(key);
      _betaStart = null;
      _lsm = null;
      _glmp = null;
      _cols = null;
      _aryKey = null;
    }

    @Override
    public void reduce(DRemoteTask drt) {
      GLMXValTask other = (GLMXValTask)drt;
      if(other._models != _models){
        for(int i = 0; i < _models.length; ++i)
          if(_models[i] == null)
            _models[i] = other._models[i];
          else
            assert other._models[i] == null;
      }
    }
  }

  public static class GLMModel extends water.Model {
    public enum Status {NotStarted,ComputingModel,ComputingValidation,Done,Cancelled,Error};
    String _error;
    final Sampling _s;
    final int [] _colCatMap;
    public final boolean _converged;
    public final boolean _standardized;

    public final int _iterations;     // Iterations used to solve
    public final long _time;          // Total solve time in millis
    public final LSMSolver _solver;   // Which solver is used
    public final GLMParams _glmParams;

    final public double [] _beta;            // The output coefficients!  Main model result.
    final public double [] _normBeta;        // normalized coefficients

    public String [] _warnings;
    public GLMValidation [] _vals;
    // Empty constructor for deseriaization

    Status _status;

    public Status status(){
      return _status;
    }
    public String error(){
      return _error;
    }
    public int rank(){
      if(_beta == null)return -1;
      int res = 0;
      for(double b:_beta)
        if(b != 0)++res;
      return res;
    }

    public boolean isSolved() { return _beta != null; }
    public static final String NAME = GLMModel.class.getSimpleName();
    public static final String KEY_PREFIX = "__GLMModel_";
    // Hand out the coffients.  Must be treated as a read-only array.
    public double[] beta() { return _beta; }
    // Warm-start setup; clone the incoming array since we will be mutating it

    public static final Key makeKey(boolean visible) {
      return visible?Key.make(KEY_PREFIX + Key.make()):Key.make(Key.make()._kb,(byte)0,Key.DFJ_INTERNAL_USER,H2O.SELF);
    }

    /**
     * Ids of selected columns (the last idx is the response variable) of the original dataset,
     * if it still exists in H2O, or null.
     *
     * @return array of column ids, the last is the response var.
     */
    public int [] selectedColumns(){
      if(DKV.get(_dataKey) == null) return null;
      ValueArray ary = DKV.get(_dataKey).get();
      HashSet<String> colNames = new HashSet<String>();
      for(int i = 0; i < _va._cols.length-1; ++i)
        colNames.add(_va._cols[i]._name);
      String responseCol = _va._cols[_va._cols.length-1]._name;
      int [] res = new int[colNames.size()+1];
      int j = 0;
      for(int i = 0; i < ary._cols.length; ++i)
        if(colNames.contains(ary._cols[i]._name))res[j++] = i;
        else if(ary._cols[i]._name.equals(responseCol))
          res[res.length-1] = i;
      return res;
    }

    /**
     * Expanded (categoricals expanded to vector of levels) ordered list of column names.
     * @return
     */
    public String xcolNames(){
      StringBuilder sb = new StringBuilder();
      for( ValueArray.Column C : _va._cols ) {
        if( C._domain != null )
          for(int i = 1; i < C._domain.length; ++i)
            sb.append(C._name).append('.').append(C._domain[i]).append(',');
        else
          sb.append(C._name).append(',');
      }
      sb.setLength(sb.length()-1); // Remove trailing extra comma
      return sb.toString();
    }

    @Override public void delete() {
      // nuke sub-models
      if(_vals != null)
        for(GLMValidation v:_vals)
          UKV.remove(v._key);
    }

    public GLMModel(){
      _status = Status.NotStarted;
      _colCatMap = null;
      _beta = null;
      _normBeta = null;
      _glmParams = null;
      _s = null;
      _standardized = false;
      _converged = false;
      _iterations = 0;
      _time = 0;
      _solver = null;
    }

    public GLMModel(Status status, float progress, Key k, DataFrame data, double [] beta, double [] normBeta, GLMParams glmp, LSMSolver solver, boolean converged, int iters, long time, String [] warnings){
      this(status, progress, k,data._ary, data._modelDataMap, data._colCatMap, data._standardized, data.getSampling(),beta,normBeta, glmp,solver,converged,iters,time,warnings);
    }

    public GLMModel(Status status, float progress, Key k, ValueArray ary, int [] colIds, int [] colCatMap, boolean standardized, Sampling s, double [] beta, double [] normBeta,  GLMParams glmp, LSMSolver solver, boolean converged, int iters, long time, String [] warnings){
      super(k,colIds, ary._key);
      _status = status;
      _colCatMap = colCatMap;
      _beta = beta;
      _normBeta = normBeta;
      _glmParams = glmp;
      _s = s;
      _standardized = standardized;
      _converged = converged;
      _iterations = iters;
      _time = time;
      _solver = solver;
      _warnings = warnings;
    }

    public boolean converged() { return _converged; }

    public void store() {
      UKV.put(_selfKey,this);
    }

    public void remove(){
      UKV.remove(_selfKey);
      if(_vals != null) for (GLMValidation val:_vals)
        if(val._modelKeys != null)for(Key k:val._modelKeys)
          UKV.remove(k);
    }
    // Validate on a dataset.  Columns must match, including the response column.
    public GLMValidation validateOn(Job job, ValueArray ary, Sampling s, double [] thresholds ) throws JobCancelledException {
      int [] modelDataMap = ary.getColumnIds(_va.colNames());//columnMapping(ary.colNames());
      if( !isCompatible(modelDataMap) ) // This dataset is compatible or not?
        throw new GLMException("incompatible dataset");
      DataFrame data = new DataFrame(ary, modelDataMap, s, false, true);
      GLMValidationFunc f = new GLMValidationFunc(_glmParams,_beta, thresholds,ary._cols[modelDataMap[modelDataMap.length-1]]._mean);
      GLMValidation val = f.apply(job,data);
      val._modelKey = _selfKey;
      if(_vals == null)
        _vals = new GLMValidation[]{val};
      else {
        int n = _vals.length;
        _vals = Arrays.copyOf(_vals, n+1);
        _vals[n] = val;
      }
      return val;
    }

    public GLMValidation xvalidate(Job job, ValueArray ary,int folds, double [] thresholds, boolean parallel) throws JobCancelledException {
      int [] modelDataMap = ary.getColumnIds(_va.colNames());//columnMapping(ary.colNames());
      if( !isCompatible(modelDataMap) )  // This dataset is compatible or not?
        throw new GLMException("incompatible dataset");
      final int myNodeId = H2O.SELF.index();
      final int cloudsize = H2O.CLOUD.size();
      Key [] keys = new Key[folds];
      for(int i = 0; i < folds; ++i)
        DKV.put(keys[i] = Key.make(Key.make()._kb,(byte)0,Key.DFJ_INTERNAL_USER,H2O.CLOUD._memary[(myNodeId + i)%cloudsize]), new GLMXvalSetup(i));
      DKV.write_barrier();
      GLMXValTask tsk = new GLMXValTask(job, folds, ary, modelDataMap, _standardized, _solver, _glmParams, _normBeta, thresholds, parallel);
      long t1 = System.currentTimeMillis();
      if(parallel)
        tsk.invoke(keys);
      else {
        tsk.keys(keys);
        tsk.init();
        for( int i = 0; i < keys.length; i++ ) {
          GLMXValTask child = new GLMXValTask(job, folds, ary, modelDataMap, _standardized, _solver, _glmParams, _normBeta, thresholds, parallel);
          child.keys(keys);
          child.init();
          child.map(keys[i]);
          tsk.reduce(child);
        }
      }
      if(job.cancelled())
        throw new JobCancelledException();
      GLMValidation res = new GLMValidation(_selfKey,tsk._models, ErrMetric.SUMC,thresholds, System.currentTimeMillis() - t1);
      if(_vals == null)_vals = new GLMValidation[]{res};
      else {
        _vals = Arrays.copyOf(_vals, _vals.length+1);
        _vals[_vals.length-1] = res;
      }
      return res;
    }

    public JsonObject toJson(){
      JsonObject res = new JsonObject();
      res.addProperty("time", _time);
      res.addProperty(Constants.MODEL_KEY, _selfKey.toString());
      if( _warnings != null ) {
        JsonArray warnings = new JsonArray();
        for( String w : _warnings )
          warnings.add(new JsonPrimitive(w));
        res.add("warnings", warnings);
      }
      if( _beta == null ) return res; // Not solved!

      // Get the coefficents out in a pretty format
      JsonObject coefs = new JsonObject();
      JsonObject normalizedCoefs = new JsonObject();
      int idx=0;
      JsonArray colNames = new JsonArray();
      for( int i=0; i<_va._cols.length-1; i++ ) {
        ValueArray.Column C = _va._cols[i];
        if( C._domain != null )
          for(int j = 1; j < C._domain.length;++j){
            String d = C._domain[j];
            String cname = C._name+"."+d;
            colNames.add(new JsonPrimitive(cname));
            if(_standardized)normalizedCoefs.addProperty(cname,_normBeta[idx]);
            coefs.addProperty(cname,_beta[idx++]);
          }
        else {
          colNames.add(new JsonPrimitive(C._name));
          if(_standardized)normalizedCoefs.addProperty(C._name,_normBeta[idx]);
          double b = _beta[idx];//*_normMul[idx];
          coefs.addProperty(C._name,b);
          //norm += b*_normSub[idx]; // Also accumulate the intercept adjustment
          idx++;
        }
      }
      res.add("column_names",colNames);
      if(_standardized)normalizedCoefs.addProperty("Intercept",_normBeta[_normBeta.length-1]);
      coefs.addProperty("Intercept",_beta[_beta.length-1]);
      res.add("coefficients", coefs);
      if(_standardized)res.add("normalized_coefficients", normalizedCoefs);
      res.add("LSMParams",_solver.toJson());
      res.add("GLMParams",_glmParams.toJson());
      res.addProperty("iterations", _iterations);
      if(_vals != null) {
        JsonArray vals = new JsonArray();
        for(GLMValidation v:_vals)
          vals.add(v.toJson());
        res.add("validations", vals);
      }
      return res;
    }

    /** Single row scoring, on properly ordered data.  Will return NaN if any
     *  data element contains a NaN.  */
    protected double score0( double[] data ) {
      double p = 0;             // Prediction; scored value
      for( int i = 0; i < data.length; i++ ) {
        int idx   =  _colCatMap[i  ];
        if( idx+1 == _colCatMap[i+1] ) { // No room for categories ==> numerical
          // No normalization???  These betas came from the JSON, not the
          // original model build.  The JSON has the beta's AFTER being
          // denormalized, so that the user-visible equation is to simply apply
          // the predictors directly (instead of normalizing them).
          double d = data[i];// - _normSub[idx]) * _normMul[idx];
          p += _beta[idx]*d;
        } else {
          int d = (int)data[i]; // Enum value
          idx += d;             // Which expanded column to use
          if( idx < _colCatMap[i+1] )
            p += _beta[idx]/* *1.0 */;
          else             // Enum out of range?
            p = Double.NaN;// Can use a zero, or a NaN
        }
      }
      p += _beta[_beta.length-1]; // And the intercept as the last beta
      double pp = _glmParams._link.linkInv(p);
      if( _glmParams._family == Family.binomial )
        return pp >= _vals[0].bestThreshold() ? 1.0 : 0.0;
      return pp;
    }

    /** Single row scoring, on a compatible ValueArray (when pushed throw the mapping) */
    protected double score0( ValueArray data, int row, int[] mapping ) {
      throw H2O.unimpl();
    }

    /** Bulk scoring API, on a compatible ValueArray (when pushed throw the mapping) */
    protected double score0( ValueArray data, AutoBuffer ab, int row_in_chunk, int[] mapping ) {
      throw H2O.unimpl();
    }
  }


  public static class GLMValidation extends Iced {
    public final Key [] _modelKeys; // Multiple models for n-fold cross-validation
    public static final String KEY_PREFIX = "__GLMValidation_";
    Key _key;
    Key _dataKey;
    Key _modelKey;
    Sampling _s;
    public long _n;
    public long _caseCount;
    public long _dof;
    public double _aic;
    public double _deviance;
    public double _nullDeviance;
    public double _err;
    ErrMetric _errMetric = ErrMetric.SUMC;
    double _auc = Double.NaN;
    public ConfusionMatrix [] _cm;
    int _tid;
    double [] _thresholds;
    long _time;

    public final long computationTime(){
      return _time;
    }

    public GLMValidation(){_modelKeys = null;}
    public GLMValidation(Key modelKey, Key [] modelKeys, ErrMetric m, double [] thresholds, long time) {
      _time = time;
      _errMetric = m;
      _modelKey = modelKey;
      GLMModel [] models = new GLMModel[modelKeys.length];
      for (int i = 0; i < models.length; ++i)
        models[i] = DKV.get(modelKeys[i]).get();
      _dataKey = models[0]._dataKey;
      _modelKeys = new Key[models.length];
      int i = 0;
      boolean solved = true;
      for(GLMModel xm:models){
        _modelKeys[i++] = xm._selfKey;
        if(!xm.isSolved())solved = false;
      }
      if( !solved ) {
        _aic = Double.NaN;
        _dof = -1;
        _auc = Double.NaN;
        _deviance = Double.NaN;
        _nullDeviance = Double.NaN;
        _err = Double.NaN;
        _n = -1;
        return;
      }
      long n = 0;
      double nDev = 0;
      double dev = 0;
      double aic = 0;
      double err = 0;
      int rank = -1;
      if(models[0]._vals[0]._cm != null){
        int nthresholds = models[0]._vals[0]._cm.length;
        _cm = new ConfusionMatrix[nthresholds];
        for(int t = 0; t < nthresholds; ++t)
          _cm[t] = models[0]._vals[0]._cm[t].clone();
        n += models[0]._vals[0]._n;
        dev = models[0]._vals[0]._deviance;
        rank = models[0].rank();
        aic = models[0]._vals[0]._aic - 2*rank;
        _auc = models[0]._vals[0]._auc;
        nDev = models[0]._vals[0]._nullDeviance;
        for(i = 1; i < models.length; ++i){
          int xm_rank = models[i].rank();
          rank = Math.max(xm_rank,rank);
          n += models[i]._vals[0]._n;
          dev += models[i]._vals[0]._deviance;
          aic += models[i]._vals[0]._aic - 2*xm_rank;
          nDev += models[i]._vals[0]._nullDeviance;
          _auc += models[i]._vals[0]._auc;
          for(int t = 0; t < nthresholds; ++t)
            _cm[t].add(models[i]._vals[0]._cm[t]);
        }
        _thresholds = thresholds;
        computeBestThreshold(m);
        _auc /= models.length;
      } else {
        for(GLMModel xm:models) {
          int xm_rank = xm.rank();
          rank = Math.max(xm_rank,rank);
          n += xm._vals[0]._n;
          dev += xm._vals[0]._deviance;
          nDev += xm._vals[0]._nullDeviance;
          err += xm._vals[0]._err;
          aic += (xm._vals[0]._aic - 2*xm_rank);
        }
      }
      _err = err/models.length;
      _deviance = dev;
      _nullDeviance = nDev;
      _n = n;
      _aic = aic + 2*rank;
      _dof = _n - models[0]._beta.length - 1;
    }

    public Key dataKey() {return _dataKey;}
    public Key modelKey() {return _modelKey;}

    public Iterable<GLMModel> models(){
      final Key [] keys = _modelKeys;
      return new Iterable<GLMModel> (){
        int idx;
        @Override
        public Iterator<GLMModel> iterator() {
          return new Iterator<GLMModel>() {
            @Override
            public void remove() {throw new UnsupportedOperationException();}
            @Override
            public GLMModel next() {
              if(idx == keys.length) throw new NoSuchElementException();
              return DKV.get(keys[idx++]).get();
            }
              @Override
            public boolean hasNext() {
              return idx < keys.length;
            }
          };
        }
      };
    }

    public int fold(){
      return (_modelKeys == null)?1:_modelKeys.length;
    }


    public ConfusionMatrix bestCM(){
      if(_cm == null)return null;
      return bestCM(ErrMetric.SUMC);
    }

    public double err() {
      if(_cm != null)return bestCM().err();
      return _err;
    }
    public ConfusionMatrix bestCM(ErrMetric errM){
      computeBestThreshold(errM);
      return _cm[_tid];
    }

    public double bestThreshold() {
      return (_thresholds != null)?_thresholds[_tid]:0;
    }
    public void computeBestThreshold(ErrMetric errM){
      if(_cm == null)return;
      double e = errM.computeErr(_cm[0]);
      _tid = 0;
      for(int i = 1; i < _cm.length; ++i){
        double r = errM.computeErr(_cm[i]);
        if(r < e){
          e = r;
          _tid = i;
        }
      }
    }


    double [] err(int c) {
      double [] res = new double[_cm.length];
      for(int i = 0; i < res.length; ++i)
        res[i] = _cm[i].classErr(c);
      return res;
    }

    double err(int c, int threshold) {
      return _cm[threshold].classErr(c);
    }

    public double [] classError() {
      return _cm[_tid].classErr();
    }
    private double trapeziod_area(double x1, double x2, double y1, double y2){
      double base = Math.abs(x1-x2);
      double havg = 0.5*(y1 + y2);
      return base*havg;
    }

    public double AUC(){
      return _auc;
    }

    /**
     * Computes area under the ROC curve.
     * The ROC curve is computed from the confusion matrices (there is one for
     * each computed threshold).  Area under this curve is then computed as a
     * sum of areas of trapezoids formed by each neighboring points.
     *
     * @return estimate of the area under ROC curve of this classifier.
     */
    protected void computeAUC() {
      if(_cm == null)return;
      double auc = 0;           // Area-under-ROC
      double TPR_pre = 1;
      double FPR_pre = 1;
      for(int t = 0; t < _cm.length; ++t){
        double TPR = 1 - _cm[t].classErr(1); // =TP/(TP+FN) = true -positive-rate
        double FPR =     _cm[t].classErr(0); // =FP/(FP+TN) = false-positive-rate
        auc += trapeziod_area(FPR_pre, FPR, TPR_pre, TPR);
        TPR_pre = TPR;
        FPR_pre = FPR;
      }
      auc += trapeziod_area(FPR_pre, 0, TPR_pre, 0);
      _auc = auc;
    }


    public JsonObject toJson() {
      JsonObject res = new JsonObject();
      if(_dataKey != null)
        res.addProperty("dataset", _dataKey.toString());
      else
        res.addProperty("dataset", "");
      if(_s != null)
        res.addProperty("sampling", _s.toString());
      res.addProperty("nrows", _n);
      res.addProperty("dof", _dof);
      res.addProperty("resDev", _deviance);
      res.addProperty("nullDev", _nullDeviance);
      if(!Double.isNaN(_auc))res.addProperty("auc", _auc);
      if(!Double.isNaN(_aic))res.addProperty("aic", _aic);

      if(_cm != null) {
        double [] err = _cm[_tid].classErr();
        JsonArray arr = new JsonArray();
        for(int i = 0; i < err.length; ++i)
          arr.add(new JsonPrimitive(err[i]));
        res.add("classErr", arr);
        res.addProperty("err", err());
        res.addProperty("threshold", _thresholds[_tid]);
        res.add("cm", _cm[_tid].toJson());
      } else
        res.addProperty("err", _err);
      if(_modelKeys != null){
        JsonArray arr = new JsonArray();
        for(Key k:_modelKeys)
          arr.add(new JsonPrimitive(k.toString()));
        res.add("xval_models", arr);
      }
      return res;
    }

    public double AIC() {
      return _aic;
    }
  }

  public static class GramMatrixFunc extends RowFunc<Gram>{
    final int _dense;
    final int _N;
    final long _nobs; // number of observations in the dataset
    boolean _computeXX = true;
    final boolean _weighted;
    final Family _family;
    final Link _link;
    double [] _beta;
    final CaseMode _cMode;
    final double _cVal;

    public GramMatrixFunc(DataFrame data, GLMParams glmp, double [] beta){
      _nobs = data._nobs;
      _beta = beta;
      _dense = data.dense();
      _weighted = glmp._family != Family.gaussian;
      _family = glmp._family;
      _link = glmp._link;
      _cMode = glmp._caseMode;
      _cVal = glmp._caseVal;
      _N = data.expandedSz();
    }

    @Override
    public Gram newResult(){
      if(_computeXX)return new Gram(_N);
      // else we do not have to allocate XX
      Gram res = new Gram();
      res._xy = MemoryManager.malloc8d(_N);
      return res;
    }

    @Override
    public long memReq(){return ValueArray.CHUNK_SZ + ((_N*_N  + _N) * 8);}

    public final double computeEta(double[] x, int[] indexes){
      double mu = 0;
      for(int i = 0; i < _dense; ++i)
        mu += x[i]*_beta[i];
      for(int i = _dense; i < indexes.length; ++i)
        mu += x[i]*_beta[indexes[i]];
      return mu;
    }



    @Override
    public final void processRow(Gram gram, double[] x,int[] indexes){
      final int yidx = x.length-1;
      double y = x[yidx];
      assert ((_family != Family.gamma) || y > 0):"illegal response column, y must be > 0  for family=Gamma.";
      x[yidx] = 1; // put intercept in place of y
      if(_cMode != CaseMode.none)
        y = (_cMode.isCase(y,_cVal))?1:0;
      double w = 1;
      if(_weighted) {
        double eta,mu,var;
        if(_beta == null){
          mu = _family.mustart(y);
          eta = _link.link(mu);
        } else {
          eta = computeEta(x,indexes);
          mu = _link.linkInv(eta);
        }
        var = Math.max(1e-5, _family.variance(mu)); // avoid numerical problems with 0 variance
        if(_family == Family.binomial || _family == Family.poisson){
          w = var;
          y = eta + (y-mu)/var;
        } else {
          double dp = _link.linkInvDeriv(eta);
          w = dp*dp/var;
          y = eta + (y - mu)/dp;
        }
      }
      assert w >= 0:"invalid weight " + w;
      gram._yy += 0.5*w*y*y;
      ++gram._nobs;
      for(int i = 0; i < _dense; ++i){
        if(_computeXX) for(int j = 0; j <= i; ++j)
          gram._xx[i][j] += w*x[i]*x[j];
        gram._xy[i] += w*y*x[i];
      }
      for(int i = _dense; i < indexes.length; ++i){
        int ii = indexes[i];
        if(_computeXX) {
          for(int j = 0; j < _dense; ++j)
            gram._xx[ii][j] += w*x[i]*x[j];
          for(int j = _dense; j <= i; ++j)
            gram._xx[ii][indexes[j]] += w*x[i]*x[j];
        }
        gram._xy[ii] += w*y*x[i];
      }
    }
    @Override
    public Gram reduce(Gram x, Gram y) {
      assert x != y;
      x.add(y);
      return x;
    }

    @Override public Gram result(Gram g){
      final int N = g._xy.length;
      double nobsInv = 1.0/_nobs;
      for(int i = 0; i < N; ++i){
        g._xy[i] *= nobsInv;
        if(g._xx != null) for(int j = 0; j < g._xx[i].length; ++j)
          g._xx[i][j] *= nobsInv;
      }
      g._yy *= nobsInv;
      return g;
    }
  }

  public static class GLMValidationFunc extends RowFunc<GLMValidation> {
    final GLMParams _glmp;
    final double [] _beta;
    final double [] _thresholds;
    final double    _ymu;

    public GLMValidationFunc(GLMParams params, double [] beta, double [] thresholds, double ymu){
      _glmp = params;
      _beta = beta;
      _thresholds = Objects.firstNonNull(thresholds,DEFAULT_THRESHOLDS);
      _ymu = ymu;
    }

    @Override
    public GLMValidation apply(Job job,DataFrame data) throws JobCancelledException {
      long t1 = System.currentTimeMillis();
      NewRowVecTask<GLMValidation> tsk = new NewRowVecTask<GLMValidation>(job,this, data);
      tsk.invoke(data._ary._key);
      if(job != null && job.cancelled())
        throw new JobCancelledException();
      GLMValidation res = tsk._result;
      res._time = System.currentTimeMillis()-t1;
      if(_glmp._family != Family.binomial)
        res._err = Math.sqrt(res._err/res._n);
      res._dataKey = data._ary._key;
      res._thresholds = _thresholds;
      res._s = data.getSampling();
      res.computeBestThreshold(ErrMetric.SUMC);
      res.computeAUC();
      switch(_glmp._family) {
      case gaussian:
        res._aic =  res._n*(Math.log(res._deviance/res._n * 2 *Math.PI)+1)+2;
        break;
      case binomial:
        res._aic = res._deviance;
        break;
      case poisson:
        res._aic *= -2;
        break; // aic is set during the validation task
      case gamma:
        res._aic = Double.NaN;
        break; // aic for gamma is not computed
      default:
        assert false:"missing implementation for family " + _glmp._family;
      }
      res._aic += 2*_beta.length;//_glmp._family.aic(res._deviance, res._n, _beta.length);
      return res;
    }

    @Override
    public GLMValidation newResult() {
      GLMValidation res = new GLMValidation();
      if(_glmp._family == Family.binomial){
        res._cm = new ConfusionMatrix[_thresholds.length];
        for(int i = 0; i < _thresholds.length; ++i)
          res._cm[i] = new ConfusionMatrix(2);
      }
      return res;
    }

    @Override
    public void processRow(GLMValidation res, double[] x, int[] indexes) {
      ++res._n;
      double yr = x[x.length-1];
      x[x.length-1] = 1.0;
      if(_glmp._caseMode != CaseMode.none)
        yr = (_glmp._caseMode.isCase(yr, _glmp._caseVal))?1:0;
      if(yr == 1)
        ++res._caseCount;
      double ym = 0;
      for(int i = 0; i < x.length; ++i)
        ym += _beta[indexes[i]] * x[i];
      ym = _glmp._link.linkInv(ym);
      res._deviance += _glmp._family.deviance(yr, ym);
      res._nullDeviance += _glmp._family.deviance(yr, _ymu);
      if(_glmp._family == Family.poisson) { // aic for poisson
        res._err += (ym - yr)*(ym - yr);
        long y = Math.round(yr);
        double logfactorial = 0;
        for(long i = 2; i <= y; ++i)logfactorial += Math.log(i);
        res._aic += (yr*Math.log(ym) - logfactorial - ym);
      } else if(_glmp._family == Family.binomial) { // cm computation for binomial
        if(yr < 0 || yr > 1 )
          throw new Error("response variable value out of range: " + yr);
        int i = 0;
        for(double t:_thresholds){
          int p = ym >= t?1:0;
          res._cm[i++].add((int)yr,p);
        }
      } else
        res._err += (ym - yr)*(ym - yr);
    }

    @Override
    public GLMValidation reduce(GLMValidation x, GLMValidation y) {
      x._n += y._n;
      x._nullDeviance += y._nullDeviance;
      x._deviance += y._deviance;
      x._aic += y._aic;
      x._err += y._err;
      x._caseCount += y._caseCount;
      if(x._cm != null) {
        for(int i = 0; i < _thresholds.length; ++i)
          x._cm[i].add(y._cm[i]);
      } else
        x._cm = y._cm;
      return x;
    }
  }

  private static double betaDiff(double [] b1, double [] b2){
    double res = Math.abs(b1[0] - b2[0]);
    for(int i = 1; i < b1.length; ++i)
      res = Math.max(res,Math.abs(b1[i] - b2[i]));
    return res;
  }


  public static DataFrame getData(ValueArray ary, int [] xs, int y, Sampling s, boolean standardize){
    int [] colIds = Arrays.copyOf(xs, xs.length+1);
    colIds[xs.length] = y;
    return getData(ary, colIds, s, standardize);
  }
  public static DataFrame getData(ValueArray ary, int [] colIds, Sampling s, boolean standardize){
    ArrayList<Integer> numeric = new ArrayList<Integer>();
    ArrayList<Integer> categorical = new ArrayList<Integer>();
    for(int i = 0; i < colIds.length-1; ++i){
      int c = colIds[i];
      if(ary._cols[c]._domain != null)
        categorical.add(c);
      else
        numeric.add(c);
    }
    if(!categorical.isEmpty() && !numeric.isEmpty()){
      int idx = 0;
      for(int i:numeric)colIds[idx++] = i;
      for(int i:categorical)colIds[idx++] = i;
    }
    return new DataFrame(ary, colIds, s, standardize, true);
  }

  public static GLMJob startGLMJob(final DataFrame data, final LSMSolver lsm, final GLMParams params, final double [] betaStart, final int xval, final boolean parallel) {
    final GLMJob job = new GLMJob(data._ary,GLMModel.makeKey(true),xval,params);
    final double [] beta;
    final double [] denormalizedBeta;
    if(betaStart != null){
      beta = betaStart.clone();
      denormalizedBeta = data.denormalizeBeta(beta);
    } else {
      beta = denormalizedBeta = null;
    }
    UKV.put(job.dest(), new GLMModel(Status.ComputingModel,0.0f,job.dest(),data, denormalizedBeta, beta, params, lsm, false, 0, 0, null));
    job.start();
    H2O.submitTask(new H2OCountedCompleter() {
        @Override public void compute2() {
          try{
            buildModel(job, job.dest(), data, lsm, params, beta, xval, parallel);
            assert !job.cancelled();
            job.remove();
          } catch(JobCancelledException e){
            UKV.remove(job.dest());
          }
          tryComplete();
        }
        @Override
        public boolean onExceptionalCompletion(Throwable ex, CountedCompleter caller) {
          if(job != null) job.onException(ex);
          return super.onExceptionalCompletion(ex, caller);
        }
    });
    return job;
  }

  public static GLMModel buildModel(Job job, Key resKey, DataFrame data, LSMSolver lsm, GLMParams params, double [] oldBeta, int xval, boolean parallel) throws JobCancelledException {
    GLMModel currentModel = null;
    ArrayList<String> warns = new ArrayList<String>();
    long t1 = System.currentTimeMillis();
    // make sure we have valid response variable for the current family
    Column ycol = data._ary._cols[data._modelDataMap[data._modelDataMap.length-1]];
    params.checkResponseCol(ycol,warns);
    // filter out constant columns...
    GramMatrixFunc gramF = new GramMatrixFunc(data, params, oldBeta);
    double [] newBeta = MemoryManager.malloc8d(data.expandedSz());
    boolean converged = true;
    Gram gram = gramF.apply(job,data);
    int iter = 1;
    try {
      lsm.solve(gram.getXX(), gram.getXY(), gram.getYY(), newBeta);
    } catch (NonSPDMatrixException e) {
      if(!(lsm instanceof GeneralizedGradientSolver)){ // if we failed with ADMM, try Generalized gradient
        lsm = new GeneralizedGradientSolver(lsm._lambda, lsm._alpha);
        warns.add("Switched to generalized gradient solver due to Non SPD matrix.");
        lsm.solve(gram.getXX(), gram.getXY(), gram.getYY(), newBeta);
      }
    }
    if(params._family == Family.gaussian) {
       currentModel = new GLMModel(Status.ComputingValidation, 0.0f,resKey,data, data.denormalizeBeta(newBeta), newBeta, params, lsm, converged, iter, System.currentTimeMillis() - t1, null);
    } else do{ // IRLSM
      if(oldBeta == null)
        oldBeta = MemoryManager.malloc8d(data.expandedSz());
      if(job.cancelled())
        throw new JobCancelledException();
      double [] b = oldBeta;
      oldBeta = (gramF._beta = newBeta);
      newBeta = b;
      gram = gramF.apply(job,data);
      if(gram.hasNaNsOrInfs()) // we can't solve this problem any further, user should increase regularization and try again
        break;
      try {
        lsm.solve(gram.getXX(), gram.getXY(), gram.getYY(), newBeta);
      } catch (NonSPDMatrixException e) {
        if(!(lsm instanceof GeneralizedGradientSolver)){ // if we failed with ADMM, try Generalized gradient
          lsm = new GeneralizedGradientSolver(lsm._lambda, lsm._alpha);
          warns.add("Switched to generalized gradient solver due to Non SPD matrix.");
          lsm.solve(gram.getXX(), gram.getXY(), gram.getYY(), newBeta);
        }
      }
      String [] warnings = new String[warns.size()];
      warns.toArray(warnings);
      double betaDiff = betaDiff(oldBeta,newBeta);
      converged = (betaDiff < params._betaEps);
      float progress = Math.max((float)iter/params._maxIter,Math.min((float)(params._betaEps/betaDiff),1.0f));
      currentModel = new GLMModel(Status.ComputingModel,progress,resKey,data, data.denormalizeBeta(newBeta), newBeta, params, lsm, converged, iter, System.currentTimeMillis() - t1, warnings);
      currentModel.store();
    } while( ++iter < params._maxIter && !converged );
    currentModel._status = Status.ComputingValidation;
    currentModel.store();
    if( xval > 1 ) // ... and x-validate
      currentModel.xvalidate(job,data._ary,xval,DEFAULT_THRESHOLDS,parallel);
    else
      currentModel.validateOn(job,data._ary, data.getSamplingComplement(),DEFAULT_THRESHOLDS); // Full scoring on original dataset
    currentModel._status = Status.Done;
    String [] warnings = new String[warns.size()];
    warns.toArray(warnings);
    currentModel.store();
    DKV.write_barrier();
    return currentModel;
  }

  static double [] DEFAULT_THRESHOLDS = new double [] {
    0.00, 0.01, 0.02, 0.03, 0.04, 0.05, 0.06, 0.07, 0.08, 0.09,
    0.10, 0.11, 0.12, 0.13, 0.14, 0.15, 0.16, 0.17, 0.18, 0.19,
    0.20, 0.21, 0.22, 0.23, 0.24, 0.25, 0.26, 0.27, 0.28, 0.29,
    0.30, 0.31, 0.32, 0.33, 0.34, 0.35, 0.36, 0.37, 0.38, 0.39,
    0.40, 0.41, 0.42, 0.43, 0.44, 0.45, 0.46, 0.47, 0.48, 0.49,
    0.50, 0.51, 0.52, 0.53, 0.54, 0.55, 0.56, 0.57, 0.58, 0.59,
    0.60, 0.61, 0.62, 0.63, 0.64, 0.65, 0.66, 0.67, 0.68, 0.69,
    0.70, 0.71, 0.72, 0.73, 0.74, 0.75, 0.76, 0.77, 0.78, 0.79,
    0.80, 0.81, 0.82, 0.83, 0.84, 0.85, 0.86, 0.87, 0.88, 0.89,
    0.90, 0.91, 0.92, 0.93, 0.94, 0.95, 0.96, 0.97, 0.98, 0.99,
    1.00
  };
}
