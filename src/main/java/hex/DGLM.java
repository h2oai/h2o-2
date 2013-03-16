package hex;

import hex.ConfusionMatrix.ErrMetric;
import hex.DLSM.ADMMSolver.NonSPDMatrixException;
import hex.DLSM.GeneralizedGradientSolver;
import hex.DLSM.LSMSolver;
import hex.NewRowVecTask.DataFrame;
import hex.NewRowVecTask.RowFunc;
import hex.RowVecTask.Sampling;

import java.util.*;

import water.*;
import water.ValueArray.Column;
import water.api.Constants;

import com.google.gson.*;




public abstract class DGLM {
  public static final int DEFAULT_MAX_ITER = 50;
  public static final double DEFAULT_BETA_EPS = 1e-4;

  public static class GLMException extends RuntimeException {
    public GLMException(String msg){super(msg);}
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

    public void checkResponseCol(Column ycol){
      switch(_family){
      case poisson:
        if(ycol._min < 0)
          throw new GLMException("Invalid response variable " + ycol._name + ", Poisson family requires response to be >= 0. ");
        break;
      case gamma:
        if(ycol._min <= 0)
          throw new GLMException("Invalid response variable " + ycol._name + ", Gamma family requires response to be > 0. ");
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
    public double aic(double dev, long nobs, int betaLen){
      switch(this){
      case gaussian:
        return nobs *(Math.log(dev/nobs * 2 *Math.PI)+1)+2 + 2*betaLen;
      case binomial:
        return 2*betaLen + dev;
      case poisson:
        return 2*betaLen + dev;
      case gamma:
        return Double.NaN;
      default:
        throw new Error("unknown family Id " + this);
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
        //return -2*(yr * ym - Math.log(1 + Math.exp(ym)));
      case poisson:
        //ym = Math.exp(ym);
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

  public static class GLMModel extends Model {
    final Sampling _s;
    final int [] _colCatMap;
    public final boolean _converged;
    public final boolean _standardized;

    public final int _iterations;     // Iterations used to solve
    public final long _time;          // Total solve time in millis
    public final LSMSolver _solver; // Which solver is used
    public final GLMParams _glmParams;

    public transient int _responseCol; // Response column

    final double [] _beta;            // The output coefficients!  Main model result.
    final double [] _normBeta;        // normalized coefficients

    public String [] _warnings;
    public GLMValidation [] _vals;
    // Empty constructor for deseriaization


    public boolean isSolved() { return _beta != null; }
    public static final String KEY_PREFIX = "__GLMModel_";
    // Hand out the coffients.  Must be treated as a read-only array.
    public double[] beta() { return _beta; }
    // Warm-start setup; clone the incoming array since we will be mutating it

    public static final Key makeKey() {
      return Key.make(KEY_PREFIX + Key.make());
    }


    /**
     * Non expanded ordered list of names of selected columns.
     *
     * @return
     */
    public String selectedCols(){
      StringBuilder sb = new StringBuilder();
      for( ValueArray.Column C : _va._cols ) {
        sb.append(C._name).append(',');
      }
      sb.setLength(sb.length()-1); // Remove trailing extra comma
      return sb.toString();
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

    public GLMModel(DataFrame data, double [] beta, double [] normBeta, GLMParams glmp, LSMSolver solver, boolean converged, int iters, long time, String [] warnings){
      this(data._ary, data._modelDataMap, data._colCatMap, data._standardized, data.getSampling(),beta,normBeta, glmp,solver,converged,iters,time,warnings);
    }

    public GLMModel(ValueArray ary, int [] colIds, int [] colCatMap, boolean standardized, Sampling s, double [] beta, double [] normBeta,  GLMParams glmp, LSMSolver solver, boolean converged, int iters, long time, String [] warnings){
      super(makeKey(),colIds, ary._key);
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





    // Validate on a dataset.  Columns must match, including the response column.
    public GLMValidation validateOn( ValueArray ary, Sampling s, double [] thresholds ) {
      int[] modelDataMap = columnMapping(ary.colNames());
      if( !isCompatible(modelDataMap) ) // This dataset is compatible or not?
        throw new GLMException("incompatible dataset");
      DataFrame data = new DataFrame(ary, modelDataMap, s, false, true);
      GLMValidationFunc f = new GLMValidationFunc(_glmParams,_beta, thresholds,ary._cols[modelDataMap[modelDataMap.length-1]]._mean);
      GLMValidation val = f.apply(data);
      val._modelKey = _selfKey;
      if(_vals == null)
        _vals = new GLMValidation[]{val};
      else {
        int n = _vals.length;
        _vals = Arrays.copyOf(_vals, n+1);
        _vals[n] = val;
      }
      UKV.put(_selfKey,this);
      return val;
    }

    public GLMValidation xvalidate(ValueArray ary,int folds, double [] thresholds) {
      String[] colNames = ary.colNames();
      int[] modelDataMap = columnMapping(colNames);
      if( !isCompatible(modelDataMap) ) // This dataset is compatible or not?
        throw new GLMException("incompatible dataset");;

      GLMModel [] models = new GLMModel[folds];
      for(int i = 0; i < folds; ++i){
        models[i] = buildModel(DGLM.getData(ary, modelDataMap, new Sampling(i,folds,false),true), _solver, _glmParams);
        if(models[i].isSolved())
          models[i].validateOn(ary, new Sampling(i, folds, true),thresholds);
      }
      GLMValidation res = new GLMValidation(models, ErrMetric.SUMC,thresholds);
      if(_vals == null)_vals = new GLMValidation[]{res};
      else {
        _vals = Arrays.copyOf(_vals, _vals.length+1);
        _vals[_vals.length-1] = res;
      }
      UKV.put(_selfKey,this);
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
    double _auc;
    public ConfusionMatrix [] _cm;
    int _tid;
    double [] _thresholds;

    public GLMValidation(){_modelKeys = null;}
    public GLMValidation(GLMParams glmp, long n, long dof, double dev, double nullDev, double err){
      this(glmp, n,dof,dev,nullDev,err,null,null);
    }

    public GLMValidation(GLMParams glmp, long n, long dof, double dev, double nullDev, double err, double [] thresholds, ConfusionMatrix [] cms) {
      _modelKeys = null;
      _n = n;
      _dof = dof;
      _deviance = dev;
      _nullDeviance = nullDev;
      _aic = glmp._family.aic(_deviance, _n, (int)(_n - dof));
      _err = err;
      _thresholds = thresholds;
      _cm = cms;
    }

    public GLMValidation(GLMModel [] models, ErrMetric m, double [] thresholds) {
      _errMetric = m;
      _modelKey = models[0]._selfKey;
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
      double err = 0;
      if(models[0]._vals[0]._cm != null){
        int nthresholds = models[0]._vals[0]._cm.length;
        _cm = new ConfusionMatrix[nthresholds];
        for(int t = 0; t < nthresholds; ++t)
          _cm[t] = models[0]._vals[0]._cm[t];
        n += models[0]._vals[0]._n;
        dev = models[0]._vals[0]._deviance;
        nDev = models[0]._vals[0]._nullDeviance;
        for(i = 1; i < models.length; ++i){
          n += models[i]._vals[0]._n;
          dev += models[0]._vals[0]._deviance;
          nDev += models[0]._vals[0]._nullDeviance;
          for(int t = 0; t < nthresholds; ++t)
            _cm[t].add(models[i]._vals[0]._cm[t]);
        }
        _thresholds = thresholds;
        computeBestThreshold(m);
        computeAUC();
      } else {
        for(GLMModel xm:models){
          n += xm._vals[0]._n;
          dev += xm._vals[0]._deviance;
          nDev += xm._vals[0]._nullDeviance;
          err += xm._vals[0]._err;
        }
      }
      _err = err;
      _deviance = dev;
      _nullDeviance = nDev;
      _n = n;
      _aic = models[0]._glmParams._family.aic(_deviance, _n, models[0]._beta.length);
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
              return DKV.get(keys[idx++]).get(new GLMModel());
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
      res.addProperty("auc", _auc);

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
      _thresholds = thresholds;
      _ymu = ymu;
    }

    public GLMValidation apply(DataFrame data){
      NewRowVecTask<GLMValidation> tsk = new NewRowVecTask<GLMValidation>(this, data);
      tsk.invoke(data._ary._key);
      GLMValidation res = tsk._result;
      res._dataKey = data._ary._key;
      res._thresholds = _thresholds;
      res._s = data.getSampling();
      res.computeBestThreshold(ErrMetric.SUMC);
      res.computeAUC();
      res._aic = _glmp._family.aic(res._deviance, res._n, _beta.length);
      return res;
    }

    @Override
    public GLMValidation newResult() {
      GLMValidation res = new GLMValidation();
      if(_thresholds != null){
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
      if(_glmp._family == Family.binomial) {
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

  public static GLMModel buildModel(DataFrame data, LSMSolver lsm, GLMParams params) {
//    double [] beta = new double[data.expandedSz()];
//    Arrays.fill(beta, params._link.defaultBeta);
//    if(params._family == Family.gamma)beta[beta.length-1] = 1;
    return buildModel(data, lsm, params, null);
  }


  public static GLMModel buildModel(DataFrame data, LSMSolver lsm, GLMParams params, double [] oldBeta) {
    long t1 = System.currentTimeMillis();
    // make sure we have valid response variable for the current family
    Column ycol = data._ary._cols[data._modelDataMap[data._modelDataMap.length-1]];
    params.checkResponseCol(ycol);
    // filter out constant columns...
    GramMatrixFunc gramF = new GramMatrixFunc(data, params, oldBeta);
    double [] newBeta = MemoryManager.malloc8d(data.expandedSz());
    ArrayList<String> warns = new ArrayList<String>();
    boolean converged = true;
    Gram gram = gramF.apply(data);
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
    if(params._family != Family.gaussian) { // IRLSM
      if(oldBeta == null)oldBeta = MemoryManager.malloc8d(data.expandedSz());
      do{
        double [] b = oldBeta;
        oldBeta = (gramF._beta = newBeta);
        newBeta = b;
        gram = gramF.apply(data);
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
      } while(++iter < params._maxIter && betaDiff(newBeta,oldBeta) > params._betaEps);
      converged = converged && (betaDiff(oldBeta,newBeta) < params._betaEps);
    }
    String [] warnings = new String[warns.size()];
    warns.toArray(warnings);
    double [] standardizedBeta = newBeta;
    if(data._standardized){ // denormalize coefficients
      newBeta = newBeta.clone();
      double norm = 0.0;        // Reverse any normalization on the intercept
      for( int i=0; i<newBeta.length-1; i++ ) {
        double b = newBeta[i]*data._normMul[i];
        norm += b*data._normSub[i]; // Also accumulate the intercept adjustment
        newBeta[i] = b;
      }
      newBeta[newBeta.length-1] -= norm;
    }
    return new GLMModel(data, newBeta, standardizedBeta, params, lsm, converged, iter, System.currentTimeMillis() - t1, warnings);
  }
}
