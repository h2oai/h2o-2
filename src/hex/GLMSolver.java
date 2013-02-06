package hex;

import Jama.Matrix;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.gson.*;
import hex.LSMSolver.LSMSolverException;
import hex.RowVecTask.Sampling;
import java.util.*;
import water.*;
import water.ValueArray.Column;
import water.api.Constants;

public class GLMSolver {
  public static final int DEFAULT_MAX_ITER = 50;
  public static final double DEFAULT_BETA_EPS = 1e-4;
  private static final double MAX_SQRT = Math.sqrt(Double.MAX_VALUE);

  public static class GLMException extends RuntimeException {
    public GLMException(String msg){super(msg);}
  }

  public enum CaseMode {
    none("n/a"),
    lt("<"),
    gt(">"),
    lte("<="),
    gte(">="),
    eq("=");
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
    log(0),
//    probit(0),
//    cauchit(0),
//    cloglog(0),
//    sqrt(0),
    inverse(1.0),
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
        return 1/x;
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
        return 1/x;
      default:
        throw new Error("unexpected link function id  " + this);
      }
    }

    public final double linkDeriv(double x){
      switch(this){
        case identity:
          return 1;
        case logit:
          if( x == 1 || x == 0 ) return MAX_SQRT;
          return 1 / (x * (1 - x));
        case log:
          return (x == 0)?MAX_SQRT:1/x;
        case inverse:
          return -1/(x*x);
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
    poisson(Link.log,null);
    //gamma(Link.inverse,null);
    public final Link defaultLink;
    public final double [] defaultArgs;
    Family(Link l, double [] d){defaultLink = l; defaultArgs = d;}

    public double aic(double dev, long nobs, int betaLen){
      switch(this){
      case gaussian:
        return nobs *(Math.log(dev/nobs * 2 *Math.PI)+1)+2 + 2*betaLen;
      case binomial:
        return 2*betaLen + dev;
      case poisson:
        return 2*betaLen + dev;
//      case gamma:
//        return Double.NaN;
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
//      case gamma:
//        return mu*mu;
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
//      case gamma:
//        if(yr == 0)return -2;
//        return -2*(Math.log(yr/ym) - (yr - ym)/ym);
      default:
        throw new Error("unknown family Id " + this);
      }
    }
  }

  public static final int FAMILY_ARGS_CASE = 0;
  public static final int FAMILY_ARGS_WEIGHT = 1;
  public static final double [] DEFAULT_THRESHOLDS = new double [] {0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9};
  int [] _colIds;
  LSMSolver _solver;
  Key _trainingDataset;
  Sampling _sampling;
  double [] _beta;
  int _iterations;
  boolean _finished;
  transient ValueArray _ary;
  int [] _categoricals;
  int [] _colOffsets;
  double [] _normSub;
  double [] _normMul;


  public static class GLMModel extends Model {
    final Sampling _s;
    boolean _isDone;            // Model is "being worked on" or "is stable"
    private boolean _converged = false;

    public int _iterations;     // Iterations used to solve
    public long _time;          // Total solve time in millis
    public       LSMSolver _solver; // Which solver is used
    public final GLMParams _glmParams;

    public transient ValueArray _ary;  // Data used to build the model
    public transient int _responseCol; // Response column

    // The model columns are dense packed - but there will be columns in the
    // data being ignored.  This is a map from the model's columns to the
    // building dataset's columns.  Note that this map is for *building* the
    // model only.  If the same model is later used on unrelated datasets,
    // those datasets will need their own mappings.  Not thread-safe for
    // concurrent scoring, only thread-safe during construction.
    transient int[] _modelDataMap;

    // Input columns are dense packed (all ignores already removed).  However,
    // GLM expands categorical columns in the beta[], normSub[], and normMul[]
    // arrays, requiring a level of indirection to find the correct beta/norm
    // entry.  CAT expansion columns are placed in-order and inline.  To find a
    // non-CAT column just indirect through _colCatMap.  To find a CAT column
    // first indirect, then add the categorical value as an int.  Note that
    // this map is NOT related to the dataset being used to score and is final
    // and invariant (unlike _modelDataMap above).
    final int [] _colCatMap;
    public double [] _normSub;         // Normalization base
    public double [] _normMul;         // Normalization scale
    double [] _beta;            // The output coefficients!  Main model result.

    public String [] _warnings;
    public GLMValidation [] _vals;
    // Empty constructor for deseriaization
    public GLMModel() {
      _s = null;
      _solver = null;
      _glmParams = null;
      _colCatMap = null;
    }

    public boolean isSolved() { return _beta != null; }
    public static final String KEY_PREFIX = "__GLMModel_";
    // Hand out the coffients.  Must be treated as a read-only array.
    public double[] beta() { return _beta; }
    // Warm-start setup; clone the incoming array since we will be mutating it
    public void setBeta(double[]beta) { _beta = beta.clone(); }

    public static final Key makeKey() {
      return Key.make(KEY_PREFIX + Key.make());
    }

    public String xcolNames(){
      StringBuilder sb = new StringBuilder();
      for( ValueArray.Column C : _va._cols ) {
        if( C._domain != null )
          for( String d : C._domain )
            sb.append(C._name).append('.').append(d).append(',');
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

    public GLMModel( Key selfKey, GLMModel other, Sampling s ) {
      super(selfKey,other);
      _ary = other._ary;
      _responseCol = other._responseCol;
      _s = s;
      _solver = other._solver;
      _glmParams = other._glmParams;
      _colCatMap = other._colCatMap;
      _modelDataMap = other._modelDataMap;
      _beta = other._beta;
      _normMul = other._normMul;
      _normSub = other._normSub;
    }

    // Describe a model layout, which is just a collection of columns, plus the
    // various solvers & params & sample rate.  Pre-compute the categorical
    // column expansion.
    public GLMModel(ValueArray ary, int [] colIds, LSMSolver lsm, GLMParams params, Sampling s) {
      super(makeKey(), colIds, ary._key);
      _solver = lsm;
      _glmParams = params;
      _s = s;
      _ary = ary;
      _responseCol = colIds[colIds.length-1];

      // A mapping from the model's columns to the data's columns
      _modelDataMap = columnMapping(ary.colNames());
      assert isCompatible(_modelDataMap); // Should be checked by the caller

      // Come up with a mapping expanding the categorical columns.
      // Each unique enum value will have its own beta/norm entries.
      _colCatMap = new int[_va._cols.length+1];
      int len=0;
      for( int i=0; i<_va._cols.length; i++ ) {
        _colCatMap[i] = len++;
        ValueArray.Column C = _va._cols[i];
        if( C._domain != null )
          len += C._domain.length-1/*remove self column and replace with enums*/;
      }
      // Number of columns stored in the last entry of the colCatMap
      _colCatMap[_va._cols.length] = len;

      _beta    = new double[len];   Arrays.fill(_beta, _glmParams._l.defaultBeta);
      _normMul = new double[len];   Arrays.fill(_normMul, 1.0);
      _normSub = new double[len];   Arrays.fill(_normSub, 0.0);

      // Normalize non-categoricals.  Note that we pulled the sigma & mean from
      // the stored ValueArray, not from the given dataset.
      // TODO compute histogram and normalization values for categoricals
      if( lsm==null || lsm.normalize() ) {
        int j=0;
        for( int i=0; i<_va._cols.length-1; i++ ) {
          ValueArray.Column C = _va._cols[i];
          if( C._domain == null ) { // Non cat?
            _normSub[j] = C._mean;  // Will subtract the mean
            if( C._sigma != 0 )     // And multiple by inverse sigma
              _normMul[j] = 1.0/C._sigma;
            j++;
          } else {
            j += C._domain.length;
          }
        }
      }
    }

    public boolean converged() { return _converged; }

    // Build a model from the given dataset & selected columns.
    public void build( ) {
      // check if response variable is within range
      if( _glmParams._f == Family.binomial ) {
        Column ycol = _ary._cols[_responseCol];
        if( ycol._min < 0 || ycol._max > 1 ) {
          if( _glmParams._caseMode == CaseMode.none )
            throw new GLMException("Response variable is out of (0,1) range for family=binomial.  Pick different column or set case.");
        }
      }

      _iterations = 0;
      _isDone = false;
      _vals = null;
      GramMatrixTask gtask = null;
      ArrayList<String> warns = new ArrayList();
      long t1 = System.currentTimeMillis();

      while(!_converged && _iterations++ < _glmParams._maxIter ) {
        // Compute the Gram Matrix
        assert !hasNaNsOrInfs(_beta);
        gtask = new GramMatrixTask(this);
        gtask.invoke(_dataKey);
        Matrix xx = gtask._gram.getXX();
        Matrix xy = gtask._gram.getXY();

        // Gram is broken?  Raise lambda penalty and try again
        if( gtask._gram.hasNaNsOrInfs() ){
          warns.add("Stopping at iteration " + _iterations + ". Gram has Infs or NaNs.");
          break;
        }
        // Solve the equation with the given Gram matrix.
        // Run up to twenty iterations of bumping _rho to solve SPD issues
        double [] beta = null;
        try {
          beta = _solver.solve(xx,xy);
        } catch(LSMSolverException e) {
          warns.add("Stopping at iteration " + _iterations + ". " + e.getMessage());
          break;
        }
        if(hasNaNsOrInfs(beta)) { // Clean set of betas?
          warns.add("Stopping at iteration " + _iterations + ". Beta has Infs or NaNs.");
          break;                   // Consider it a solution
        }
        // Compute max change in coef's from last iteration to this one,
        // and exit if nothing has changed more than _betaEps
        if(_glmParams._f == Family.gaussian){
          _converged = true;
        } else {
          double diff = 0.0;
          for(int i = 0; i < gtask._beta.length; ++i)
            diff = Math.max(diff, Math.abs(beta[i] - _beta[i]));
          _converged = diff < _glmParams._betaEps;
        }
        _time = System.currentTimeMillis() - t1;
        _beta = beta;
      }
      if(_iterations == _glmParams._maxIter)
        warns.add("Reached max # of iterations!");
      _isDone = true;
      if( !warns.isEmpty() )
        _warnings = warns.toArray(new String[warns.size()]);
      if( isSolved() )
        UKV.put(_selfKey,this);
    }

    private static boolean hasNaNsOrInfs( double[] ds ) {
      for( double d : ds )
        if( Double.isNaN(d) || Double.isInfinite(d) )
          return true;
      return false;
    }


    // Validate on a dataset.  Columns must match, including the response column.
    public GLMValidation validateOn( ValueArray ary, Sampling s, double [] thresholds ) {
      int[] modelDataMap = columnMapping(ary.colNames());
      if( !isCompatible(modelDataMap) ) // This dataset is compatible or not?
        throw new GLMException("incompatible dataset");

      GLMValidationTask valTsk = new GLMValidationTask(ary,s,_modelDataMap,_colCatMap,_normSub,_normMul);
      valTsk._beta = _beta;
      valTsk._f = _glmParams._f;
      valTsk._l = _glmParams._l;
      valTsk._caseMode = _glmParams._caseMode;
      valTsk._caseVal = _glmParams._caseVal;
      valTsk._ymu = ary._cols[modelDataMap[modelDataMap.length-1]]._mean;
      valTsk._thresholds = thresholds;
      valTsk.invoke(ary._key);
      GLMValidation val = new GLMValidation(valTsk);
      val._dataKey = ary._key;
      val._s = s;
      val._f = _glmParams._f;
      val._l = _glmParams._l;
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
        models[i] = new GLMModel(makeKey(),this,new Sampling(i,folds,false));
        models[i].build();
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
      res.addProperty("isDone", _isDone);
      JsonArray colNames = new JsonArray();
      for( String s : _va.colNames() )
        colNames.add(new JsonPrimitive(s));
      res.add("column_names",colNames);
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
      double norm = 0.0;        // Reverse any normalization on the intercept
      int idx=0;
      for( int i=0; i<_va._cols.length-1; i++ ) {
        ValueArray.Column C = _va._cols[i];
        if( C._domain != null )
          for( String d : C._domain )
            coefs.addProperty(C._name+"."+d,_beta[idx++]);
        else {
          double b = _beta[idx]*_normMul[idx];
          coefs.addProperty(C._name,b);
          norm += b*_normSub[idx]; // Also accumulate the intercept adjustment
          idx++;
        }
      }

      double icpt = _beta[_beta.length-1];
      icpt -= norm;
      coefs.addProperty("Intercept",icpt);
      res.add("coefficients", coefs);
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
          double d = (data[i] - _normSub[idx]) * _normMul[idx];
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
      double pp = _glmParams._l.linkInv(p);
      if( _glmParams._f == Family.binomial )
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

  public static class GLMParams extends Iced {
    public Family _f = Family.gaussian;
    public Link _l;
    public double _betaEps;
    public int _maxIter = 10;
    public double _caseVal;
    public double _caseWeight = 1.0;
    public CaseMode _caseMode = CaseMode.none;

    public JsonObject toJson(){
      JsonObject res = new JsonObject();
      res.addProperty("family", _f.toString());
      res.addProperty("link", _l.toString());
      res.addProperty("betaEps", _betaEps);
      res.addProperty("maxIter", _maxIter);
      if(_caseMode != CaseMode.none){
        res.addProperty("caseVal",_caseMode.exp(_caseVal));
        res.addProperty("weight",_caseWeight);
      }
      return res;
    }
  }

  GLMParams _glmParams;

  public enum ErrMetric {
    MAXC,
    SUMC,
    TOTAL;

    public double computeErr(ConfusionMatrix cm){
      double [] cerr = cm.classErr();
      double res = 0;
      switch(this){
      case MAXC:
         res = cerr[0];
        for(double d:cerr)if(d > res)res = d;
        break;
      case SUMC:
        for(double d:cerr)res += d;
        break;
      case TOTAL:
        res = cm.err();
        break;
      default:
        throw new Error("unexpected err metric " + this);
      }
      return res;
    }

  }


  public static class GramMatrixTask extends RowVecTask {
    Family _family;
    Link _link;
    double [] _beta;
    GramMatrix _gram;
    CaseMode _cMode;
    double _cVal;
    double _cWeight;

    public GramMatrixTask( GLMModel m ) {
      super(m._ary,m._s,m._modelDataMap,m._colCatMap,m._normSub,m._normMul);
      _family = m._glmParams._f;
      _link = m._glmParams._l;
      _cMode = m._glmParams._caseMode;
      _cVal = m._glmParams._caseVal;
      _cWeight = m._glmParams._caseWeight;
      _beta = m._beta;
    }

    public void processRow(double [] x, int [] indexes){
      double y = x[x.length-1];
      double w = 1;
      if(_cMode != CaseMode.none) {
        if(_cMode.isCase(y, _cVal)){
          y = 1;
          w = _cWeight;
        } else
          y = 0;
      }

      // set the intercept
      x[x.length-1] = 1.0; // constant (Intercept)
      double gmu = 0;
      for(int i = 0; i < x.length; ++i)
        gmu += x[i] * _beta[indexes[i]];
      // get the inverse to get estimate of p(Y=1|X) according to previous model
      double mu = _link.linkInv(gmu);
      double dgmu = _link.linkDeriv(mu);
      y = gmu + (y - mu) * dgmu; // z = y approx by Taylor
                                               // expansion at the point of our
                                               // estimate (mu), done to avoid
                                               // log(0),log(1)
      // Step 2
      double vary = _family.variance(mu); // variance of y according to our model

      // compute the weights (inverse of variance of z)
      double var = dgmu * dgmu * vary;
      // Apply the weight. We want each data point to have weight of inverse of
      // the variance of y at this point.
      // Since we compute x'x, we take sqrt(w) and apply it to both x and y
      // (we also compute X*y)
      w = Math.sqrt(w/var);
      for(int i = 0; i < x.length; ++i)
        x[i] *= w;
      _gram.addRow(x, indexes, y * w);
    }

    @Override
    protected void init2(){
      _gram = new GramMatrix(_beta.length);
    }

    @Override
    public void reduce(DRemoteTask drt) {
      GramMatrixTask other = (GramMatrixTask)drt;
      if(_gram != null)_gram.add(other._gram);
      else _gram = other._gram;
    }
  }


  public static final class ConfusionMatrix extends Iced {
    public long [][] _arr;
    long _n;
    double _threshold;

    public ConfusionMatrix(int n){
      _arr = new long[n][n];
    }
    public void add(int i, int j){
      add(i, j, 1);
    }
    public void add(int i, int j, int c){
      _arr[i][j] += c;
      _n += c;
    }

    public final double [] classErr(){
      double [] res = new double[_arr.length];
      for(int i = 0; i < res.length; ++i)
        res[i] = classErr(i);
      return res;
    }
    public final int size() {return _arr.length;}

    public final double classErr(int c){
      long s = 0;
      for( long x : _arr[c] )
        s += x;
      if( s==0 ) return 0.0;    // Either 0 or NaN, but 0 is nicer
      return (double)(s-_arr[c][c])/s;
    }

    public double err(){
      long err = _n;
      for(int i = 0; i < _arr.length;++i){
        err -= _arr[i][i];
      }
      return (double)err/_n;
    }

    public void add(ConfusionMatrix other){
      _n += other._n;
      for(int i = 0; i < _arr.length; ++i)
        for(int j = 0; j < _arr.length; ++j)
          _arr[i][j] += other._arr[i][j];
    }

    public JsonArray toJson(){
      JsonArray res = new JsonArray();
      JsonArray header = new JsonArray();
      header.add(new JsonPrimitive("Actual / Predicted"));
      for(int i = 0; i < _arr.length;++i)
        header.add(new JsonPrimitive("class " + i));
      header.add(new JsonPrimitive("Error"));
      res.add(header);
      for(int i = 0; i < _arr.length; ++i){
        JsonArray row = new JsonArray();
        row.add(new JsonPrimitive("class " + i));
        long s = 0;
        for(int j = 0; j < _arr.length; ++j){
          s += _arr[i][j];
          row.add(new JsonPrimitive(_arr[i][j]));
        }
        double err = s - _arr[i][i];
        err /= s;
        row.add(new JsonPrimitive(err));
        res.add(row);
      }
      JsonArray totals = new JsonArray();
      totals.add(new JsonPrimitive("Totals"));
      long S = 0;
      long DS = 0;
      for(int i = 0; i < _arr.length; ++i){
        long s = 0;
        for(int j = 0; j < _arr.length; ++j)
          s += _arr[j][i];
        totals.add(new JsonPrimitive(s));
        S += s;
        DS += _arr[i][i];
      }
      double err = (S - DS)/(double)S;
      totals.add(new JsonPrimitive(err));
      res.add(totals);
      return res;
    }
  }

  public static class GLMValidation extends Iced {
    public final Key [] _modelKeys; // Multiple models for n-fold cross-validation
    public static final String KEY_PREFIX = "__GLMValidation_";

    Link _l;
    Family _f;
    Key _key;
    Key _dataKey;
    Key _modelKey;
    Sampling _s;
    public final long _n;
    public final double _dof;
    public final double _aic;
    public final double _deviance;
    public final double _nullDeviance;
    public final double _err;
    ErrMetric _errMetric = ErrMetric.SUMC;
    double _auc;
    public ConfusionMatrix [] _cm;
    int _tid;
    double [] _thresholds;

    public GLMValidation(GLMValidationTask tsk) {
      _modelKeys = null;
      _n = tsk._n;
      _deviance = tsk._deviance;
      _err = tsk._err;
      _cm = tsk._cm;
      _dof = _n-1-tsk._beta.length;
      _aic = tsk._f.aic(_deviance, _n, tsk._beta.length);
      _thresholds = tsk._thresholds;
      _dataKey = tsk._ary._key;
      if(_cm != null){
        computeBestThreshold(ErrMetric.SUMC);
        computeAUC();
      }
      if(tsk._f == Family.binomial){
        double p = tsk._caseCount/(double)tsk._n;
        _nullDeviance = -2*(tsk._caseCount*Math.log(p) + (tsk._n - tsk._caseCount)*Math.log(1-p));
      } else
        _nullDeviance = tsk._nullDeviance;
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
        _dof = Double.NaN;
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
      _aic = models[0]._glmParams._f.aic(_deviance, _n, models[0]._beta.length);
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
          return new Iterator<GLMSolver.GLMModel>() {
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

  private static class GLMValidationTask extends RowVecTask {
    Link _l;
    Family _f;
    double _ymu;
    double _deviance;
    double _nullDeviance;
    double [] _beta;
    double [] _thresholds;
    ConfusionMatrix [] _cm;
    double _err;
    long _n;
    double _w;
    double _caseVal;
    CaseMode _caseMode;
    long _caseCount;

    public GLMValidationTask(ValueArray ary, Sampling s, int[] modelDataMap, int[] colCatMap, double[] normSub, double[] normMul ) {
      super(ary,s,modelDataMap,colCatMap,normSub,normMul);
    }
    @Override protected void init2() {
      if(_f == Family.binomial) {
        _cm = new ConfusionMatrix[_thresholds.length];
        for(int i = 0; i < _thresholds.length; ++i)
          _cm[i] = new ConfusionMatrix(2);
      }
    }

    @Override
    void processRow(double[] x, int[] indexes) {
      ++_n;
      double yr = x[x.length-1];
      x[x.length-1] = 1.0;
      double ym = 0;
      for(int i = 0; i < x.length; ++i)
        ym += _beta[indexes[i]] * x[i];
      ym = _l.linkInv(ym);
      if(_caseMode != CaseMode.none)
        yr = _caseMode.isCase(yr, _caseVal)?1:0;
      if(yr == 1)
        ++_caseCount;
      _deviance += _f.deviance(yr, ym);
      _nullDeviance += _f.deviance(yr, _ymu);
      if(_f == Family.binomial) {
        if(yr < 0 || yr > 1 )
          throw new Error("response variable value out of range: " + yr);
        int i = 0;
        for(double t:_thresholds){
          int p = ym >= t?1:0;
          _cm[i++].add((int)yr,p);
        }
      } else
        _err += (ym - yr)*(ym - yr);
    }

    @Override
    public void reduce(DRemoteTask drt) {
      GLMValidationTask other = (GLMValidationTask)drt;
      _n += other._n;
      _nullDeviance += other._nullDeviance;
      _deviance += other._deviance;
      _err += other._err;
      _caseCount += other._caseCount;
      if(_cm != null) {
        for(int i = 0; i < _thresholds.length; ++i)
          _cm[i].add(other._cm[i]);
      } else
        _cm = other._cm;
    }
  }
}
