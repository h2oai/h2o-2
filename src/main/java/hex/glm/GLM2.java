package hex.glm;

import dontweave.gson.JsonObject;
import hex.FrameTask.DataInfo;
import hex.FrameTask.DataInfo.TransformType;
import hex.GridSearch.GridSearchProgress;
import hex.glm.GLMModel.GLMXValidationTask;
import hex.glm.GLMModel.Submodel;
import hex.glm.GLMParams.Family;
import hex.glm.GLMParams.Link;
import hex.glm.GLMTask.GLMInterceptTask;
import hex.glm.GLMTask.GLMIterationTask;
import hex.glm.GLMTask.YMUTask;
import hex.glm.LSMSolver.ADMMSolver;
import jsr166y.CountedCompleter;
import water.*;
import water.H2O.H2OCallback;
import water.H2O.H2OCountedCompleter;
import water.H2O.H2OEmptyCompleter;
import water.api.DocGen;
import water.api.ParamImportance;
import water.api.RequestServer.API_VERSION;
import water.fvec.Frame;
import water.fvec.Vec;
import water.util.Log;
import water.util.ModelUtils;
import water.util.RString;
import water.util.Utils;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class GLM2 extends Job.ModelJobWithoutClassificationField {
  public static final double LS_STEP = .9;
  static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
  public static DocGen.FieldDoc[] DOC_FIELDS;
  public static final String DOC_GET = "GLM2";
  public final String _jobName;
  transient public boolean _done = false;

  // API input parameters BEGIN ------------------------------------------------------------
  @API(help="Column to be used as an offset, if you have one.", required=false, filter=responseFilter.class, json = true)
  public Vec offset = null;
  class responseFilter extends SpecialVecSelect { responseFilter() { super("source"); } }

  @API(help = "Family.", filter = Default.class, json=true, importance = ParamImportance.CRITICAL)
  protected Family family = Family.gaussian;

  @API(help = "", filter = Default.class, json=true, importance = ParamImportance.SECONDARY)
  protected Link link = Link.family_default;

  @API(help = "Tweedie variance power", filter = Default.class, json=true, importance = ParamImportance.SECONDARY)
  protected double tweedie_variance_power;

  public void setTweediePower(double pwr){
    tweedie_variance_power = pwr;
    tweedie_link_power = 1 - tweedie_variance_power;
    _glm = new GLMParams(family,tweedie_variance_power,link,tweedie_link_power);
  }

  @API(help="prior probability for y==1. To be used only for logistic regression iff the data has been sampled and the mean of response does not reflect reality.",filter=Default.class, importance = ParamImportance.EXPERT)
  protected double prior = -1; // -1 is magic value for default value which is mean(y) computed on the current dataset

  @API(help="disable line search in all cases.",filter=Default.class, importance = ParamImportance.EXPERT, hide = true)
  protected boolean disable_line_search = false; // -1 is magic value for default value which is mean(y) computed on the current dataset

  private double _iceptAdjust = 0; // adjustment due to the prior

  @API(help = "validation folds", filter = Default.class, lmin=0, lmax=100, json=true, importance = ParamImportance.CRITICAL)
  protected int n_folds;

  @API(help = "distribution of regularization between L1 and L2.", filter = Default.class, json=true, importance = ParamImportance.SECONDARY)
  protected double [] alpha = new double[]{0.5};

  public final double DEFAULT_LAMBDA = 1e-5;
  @API(help = "regularization strength", filter = Default.class, json=true, importance = ParamImportance.SECONDARY)
  protected double [] lambda = new double[]{DEFAULT_LAMBDA};

  @API(help="use lambda search starting at lambda max, given lambda is then interpreted as lambda min",filter=Default.class, importance = ParamImportance.SECONDARY)
  protected boolean lambda_search;

  @API(help="number of lambdas to be used in a search",filter=Default.class, importance = ParamImportance.EXPERT)
  protected int nlambdas = 100;

  @API(help="min lambda used in lambda search, specified as a ratio of lambda_max",filter=Default.class, importance = ParamImportance.EXPERT)
  protected double lambda_min_ratio = -1;

  @API(help="lambda_search stop condition: stop training when model has more than than this number of predictors (or don't use this option if -1).",filter=Default.class, importance = ParamImportance.EXPERT)
  protected int max_predictors = -1;

  public void setLambda(double l){ lambda = new double []{l};}
  private double _currentLambda = Double.POSITIVE_INFINITY;
  public int MAX_ITERATIONS_PER_LAMBDA = 10;

  @API(help="use strong rules to filter out inactive columns",filter=Default.class, importance = ParamImportance.SECONDARY)
  protected boolean strong_rules = true;
  // intentionally not declared as API now
  int sparseCoefThreshold = 1000; // if more than this number of predictors, result vector of coefficients will be stored sparse

  double [] beta_start = null;

  @API(help = "Standardize numeric columns to have zero mean and unit variance.", filter = Default.class, json=true, importance = ParamImportance.CRITICAL)
  protected boolean standardize = true;

  @API(help = "Include intercept term in the model.", filter = Default.class, json=true, importance = ParamImportance.CRITICAL)
  protected boolean intercept = true;

  @API(help = "Restrict coefficients to be non-negative.", filter = Default.class, json=true, importance = ParamImportance.CRITICAL)
  protected boolean non_negative = false;

  @API(help="lower bounds for coefficients",filter=Default.class,hide=true)
  protected Frame beta_constraints = null;

  @API(help="By default, first factor level is skipped from the possible set of predictors. Set this flag if you want use all of the levels. Needs sufficient regularization to solve!",filter=Default.class, importance = ParamImportance.SECONDARY)
  protected boolean use_all_factor_levels = false;

  /**
   * Whether to compute variable importances for input features, based on the absolute
   * value of the coefficients.  For safety this should only be done if
   * use_all_factor_levels, because an important factor level can be skipped and not
   * appear if !use_all_factor_levels.
   */
  @API(help = "Compute variable importances for input features.  NOTE: If use_all_factor_levels is off the importance of the base level will NOT be shown.", filter = Default.class, json=true, importance = ParamImportance.SECONDARY)
  public boolean variable_importances = false;

  @API(help = "beta_eps", filter = Default.class, json=true, importance = ParamImportance.SECONDARY)
  protected double beta_epsilon = DEFAULT_BETA_EPS;

  @API(help = "max-iterations", filter = Default.class, lmin=1, lmax=1000000, json=true, importance = ParamImportance.CRITICAL)
  public int max_iter = 100;

  @API(help="use line search (slower speed, to be used if glm does not converge otherwise)",filter=Default.class, importance = ParamImportance.SECONDARY)
  protected boolean higher_accuracy = false;

  // API input parameters END ------------------------------------------------------------

  // API output parameters BEGIN ------------------------------------------------------------

  @API(help = "", json=true, importance = ParamImportance.SECONDARY)
  private double [] _wgiven;

  @API(help = "", json=true, importance = ParamImportance.SECONDARY)
  private double _proximalPenalty;

  @API(help = "", json=true, importance = ParamImportance.SECONDARY)
  private double [] _beta;

  @API(help = "", json=true, importance = ParamImportance.SECONDARY)
  private boolean _runAllLambdas = true;


  @API(help = "Tweedie link power", json=true, importance = ParamImportance.SECONDARY)
  double tweedie_link_power;

  @API(help = "lambda_value max", json=true, importance = ParamImportance.SECONDARY)
  double lambda_max = Double.NaN;
  double lambda_min = Double.NaN;
  long _nobs = 0;

  private double _nullDeviance;

  public static int MAX_PREDICTORS = 7000;


  // API output parameters END ------------------------------------------------------------
  private static double GLM_GRAD_EPS = 1e-4; // done (converged) if subgrad < this value.

  private boolean highAccuracy(){return higher_accuracy;}
  public GLM2 setHighAccuracy(){
    higher_accuracy = true;
    return this;
  }

  private Key _progressKey;
  private DataInfo _srcDinfo;
  private int [] _activeCols;
  private boolean _allIn;
  private DataInfo _activeData;
  public GLMParams _glm;
  private boolean _grid;

  private double ADMM_GRAD_EPS = 1e-4; // default addm gradietn eps
  private static final double MIN_ADMM_GRAD_EPS = 1e-5; // min admm gradient eps

  int _lambdaIdx = -1;

  private double _addedL2;
  private boolean _failedLineSearch;

  public static final double DEFAULT_BETA_EPS = 5e-5;
  private double _ymu;
  private int    _iter;


  @Override protected void registered(API_VERSION ver) {
    super.registered(ver);
    Argument c = find("ignored_cols");
    Argument r = find("offset");
    int ci = _arguments.indexOf(c);
    int ri = _arguments.indexOf(r);
    _arguments.set(ri, c);
    _arguments.set(ci, r);
    ((FrameKeyMultiVec) c).ignoreVec((FrameKeyVec)r);
  }

  private double objval(GLMIterationTask glmt){
    return glmt._val.residual_deviance / glmt._nobs + 0.5 * l2pen() * l2norm(glmt._beta) + l1pen() * l1norm(glmt._beta) + proxPen(glmt._beta);
  }

  private IterationInfo makeIterationInfo(int i, GLMIterationTask glmt, final int [] activeCols, double [] gradient){
    IterationInfo ii = new IterationInfo(_iter, glmt,activeCols,gradient);
    if(ii._glmt._grad == null)
      ii._glmt._grad = contractVec(gradient,activeCols);
    return ii;
  }
  private static  class IterationInfo extends Iced {
    final int _iter;
    private double [] _fullGrad;


    public double [] fullGrad(double alpha, double lambda){
      if(_fullGrad == null)return null;
      double [] res = _fullGrad.clone();
      double l2 = (1-alpha)*lambda; // no 0.5 mul here since we're adding derivative of 0.5*|b|^2
      if(_activeCols != null)
        for(int i = 0; i < _glmt._beta.length-1; ++i)
          res[_activeCols[i]] += _glmt._beta[i]*l2;
      else for(int i = 0; i < _glmt._beta.length; ++i) {
        res[i] += _glmt._beta[i]*l2;
      }
      return res;
    }
    private final GLMIterationTask _glmt;
    final int [] _activeCols;
    IterationInfo(int i, GLMIterationTask glmt, final int [] activeCols, double [] gradient){
      _iter = i;
      _glmt = glmt.clone();
      assert _glmt._grad != null;
      _activeCols = activeCols;
      _fullGrad = gradient;
      // NOTE: _glmt._beta CAN BE NULL (unlikely but possible, if activecCols were empty)
      assert _glmt._val != null:"missing validation";
    }
  }

  private IterationInfo _lastResult;

  @Override
  public JsonObject toJSON() {
    JsonObject jo = super.toJSON();
    if (lambda == null) jo.addProperty("lambda_value", "automatic"); //better than not printing anything if lambda_value=null
    return jo;
  }

  @Override public Key defaultDestKey(){
    return null;
  }
  @Override public Key defaultJobKey() {return null;}

  public GLM2() {_jobName = "";}

  public static class Source {
    public final Frame fr;
    public final Vec response;
    public final Vec offset;
    public final boolean standardize;
    public final boolean intercept;
    public Source(Frame fr,Vec response, boolean standardize){ this(fr,response,standardize,true,null);}
    public Source(Frame fr,Vec response, boolean standardize, boolean intercept){ this(fr,response,standardize,intercept,null);}
    public Source(Frame fr,Vec response, boolean standardize, boolean intercept, Vec offset){
      this.fr = fr;
      this.response = response;
      this.offset = offset;
      this.standardize = standardize;
      this.intercept = intercept;
    }
  }


  public GLM2(String desc, Key jobKey, Key dest, Source src, Family family){
    this(desc,jobKey,dest,src,family,Link.family_default);
  }
  public GLM2(String desc, Key jobKey, Key dest, Source src, Family family, Link l){
    this(desc, jobKey, dest, src, family, l, 0, false);
  }
  public GLM2(String desc, Key jobKey, Key dest, Source src, Family family, Link l, int nfolds, boolean highAccuracy) {
    job_key = jobKey;
    description = desc;
    destination_key = dest;
    this.offset = src.offset;
    this.intercept = src.intercept;
    this.family = family;
    this.link = l;
    n_folds = nfolds;
    source = src.fr;
    this.response = src.response;
    this.standardize = src.standardize;
    _jobName = dest.toString() + ((nfolds > 1)?("[" + 0 + "]"):"");
    higher_accuracy = highAccuracy;

  }
  public GLM2 doInit(){
    init();
    return this;
  }


   public GLM2 setNonNegative(boolean val){
     non_negative = val;
     return this;
   }

  public GLM2 setRegularization(double [] alpha, double [] lambda){
    this.alpha = alpha;
    this.lambda = lambda;
    return this;
  }
  public GLM2 setBetaConstraints(Frame f){
    beta_constraints = f;
    return this;
  }

  static String arrayToString (double[] arr) {
    if (arr == null) {
      return "(null)";
    }
    StringBuffer sb = new StringBuffer();
    for (int i = 0; i < arr.length; i++) {
      if (i > 0) {
        sb.append(", ");
      }
      sb.append(arr[i]);
    }
    return sb.toString();
  }

  public transient float [] thresholds = ModelUtils.DEFAULT_THRESHOLDS;

  /** Return the query link to this page */
  public static String link(Key k, String content) {
    RString rs = new RString("<a href='GLM2.query?source=%$key'>%content</a>");
    rs.replace("key", k.toString());
    rs.replace("content", content);
    return rs.toString();
  }

  public GLMGridSearch gridSearch(){
    return new GLMGridSearch(4, this, destination_key).fork();
  }

  private transient AtomicBoolean _jobdone = new AtomicBoolean(false);


  @Override public void cancel(String msg){
    if(!_grid) {
      source.unlock(self());
    }
    DKV.remove(_progressKey);
    Value v = DKV.get(destination_key);
    if(v != null){
      GLMModel m = v.get();
      Key [] xvals = m.xvalModels();
      if(xvals != null)
        for(Key k:xvals)
          DKV.remove(k);
      DKV.remove(destination_key);
    }
    DKV.remove(destination_key);
    super.cancel(msg);
  }

  private boolean sorted(int [] ary){
    for(int i = 0; i < ary.length-1; ++i)
      if(ary[i+1] < ary[i])return false;
    return true;
  }

  private double computeIntercept(DataInfo dinfo, double ymu, Vec offset, Vec response){
    double mul = 1, sub = 0;
    int vecId = dinfo._adaptedFrame.find(offset);
    if(dinfo._normMul != null)
      mul = dinfo._normMul[vecId-dinfo._cats];
    if(dinfo._normSub != null)
      sub = dinfo._normSub[vecId-dinfo._cats];
    double icpt =  ymu - (offset.mean() - sub)*mul;
    double icpt2 = new GLMInterceptTask(_glm,sub,mul,icpt).doAll(offset,response)._icpt;
    double diff = icpt2 - icpt;
    int iter = 0;
    while((1e-4 < diff || diff < -1e-4) && ++iter <= 10){
      icpt = icpt2;
      icpt2 = new GLMInterceptTask(_glm,sub,mul,icpt).doAll(offset,response)._icpt;
      diff = icpt2 - icpt;
    }
    return icpt;
  }

  private transient Frame source2; // adapted source with reordered (and removed) vecs we do not want to push back into KV

  private int _noffsets = 0;
  private int _intercept = 1; // 1 or 0

  private double [] _lbs;
  private double [] _ubs;
  private double [] _bgs;
  private double [] _rho;

  boolean toEnum = false;

  private double [] makeAry(int sz, double val){
    double [] res = MemoryManager.malloc8d(sz);
    Arrays.fill(res,val);
    return res;
  }

  private double [] mapVec(double [] src, double [] tgt, int [] map){
    for(int i = 0; i < src.length; ++i)
      if(map[i] != -1) tgt[map[i]] = src[i];
    return tgt;
  }
  @Override public void init(){
    try {
      super.init();
      if (family == Family.gamma)
        setHighAccuracy();
      if (link == Link.family_default)
        link = family.defaultLink;
      _intercept = intercept ? 1 : 0;
      tweedie_link_power = 1 - tweedie_variance_power;// TODO
      if (tweedie_link_power == 0) link = Link.log;
      _glm = new GLMParams(family, tweedie_variance_power, link, tweedie_link_power);
      source2 = new Frame(source);
      assert sorted(ignored_cols);
      if (offset != null) {
        if (offset.isEnum())
          throw new IllegalArgumentException("Categorical offsets are not supported. Can not use column '" + source2.names()[source2.find(offset)] + "' as offset");
        int id = source.find(offset);
        int idx = Arrays.binarySearch(ignored_cols, id);
        if (idx >= 0) Utils.remove(ignored_cols, idx);
        String name = source2.names()[id];
//        source2.add(name, source2.remove(id));
        _noffsets = 1;
      }
      if (nlambdas == -1)
        nlambdas = 100;
      if (lambda_search && lambda.length > 1)
        throw new IllegalArgumentException("Can not supply both lambda_search and multiple lambdas. If lambda_search is on, GLM expects only one value of lambda_value, representing the lambda_value min (smallest lambda_value in the lambda_value search).");
      // check the response
      if (response.isEnum() && family != Family.binomial)
        throw new IllegalArgumentException("Invalid response variable, trying to run regression with categorical response!");
      switch (family) {
        case poisson:
        case tweedie:
          if (response.min() < 0)
            throw new IllegalArgumentException("Illegal response column for family='" + family + "', response must be >= 0.");
          break;
        case gamma:
          if (response.min() <= 0)
            throw new IllegalArgumentException("Invalid response for family='Gamma', response must be > 0!");
          break;
        case binomial:
          if (response.min() < 0 || response.max() > 1)
            throw new IllegalArgumentException("Illegal response column for family='Binomial', response must in <0,1> range!");
          break;
        default:
          //pass
      }
      toEnum = family == Family.binomial && (!response.isEnum() && (response.min() < 0 || response.max() > 1));
      String offsetName = "";
      int offsetId = -1;
      if(offset != null) {
        offsetId = source2.find(offset);
        offsetName = source2.names()[offsetId];
        source2.remove(offsetId);
      }

      Frame fr = DataInfo.prepareFrame(source2, response, ignored_cols, toEnum, true, true);
      if(offset != null){ // now put the offset just before response
        String responseName = fr.names()[fr.numCols()-1];
        Vec responseVec = fr.remove(fr.numCols()-1);
        fr.add(offsetName, offset);
        fr.add(responseName,responseVec);
      }
      TransformType dt = TransformType.NONE;
      if (standardize)
        dt = intercept ? TransformType.STANDARDIZE : TransformType.DESCALE;
      _srcDinfo = new DataInfo(fr, 1, intercept, use_all_factor_levels || lambda_search, dt, DataInfo.TransformType.NONE);
      if(offset != null && dt != TransformType.NONE) { // do not standardize offset
        if(_srcDinfo._normMul != null)
          _srcDinfo._normMul[_srcDinfo._normMul.length-1] = 1;
        if(_srcDinfo._normSub != null)
          _srcDinfo._normSub[_srcDinfo._normSub.length-1] = 0;
      }
      if (!intercept && _srcDinfo._cats > 0)
        throw new IllegalArgumentException("Models with no intercept are only supported with all-numeric predictors.");
      _activeData = _srcDinfo;
      if (higher_accuracy) setHighAccuracy();
      if (beta_constraints != null) {
        Vec v;
        v = beta_constraints.vec("names");
        // for now only enums allowed here
        String [] dom = v.domain();
        String [] names = Utils.append(_srcDinfo.coefNames(), "Intercept");
        int [] map = Utils.asInts(v);
        if(!Arrays.deepEquals(dom,names)) { // need mapping
          HashMap<String,Integer> m = new HashMap<String, Integer>();
          for(int i = 0; i < names.length; ++i)
            m.put(names[i],i);
          int [] newMap = MemoryManager.malloc4(dom.length);
          for(int i = 0; i < map.length; ++i) {
            Integer I = m.get(dom[map[i]]);
            newMap[i] = I == null?-1:I;
          }
          map = newMap;
        }
        final int numoff = _srcDinfo.numStart();
        if((v = beta_constraints.vec("lower_bounds")) != null) {
          _lbs = map == null ? Utils.asDoubles(v) : mapVec(Utils.asDoubles(v), makeAry(names.length, Double.NEGATIVE_INFINITY), map);
//            for(int i = 0; i < _lbs.length; ++i)
//            if(_lbs[i] > 0) throw new IllegalArgumentException("lower bounds must be non-positive");
          if(_srcDinfo._normMul != null) {
            for (int i = numoff; i < _srcDinfo.fullN(); ++i) {
              if (Double.isInfinite(_lbs[i])) continue;
              _lbs[i] /= _srcDinfo._normMul[i - numoff];
            }
          }
        }
        if((v = beta_constraints.vec("upper_bounds")) != null) {
          _ubs = map == null ? Utils.asDoubles(v) : mapVec(Utils.asDoubles(v), makeAry(names.length, Double.POSITIVE_INFINITY), map);
          System.out.println("upper bounds = " + Arrays.toString(_ubs));
//          for(int i = 0; i < _ubs.length; ++i)
//            if (_ubs[i] < 0) throw new IllegalArgumentException("lower bounds must be non-positive");
          if(_srcDinfo._normMul != null)
            for(int i = numoff; i < _srcDinfo.fullN(); ++i) {
              if(Double.isInfinite(_ubs[i]))continue;
              _ubs[i] /= _srcDinfo._normMul[i - numoff];
            }
        }

        if(_lbs != null && _ubs != null) {
          for(int i = 0 ; i < _lbs.length; ++i)
            if(_lbs[i] > _ubs[i])
              throw new IllegalArgumentException("Invalid upper/lower bounds: lower bounds must be <= upper bounds for all variables.");
        }
        if((v =  beta_constraints.vec("beta_given")) != null) {
          _bgs = map == null ? Utils.asDoubles(v) : mapVec(Utils.asDoubles(v), makeAry(names.length, 0), map);
          if(_srcDinfo._normMul != null) {
            double norm = 0;
            for (int i = numoff; i < _srcDinfo.fullN(); ++i) {
              norm += _bgs[i] * _srcDinfo._normSub[i-numoff];
              _bgs[i] /= _srcDinfo._normMul[i-numoff];

            }
            if(_intercept == 1)
              _bgs[_bgs.length-1] -= norm;
          }
        }
        if((v = beta_constraints.vec("rho")) != null)
          _rho = map == null?Utils.asDoubles(v):mapVec(Utils.asDoubles(v),makeAry(names.length,0),map);
        else if(_bgs != null)
          throw new IllegalArgumentException("Missing vector of penalties (rho) in beta_constraints file.");
      }
      if (non_negative) { // make srue lb is >= 0
        if (_lbs == null)
          _lbs = new double[_srcDinfo.fullN()];
        for (int i = 0; i < _lbs.length; ++i)
          if (_lbs[i] < 0)
            _lbs[i] = 0;
      }
    } catch(RuntimeException e) {
      e.printStackTrace();
      cleanup();
      throw e;
    }
  }
  @Override protected void cleanup(){
    super.cleanup();
    if(toEnum && _srcDinfo != null){
      Futures fs = new Futures();
      _srcDinfo._adaptedFrame.lastVec().remove(fs);
      fs.blockForPending();
    }
  }
  @Override protected boolean filterNaCols(){return true;}
  @Override protected Response serve() {
    try {
      init();
      if (alpha.length > 1) { // grid search
        if (destination_key == null) destination_key = Key.make("GLMGridResults_" + Key.make());
        if (job_key == null) job_key = Key.make((byte) 0, Key.JOB, H2O.SELF);
        GLMGridSearch j = gridSearch();
        _fjtask = j._fjtask;
        assert _fjtask != null;
        return GLMGridView.redirect(this, j.dest());
      } else {
        if (destination_key == null) destination_key = Key.make("GLMModel_" + Key.make());
        if (job_key == null) job_key = Key.make("GLM2Job_" + Key.make());
        fork();
        assert _fjtask != null;
        return GLMProgress.redirect(this, job_key, dest());
      }
    }catch(Throwable ex){
      return Response.error(ex.getMessage());
    }
  }
  private static double beta_diff(double[] b1, double[] b2) {
    if(b1 == null || b1.length == 0)return Double.MAX_VALUE;
    double res = b1[0] >= b2[0]?b1[0] - b2[0]:b2[0] - b1[0];
    for( int i = 1; i < b1.length; ++i ) {
      double diff = b1[i] - b2[i];
      if(diff > res)
        res = diff;
      else if( -diff > res)
        res = -diff;
    }
    return res;
  }
//private static double beta_diff(double[] b1, double[] b2) {
//  double res = 0;
//  for(int i = 0; i < b1.length; ++i)
//    res += (b1[i]-b2[i])*(b1[i]-b2[i]);
//  return res;
//}
  private static class GLM2_Progress extends Iced{
    final long _total;
    double _done;
    public GLM2_Progress(int total){_total = total;
      assert _total > 0:"total = " + _total;
    }
    public float progess(){
      return 0.01f*((int)(100*_done/(double)_total));
    }
  }

  private static class GLM2_ProgressUpdate extends TAtomic<GLM2_Progress> {
    final int _i;
    public GLM2_ProgressUpdate(){_i = 1;}
    public GLM2_ProgressUpdate(int i){_i = i;}
    @Override
    public GLM2_Progress atomic(GLM2_Progress old) {
      if(old == null)return old;
      old._done += _i;
      return old;
    }
  }

  @Override public float progress(){
    if(isDone())return 1.0f;
    Value v = DKV.get(_progressKey);
    if(v == null)return 0;
    float res = v.<GLM2_Progress>get().progess();
    if(res > 1f)
      res = 1f;
    return res;
  }

  protected double l2norm(double[] beta){
    if(_beta == null)return 0;
    double l2 = 0;
    for (double aBeta : beta) l2 += aBeta * aBeta;
    return l2;
  }
  protected double l1norm(double[] beta){
    if(_beta == null)return 0;
    double l2 = 0;
    for (double aBeta : beta) l2 += Math.abs(aBeta);
    return l2;
  }

  private final double [] expandVec(double [] beta, final int [] activeCols){
    assert beta != null;
    if (activeCols == null)
      return beta;
    double[] res = MemoryManager.malloc8d(_srcDinfo.fullN() + _intercept -_noffsets);
    int i = 0;
    for (int c = 0; c < activeCols.length-_noffsets; ++c)
      res[_activeCols[c]] = beta[i++];
    if(_intercept == 1)
      res[res.length - 1] = beta[beta.length - 1];
    for(int j = beta.length-_noffsets; j < beta.length-1; ++j)
      beta[j] = 1;
    return res;
  }

  private final double [] contractVec(double [] beta, final int [] activeCols){ return contractVec(beta,activeCols,_intercept);}
  private final double [] contractVec(double [] beta, final int [] activeCols, int intercept){
    if(beta == null)return null;
    if(activeCols == null)return beta.clone();
    final int N = activeCols.length - _noffsets;
    double [] res = MemoryManager.malloc8d(N+intercept);
    for(int i = 0; i < N; ++i)
      res[i] = beta[activeCols[i]];
    if(intercept == 1)
      res[res.length-1] = beta[beta.length-1];
    return res;
  }
  private final double [] resizeVec(double[] beta, final int[] activeCols, final int[] oldActiveCols){
    if(beta == null || Arrays.equals(activeCols,oldActiveCols))return beta;
    double [] full = expandVec(beta, oldActiveCols);
    if(activeCols == null)return full;
    return contractVec(full,activeCols,_intercept);
  }
//  protected boolean needLineSearch(final double [] beta,double objval, double step){
  protected boolean needLineSearch(final GLMIterationTask glmt) {
    if(disable_line_search)
      return false;
    if(_glm.family == Family.gaussian)
      return false;
    if(glmt._beta == null)
      return false;
    if (Utils.hasNaNsOrInfs(glmt._xy) || (glmt._grad != null && Utils.hasNaNsOrInfs(glmt._grad)) || (glmt._gram != null && glmt._gram.hasNaNsOrInfs())) {
      return true;
    }
    if(glmt._val != null && Double.isNaN(glmt._val.residualDeviance())){
      return true;
    }
    if(glmt._val == null) // no validation info, no way to decide
      return false;
    final double [] grad = Arrays.equals(_activeCols,_lastResult._activeCols)
      ?_lastResult._glmt.gradient(alpha[0],_currentLambda)
      :contractVec(_lastResult.fullGrad(alpha[0],_currentLambda),_activeCols);
    return needLineSearch(1, objval(_lastResult._glmt),objval(glmt),diff(glmt._beta,_lastResult._glmt._beta),grad);
  }

  private static double [] diff(double [] x, double [] y){
    if(y == null)return x.clone();
    double [] res = MemoryManager.malloc8d(x.length);
    for(int i = 0; i < x.length; ++i)
      res[i] = x[i] - y[i];
    return res;
  }
  public static final double c1 = 1e-2;
//  protected boolean needLineSearch(final double [] beta,double objval, double step){

  // Armijo line-search rule enhanced with generalized gradient to handle l1 pen
  protected final boolean needLineSearch(double step, final double objOld, final double objNew, final double [] pk, final double [] gradOld){
    // line search
    double f_hat = 0;
    for(int i = 0; i < pk.length; ++i)
      f_hat += gradOld[i] * pk[i];
    f_hat = step*f_hat + objOld;
    return objNew > (f_hat + 1/(2*step)*l2norm(pk));
  }

  private class LineSearchIteration extends H2OCallback<GLMTask.GLMLineSearchTask> {
    final GLMIterationTask _glmt;
    LineSearchIteration(GLMIterationTask glmt, CountedCompleter cmp){super((H2OCountedCompleter)cmp); cmp.addToPendingCount(1); _glmt = glmt;}
    @Override public void callback(final GLMTask.GLMLineSearchTask glmt) {
      assert getCompleter().getPendingCount() >= 1:"unexpected pending count, expected 1, got " + getCompleter().getPendingCount();
      double step = LS_STEP;
      for(int i = 0; i < glmt._glmts.length; ++i){
        if(!needLineSearch(glmt._glmts[i]) || (i == glmt._glmts.length-1 && objval(glmt._glmts[i]) < objval(_lastResult._glmt))){
          LogInfo("line search: found admissible step = " + step + ",  objval = " + objval(glmt._glmts[i]));
          setHighAccuracy();
          new GLMIterationTask(_noffsets,GLM2.this.self(),_activeData,_glm,true,true,true,glmt._glmts[i]._beta,_ymu,1.0/_nobs,thresholds, new Iteration(getCompleter(),false,false)).asyncExec(_activeData._adaptedFrame);
          return;
        }
        step *= LS_STEP;
      }
      LogInfo("line search: did not find admissible step, smallest step = " + step + ",  objval = " + objval(glmt._glmts[glmt._glmts.length-1]) + ", old objval = " + objval(_lastResult._glmt));
      // check if objval of smallest step is below the previous step, if so, go on
      LogInfo("Line search did not find feasible step, converged.");
      _failedLineSearch = true;
      GLMIterationTask res = highAccuracy()?_lastResult._glmt:_glmt;
      if(_activeCols != _lastResult._activeCols && !Arrays.equals(_activeCols,_lastResult._activeCols)) {
        _activeCols = _lastResult._activeCols;
        _activeData = _srcDinfo.filterExpandedColumns(_activeCols);
      }
      checkKKTAndComplete(getCompleter(),res,res._beta,true);
    }
  }

  protected double checkGradient(final double [] newBeta, final double [] grad){
    // check the gradient
    ADMMSolver.subgrad(alpha[0], _currentLambda, newBeta, grad);
    double err = 0;
    for(double d:grad)
      if(d > err) err = d;
      else if(d < -err) err = -d;
    LogInfo("converged with max |subgradient| = " + err);
    return err;
  }

  private String LogInfo(String msg){
    msg = "GLM2[dest=" + dest() + ", iteration=" + _iter + ", lambda = " + _currentLambda + "]: " + msg;
    Log.info(msg);
    return msg;
  }

  private double [] setSubmodel(final double[] newBeta, GLMValidation val, H2OCountedCompleter cmp){
    int intercept = (this.intercept ?1:0);
    double [] fullBeta = (_activeCols == null || newBeta == null)?newBeta.clone():expandVec(newBeta,_activeCols);
    if(val != null) val.null_deviance = _nullDeviance;
    if(this.intercept)
      fullBeta[fullBeta.length-1] += _iceptAdjust;
    if(_noffsets > 0){
      fullBeta = Arrays.copyOf(fullBeta,fullBeta.length + _noffsets);
      if(this.intercept)
        fullBeta[fullBeta.length-1] = fullBeta[fullBeta.length-intercept-_noffsets];
      for(int i = fullBeta.length-intercept-_noffsets; i < fullBeta.length-intercept; ++i)
        fullBeta[i] = 1;//_srcDinfo.applyTransform(i,1);
    }
    final double [] newBetaDeNorm;
    final int numoff = _srcDinfo.numStart();
    if(_srcDinfo._predictor_transform == DataInfo.TransformType.STANDARDIZE) {
      assert this.intercept;
      newBetaDeNorm = fullBeta.clone();
      double norm = 0.0;        // Reverse any normalization on the intercept
      // denormalize only the numeric coefs (categoricals are not normalized)
      for( int i=numoff; i< fullBeta.length-intercept; i++ ) {
        double b = newBetaDeNorm[i]* _srcDinfo._normMul[i-numoff];
        norm += b* _srcDinfo._normSub[i-numoff]; // Also accumulate the intercept adjustment
        newBetaDeNorm[i] = b;
      }
      if(this.intercept)
        newBetaDeNorm[newBetaDeNorm.length-1] -= norm;
    } else if (_srcDinfo._predictor_transform == TransformType.DESCALE) {
      assert !this.intercept;
      newBetaDeNorm = fullBeta.clone();
      for( int i=numoff; i< fullBeta.length; i++ )
        newBetaDeNorm[i] *= _srcDinfo._normMul[i-numoff];
    } else
      newBetaDeNorm = null;
    GLMModel.setSubmodel(cmp, dest(), _currentLambda, newBetaDeNorm == null ? fullBeta : newBetaDeNorm, newBetaDeNorm == null ? null : fullBeta, _iter, System.currentTimeMillis() - start_time, _srcDinfo.fullN() >= sparseCoefThreshold, val);
    return fullBeta;
  }

  private transient long _callbackStart = 0;
  private transient double _rho_mul = 1.0;
  private transient double _gradientEps = ADMM_GRAD_EPS;

  private double [] lastBeta(int noffsets){
    final double [] b;
    if(_lastResult == null || _lastResult._glmt._beta == null) {
      int bsz = _activeCols == null? _srcDinfo.fullN()+1-noffsets:_activeCols.length+1;
      b = MemoryManager.malloc8d(bsz);
      b[bsz-1] = _glm.linkInv(_ymu);
    } else
      b = resizeVec(_lastResult._glmt._beta, _activeCols, _lastResult._activeCols);
    return b;
  }

  protected void checkKKTAndComplete(final CountedCompleter cc, final GLMIterationTask glmt, final double [] newBeta, final boolean failedLineSearch){
    H2OCountedCompleter cmp = (H2OCountedCompleter)cc;
    final double [] fullBeta = newBeta == null?MemoryManager.malloc8d(_srcDinfo.fullN()+_intercept-_noffsets):expandVec(newBeta,_activeCols);
    // now we need full gradient (on all columns) using this beta
    new GLMIterationTask(_noffsets,GLM2.this.self(), _srcDinfo,_glm,false,true,true,fullBeta,_ymu,1.0/_nobs,thresholds, new H2OCallback<GLMIterationTask>(cmp) {
      @Override public String toString(){
        return "checkKKTAndComplete.Callback, completer = " + getCompleter() == null?"null":getCompleter().toString();
      }
      @Override
      public void callback(final GLMIterationTask glmt2) {
        // first check KKT conditions!
        final double [] grad = glmt2.gradient(alpha[0],_currentLambda);
        if(Utils.hasNaNsOrInfs(grad)){
          _failedLineSearch = true;
          // TODO: add warning and break the lambda search? Or throw Exception?
        }
        glmt._val = glmt2._val;
        _lastResult = makeIterationInfo(_iter,glmt2,null,glmt2.gradient(alpha[0],0));
        // check the KKT conditions and filter data for next lambda_value
        // check the gradient
        double[] subgrad = grad.clone();
        ADMMSolver.subgrad(alpha[0], _currentLambda, fullBeta, subgrad);
        double grad_eps = GLM_GRAD_EPS;
        if (!failedLineSearch &&_activeCols != null) {
          for (int c = 0; c < _activeCols.length-_noffsets; ++c)
            if (subgrad[_activeCols[c]] > grad_eps) grad_eps = subgrad[_activeCols[c]];
            else if (subgrad[c] < -grad_eps) grad_eps = -subgrad[_activeCols[c]];
          int[] failedCols = new int[64];
          int fcnt = 0;
          for (int i = 0; i < grad.length - 1; ++i) {
            if (Arrays.binarySearch(_activeCols, i) >= 0) continue;
            if (subgrad[i] > grad_eps || -subgrad[i] > grad_eps) {
              if (fcnt == failedCols.length)
                failedCols = Arrays.copyOf(failedCols, failedCols.length << 1);
              failedCols[fcnt++] = i;
            }
          }
          if (fcnt > 0) {
            final int n = _activeCols.length;
            final int[] oldActiveCols = _activeCols;
            _activeCols = Arrays.copyOf(_activeCols, _activeCols.length + fcnt);
            for (int i = 0; i < fcnt; ++i)
              _activeCols[n + i] = failedCols[i];
            Arrays.sort(_activeCols);
            LogInfo(fcnt + " variables failed KKT conditions check! Adding them to the model and continuing computation.(grad_eps = " + grad_eps + ", activeCols = " + (_activeCols.length > 100?"lost":Arrays.toString(_activeCols)));
            _activeData = _srcDinfo.filterExpandedColumns(_activeCols);
            // NOTE: tricky completer game here:
            // We expect 0 pending in this method since this is the end-point, ( actually it's racy, can be 1 with pending 1 decrement from the original Iteration callback, end result is 0 though)
            // while iteration expects pending count of 1, so we need to increase it here (Iteration itself adds 1 but 1 will be subtracted when we leave this method since we're in the callback which is called by onCompletion!
            // [unlike at the start of nextLambda call when we're not inside onCompletion]))
            getCompleter().addToPendingCount(1);
            new GLMIterationTask(_noffsets,GLM2.this.self(), _activeData, _glm, true, true, true, resizeVec(newBeta, _activeCols, oldActiveCols), _ymu, glmt._reg, thresholds, new Iteration(getCompleter())).asyncExec(_activeData._adaptedFrame);
            return;
          }
        }
        int diff = MAX_ITERATIONS_PER_LAMBDA - _iter + _iter1;
        if(diff > 0)
          new GLM2_ProgressUpdate(diff).fork(_progressKey); // update progress
        GLM2.this.setSubmodel(newBeta, glmt2._val,(H2OCountedCompleter)getCompleter().getCompleter());
        _done = true;
        LogInfo("computation of current lambda done in " + (System.currentTimeMillis() - GLM2.this.start_time) + "ms");
        assert _lastResult._fullGrad != null;
      }
    }).asyncExec(_srcDinfo._adaptedFrame);
  }
  private class Iteration extends H2OCallback<GLMIterationTask> {
    public final long _iterationStartTime;
    final boolean _countIteration;
    final boolean _checkLineSearch;
    public Iteration(CountedCompleter cmp){ this(cmp,true,true);}
    public Iteration(CountedCompleter cmp, boolean countIteration,boolean checkLineSearch){
      super((H2OCountedCompleter)cmp);
      cmp.addToPendingCount(1);
      _checkLineSearch = checkLineSearch;
      _countIteration = countIteration;
      _iterationStartTime = System.currentTimeMillis(); }

    @Override public void callback(final GLMIterationTask glmt){
      if( !isRunning(self()) )  throw new JobCancelledException();
      assert _activeCols == null || glmt._beta == null || glmt._beta.length == (_activeCols.length+_intercept-glmt._noffsets):LogInfo("betalen = " + glmt._beta.length + ", activecols = " + _activeCols.length + " noffsets = " + glmt._noffsets);
      assert _activeCols == null || _activeCols.length == _activeData.fullN();
      assert getCompleter().getPendingCount() >= 1 : LogInfo("unexpected pending count, expected >=  1, got " + getCompleter().getPendingCount()); // will be decreased by 1 after we leave this callback
      if (_countIteration) ++_iter;
      _callbackStart = System.currentTimeMillis();

      double gerr = Double.NaN;
      boolean hasNaNs = glmt._gram.hasNaNsOrInfs() || Utils.hasNaNsOrInfs(glmt._xy);
      boolean needLineSearch = hasNaNs || _checkLineSearch && needLineSearch(glmt);
      if (glmt._val != null && glmt._computeGradient) { // check gradient
        final double[] grad = glmt.gradient(alpha[0], _currentLambda);
        ADMMSolver.subgrad(alpha[0], _currentLambda, glmt._beta, grad);
        gerr = 0;
        for (double d : grad)
          gerr += d*d;
        if(gerr <= GLM_GRAD_EPS*GLM_GRAD_EPS || (needLineSearch && gerr <= 5*ADMM_GRAD_EPS*ADMM_GRAD_EPS)){
          LogInfo("converged by reaching small enough gradient, with max |subgradient| = " + gerr );
          checkKKTAndComplete(getCompleter(),glmt, glmt._beta,false);
          return;
        }
      }
      if(needLineSearch){
        if(!_checkLineSearch){ // has to converge here
          LogInfo("Line search did not progress, converged.");
          checkKKTAndComplete(getCompleter(),glmt, glmt._beta,true);
          return;
        }
        LogInfo("invoking line search");
        new GLMTask.GLMLineSearchTask(_noffsets, GLM2.this.self(), _activeData,_glm, lastBeta(_noffsets), glmt._beta, 1e-4, _ymu, _nobs, new LineSearchIteration(glmt,getCompleter())).asyncExec(_activeData._adaptedFrame);
        return;
      }
      if(glmt._grad != null)
        _lastResult = makeIterationInfo(_iter,glmt,_activeCols,null);
      if(glmt._newThresholds != null) {
        thresholds = Utils.join(glmt._newThresholds[0], glmt._newThresholds[1]);
        Arrays.sort(thresholds);
      }

      final double [] newBeta = MemoryManager.malloc8d(glmt._xy.length);
      long t1 = System.currentTimeMillis();
      ADMMSolver slvr = new ADMMSolver(lambda_max, _currentLambda,alpha[0], _gradientEps, _addedL2);
      if(_lbs != null)
        slvr._lb = _activeCols == null?contractVec(_lbs,_activeCols,0):_lbs;
      if(_ubs != null)
        slvr._ub = _activeCols == null?contractVec(_ubs,_activeCols,0):_ubs;
      if(_bgs != null && _rho != null) {
        slvr._wgiven = _activeCols == null ? contractVec(_bgs, _activeCols, 0) : _bgs;
        slvr._proximalPenalties = _activeCols == null ? contractVec(_rho, _activeCols, 0) : _rho;
      }
      slvr.solve(glmt._gram,glmt._xy,glmt._yy,newBeta,Math.max(1e-8*lambda_max,_currentLambda*alpha[0]));
      // print all info about iteration
      LogInfo("Gram computed in " + (_callbackStart - _iterationStartTime) + "ms, " + (Double.isNaN(gerr)?"":"gradient = " + gerr + ",") + ", step = " + 1 + ", ADMM: " + slvr.iterations + " iterations, " + (System.currentTimeMillis() - t1) + "ms (" + slvr.decompTime + "), subgrad_err=" + slvr.gerr);
//      int [] iBlocks = new int[]{8,16,32,64,128,256,512,1024};
//      int [] rBlocks = new int[]{1,2,4,8,16,32,64,128};
//      for(int i:iBlocks)
//        for(int r:rBlocks){
//          long ttx = System.currentTimeMillis();
//          try {
//            slvr.gerr = Double.POSITIVE_INFINITY;
//            ADMMSolver.ParallelSolver pslvr = slvr.parSolver(glmt._gram, glmt._wy, newBeta, _currentLambda * alpha[0] * _rho_mul, i, r);
//            pslvr.invoke();
//            System.out.println("iBlock = " + i + ", rBlocsk = " + r + "ms");
//            LogInfo("ADMM: " + pslvr._iter + " iterations, " + (System.currentTimeMillis() - ttx) + "ms (" + slvr.decompTime + "), subgrad_err=" + slvr.gerr);
//          } catch(Throwable t){
//            System.out.println("iBlock = " + i + ", rBlocsk = " + r + " failed! err = " + t);
//          }
//        }

      if (slvr._addedL2 > _addedL2) LogInfo("added " + (slvr._addedL2 - _addedL2) + "L2 penalty");

      new GLM2_ProgressUpdate().fork(_progressKey); // update progress
      _gradientEps = Math.max(ADMM_GRAD_EPS, Math.min(slvr.gerr, 0.01));
      _addedL2 = slvr._addedL2;
      if (Utils.hasNaNsOrInfs(newBeta)) {
        throw new RuntimeException(LogInfo("got NaNs and/or Infs in beta"));
      } else {
        final double bdiff = beta_diff(glmt._beta, newBeta);
        if(_glm.family == Family.gaussian && _glm.link == Link.identity) {
          checkKKTAndComplete(getCompleter(),glmt, newBeta, false);
          return;
        } else if (bdiff < beta_epsilon || _iter >= max_iter) { // Gaussian is non-iterative and gradient is ADMMSolver's gradient => just validate and move on to the next lambda_value
          int diff = (int) Math.log10(bdiff);
          int nzs = 0;
          for (int i = 0; i < glmt._beta.length; ++i)
            if (glmt._beta[i] != 0) ++nzs;
          LogInfo("converged (reached a fixed point with ~ 1e" + diff + " precision), got " + nzs + " nzs");
          checkKKTAndComplete(getCompleter(),glmt, newBeta, false); // NOTE: do not use newBeta here, it has not been checked and can lead to NaNs in KKT check, redoing line search, coming up with the same beta and so on.
          return;
        } else { // not done yet, launch next iteration
          if (glmt._beta != null)
            setSubmodel(glmt._beta, glmt._val, (H2OCountedCompleter) getCompleter().getCompleter()); // update current intermediate result
          final boolean validate = higher_accuracy || (_iter % 5) == 0;
          new GLMIterationTask(_noffsets,GLM2.this.self(),_activeData,glmt._glm, true, validate, validate, newBeta,_ymu,1.0/_nobs,thresholds, new Iteration(getCompleter(),true,true)).asyncExec(_activeData._adaptedFrame);
        }
      }
    }
  }

  private static int nzs(double ds[]){
    int res = 0;
    for(double d:ds)if(d != 0)++res;
    return res;
  }
  private class LambdaIteration extends H2OCallback {
    public LambdaIteration(CountedCompleter cmp) {
      super((H2OCountedCompleter) cmp);
    }
    @Override
    public void callback(H2OCountedCompleter h2OCountedCompleter) {
      // check if we're done otherwise launch next lambda computation
      _done = _currentLambda <= lambda_min
              || (max_predictors != -1 && nzs(_lastResult._glmt._beta) > max_predictors); // _iter < max_iter && (improved || _runAllLambdas) && _lambdaIdx < (lambda_value.length-1);;
      if(!_done) {
        H2OCountedCompleter cmp = (H2OCountedCompleter)getCompleter();
        cmp.addToPendingCount(1);
        nextLambda(nextLambdaValue(), new LambdaIteration(cmp));
      }
    }
  }


  private class GLMJobCompleter extends H2OCountedCompleter {
    AtomicReference<CountedCompleter> _cmp = new AtomicReference<CountedCompleter>();
    public GLMJobCompleter(H2OCountedCompleter cmp){super(cmp);}
    @Override
    public void compute2() {
      run(true,this);
    }
    private transient boolean _failed;
    @Override public void onCompletion(CountedCompleter cmp){
      if(!_grid)source.unlock(self());
      if(!_failed) {
        assert _cmp.compareAndSet(null, cmp) : "double completion, first from " + _cmp.get().getClass().getName() + ", second from " + cmp.getClass().getName();
        _done = true;
        // TODO: move these updates to Model into a DKeyTask so that it runs remotely on the model's home
        GLMModel model = DKV.get(dest()).get();
        model.maybeComputeVariableImportances();
        model.stop_training();
        if (_addedL2 > 0) {
          String warn = "Added L2 penalty (rho = " + _addedL2 + ")  due to non-spd matrix. ";
          model.addWarning(warn);
        }
        if(_failedLineSearch && !highAccuracy())
          model.addWarning("High accuracy settings recommended.");
        state = JobState.DONE;
        DKV.remove(_progressKey);
        model.get_params().state = state;
        model.update(self());
        getCompleter().addToPendingCount(1);
        new GLMModel.UnlockModelTask(new H2OCallback((H2OCountedCompleter) getCompleter()) {
          @Override
          public void callback(H2OCountedCompleter h2OCountedCompleter) {
            remove(); // Remove/complete job only for top-level, not xval GLM2s
          }
        }, model._key, self()).forkTask();
        cleanup();
      }
    }
    @Override public boolean onExceptionalCompletion(Throwable t, CountedCompleter cmp){
      if(_cmp.compareAndSet(null, cmp)) {
        _done = true;
        GLM2.this.cancel(t);
       cleanup();
        if(_grid){
          _failed = true;
          tryComplete();
        }
      }
      return !_grid;
    }
  }

  @Override
  public GLM2 fork(){return fork(null);}

  public GLM2 fork(H2OCountedCompleter cc){
    if(!_grid)source.read_lock(self());
    // keep *this* separate from what's stored in K/V as job (will be changing it!)
    Futures fs = new Futures();
    _progressKey = Key.make(dest().toString() + "_progress", (byte) 1, Key.HIDDEN_USER_KEY, dest().home_node());
    int total = max_iter;
    if(lambda_search)
      total = MAX_ITERATIONS_PER_LAMBDA*nlambdas;
    GLM2_Progress progress = new GLM2_Progress(total*(n_folds > 1?(n_folds+1):1));
    LogInfo("created progress " + progress);
    DKV.put(_progressKey,progress,fs);
    fs.blockForPending();
    _fjtask = new H2O.H2OEmptyCompleter(cc);
    H2OCountedCompleter fjtask = new GLMJobCompleter(_fjtask);
    GLM2 j = (GLM2)clone();
    j.start(_fjtask); // modifying GLM2 object, don't want job object to be the same instance
    H2O.submitTask(fjtask);
    return j;
  }


  transient GLM2 [] _xvals;

  private class XvalidationCallback extends H2OCallback {
    public XvalidationCallback(H2OCountedCompleter cmp){super(cmp);}
    @Override
    public void callback(H2OCountedCompleter cc) {
      ParallelGLMs pgs = (ParallelGLMs)cc;
      _xvals = pgs._glms;
      for(int i = 0; i < _xvals.length; ++i){
        assert _xvals[i]._lastResult._fullGrad != null:LogInfo("last result missing full gradient!");
        assert _xvals[i]._lastResult._glmt._val != null:LogInfo("last result missing validation!");
      }
      _iter = _xvals[0]._iter;
      thresholds = _xvals[0].thresholds;
      _lastResult = (IterationInfo)pgs._glms[0]._lastResult.clone();
      final GLMModel [] xvalModels = new GLMModel[_xvals.length-1];
      final double curentLambda = _currentLambda;
      final H2OCountedCompleter mainCmp = (H2OCountedCompleter)getCompleter().getCompleter();
      mainCmp.addToPendingCount(1);
      final GLMModel.GetScoringModelTask [] tasks = new GLMModel.GetScoringModelTask[pgs._glms.length];
      H2OCallback c = new H2OCallback(mainCmp) {
        @Override public String toString(){
          return "GetScoringModelTask.Callback, completer = " + getCompleter() == null?"null":getCompleter().toString();
        }
        AtomicReference<CountedCompleter> _cmp = new AtomicReference<CountedCompleter>();
        @Override
        public void callback(H2OCountedCompleter cc) {
          assert _cmp.compareAndSet(null,cc):"Double completion, first " + _cmp.get().getClass().getName() + ", second from " + cc.getClass().getName();
          for(int i = 1; i < tasks.length; ++i)
            xvalModels[i-1] = tasks[i]._res;
          mainCmp.addToPendingCount(1);
          new GLMXValidationTask(tasks[0]._res, curentLambda, xvalModels, thresholds,mainCmp).asyncExec(_srcDinfo._adaptedFrame);
        }
      };
      c.addToPendingCount(tasks.length-1);
      for(int i = 0; i < tasks.length; ++i)
        (tasks[i] = new GLMModel.GetScoringModelTask(c,pgs._glms[i].dest(),curentLambda)).forkTask();
    }
  }



  private GLMModel addLmaxSubmodel(GLMModel m,GLMValidation val, double [] beta){
    m.submodels = new GLMModel.Submodel[]{new GLMModel.Submodel(lambda_max,beta,beta,0,0, beta.length >= sparseCoefThreshold)};
    m.submodels[0].validation = val;
    assert val != null;
    return m;
  }

  public void run(boolean doLog, H2OCountedCompleter cmp){
    if(doLog) logStart();
    // if this is cross-validated task, don't do actual computation,
    // just fork off the nfolds+1 tasks and wait for the results
    assert alpha.length == 1;
    start_time = System.currentTimeMillis();

    if(nlambdas == -1)nlambdas = 100;
    if(lambda_search && nlambdas <= 1)
      throw new IllegalArgumentException(LogInfo("GLM2: nlambdas must be > 1 when running with lambda search."));
    Futures fs = new Futures();
    Key dst = dest();

    new YMUTask(GLM2.this.self(), _srcDinfo, n_folds,new H2OCallback<YMUTask>(cmp) {
      @Override
      public String toString(){
        return "YMUTask callback. completer = " + getCompleter() != null?"null":getCompleter().toString();
      }

      @Override
      public void callback(final YMUTask ymut) {
        if (ymut._ymin == ymut._ymax)
          throw new IllegalArgumentException(LogInfo("GLM2: attempted to run with constant response. Response == " + ymut._ymin + " for all rows in the training set."));
        if(ymut.nobs() == 0)
          throw new IllegalArgumentException(LogInfo("GLM2: got no active rows in the dataset after discarding rows with NAs"));
        _ymu = ymut.ymu();
        _nobs = ymut.nobs();
        if(_glm.family == Family.binomial && prior != -1 && prior != _ymu && !Double.isNaN(prior)) {
          _iceptAdjust = -Math.log(_ymu * (1-prior)/(prior * (1-_ymu)));
        } else prior = _ymu;
        H2OCountedCompleter cmp = (H2OCountedCompleter)getCompleter();
        cmp.addToPendingCount(1);
        // public GLMIterationTask(int noff, Key jobKey, DataInfo dinfo, GLMParams glm, boolean computeGram, boolean validate, boolean computeGradient, double [] beta, double ymu, double reg, float [] thresholds, H2OCountedCompleter cmp) {
        new GLMIterationTask(_noffsets,GLM2.this.self(), _srcDinfo, _glm, false, true, true, nullModelBeta(_srcDinfo,_ymu), _ymu, 1.0/_nobs, thresholds, new H2OCallback<GLMIterationTask>(cmp){
          @Override
          public String toString(){
            return "LMAXTask callback. completer = " + (getCompleter() != null?"NULL":getCompleter().toString());
          }

          @Override public void callback(final GLMIterationTask glmt){
            double [] beta = glmt._beta;
            if(beta_start == null) {
              beta_start = beta;
            }
            _nullDeviance = glmt._val.residualDeviance();
            _currentLambda = lambda_max = Math.max(Utils.maxValue(glmt._grad),-Utils.minValue(glmt._grad))/Math.max(1e-3,alpha[0]);
            _lastResult = makeIterationInfo(0,glmt,null,glmt.gradient(0,0));
            GLMModel model = new GLMModel(GLM2.this, dest(), _srcDinfo, _glm, glmt._val, beta_epsilon, alpha[0], lambda_max, _ymu, prior);
            model.start_training(start_time);
            if(lambda_search) {
              assert !Double.isNaN(lambda_max) : LogInfo("running lambda_value search, but don't know what is the lambda_value max!");
              model = addLmaxSubmodel(model, glmt._val, beta);
              if (nlambdas == -1) {
                lambda = null;
              } else {
                if (lambda_min_ratio == -1)
                  lambda_min_ratio = _nobs > 25 * _srcDinfo.fullN() ? 1e-4 : 1e-2;
                final double d = Math.pow(lambda_min_ratio, 1.0 / (nlambdas - 1));
                if (nlambdas == 0)
                  throw new IllegalArgumentException("nlambdas must be > 0 when running lambda search.");
                lambda = new double[nlambdas];
                lambda[0] = lambda_max;
                if (nlambdas == 1)
                  throw new IllegalArgumentException("Number of lambdas must be > 1 when running with lambda_search!");
                for (int i = 1; i < lambda.length; ++i)
                  lambda[i] = lambda[i - 1] * d;
                lambda_min = lambda[lambda.length - 1];
                max_iter = MAX_ITERATIONS_PER_LAMBDA * nlambdas;
              }
              _runAllLambdas = false;
            } else {
              if(lambda == null || lambda.length == 0)
                lambda = new double[]{DEFAULT_LAMBDA};
              int i = 0;
              while(i < lambda.length && lambda[i] > lambda_max)++i;
              if(i == lambda.length)
                throw new IllegalArgumentException("Given lambda(s) are all > lambda_max = " + lambda_max + ", have nothing to run with. lambda = " + Arrays.toString(lambda));
              if(i > 0) {
                model.addWarning("Removed " + i + " lambdas greater than lambda_max.");
                lambda = Utils.append(new double[]{lambda_max},Arrays.copyOfRange(lambda,i,lambda.length));
                addLmaxSubmodel(model,glmt._val, beta);
              }
            }
            model.delete_and_lock(self());
            lambda_min = lambda[lambda.length-1];
            if(n_folds > 1){
              final H2OCountedCompleter futures = new H2OEmptyCompleter();
              final GLM2 [] xvals = new GLM2[n_folds+1];
              futures.addToPendingCount(xvals.length-2);
              for(int i = 0; i < xvals.length; ++i){
                xvals[i] = (GLM2)GLM2.this.clone();
                xvals[i].n_folds = 0;
                xvals[i].standardize = standardize;
                xvals[i].family = family;
                xvals[i].link = link;
                xvals[i].beta_epsilon = beta_epsilon;
                xvals[i].max_iter = max_iter;
                xvals[i].variable_importances = variable_importances;
                if(i != 0){
                  xvals[i]._srcDinfo = _srcDinfo.getFold(i-1,n_folds);
                  xvals[i].destination_key = Key.make(dest().toString() + "_xval_" + i, (byte) 1, Key.HIDDEN_USER_KEY, H2O.SELF);
                  xvals[i]._nobs = ymut.nobs(i-1);
                  xvals[i]._ymu = ymut.ymu(i-1);
                  final int fi = i;
                  final double ymu = ymut.ymu(fi-1);
                  // new GLMIterationTask(offset_cols.length,GLM2.this.self(), _srcDinfo, _glm, false, true, true,nullModelBeta(),_ymu,1.0/_nobs, thresholds, new H2OCallback<GLMIterationTask>(cmp){
                  new GLMIterationTask(_noffsets,self(),xvals[i]._srcDinfo,_glm, false,true,true, nullModelBeta(xvals[fi]._srcDinfo,ymu),ymu,1.0/ymut.nobs(fi-1),thresholds,new H2OCallback<GLMIterationTask>(futures){
                    @Override
                    public String toString(){
                      return "Xval LMAXTask callback., completer = " + getCompleter() == null?"null":getCompleter().toString();
                    }
                    @Override
                    public void callback(GLMIterationTask t) {
                      xvals[fi].beta_start = t._beta;
                      xvals[fi]._currentLambda = xvals[fi].lambda_max = Math.max(Utils.maxValue(glmt._grad),-Utils.minValue(glmt._grad))/Math.max(1e-3,alpha[0]);
                      assert xvals[fi].lambda_max > 0;
                      xvals[fi]._lastResult = makeIterationInfo(0,t,null,t.gradient(alpha[0],0));
                      GLMModel m = new GLMModel(GLM2.this, xvals[fi].destination_key, xvals[fi]._srcDinfo, _glm, t._val, beta_epsilon, alpha[0], xvals[fi].lambda_max, xvals[fi]._ymu, prior);//.delete_and_lock(self());
                      m.submodels = new Submodel[]{new Submodel(xvals[fi].lambda_max,t._beta,t._beta,0,0, t._beta.length >= sparseCoefThreshold)};
                      m.submodels[0].validation = t._val;
                      assert t._val != null;
                      m.setSubmodelIdx(0);
                      m.delete_and_lock(self());
                      if(xvals[fi].lambda_max > lambda_max){
                        futures.addToPendingCount(1);
                        new ParallelGLMs(GLM2.this,new GLM2[]{xvals[fi]},lambda_max,1,futures).fork();
                      }
                    }
                  }).asyncExec(xvals[i]._srcDinfo._adaptedFrame);
                }
              }
              _xvals = xvals;
              futures.join();
            }
            getCompleter().addToPendingCount(1);
            nextLambda(nextLambdaValue(), new LambdaIteration(getCompleter()));
          }
        }).asyncExec(_srcDinfo._adaptedFrame);
      }
    }).asyncExec(_srcDinfo._adaptedFrame);
  }

  private double [] nullModelBeta(DataInfo dinfo, double ymu){
    double[] beta = MemoryManager.malloc8d(_srcDinfo.fullN() + (dinfo._hasIntercept?1:0) - _noffsets);
    if(intercept) {
      double icpt = _noffsets == 0?_glm.link(ymu):computeIntercept(dinfo,ymu,offset,response);
      if (dinfo._hasIntercept) beta[beta.length - 1] = icpt;
    }
    return beta;
  }

  public double nextLambdaValue(){
    assert lambda == null || lambda_min == lambda[lambda.length-1];
    return (lambda == null)?pickNextLambda():lambda[++_lambdaIdx];
  }

  private transient int _iter1 = 0;
  void nextLambda(final double currentLambda, final H2OCountedCompleter cmp){
    if(currentLambda > lambda_max){
      _done = true;
      cmp.tryComplete();
      return;
    }
    if(_beta != null)
      beta_start = _beta;
    _iter1 = _iter;
    LogInfo("starting computation of lambda = " + currentLambda + ", previous lambda = " + _currentLambda);
    _done = false;
    final double previousLambda = _currentLambda;

    _currentLambda = currentLambda;
    if(n_folds > 1){ // if we're cross-validated tasks, just fork off the parallel glms and wait for result!
      for(int i = 0; i < _xvals.length; ++i)
        if(_xvals[i]._lastResult._fullGrad == null){
          RuntimeException re = new RuntimeException(LogInfo("missing full gradient at lambda = " + previousLambda + " at fold " + i));
          Log.err(re);
          throw re;
        }
      ParallelGLMs pgs = new ParallelGLMs(this,_xvals,currentLambda, H2O.CLOUD.size(),new XvalidationCallback(cmp));
      pgs.fork();
      return;
    } else {
      if(lambda_search){ // if we are in lambda_search, we want only limited number of iters per lambda!
        max_iter = _iter + MAX_ITERATIONS_PER_LAMBDA;
      }
      final double[] grad = _lastResult.fullGrad(alpha[0],previousLambda);
      assert grad != null;
      activeCols(_currentLambda, previousLambda, grad);
      if(_activeCols != null && _activeCols.length == _noffsets) {
        // nothing to do but to store the null model and report back...
        setSubmodel(_lastResult._glmt._beta,_lastResult._glmt._val,cmp);
        _done = true;
        cmp.tryComplete();
        return;
      }
      assert cmp.getPendingCount() == 0;
      // expand the beta
      // todo make this work again
//      if (Arrays.equals(_lastResult._activeCols, _activeCols) && _lastResult._glmt._gram != null) { // set of coefficients did not change
//        new Iteration(cmp, false).callback(_lastResult._glmt);
//        _lastResult._glmt.tryComplete();  // shortcut to reuse the last gram if same active columns
//      } else
      new GLMIterationTask(_noffsets,GLM2.this.self(), _activeData, _glm, true, false, false, resizeVec(_lastResult._glmt._beta, _activeCols, _lastResult._activeCols), _ymu, 1.0 / _nobs, thresholds, new Iteration(cmp)).asyncExec(_activeData._adaptedFrame);;
    }
  }

  private final double l2pen(){return 0.5*_currentLambda*(1-alpha[0]);}
  private final double l1pen(){return _currentLambda*alpha[0];}
  private final double proxPen(double [] beta){
    double [] fullBeta = expandVec(beta,_activeCols);
    double res = 0;
    if(_bgs != null && _rho != null) {
      for(int i = 0; i < _bgs.length; ++i){
        double diff = fullBeta[i] - _bgs[i];
        res += .5*_rho[i]*diff*diff;
      }
    }
    return res;
  }

  //  // filter the current active columns using the strong rules
//  // note: strong rules are update so tha they keep all previous coefficients in, to prevent issues with line-search
  private double pickNextLambda(){
    final double[] grad = _lastResult.fullGrad(alpha[0],_currentLambda);
    return pickNextLambda(_currentLambda, grad, Math.max((int) (Math.min(_srcDinfo.fullN(),_nobs) * 0.05), 1));
  }
  private double pickNextLambda(final double oldLambda, final double[] grad, int maxNewVars){
    double [] g = grad.clone();
    for(int i = 0; i < g.length; ++i)
      g[i] = g[i] < 0?g[i]:-g[i];
    if(_activeCols != null) { // not interested in cols which are already active!
      for (int i : _activeCols) g[i] *= -1;
    }
    Arrays.sort(g);
    if(maxNewVars < (g.length-1) && g[maxNewVars] == g[maxNewVars+1]){
      double x = g[maxNewVars];
      while(maxNewVars > 0 && g[maxNewVars] == x)--maxNewVars;
    }
    double res = 0.5*(-g[maxNewVars]/Math.max(1e-3,alpha[0]) + oldLambda);
    return res < oldLambda?res:oldLambda*0.9;
  }
  // filter the current active columns using the strong rules
  private int [] activeCols(final double l1, final double l2, final double [] grad){
    if(_allIn) return null;

    final double rhs = alpha[0]*(2*l1-l2);
    int [] cols = MemoryManager.malloc4(_srcDinfo.fullN());
    int selected = 0;
    int j = 0;
    if(_activeCols == null)_activeCols = new int[]{-1};
    for(int i = 0; i < _srcDinfo.fullN() - _noffsets; ++i)
      if((j < _activeCols.length && i == _activeCols[j]) ||  !(grad[i] < rhs && grad[i] > -rhs) /* note negated here to have column included in case its gradient came as NaN */){
        cols[selected++] = i;
        if(j < _activeCols.length && i == _activeCols[j])++j;
      }
    for(int c = _srcDinfo.fullN()-_noffsets; c < _srcDinfo.fullN(); ++c)
      cols[selected++] = c;
    if(!strong_rules || selected == _srcDinfo.fullN()){
      _activeCols = null;
      _allIn = true;
      _activeData._adaptedFrame = _srcDinfo._adaptedFrame;
      _activeData = _srcDinfo;
    } else {
      _activeCols = Arrays.copyOf(cols,selected);
      _activeData = _srcDinfo.filterExpandedColumns(_activeCols);
    }
    LogInfo("strong rule at lambda_value=" + l1 + ", got " + (selected - _noffsets) + " active cols out of " + (_srcDinfo.fullN() - _noffsets) + " total.");
    assert _activeCols == null || _activeData.fullN() == _activeCols.length:LogInfo("mismatched number of cols, got " + _activeCols.length + " active cols, but data info claims " + _activeData.fullN());
    return _activeCols;
  }

  // Expand grid search related argument sets
  @Override protected NanoHTTPD.Response serveGrid(NanoHTTPD server, Properties parms, RequestType type) {
    return superServeGrid(server, parms, type);
  }

  public static final DecimalFormat AUC_DFORMAT = new DecimalFormat("#.###");

  public static final String aucStr(double auc){
    return AUC_DFORMAT.format(Math.round(1000 * auc) * 0.001);
  }
  public static final DecimalFormat AIC_DFORMAT = new DecimalFormat("###.###");

  public static final String aicStr(double aic){
    return AUC_DFORMAT.format(Math.round(1000*aic)*0.001);
  }
  public static final DecimalFormat DEV_EXPLAINED_DFORMAT = new DecimalFormat("#.###");
  public static final String devExplainedStr(double dev){
    return AUC_DFORMAT.format(Math.round(1000*dev)*0.001);
  }


  public static class GLMGrid extends Lockable<GLMGrid> {
    static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
    static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.
    final Key _jobKey;
    final long _startTime;
    @API(help="mean of response in the training dataset")
    public final Key [] destination_keys;
    final double [] _alphas;

    public GLMGrid (Key gridKey,Key jobKey, GLM2 [] jobs){
      super(gridKey);
      _jobKey = jobKey;
      _alphas = new double [jobs.length];
      destination_keys = new Key[jobs.length];
      for(int i = 0; i < jobs.length; ++i){
        destination_keys[i] = jobs[i].destination_key;
        _alphas[i] = jobs[i].alpha[0];
      }
      _startTime = System.currentTimeMillis();
    }

    public static class UnlockGridTsk extends DTask.DKeyTask<UnlockGridTsk,GLMGrid> {
      final Key _jobKey;

      public UnlockGridTsk(Key gridKey, Key jobKey, H2OCountedCompleter cc){
        super(cc,gridKey);
        _jobKey = jobKey;
      }
      @Override
      public void map(GLMGrid g) {
        addToPendingCount(g.destination_keys.length);
        for(Key k:g.destination_keys)
          new GLMModel.UnlockModelTask(this,k,_jobKey).forkTask();
        g.unlock(_jobKey);
      }
    }

    public static class DeleteGridTsk extends DTask.DKeyTask<DeleteGridTsk,GLMGrid> {
      public DeleteGridTsk(H2OCountedCompleter cc, Key gridKey){
        super(cc,gridKey);
      }
      @Override
      public void map(GLMGrid g) {
        addToPendingCount(g.destination_keys.length);
        for(Key k:g.destination_keys)
          new GLMModel.DeleteModelTask(this,k).forkTask();
        assert g.is_unlocked():"not unlocked??";
        g.delete();
      }
    }
    @Override
    protected Futures delete_impl(Futures fs) {return fs;}

    @Override
    protected String errStr() {
      return null;
    }
  }


  public class GLMGridSearch extends Job {
    public final int _maxParallelism;
    transient private AtomicInteger _idx;

    public final GLM2 [] _jobs;
    public final GLM2 _glm2;

    public GLMGridSearch(int maxP, GLM2 glm2, Key destKey){
      super(glm2.self(), destKey);
      _glm2 = glm2;
      description = "GLM Grid on data " + glm2._srcDinfo.toString() ;
      _maxParallelism = maxP;
      _jobs = new GLM2[glm2.alpha.length];
      _idx = new AtomicInteger(_maxParallelism);
      for(int i = 0; i < _jobs.length; ++i) {
        _jobs[i] = (GLM2)_glm2.clone();
        _jobs[i]._grid = true;
        _jobs[i].alpha = new double[]{glm2.alpha[i]};
        _jobs[i].destination_key = Key.make(glm2.destination_key + "_" + i);
        _jobs[i]._progressKey = Key.make(dest().toString() + "_progress_" + i, (byte) 1, Key.HIDDEN_USER_KEY, dest().home_node());
        _jobs[i].job_key = Key.make(glm2.job_key + "_" + i);
      }
    }

    @Override public float progress(){
      float sum = 0f;
      for(GLM2 g:_jobs)sum += g.progress();
      return sum/_jobs.length;
    }
    private transient boolean _cancelled;
    @Override public void cancel(){
      _cancelled = true;
      for(GLM2 g:_jobs)
        g.cancel();
      source.unlock(self());
      DKV.remove(destination_key);
      super.cancel();
    }
    @Override
    public GLMGridSearch fork(){
      System.out.println("read-locking " + source._key + " by job " + self());
      source.read_lock(self());
      Futures fs = new Futures();
      new GLMGrid(destination_key,self(),_jobs).delete_and_lock(self());
      // keep *this* separate from what's stored in K/V as job (will be changing it!)
      assert _maxParallelism >= 1;
      final Job job = this;

      _fjtask = new H2O.H2OEmptyCompleter();
      H2OCountedCompleter fjtask = new H2OCallback<ParallelGLMs>(_fjtask) {
        @Override public String toString(){
          return "GLMGrid.Job.Callback, completer = " + getCompleter() == null?"null":getCompleter().toString();
        }
        @Override
        public void callback(ParallelGLMs parallelGLMs) {
          _glm2._done = true;
          // we're gonna get success-callback after cancelling forked tasks since forked glms do not propagate exception if part of grid search
          if(!_cancelled) {
            source.unlock(self());
            Lockable.unlock_lockable(destination_key, self());
            remove();
          }
        }
        @Override public boolean onExceptionalCompletion(Throwable t, CountedCompleter cmp){
          if(!(t instanceof JobCancelledException) && (t.getMessage() == null || !t.getMessage().contains("job was cancelled"))) {
            job.cancel(t);
          }
          return true;
        }
      };
      start(_fjtask); // modifying GLM2 object, don't want job object to be the same instance
      fs.blockForPending();
      H2O.submitTask(new ParallelGLMs(this,_jobs,Double.NaN,H2O.CLOUD.size(),fjtask));
      return this;
    }

    @Override public Response redirect() {
      String n = GridSearchProgress.class.getSimpleName();
      return Response.redirect( this, n, "job_key", job_key, "destination_key", destination_key);
    }
  }

  private static class GLMT extends DTask<GLMT> {
    private final GLM2 _glm;
    private final double _lambda;

    public GLMT(H2OCountedCompleter cmp, GLM2 glm, double lambda){
      super(cmp);
      _glm = glm;
      _lambda = lambda;
    }
    @Override
    public void compute2() {
      assert Double.isNaN(_lambda) || _glm._lastResult._fullGrad != null:_glm.LogInfo("missing full gradient");
      if(Double.isNaN(_lambda))
        _glm.fork(this);
      else {
        _glm.nextLambda(_lambda, this);
      }
    }
    @Override public void onCompletion(CountedCompleter cc){
      if(!Double.isNaN(_lambda)) {
        assert _glm._done : _glm.LogInfo("GLMT hit onCompletion but glm is not done yet!");
        assert _glm._lastResult._fullGrad != null : _glm.LogInfo(" GLMT done with missing full gradient");
      }
    }
  }
  // class to execute multiple GLM runs in parallel
  // (with  user-given limit on how many to run in parallel)
  public static class ParallelGLMs extends H2OCountedCompleter {
    transient final private GLM2 [] _glms;
    transient final private GLMT [] _tasks;
    transient final Job _job;
    transient final public int _maxP;
    transient private AtomicInteger _nextTask;
    public final double _lambda;
    public ParallelGLMs(Job j, GLM2 [] glms){this(j,glms,Double.NaN);}
    public ParallelGLMs(Job j, GLM2 [] glms, double lambda){this(j,glms,lambda, H2O.CLOUD.size());}
    public ParallelGLMs(Job j, GLM2 [] glms, double lambda, int maxP){
      _job = j; _lambda = lambda;  _glms = glms; _maxP = maxP;
      _tasks = new GLMT[_glms.length];
      addToPendingCount(_glms.length);
    }
    public ParallelGLMs(Job j, GLM2 [] glms, double lambda, int maxP, H2OCountedCompleter cmp){
      super(cmp); _lambda = lambda; _job = j; _glms = glms; _maxP = maxP;
      _tasks = new GLMT[_glms.length];
      addToPendingCount(_glms.length);
    }

    private void forkDTask(int i){
      int nodeId = i%H2O.CLOUD.size();
      forkDTask(i,H2O.CLOUD._memary[nodeId]);
    }
    private void forkDTask(final int i, H2ONode n){
      _tasks[i] = new GLMT(new Callback(n,i),_glms[i],_lambda);
      assert Double.isNaN(_lambda) || _tasks[i]._glm._lastResult._fullGrad != null;
      if(n == H2O.SELF) H2O.submitTask(_tasks[i]);
      else new RPC(n,_tasks[i]).call();
    }
    class Callback extends H2OCallback<H2OCountedCompleter> {
      final int i;
      final H2ONode n;
      public Callback(H2ONode n, int i){super(ParallelGLMs.this); this.n = n; this.i = i;}
      @Override public void callback(H2OCountedCompleter cc){
        int i;
        if((i = _nextTask.getAndIncrement()) < _glms.length) { // not done yet
          forkDTask(i, n);
        }
      }
      @Override public boolean onExceptionalCompletion(Throwable ex, CountedCompleter caller){
        _job.cancel(ex);
        return true;
      }
    }
    @Override public void compute2(){
      final int n = Math.min(_maxP, _glms.length);
      _nextTask = new AtomicInteger(n);
      for(int i = 0; i < n; ++i)
        forkDTask(i);
      tryComplete();
    }
    @Override public void onCompletion(CountedCompleter cc){
      if(!Double.isNaN(_lambda))
        for(int i= 0; i < _tasks.length; ++i) {
          assert _tasks[i]._glm._lastResult._fullGrad != null;
          _glms[i] = _tasks[i]._glm;
        }
    }
  }

}
