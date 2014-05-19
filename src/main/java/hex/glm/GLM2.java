package hex.glm;

import com.google.gson.JsonObject;
import hex.FrameTask.DataInfo;
import hex.GridSearch.GridSearchProgress;
import hex.glm.GLMModel.GLMXValidationTask;
import hex.glm.GLMParams.Family;
import hex.glm.GLMParams.Link;
import hex.glm.GLMTask.GLMIterationTask;
import hex.glm.GLMTask.LMAXTask;
import hex.glm.GLMTask.YMUTask;
import hex.glm.LSMSolver.ADMMSolver;
import jsr166y.CountedCompleter;
import water.*;
import water.H2O.H2OCallback;
import water.H2O.H2OCountedCompleter;
import water.api.DocGen;
import water.api.ParamImportance;
import water.fvec.Frame;
import water.util.Log;
import water.util.RString;
import water.util.Utils;

import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

public class GLM2 extends Job.ModelJobWithoutClassificationField {
  static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
  public static DocGen.FieldDoc[] DOC_FIELDS;
  public static final String DOC_GET = "GLM2";
  public final String _jobName;

  // API input parameters BEGIN ------------------------------------------------------------

  @API(help = "max-iterations", filter = Default.class, lmin=1, lmax=1000000, json=true, importance = ParamImportance.CRITICAL)
  int max_iter = 100;

  @API(help = "Standardize numeric columns to have zero mean and unit variance.", filter = Default.class, json=true, importance = ParamImportance.CRITICAL)
  boolean standardize = true;

  @API(help = "validation folds", filter = Default.class, lmin=0, lmax=100, json=true, importance = ParamImportance.CRITICAL)
  int n_folds;

  @API(help = "Family.", filter = Default.class, json=true, importance = ParamImportance.CRITICAL)
  Family family = Family.gaussian;

  @API(help = "Tweedie variance power", filter = Default.class, json=true, importance = ParamImportance.SECONDARY)
  double tweedie_variance_power;

  @API(help = "distribution of regularization between L1 and L2.", filter = Default.class, json=true, importance = ParamImportance.SECONDARY)
  double [] alpha = new double[]{0.5};

  @API(help = "regularization strength", filter = Default.class, json=true, importance = ParamImportance.SECONDARY)
  public double [] lambda = new double[]{1e-5};

  @API(help = "beta_eps", filter = Default.class, json=true, importance = ParamImportance.SECONDARY)
  double beta_epsilon = DEFAULT_BETA_EPS;

  @API(help="use line search (slower speed, to be used if glm does not converge otherwise)",filter=Default.class)
  boolean higher_accuracy;

  @API(help="By default, first factor level is skipped from the possible set of predictors. Set this flag if you want use all of the levels. Needs sufficient regularization to solve!",filter=Default.class)
  boolean use_all_factor_levels;

  @API(help="use lambda search starting at lambda max, given lambda is then interpreted as lambda min",filter=Default.class)
  boolean lambda_search;

  @API(help="use strong rules to filter out inactive columns",filter=Default.class)
  boolean strong_rules_enabled = true;

  // intentionally not declared as API now
  int sparseCoefThreshold = 1000; // if more than this number of predictors, result vector of coefficients will be stored sparse

  @API(help="number of lambdas to be used in a search",filter=Default.class)
  int nlambdas = 100;

  @API(help="min lambda used in lambda search, specified as a ratio of lambda_max",filter=Default.class)
  double lambda_min_ratio = 0.0001;

  @API(help="prior probability for y==1. To be used only for logistic regression iff the data has been sampled and the mean of response does not reflect reality.",filter=Default.class)
  double prior = -1; // -1 is magic value for default value which is mean(y) computed on the current dataset
  private transient double _iceptAdjust; // adjustment due to the prior
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

  @API(help = "", json=true, importance = ParamImportance.SECONDARY)
  Link link = Link.identity;

  @API(help = "Tweedie link power", json=true, importance = ParamImportance.SECONDARY)
  double tweedie_link_power;

  @API(help = "lambda max", json=true, importance = ParamImportance.SECONDARY)
  double lambda_max = Double.NaN;

  public static int MAX_PREDICTORS = 10000;

  // API output parameters END ------------------------------------------------------------


  private static double GLM_GRAD_EPS = 1e-5; // done (converged) if subgrad < this value.

  private boolean highAccuracy(){return higher_accuracy;}
  private void setHighAccuracy(){
    higher_accuracy = true;
  }

  private DataInfo _dinfo;
  private transient int [] _activeCols;
  private DataInfo _activeData;
  public GLMParams _glm;
  private boolean _grid;

  private double ADMM_GRAD_EPS = 1e-4; // default addm gradietn eps
  private static final double MIN_ADMM_GRAD_EPS = 1e-5; // min admm gradient eps

  int _lambdaIdx = 0;

  private transient double _addedL2;

  public static final double DEFAULT_BETA_EPS = 1e-4;

  private transient double _ymu;
  private transient double _reg;
  private transient int    _iter;
  private transient GLMModel _model;

  private double objval(GLMIterationTask glmt){
    return glmt._val.residual_deviance/ glmt._nobs + 0.5*l2pen()*l2norm(glmt._beta) + l1pen()*l1norm(glmt._beta);
  }
  private static class IterationInfo {
    final int _iter;
    double [] _fullGrad;

    private final GLMIterationTask _glmt;
    final int [] _activeCols;

    public IterationInfo(int i, GLMIterationTask glmt, final int [] activeCols){
      _iter = i;
      _glmt = glmt.clone();
      _activeCols = activeCols;
      assert _glmt._beta != null && _glmt._val != null;
    }
  }

  private transient IterationInfo _lastResult;

  @Override
  public JsonObject toJSON() {
    JsonObject jo = super.toJSON();
    if (lambda == null) jo.addProperty("lambda", "automatic"); //better than not printing anything if lambda=null
    return jo;
  }

  @Override public Key defaultDestKey(){
    return null;
  }
  @Override public Key defaultJobKey() {return null;}

  public GLM2() {_jobName = "";}
  public GLM2(String desc, Key jobKey, Key dest, DataInfo dinfo, GLMParams glm, double [] lambda){
    this(desc,jobKey,dest,dinfo,glm,lambda,0.5,0);
  }
  public GLM2(String desc, Key jobKey, Key dest, DataInfo dinfo, GLMParams glm, double [] lambda, double alpha){
    this(desc,jobKey,dest,dinfo,glm,lambda,alpha,0);
  }
  public GLM2(String desc, Key jobKey, Key dest, DataInfo dinfo, GLMParams glm, double [] lambda, double alpha, int nfolds){
    this(desc,jobKey,dest,dinfo,glm,lambda,0.5,nfolds,DEFAULT_BETA_EPS);
  }
  public GLM2(String desc, Key jobKey, Key dest, DataInfo dinfo, GLMParams glm, double [] lambda, double alpha,int nfolds, double betaEpsilon){
    this(desc,jobKey,dest,dinfo,glm,lambda,alpha,nfolds,betaEpsilon,null);
  }
  public GLM2(String desc, Key jobKey, Key dest, DataInfo dinfo, GLMParams glm, double [] lambda, double alpha, int nfolds, double betaEpsilon, Key parentJob){
    this(desc,jobKey,dest,dinfo,glm,lambda,alpha,nfolds,betaEpsilon,parentJob, null,false,-1,0);
  }
  public GLM2(String desc, Key jobKey, Key dest, DataInfo dinfo, GLMParams glm, double [] lambda, double alpha, int nfolds, double betaEpsilon, Key parentJob, double [] beta, boolean highAccuracy, double prior, double proximalPenalty) {
    assert beta == null || beta.length == (dinfo.fullN()+1):"unexpected size of beta, got length " + beta.length + ", expected " + dinfo.fullN();
    job_key = jobKey;
    description = desc;
    destination_key = dest;
    beta_epsilon = betaEpsilon;
    _beta = beta;
    _dinfo = dinfo;
    _glm = glm;
    this.lambda = lambda;
    _beta = beta;
    if((_proximalPenalty = proximalPenalty) != 0)
      _wgiven = beta;
    this.alpha= new double[]{alpha};
    n_folds = nfolds;
    source = dinfo._adaptedFrame;
    response = dinfo._adaptedFrame.lastVec();
    _jobName = dest.toString() + ((nfolds > 1)?("[" + dinfo._foldId + "]"):"");
    higher_accuracy = highAccuracy;
    this.prior = prior;
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

  /** Return the query link to this page */
  public static String link(Key k, String content) {
    RString rs = new RString("<a href='GLM2.query?source=%$key'>%content</a>");
    rs.replace("key", k.toString());
    rs.replace("content", content);
    return rs.toString();
  }

  public static Job gridSearch(Key jobKey, Key destinationKey, DataInfo dinfo, GLMParams glm, double [] lambda, boolean lambda_search, double [] alpha, boolean higher_accuracy, int nfolds){
    return gridSearch(jobKey, destinationKey, dinfo, glm, lambda, lambda_search, alpha, higher_accuracy, nfolds, DEFAULT_BETA_EPS);
  }
  public static Job gridSearch(Key jobKey, Key destinationKey, DataInfo dinfo, GLMParams glm, double [] lambda, boolean lambda_search, double [] alpha, boolean high_accuracy, int nfolds, double betaEpsilon){
    return new GLMGridSearch(4, jobKey, destinationKey,dinfo,glm,lambda, lambda_search, alpha, high_accuracy, nfolds,betaEpsilon).fork();
  }

  protected void complete(){
    if(_addedL2 > 0){
      String warn = "Added L2 penalty (rho = " + _addedL2 + ")  due to non-spd matrix. ";
      if(_model.warnings == null || _model.warnings.length == 0)
        _model.warnings = new String[]{warn};
      else {
        _model.warnings = Arrays.copyOf(_model.warnings,_model.warnings.length+1);
        _model.warnings[_model.warnings.length-1] = warn;
      }
      _model.update(self());
    }
    _model.unlock(self());
    if( _dinfo._nfolds == 0 && !_grid)remove(); // Remove/complete job only for top-level, not xval GLM2s
    state = JobState.DONE;
    if(_fjtask != null)_fjtask.tryComplete();
  }

  @Override public void cancel(Throwable ex){
    if(isCancelledOrCrashed())return;
    if( _model != null ) _model.unlock(self());
    if(ex instanceof JobCancelledException){
      if(!isCancelledOrCrashed())cancel();
    } else super.cancel(ex);
  }

  @Override public void init(){
    super.init();
    if(lambda_search && lambda.length > 1)
      throw new IllegalArgumentException("Can not supply both lambda_search and multiple lambdas. If lambda_search is on, GLM expects only one value of lambda, representing the lambda min (smallest lambda in the lambda search).");
    // check the response
    if( response.isEnum() && family != Family.binomial)throw new IllegalArgumentException("Invalid response variable, trying to run regression with categorical response!");
    switch( family ) {
      case poisson:
      case tweedie:
        if( response.min() < 0 ) throw new IllegalArgumentException("Illegal response column for family='" + family + "', response must be >= 0.");
        break;
      case gamma:
        if( response.min() <= 0 ) throw new IllegalArgumentException("Invalid response for family='Gamma', response must be > 0!");
        break;
      case binomial:
        if(response.min() < 0 || response.max() > 1) throw new IllegalArgumentException("Illegal response column for family='Binomial', response must in <0,1> range!");
        break;
      default:
        //pass
    }
    Frame fr = DataInfo.prepareFrame(source, response, ignored_cols, family==Family.binomial, true,true);
    _dinfo = new DataInfo(fr, 1, use_all_factor_levels || lambda_search, standardize,false);
    if(higher_accuracy)setHighAccuracy();
  }
  @Override protected boolean filterNaCols(){return true;}
  @Override protected Response serve() {
    init();
    link = family.defaultLink;// TODO
    tweedie_link_power = 1 - tweedie_variance_power;// TODO
    _glm = new GLMParams(family, tweedie_variance_power, link, tweedie_link_power);
    if(alpha.length > 1) { // grid search
      if(destination_key == null)destination_key = Key.make("GLMGridResults_"+Key.make());
      if(job_key == null)job_key = Key.make((byte) 0, Key.JOB, H2O.SELF);;
      Job j = gridSearch(self(),destination_key, _dinfo, _glm, lambda, lambda_search, alpha, higher_accuracy, n_folds);
      return GLMGridView.redirect(this,j.dest());
    } else {
      if(destination_key == null)destination_key = Key.make("GLMModel_"+Key.make());
      if(job_key == null)job_key = Key.make("GLM2Job_"+Key.make());
      fork();
      return GLMProgress.redirect(this,job_key, dest());
    }
  }
  private static double beta_diff(double[] b1, double[] b2) {
    if(b1 == null)return Double.MAX_VALUE;
    double res = Math.abs(b1[0] - b2[0]);
    for( int i = 1; i < b1.length; ++i )
      res = Math.max(res, Math.abs(b1[i] - b2[i]));
    return res;
  }
  @Override public float progress(){ return (float)_iter/max_iter;}

  protected double l2norm(double[] beta){
    double l2 = 0;
    for(int i = 0; i < beta.length; ++i)
      l2 += beta[i]*beta[i];
    return l2;
  }
  protected double l1norm(double[] beta){
    double l2 = 0;
    for(int i = 0; i < beta.length; ++i)
      l2 += Math.abs(beta[i]);
    return l2;
  }

  private final double [] expandVec(double [] beta, final int [] activeCols){
    if(activeCols == null)return beta;
    double [] res = MemoryManager.malloc8d(_dinfo.fullN()+1);
    int i = 0;
    for(int c:activeCols)
      res[c] = beta[i++];
    res[res.length-1] = beta[beta.length-1];
    return res;
  }

  private final double [] contractVec(double [] beta, final int [] activeCols){
    if(activeCols == null)return beta.clone();
    double [] res = MemoryManager.malloc8d(activeCols.length+1);
    int i = 0;
    for(int c:activeCols)
      res[i++] = beta[c];
    res[res.length-1] = beta[beta.length-1];
    return res;
  }
  private final double [] resizeVec(double[] beta, final int[] activeCols, final int[] oldActiveCols){
    if(Arrays.equals(activeCols,oldActiveCols))return beta;
    double [] full = expandVec(beta,oldActiveCols);
    if(activeCols == null)return full;
    return contractVec(full,activeCols);
  }
  protected boolean needLineSearch(final double [] beta,double objval, double step){
    if(Double.isNaN(objval))return true; // needed for gamma (and possibly others...)
    final double [] grad = _activeCols == _lastResult._activeCols
      ?_lastResult._glmt.gradient(l2pen())
      :contractVec(_lastResult._fullGrad,_activeCols);
    // line search
    double f_hat = 0;
    ADMMSolver.subgrad(alpha[0],lambda[_lambdaIdx],beta,grad);
    final double [] oldBeta = resizeVec(_lastResult._glmt._beta, _activeCols,_lastResult._activeCols);
    for(int i = 0; i < beta.length; ++i){
      double diff = beta[i] - oldBeta[i];
      f_hat += grad[i]*diff;
    }
    f_hat = objval(_lastResult._glmt) + 0.25*step*f_hat;
    return objval > f_hat;
  }
  private class LineSearchIteration extends H2OCallback<GLMTask.GLMLineSearchTask> {
    @Override public void callback(final GLMTask.GLMLineSearchTask glmt) {
      double step = 0.5;
      for(int i = 0; i < glmt._objvals.length; ++i){
        if(!needLineSearch(glmt._betas[i],glmt._objvals[i],step)){
          Log.info("GLM2 (iteration=" + _iter + ") line search: found admissible step=" + step);
          _lastResult = null; // set last result to null so that the Iteration will not attempt to verify whether or not it should do the line search.
          new GLMIterationTask(GLM2.this,_activeData,_glm,true,true,true,glmt._betas[i],_ymu,_reg,new Iteration()).asyncExec(_activeData._adaptedFrame);
          return;
        }
        step *= 0.5;
      } // no line step worked, forcibly converge
      Log.info("GLM2 (iteration=" + _iter + ") line search failed to find feasible step. Forcibly converged.");
      nextLambda(_lastResult._glmt.clone(),resizeVec(_lastResult._glmt._beta,_activeCols,_lastResult._activeCols));
    }
    @Override public boolean onExceptionalCompletion(Throwable ex, CountedCompleter caller){
      GLM2.this.cancel(ex);
      return true;
    }
  }

  protected double checkGradient(final double [] newBeta, final double [] grad){
    // check the gradient
    ADMMSolver.subgrad(alpha[0], lambda[_lambdaIdx], newBeta, grad);
    double err = 0;
    for(double d:grad)
      if(d > err) err = d;
      else if(d < -err) err = -d;
    Log.info("GLM converged with max |subgradient| = " + err);
    return err;
  }

  protected void nextLambda(final GLMIterationTask glmt, GLMValidation val){
    currentLambdaIter = 0;
    boolean improved = _model.setAndTestValidation(_lambdaIdx,val);
    _model.clone().update(self());
    boolean done = false; // _iter < max_iter && (improved || _runAllLambdas) && _lambdaIdx < (lambda.length-1);
    if(_iter == max_iter){
      Log.info("GLM2 reached max #iterations.");
      done = true;
    } else if(!improved && !_runAllLambdas){
      Log.info("GLM2 converged as solution stopped improving with decreasing lambda.");
      done = true;
    } else if(_lambdaIdx == lambda.length-1){
      Log.info("GLM2 done with all given lambdas.");
      done = true;
    } else if(_activeCols != null && _activeCols.length + 1 >= MAX_PREDICTORS){
      Log.info("GLM2 reached maximum allowed number of predictors at lambda = " + lambda[_lambdaIdx]);
      done = true;
    }
    if(!done){ // continue with next lambda value?
      ++_lambdaIdx;
      glmt._val = null;
      if(glmt._gram == null){ // assume we had lambda search with strong rules
        // we use strong rules so we can't really used this gram for the next lambda computation (different sets of coefficients)
        // I expect that:
        //  1) beta has been expanded to match current set of active cols
        //  2) it is new GLMIteration ready to be launched
        // caller (nextLambda(glmt,beta)) is expected to ensure this...
        assert _activeCols == null || (glmt._beta.length == _activeCols.length+1);
        assert !glmt.isDone();
        glmt.asyncExec(_activeData._adaptedFrame);
      } else // we have the right gram, just solve with with next lambda
        new Iteration().callback(glmt);
    } else    // nope, we're done
      GLM2.this.complete(); // signal we're done to anyone waiting for the job
  }

  private void nextLambda(final GLMIterationTask glmt, final double [] newBeta){
    final double [] fullBeta = setNewBeta(newBeta);
    // now we need full gradient (on all columns) using this beta
    new GLMIterationTask(GLM2.this,_dinfo,_glm,false,true,true,fullBeta,_ymu,_reg,new H2OCallback<GLMIterationTask>(GLM2.this){
      @Override public void callback(final GLMIterationTask glmt2){
        final double [] grad = glmt2.gradient(l2pen());
        if(_lastResult != null && _lambdaIdx < (lambda.length-1))
          _lastResult._fullGrad = glmt2.gradient(l2pen(_lambdaIdx+1));
        // check the KKT conditions and filter data for next lambda
        // check the gradient
        ADMMSolver.subgrad(alpha[0], lambda[_lambdaIdx], fullBeta, grad);
        double err = 0;
        if(_activeCols != null){
          for(int c:_activeCols)
            if(grad[c] > err) err = grad[c];
            else if(grad[c] < -err) err = -grad[c];
          int [] failedCols = new int[64];
          int fcnt = 0;
          for(int i = 0; i < grad.length-1; ++i){
            if(Arrays.binarySearch(_activeCols,i) >= 0)continue;
            if(grad[i] > GLM_GRAD_EPS || -grad[i] < -GLM_GRAD_EPS){
              if(fcnt == failedCols.length)
                failedCols = Arrays.copyOf(failedCols,failedCols.length << 1);
              failedCols[fcnt++] = i;
            }
          }
          if(fcnt > 0){
            Log.info("GLM2: " + fcnt + " variables failed KKT conditions check! Adding them to the model and continuing computation...");
            final int n = _activeCols.length;
            final int [] oldActiveCols = _activeCols;
            _activeCols = Arrays.copyOf(_activeCols,_activeCols.length+fcnt);
            for(int i = 0; i < fcnt; ++i)
              _activeCols[n+i] = failedCols[i];
            Arrays.sort(_activeCols);
            _activeData = _dinfo.filterExpandedColumns(_activeCols);
            new GLMIterationTask(GLM2.this, _activeData,_glm,true,false,false, resizeVec(newBeta,_activeCols,oldActiveCols),glmt._ymu,glmt._reg,new Iteration()).asyncExec(_activeData._adaptedFrame);
            return;
          }
        } else {
          for(double d:grad)
            if(d > err) err = d;
            else if(d < -err) err = -d;
        }
        final GLMIterationTask glmt3;
        // now filter out the cols for the next lambda...
        if(lambda.length > 1 && _lambdaIdx < lambda.length-1 && _activeCols != null){
          final int [] oldCols = _activeCols;
          activeCols(lambda[_lambdaIdx+1],lambda[_lambdaIdx],glmt2.gradient(l2pen()));
          // epxand the beta
          final double [] fullBeta = glmt2._beta;
          final double [] newBeta;
          if(_activeCols != null){
            newBeta = MemoryManager.malloc8d(_activeCols.length+1);
            newBeta[newBeta.length-1] = fullBeta[fullBeta.length-1];
            int j = 0;
            for(int c:_activeCols)
              newBeta[j++] = fullBeta[c];
            assert j == newBeta.length-1;
          } else
            newBeta = fullBeta;
          if(Arrays.equals(oldCols,_activeCols) && (glmt._gram.fullN() == _activeCols.length+1)) // set of coefficients did not change
            glmt3 = glmt;
          else
            glmt3 = new GLMIterationTask(GLM2.this,_activeData,glmt._glm,true,false,false,newBeta,glmt._ymu,glmt._reg,new Iteration());
        } else glmt3 = glmt;
        if(n_folds > 1)
          xvalidate(_model,_lambdaIdx,new H2OCallback<GLMModel.GLMValidationTask>(GLM2.this) {
            @Override public void callback(GLMModel.GLMValidationTask v){ nextLambda(glmt3,v._res);}
          });
        else  nextLambda(glmt3,glmt2._val);
      }
    }).asyncExec(_dinfo._adaptedFrame);
  }

  private double [] setNewBeta(final double [] newBeta){
    final double [] fullBeta = (_activeCols == null)?newBeta:expandVec(newBeta,_activeCols);
    final double [] newBetaDeNorm;
    if(_dinfo._standardize) {
      newBetaDeNorm = fullBeta.clone();
      double norm = 0.0;        // Reverse any normalization on the intercept
      // denormalize only the numeric coefs (categoricals are not normalized)
      final int numoff = _dinfo.numStart();
      for( int i=numoff; i< fullBeta.length-1; i++ ) {
        double b = newBetaDeNorm[i]*_dinfo._normMul[i-numoff];
        norm += b*_dinfo._normSub[i-numoff]; // Also accumulate the intercept adjustment
        newBetaDeNorm[i] = b;
      }
      newBetaDeNorm[newBetaDeNorm.length-1] -= norm;
    } else
      newBetaDeNorm = null;
    _model.setLambdaSubmodel(_lambdaIdx, newBetaDeNorm == null ? fullBeta : newBetaDeNorm, newBetaDeNorm == null ? null : fullBeta, (_iter + 1),_dinfo.fullN() >= sparseCoefThreshold);
    _model.clone().update(self());
    return fullBeta;
  }
  private class Iteration extends H2OCallback<GLMIterationTask> {
    public final long _iterationStartTime;
    public Iteration(){super(GLM2.this); _iterationStartTime = System.currentTimeMillis(); _model.start_training(null);}
    @Override public void callback(final GLMIterationTask glmt){
      _model.stop_training();
      Log.info("GLM2 iteration(" + _iter + ") done in " + (System.currentTimeMillis() - _iterationStartTime) + "ms");
      if( !isRunning(self()) )  throw new JobCancelledException();
      currentLambdaIter++;
      if(glmt._val != null){
        if(!(glmt._val.residual_deviance < glmt._val.null_deviance)){ // complete fail, look if we can restart with higher_accuracy on
          if(!highAccuracy()){
            Log.info("GLM2 reached negative explained deviance without line-search, rerunning with high accuracy settings.");
            setHighAccuracy();
            if(_lastResult != null)
              new GLMIterationTask(GLM2.this,_activeData,glmt._glm, true, true, true, _lastResult._glmt._beta,_ymu,_reg,new Iteration()).asyncExec(_activeData._adaptedFrame);
            else if(_lambdaIdx > 2) // > 2 because 0 is null model, we don't wan to run with that
              new GLMIterationTask(GLM2.this,_activeData,glmt._glm, true, true, true, _model.submodels[_lambdaIdx-1].norm_beta,_ymu,_reg,new Iteration()).asyncExec(_activeData._adaptedFrame);
            else // no sane solution to go back to, start from scratch!
              new GLMIterationTask(GLM2.this,_activeData,glmt._glm, true, false, false, null,_ymu,_reg,new Iteration()).asyncExec(_activeData._adaptedFrame);
            _lastResult = null;
            return;
          }
        }
        _model.setAndTestValidation(_lambdaIdx,glmt._val);
        _model.clone().update(self());
      }

      if(glmt._val != null && glmt._computeGradient){ // check gradient
        final double [] grad = glmt.gradient(l2pen());
        ADMMSolver.subgrad(alpha[0], lambda[_lambdaIdx], glmt._beta, grad);
        double err = 0;
        for(double d:grad)
          if(d > err) err = d;
          else if(d < -err) err = -d;
        Log.info("GLM2 gradient after " + _iter + " iterations = " + err);
        if(err <= GLM_GRAD_EPS){
          Log.info("GLM2 converged by reaching small enough gradient, with max |subgradient| = " + err);
          setNewBeta(glmt._beta);
          nextLambda(glmt, glmt._beta);
          return;
        }
      }
      if(glmt._beta != null && glmt._val!=null && glmt._computeGradient && _glm.family != Family.tweedie){
        if(_lastResult != null && needLineSearch(glmt._beta,objval(glmt),1)){
          if(!highAccuracy()){
            setHighAccuracy();
            if(_lastResult._iter < (_iter-2)){ // there is a gap form last result...return to it and start again
              final double [] prevBeta = _lastResult._activeCols != _activeCols? resizeVec(_lastResult._glmt._beta, _activeCols, _lastResult._activeCols):_lastResult._glmt._beta;
              new GLMIterationTask(GLM2.this,_activeData,glmt._glm, true, true, true, prevBeta, _ymu,_reg,new Iteration()).asyncExec(_activeData._adaptedFrame);
              return;
            }
          }
          final double [] b = resizeVec(_lastResult._glmt._beta, _activeCols, _lastResult._activeCols);
          assert (b.length == glmt._beta.length):b.length + " != " + glmt._beta.length + ", activeCols = " + _activeCols.length;
          new GLMTask.GLMLineSearchTask(GLM2.this,_activeData,_glm, resizeVec(_lastResult._glmt._beta, _activeCols, _lastResult._activeCols),glmt._beta,1e-4,glmt._nobs,alpha[0],lambda[_lambdaIdx], new LineSearchIteration()).asyncExec(_activeData._adaptedFrame);
          return;
        }
        _lastResult = new IterationInfo(GLM2.this._iter-1, glmt,_activeCols);

      }
      final double [] newBeta = MemoryManager.malloc8d(glmt._xy.length);
      ADMMSolver slvr = new ADMMSolver(lambda[_lambdaIdx],alpha[0], ADMM_GRAD_EPS, _addedL2);
      slvr.solve(glmt._gram,glmt._xy,glmt._yy,newBeta);
      _addedL2 = slvr._addedL2;
      if(Utils.hasNaNsOrInfs(newBeta)){
        Log.info("GLM2 forcibly converged by getting NaNs and/or Infs in beta");
        nextLambda(glmt,glmt._beta);
      } else {
        setNewBeta(newBeta);
        final double bdiff = beta_diff(glmt._beta,newBeta);
        if(_glm.family == Family.gaussian || bdiff < beta_epsilon || _iter == max_iter){ // Gaussian is non-iterative and gradient is ADMMSolver's gradient => just validate and move on to the next lambda
          int diff = (int)Math.log10(bdiff);
          int nzs = 0;
          for(int i = 0; i < newBeta.length; ++i)
            if(newBeta[i] != 0) ++nzs;
          if(newBeta.length < 20)System.out.println("beta = " + Arrays.toString(newBeta));
          Log.info("GLM2 (lambda_" + _lambdaIdx + "=" + lambda[_lambdaIdx] + ") converged (reached a fixed point with ~ 1e" + diff + " precision) after " + _iter + "iterations, got " + nzs + " nzs");
          nextLambda(glmt,newBeta);
        } else { // not done yet, launch next iteration
          final boolean validate = higher_accuracy || (currentLambdaIter % 5) == 0;
          ++_iter;
          System.out.println("Iter = " + _iter);
          new GLMIterationTask(GLM2.this,_activeData,glmt._glm, true, validate, validate, newBeta,_ymu,_reg,new Iteration()).asyncExec(_activeData._adaptedFrame);
        }
      }
    }
  }

  private int currentLambdaIter = 0;

  @Override
  public GLM2 fork(){
    start(new H2O.H2OEmptyCompleter());
    run(true);
    return this;
  }
  // start inside of a parent job
  public void run(final H2OCountedCompleter fjt){
    assert GLM2.this._fjtask == null;
    GLM2.this._fjtask = fjt;
    run();
  }
  public long start = 0;

  public void run(){run(false);}
  public void run(final boolean doLog){
    if(doLog)logStart();
    System.out.println("running with " + _dinfo.fullN() + " predictors");
    _activeData = _dinfo;
    assert alpha.length == 1;
    start = System.currentTimeMillis();

    if(highAccuracy() || lambda_search) // shortcut for fast & simple mode
      new YMUTask(GLM2.this,_dinfo,new H2OCallback<YMUTask>(GLM2.this) {
        @Override public void callback(final YMUTask ymut){
          run(ymut.ymu(),ymut.nobs());
        }
      }).asyncExec(_dinfo._adaptedFrame);
    else {
      double ymu = _dinfo._adaptedFrame.lastVec().mean();
      run(ymu, _dinfo._adaptedFrame.numRows()); // shortcut for quick & simple
    }
  }
  private void run(final double ymu, final long nobs){
    if(_glm.family == Family.binomial && prior != -1 && prior != ymu && !Double.isNaN(prior)){
      double ratio = prior/ymu;
      double pi0 = 1,pi1 = 1;
      if(ratio > 1){
        pi1 = 1.0/ratio;
      } else if(ratio < 1) {
        pi0 = ratio;
      }
      _iceptAdjust = Math.log(pi0/pi1);
    } else prior  = ymu;
    if(highAccuracy() || lambda_search){
      new LMAXTask(GLM2.this, _dinfo, _glm, ymu,nobs,alpha[0],new H2OCallback<LMAXTask>(GLM2.this){
        @Override public void callback(LMAXTask t){ run(ymu,nobs,t);}
      }).asyncExec(_dinfo._adaptedFrame);
    } else run(ymu, nobs, null); // shortcut for quick & simple
  }

  private void run(final double ymu, final long nobs, LMAXTask lmaxt){
    String [] warns = null;
    if((!lambda_search || !strong_rules_enabled) && (_dinfo.fullN() > MAX_PREDICTORS))
      throw new IllegalArgumentException("Too many predictors! GLM can only handle " + MAX_PREDICTORS + " predictors, got " + _dinfo.fullN() + ", try to run with strong_rules enabled.");
    if(lambda_search){
      max_iter = Math.max(300,max_iter);
      assert lmaxt != null:"running lambda search, but don't know what is the lambda max!";
      final double lmax = lmaxt.lmax();
      final double d = Math.pow(lambda_min_ratio,1.0/nlambdas);
      lambda = new double [nlambdas];
      lambda[0] = lmax;
      for(int i = 1; i < lambda.length; ++i)
        lambda[i] = lambda[i-1]*d;
      _runAllLambdas = false;
    } else if(alpha[0] > 0 && lmaxt != null) { // make sure we start with lambda max (and discard all lambda > lambda max)
      final double lmax = lmaxt.lmax();
      int i = 0; while(i < lambda.length && lambda[i] > lmax)++i;
      if(i != 0) {
        Log.info("GLM: removing " + i + " lambdas > lambda_max: " + Arrays.toString(Arrays.copyOf(lambda,i)));
        warns = i == lambda.length?new String[] {"Removed " + i + " lambdas > lambda_max","No lambdas < lambda_max, returning null model."}:new String[] {"Removed " + i + " lambdas > lambda_max"};
      }
      lambda = i == lambda.length?new double [] {lambda_max}:Arrays.copyOfRange(lambda, i, lambda.length);
    }
    _model = new GLMModel(GLM2.this,dest(),_dinfo, _glm,beta_epsilon,alpha[0],lambda_max,lambda,ymu,prior);
    _model.warnings = warns;
    _model.clone().delete_and_lock(self());
    if(lambda[0] == lambda_max && alpha[0] > 0){ // fill-in trivial solution for lambda max
      _beta = MemoryManager.malloc8d(_dinfo.fullN()+1);
      _beta[_beta.length-1] = _glm.link(ymu) + _iceptAdjust;
      _model.setLambdaSubmodel(0,_beta,_beta,0,_dinfo.fullN() >= sparseCoefThreshold);
      if(lmaxt != null)
        _model.setAndTestValidation(0,lmaxt._val);
      _lambdaIdx = 1;
    }
    if(_lambdaIdx == lambda.length) // ran only with one lambda > lambda_max => return null model
      GLM2.this.complete(); // signal we're done to anyone waiting for the job
    else {
      ++_iter;
      if(lmaxt != null && strong_rules_enabled)
        activeCols(lambda[_lambdaIdx],lmaxt.lmax(),lmaxt.gradient(l2pen()));
      Log.info("GLM2 staring GLM after " + (System.currentTimeMillis()-start) + "ms of preprocessing (mean/lmax/strong rules computation)");
      new GLMIterationTask(GLM2.this, _activeData,_glm,true,false,false,null,_ymu = ymu,_reg = 1.0/nobs, new Iteration()).asyncExec(_activeData._adaptedFrame);
    }
  }

  private final double l2pen(){return l2pen(_lambdaIdx);}
  private final double l2pen(int lambdaIdx){return lambda[lambdaIdx]*(1-alpha[0]);}
  private final double l1pen(){return lambda[_lambdaIdx]*alpha[0];}

  // filter the current active columns using the strong rules
  // note: strong rules are update so tha they keep all previous coefficients in, to prevent issues with line-search
  private int [] activeCols(final double l1, final double l2, final double [] grad){
    final double rhs = alpha[0]*(2*l1-l2);
    int [] cols = MemoryManager.malloc4(_dinfo.fullN());
    int selected = 0;
    int j = 0;
    if(_activeCols == null)_activeCols = new int[]{-1};
    for(int i = 0; i < _dinfo.fullN(); ++i)
      if((j < _activeCols.length && i == _activeCols[j]) || grad[i] > rhs || grad[i] < -rhs){
        cols[selected++] = i;
        if(j < _activeCols.length && i == _activeCols[j])++j;
      }
    if(!strong_rules_enabled || selected == _dinfo.fullN()){
      _activeCols = null;
      _activeData._adaptedFrame = _dinfo._adaptedFrame;
      _activeData = _dinfo;
    } else {
      _activeCols = Arrays.copyOf(cols,selected);
      _activeData = _dinfo.filterExpandedColumns(_activeCols);
    }
    Log.info("GLM2 strong rule at lambda=" + l1 + ", got " + selected + " active cols out of " + _dinfo.fullN() + " total.");
    return _activeCols;
  }

  private void xvalidate(final GLMModel model, int lambdaIxd,final H2OCountedCompleter cmp){
    final Key [] keys = new Key[n_folds];
    GLM2 [] glms = new GLM2[n_folds];
    for(int i = 0; i < n_folds; ++i)
      glms[i] = new GLM2(this.description + "xval " + i, self(), keys[i] = Key.make(destination_key + "_" + _lambdaIdx + "_xval" + i), _dinfo.getFold(i, n_folds),_glm,new double[]{lambda[_lambdaIdx]},model.alpha,0, model.beta_eps,self(),model.norm_beta(_lambdaIdx),higher_accuracy,prior,0);
    H2O.submitTask(new ParallelGLMs(GLM2.this,glms,H2O.CLOUD.size(),new H2OCallback(GLM2.this) {
      @Override public void callback(H2OCountedCompleter t) {
        GLMModel [] models = new GLMModel[keys.length];
        // we got the xval models, now compute their validations...
        for(int i = 0; i < models.length; ++i)models[i] = DKV.get(keys[i]).get();
        new GLMXValidationTask(model,_lambdaIdx,models, cmp).asyncExec(_dinfo._adaptedFrame);
      }
    }));
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


  public static class GLMGrid extends Iced {
    static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
    static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.
    final Key _jobKey;
    final long _startTime;
    @API(help="mean of response in the training dataset")
    final Key [] destination_keys;
    final double [] _alphas;

    public GLMGrid (Key jobKey, GLM2 [] jobs){
      _jobKey = jobKey;
      _alphas = new double [jobs.length];
      destination_keys = new Key[jobs.length];
      for(int i = 0; i < jobs.length; ++i){
        destination_keys[i] = jobs[i].destination_key;
        _alphas[i] = jobs[i].alpha[0];
      }
      _startTime = System.currentTimeMillis();
    }
  }


  public static class GLMGridSearch extends Job {
    public final int _maxParallelism;
    transient private AtomicInteger _idx;

    public final GLM2 [] _jobs;

    public GLMGridSearch(int maxP, Key jobKey, Key dstKey, DataInfo dinfo, GLMParams glm, double [] lambdas, boolean lambda_search, double [] alphas, boolean high_accuracy, int nfolds, double betaEpsilon){
      super(jobKey, dstKey);
      description = "GLM Grid with params " + glm.toString() + "on data " + dinfo.toString() ;
      _maxParallelism = maxP;
      _jobs = new GLM2[alphas.length];
      _idx = new AtomicInteger(_maxParallelism);
      for(int i = 0; i < _jobs.length; ++i) {
        _jobs[i] = new GLM2("GLM grid(" + i + ")",self(),Key.make(dstKey.toString() + "_" + i),dinfo,glm,lambdas,alphas[i], nfolds, betaEpsilon,self());
        _jobs[i]._grid = true;
        _jobs[i].lambda_search = lambda_search;
        _jobs[i].higher_accuracy = high_accuracy;
      }
    }

    @Override public float progress(){
      float sum = 0f;
      for(GLM2 g:_jobs)sum += g.progress();
      return sum/_jobs.length;
    }
    @Override
    public Job fork(){
      DKV.put(destination_key, new GLMGrid(self(),_jobs));
      assert _maxParallelism >= 1;
      final H2OCountedCompleter fjt = new H2OCallback<ParallelGLMs>() {
        @Override public void callback(ParallelGLMs pgs){

          remove();
        }
      };
      start(fjt);
      H2O.submitTask(new ParallelGLMs(this,_jobs,H2O.CLOUD.size(),fjt));
      return this;
    }

    @Override public Response redirect() {
      String n = GridSearchProgress.class.getSimpleName();
      return Response.redirect( this, n, "job_key", job_key, "destination_key", destination_key);
    }
  }


  // class to execute multiple GLM runs in parallel
  // (with  user-given limit on how many to run in in parallel)
  public static class ParallelGLMs extends DTask {
    transient final private GLM2 [] _glms;
    transient final Job _job;
    transient final public int _maxP;
    transient private AtomicInteger _remCnt;
    transient private AtomicInteger _doneCnt;
    public ParallelGLMs(Job j, GLM2 [] glms){this(j,glms,H2O.CLOUD.size());}
    public ParallelGLMs(Job j, GLM2 [] glms, int maxP){_job = j;  _glms = glms; _maxP = maxP;}
    public ParallelGLMs(Job j, GLM2 [] glms, int maxP, H2OCountedCompleter cmp){super(cmp); _job = j; _glms = glms; _maxP = maxP;}

    private void forkDTask(int i){
      int nodeId = i%H2O.CLOUD.size();
      final GLM2 glm = _glms[i];
      new RPC(H2O.CLOUD._memary[nodeId],new DTask() {
        @Override public void compute2() {
          glm.run(this);
        }
      }).addCompleter(new Callback()).call();
    }
    class Callback extends H2OCallback<H2OCountedCompleter> {
      public Callback(){super(_job);}
      @Override public void callback(H2OCountedCompleter cc){
        int i;
        if((i = _remCnt.getAndDecrement()) > 0) // not done yet
          forkDTask(_glms.length - i);
        else if(_doneCnt.getAndDecrement() == 0) // am I the last guy to finish? if so complete parent.
          ParallelGLMs.this.tryComplete();
        // else just done myself (no more work) but others still in progress -> just return
      }
      @Override public boolean onExceptionalCompletion(Throwable ex, CountedCompleter caller){
        _job.cancel(ex);
        return true;
      }
    }
    @Override public void compute2(){
      final int n = Math.min(_maxP, _glms.length);
      _remCnt = new AtomicInteger(_glms.length-n);
      _doneCnt = new AtomicInteger(n-1);
      for(int i = 0; i < n; ++i)
        forkDTask(i);
    }
  }

}
