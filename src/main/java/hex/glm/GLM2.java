package hex.glm;

import com.google.gson.JsonObject;
import hex.FrameTask.DataInfo;
import hex.GridSearch.GridSearchProgress;
import hex.glm.GLMModel.GLMValidationTask;
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
import water.Job.ModelJob;
import water.api.DocGen;
import water.fvec.Frame;
import water.util.Log;
import water.util.RString;
import water.util.Utils;

import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

public class GLM2 extends ModelJob {
  static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
  public static DocGen.FieldDoc[] DOC_FIELDS;
  public static final String DOC_GET = "GLM2";
  public final String _jobName;
//  private transient GLM2 [] _subjobs;
//  private Key _parentjob;
  @API(help = "max-iterations", filter = Default.class, lmin=1, lmax=1000000, json=true)
  int max_iter = 100;
  @API(help = "If true, data will be standardized on the fly when computing the model.", filter = Default.class, json=true)
  boolean standardize = true;

  @API(help = "validation folds", filter = Default.class, lmin=0, lmax=100, json=true)
  int n_folds;

  @API(help = "Family.", filter = Default.class, json=true)
  Family family = Family.gaussian;

  private DataInfo _dinfo;
  private GLMParams _glm;
  @API(help = "", json=true)
  private double [] _wgiven;
  @API(help = "", json=true)
  private double _proximalPenalty;
  @API(help = "", json=true)
  private double [] _beta;

  @API(help = "", json=true)
  private boolean _runAllLambdas = true;
  private transient boolean _gen_enum; // True if we need to cleanup an enum response column at the end

//  @API(help = "Link.", filter = Default.class)
  @API(help = "", json=true)
  Link link = Link.identity;

  @API(help = "Tweedie variance power", filter = Default.class, json=true)
  double tweedie_variance_power;
  @API(help = "Tweedie link power", json=true)
  double tweedie_link_power;
  @API(help = "alpha", filter = Default.class, json=true)
  double [] alpha = new double[]{0.5};
//  @API(help = "lambda", filter = RSeq2.class)
  @API(help = "lambda max", json=true)
  double lambda_max;
  @API(help = "lambda", filter = Default.class, json=true)
  double [] lambda;// = new double[]{1e-5};
  @API(help = "beta_eps", filter = Default.class, json=true)
  double beta_epsilon = DEFAULT_BETA_EPS;
  int _lambdaIdx = 0;

  private transient double _addedL2;


  public static final double DEFAULT_BETA_EPS = 1e-4;

  private transient double _ymu;
  private transient double _reg;
  private transient int    _iter;
  private transient double _rho;
  private transient GLMModel _model;


  private static class IterationInfo {
    final int _iter;
    final double _objval;
    final GLMIterationTask _glmt;

    public IterationInfo(int i, double obj, GLMIterationTask glmt){
      _iter = i;
      _objval = obj;
      _glmt = glmt;
    }
  }

  private transient IterationInfo _lastResult;

  @Override
  protected JsonObject toJSON() {
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
    this(desc,jobKey,dest,dinfo,glm,lambda,alpha,nfolds,betaEpsilon,parentJob, null,0);
  }
  public GLM2(String desc, Key jobKey, Key dest, DataInfo dinfo, GLMParams glm, double [] lambda, double alpha, int nfolds, double betaEpsilon, Key parentJob, double [] beta, double proximalPenalty) {
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

  public static Job gridSearch(Key jobKey, Key destinationKey, DataInfo dinfo, GLMParams glm, double [] lambda, double [] alpha, int nfolds){
    return gridSearch(jobKey, destinationKey, dinfo, glm, lambda, alpha,nfolds,DEFAULT_BETA_EPS);
  }
  public static Job gridSearch(Key jobKey, Key destinationKey, DataInfo dinfo, GLMParams glm, double [] lambda, double [] alpha, int nfolds, double betaEpsilon){
    return new GLMGridSearch(4, jobKey, destinationKey,dinfo,glm,lambda,alpha, nfolds,betaEpsilon).fork();
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
    if( _dinfo._nfolds == 0 ) remove(); // Remove/complete job only for top-level, not xval GLM2s
    if(_fjtask != null)_fjtask.tryComplete();
  }

  @Override public void cancel(Throwable ex){
    if( _model != null ) _model.unlock(self());
    if(ex instanceof JobCancelledException)cancel();
    else super.cancel(ex);
  }

  @Override protected Response serve() {
    init();
    link = family.defaultLink;// TODO
    tweedie_link_power = 1 - tweedie_variance_power;// TODO
    Frame fr = DataInfo.prepareFrame(source, response, ignored_cols, family==Family.binomial, true);
    _dinfo = new DataInfo(fr, 1, standardize);
    _glm = new GLMParams(family, tweedie_variance_power, link, tweedie_link_power);
    if(alpha.length > 1) { // grid search
      if(destination_key == null)destination_key = Key.make("GLMGridModel_"+Key.make());
      if(job_key == null)job_key = Key.make("GLMGridJob_"+Key.make());
      Job j = gridSearch(self(),destination_key, _dinfo, _glm, lambda, alpha,n_folds);
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
  @Override public float progress(){
    if(DKV.get(dest()) == null)return 0;
    GLMModel m = DKV.get(dest()).get();
    float progress =  (float)m.iteration()/(float)max_iter; // TODO, do something smarter here
    return progress;
  }

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

  protected boolean needLineSearch(double [] beta,double objval, double step){
    if(Double.isNaN(objval))return true; // needed for gamma (and possibly others...)
    // line search
    double f_hat = 0;
    double l2 = 0;
    for(int i = 0; i < beta.length; ++i){
      double diff = beta[i] - _lastResult._glmt._beta[i];
      f_hat += _lastResult._glmt._grad[i]*diff;
      l2 += diff*diff;
    }
    f_hat = _lastResult._objval + f_hat +  0.001*l2/step;
    return objval > f_hat;
  }
  private class LineSearchIteration extends H2OCallback<GLMTask.GLMLineSearchTask> {
    @Override public void callback(final GLMTask.GLMLineSearchTask glmt) {
      Log.info("GLM invoking line search");
      double step = 0.5;
      for(int i = 0; i < glmt._objvals.length; ++i){
        if(!needLineSearch(glmt._betas[i],glmt._objvals[i],step)){
          Log.info("GLM line search: found admissible step=" + step);
          _lastResult = null; // set last result to null so that the Iteration will not attempt to verify whether or not it should do the line search.
          new GLMIterationTask(GLM2.this,_dinfo,_glm,glmt._betas[i],_ymu,_reg,new Iteration()).asyncExec(_dinfo._adaptedFrame);
          return;
        }
        step *= 0.5;
      } // no line step works, forcibly converge
      Log.info("GLM: line search failed to find feasible step. Forcibly converged.");
      nextLambda(_lastResult._glmt);
    }
    @Override public boolean onExceptionalCompletion(Throwable ex, CountedCompleter caller){
      GLM2.this.cancel(ex);
      return true;
    }
  }
  public void nextLambda(final GLMIterationTask glmt){
    // We're done with this lambda, launch validation
    H2OCallback fin = new H2OCallback<GLMValidationTask>() {
      @Override public void callback(GLMValidationTask tsk) {
        boolean improved = _model.setAndTestValidation(_lambdaIdx,tsk._res);
        _model.clone().update(self());
        if((improved || _runAllLambdas) && _lambdaIdx < (lambda.length-1) ){ // continue with next lambda value?
          glmt._val = null;
          ++_lambdaIdx;
          new Iteration().callback(glmt);
        } else    // nope, we're done
          GLM2.this.complete(); // signal we're done to anyone waiting for the job
      }
      @Override public boolean onExceptionalCompletion(Throwable ex, CountedCompleter cc){
        GLM2.this.cancel(ex);
        return true;
      }
    };
    if(GLM2.this.n_folds >= 2) xvalidate(_model.clone(), _lambdaIdx, fin);
    else  new GLMValidationTask(_model.clone(),_lambdaIdx,fin).asyncExec(_dinfo._adaptedFrame);
  }
  private class Iteration extends H2OCallback<GLMIterationTask> {
    @Override public void callback(final GLMIterationTask glmt) {
      if( !isRunning(self()) )  throw new JobCancelledException();
      boolean converged = false;
      if(glmt._beta != null && glmt._val != null && _glm.family != Family.tweedie){
        glmt._val.finalize_AIC_AUC();
        _model.setAndTestValidation(_lambdaIdx,glmt._val);//.store();
        _model.clone().update(self());
        converged = true;
        double l1pen = alpha[0]*lambda[_lambdaIdx]*glmt._n;
        double l2pen = (1-alpha[0])*lambda[_lambdaIdx]*glmt._n;
        final double eps = 1e-2;
        for(int i = 0; i < glmt._grad.length-1; ++i) {// add l2 reg. term to the gradient
          glmt._grad[i] += l2pen*glmt._beta[i];
          if(glmt._beta[i] < 0)
            converged &= Math.abs(glmt._grad[i] - l1pen) < eps;
          else if(glmt._beta[i] > 0)
            converged &= Math.abs(glmt._grad[i] + l1pen) < eps;
          else
            converged &= LSMSolver.shrinkage(glmt._grad[i],l1pen+eps) == 0;
        }
        if(converged) Log.info("GLM converged by reaching 0 gradient/subgradient.");
        double objval = glmt._val.residual_deviance + 0.5*l2pen*l2norm(glmt._beta);
        if(!converged && _lastResult != null && needLineSearch(glmt._beta,objval,1)){
          new GLMTask.GLMLineSearchTask(GLM2.this,_dinfo,_glm,_lastResult._glmt._beta,glmt._beta,1e-8, new LineSearchIteration()).asyncExec(_dinfo._adaptedFrame);
          return;
        }
        _lastResult = new IterationInfo(GLM2.this._iter-1, objval, glmt);
      }
      double [] newBeta = glmt._beta != null?glmt._beta.clone():MemoryManager.malloc8d(glmt._xy.length);
      double [] newBetaDeNorm = null;
      ADMMSolver slvr = new ADMMSolver(lambda[_lambdaIdx],alpha[0], _addedL2);
      slvr.solve(glmt._gram,glmt._xy,glmt._yy,newBeta);
      _addedL2 = slvr._addedL2;
      if(Utils.hasNaNsOrInfs(newBeta)){
        Log.info("GLM forcibly converged by getting NaNs and/or Infs in beta");
      } else {
        if(_dinfo._standardize) {
          newBetaDeNorm = newBeta.clone();
          double norm = 0.0;        // Reverse any normalization on the intercept
          // denormalize only the numeric coefs (categoricals are not normalized)
          final int numoff = newBeta.length - _dinfo._nums - 1;
          for( int i=numoff; i< newBeta.length-1; i++ ) {
            double b = newBetaDeNorm[i]*_dinfo._normMul[i-numoff];
            norm += b*_dinfo._normSub[i-numoff]; // Also accumulate the intercept adjustment
            newBetaDeNorm[i] = b;
          }
          newBetaDeNorm[newBetaDeNorm.length-1] -= norm;
        }
        _model.setLambdaSubmodel(_lambdaIdx,newBetaDeNorm == null?newBeta:newBetaDeNorm, newBetaDeNorm==null?null:newBeta, _iter);
        if(beta_diff(glmt._beta,newBeta) < beta_epsilon){
          Log.info("GLM converged by reaching fixed-point.");
          converged = true;
        }
        if(!converged && _glm.family != Family.gaussian && _iter < max_iter){
          ++_iter;
          new GLMIterationTask(GLM2.this, _dinfo,glmt._glm, newBeta,_ymu,_reg,new Iteration()).asyncExec(_dinfo._adaptedFrame);
          return;
        }
      }
      // done with this lambda
      nextLambda(glmt);
    }
    @Override public boolean onExceptionalCompletion(Throwable ex, CountedCompleter caller){
      GLM2.this.cancel(ex);
      return true;
    }
  }

  @Override
  public GLM2 fork(){
    start(new H2O.H2OEmptyCompleter());
    run();
    return this;
  }
  // start inside of parent job
  public void run(final H2OCountedCompleter fjt){
    assert GLM2.this._fjtask == null;
    GLM2.this._fjtask = fjt;
    run();
  }
  public void run(){
    logStart();
    assert alpha.length == 1;
    new YMUTask(this, _dinfo, new H2OCallback<YMUTask>() {
      @Override public void callback(final YMUTask ymut){
        if(ymut._ymin == ymut._ymax){
          String msg = "Attempting to run GLM on column with constant value = " + ymut._ymin;
          GLM2.this.cancel(msg);
          GLM2.this._fjtask.completeExceptionally(new JobCancelledException(msg));
        }

        new LMAXTask(GLM2.this, _dinfo, _glm, ymut.ymu(),alpha[0],new H2OCallback<LMAXTask>(){
          @Override public void callback(LMAXTask t){
            final double lmax = lambda_max = t.lmax();
            String [] warns = null;
            if(lambda == null){
              lambda = new double[]{lmax,lmax*0.9,lmax*0.75,lmax*0.66,lmax*0.5,lmax*0.33,lmax*0.25,lmax*1e-1,lmax*1e-2,lmax*1e-3,lmax*1e-4,lmax*1e-5,lmax*1e-6,lmax*1e-7,lmax*1e-8}; // todo - make it a sequence of 100 lamdbas
              _runAllLambdas = false;
            } else if(alpha[0] > 0) { // make sure we start with lambda max (and discard all lambda > lambda max)
              int i = 0; while(i < lambda.length && lambda[i] > lmax)++i;
              if(i != 0) {
                Log.info("GLM: removing " + i + " lambdas > lambda_max: " + Arrays.toString(Arrays.copyOf(lambda,i)));
                warns = i == lambda.length?new String[] {"Removed " + i + " lambdas > lambda_max","No lambdas < lambda_max, returning null model."}:new String[] {"Removed " + i + " lambdas > lambda_max"};
              }
              lambda = i == lambda.length?new double [] {lambda_max}:Arrays.copyOfRange(lambda, i, lambda.length);
            }
            _model = new GLMModel(self(),dest(),_dinfo, _glm,beta_epsilon,alpha[0],lambda_max,lambda,ymut.ymu());
            _model.warnings = warns;
            _model.clone().delete_and_lock(self());
            if(lambda[0] == lambda_max && alpha[0] > 0){ // fill-in trivial solution for lambda max
              _beta = MemoryManager.malloc8d(_dinfo.fullN()+1);
              _beta[_beta.length-1] = _glm.link(ymut.ymu());
              _model.setLambdaSubmodel(0,_beta,_beta,0);
              t._val.finalize_AIC_AUC();
              _model.setAndTestValidation(0,t._val);
              _lambdaIdx = 1;
            }
            if(_lambdaIdx == lambda.length) // ran only with one lambda > lambda_max => return null model
              GLM2.this.complete(); // signal we're done to anyone waiting for the job
            else {
              ++_iter;
              new GLMIterationTask(GLM2.this,_dinfo,_glm,null,_ymu = ymut.ymu(),_reg = 1.0/ymut.nobs(), new Iteration()).asyncExec(_dinfo._adaptedFrame);
            }
          }
          @Override public boolean onExceptionalCompletion(Throwable ex, CountedCompleter cc){
            GLM2.this.cancel(ex);
            return true;
          }
        }).asyncExec(_dinfo._adaptedFrame);
      }
      @Override public boolean onExceptionalCompletion(Throwable ex, CountedCompleter cc){
        GLM2.this.cancel(ex);
        return true;
      }
    }).asyncExec(_dinfo._adaptedFrame);
  }

  private void xvalidate(final GLMModel model, int lambdaIxd,final H2OCountedCompleter cmp){
    final Key [] keys = new Key[n_folds];
    H2OCallback callback = new H2OCallback() {
      @Override public void callback(H2OCountedCompleter t) {
        try{
          GLMModel [] models = new GLMModel[keys.length];
          // we got the xval models, now compute their validations...
          for(int i = 0; i < models.length; ++i)models[i] = DKV.get(keys[i]).get();
          new GLMXValidationTask(model,_lambdaIdx,models, cmp).asyncExec(_dinfo._adaptedFrame);
        }catch(Throwable ex){cmp.completeExceptionally(ex);}
      }
      @Override public boolean onExceptionalCompletion(Throwable ex, CountedCompleter caller){
        cmp.completeExceptionally(ex);
        return true;
      }
    };
    callback.addToPendingCount(n_folds-1);
    double proximal_penalty = 0;
    for(int i = 0; i < n_folds; ++i)
      new GLM2(this.description + "xval " + i, self(), keys[i] = Key.make(destination_key + "_" + _lambdaIdx + "_xval" + i), _dinfo.getFold(i, n_folds),_glm,new double[]{lambda[_lambdaIdx]},model.alpha,0, model.beta_eps,self(),model.norm_beta(lambdaIxd),proximal_penalty).
      run(callback);
  }

  // Expand grid search related argument sets
  @Override protected NanoHTTPD.Response serveGrid(NanoHTTPD server, Properties parms, RequestType type) {
    return superServeGrid(server, parms, type);
  }

  public static final DecimalFormat AUC_DFORMAT = new DecimalFormat("#.###");

  public static final String aucStr(double auc){
    return AUC_DFORMAT.format(Math.round(1000*auc)*0.001);
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

//    final Comparator<GLMModel> _cmp;

//    public GLMGrid (Key [] keys, double [] alphas){
//      this(keys,alphas,null);
//    }
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
    public GLMGridSearch(int maxP, Key jobKey, Key dstKey, DataInfo dinfo, GLMParams glm, double [] lambdas, double [] alphas, int nfolds, double betaEpsilon){
      super(jobKey, dstKey);
      description = "GLM Grid with params " + glm.toString() + "on data " + dinfo.toString() ;
      _maxParallelism = maxP;
      _jobs = new GLM2[alphas.length];
      _idx = new AtomicInteger(_maxParallelism);
      for(int i = 0; i < _jobs.length; ++i) _jobs[i] = new GLM2("GLM grid(" + i + ")",self(),Key.make(dstKey.toString() + "_" + i),dinfo,glm,lambdas,alphas[i], nfolds, betaEpsilon,self());
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
      final H2OCountedCompleter fjt = new H2O.H2OEmptyCompleter();
      fjt.setPendingCount(_jobs.length-1);
      start(fjt);
      for(int i = 0; i < Math.min(_jobs.length,_maxParallelism); ++i){
        _jobs[i].run(new H2OCallback(fjt) {
          @Override public void callback(H2OCountedCompleter t) {
            int nextJob = _idx.getAndIncrement();
            if(nextJob <  _jobs.length){
              _jobs[nextJob].run(clone());
            }
          }
        });
      }
      return this;
    }

    @Override public Response redirect() {
      String n = GridSearchProgress.class.getSimpleName();
      return Response.redirect( this, n, "job_key", job_key, "destination_key", destination_key);
    }
  }
  public boolean isDone(){return DKV.get(self()) == null;}
}
