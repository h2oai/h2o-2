package hex.glm;

import hex.FrameTask.DataInfo;
import hex.GridSearch.GridSearchProgress;
import hex.glm.GLMModel.GLMValidationTask;
import hex.glm.GLMModel.GLMXValidationTask;
import hex.glm.GLMParams.CaseMode;
import hex.glm.GLMParams.Family;
import hex.glm.GLMParams.Link;
import hex.glm.GLMTask.GLMIterationTask;
import hex.glm.GLMTask.LMAXTask;
import hex.glm.GLMTask.YMUTask;
import hex.glm.LSMSolver.ADMMSolver;

import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import jsr166y.CountedCompleter;
import water.*;
import water.H2O.H2OCallback;
import water.H2O.H2OCountedCompleter;
import water.H2O.JobCompleter;
import water.Job.ModelJob;
import water.api.DocGen;
import water.fvec.Frame;
import water.fvec.Vec;
import water.util.*;

public class GLM2 extends ModelJob {
//  private transient GLM2 [] _subjobs;
//  private Key _parentjob;
  @API(help = "max-iterations", filter = Default.class, lmin=1, lmax=1000000)
  int max_iter = 50;
  @API(help = "If true, data will be standardized on the fly when computing the model.", filter = Default.class)
  boolean standardize = true;

  @API(help = "validation folds", filter = Default.class, lmin=0, lmax=100)
  int n_folds;

  @API(help = "Family.", filter = Default.class)
  Family family = Family.gaussian;

  private DataInfo _dinfo;
  private GLMParams _glm;
  private double [] _wgiven;
  private double _proximalPenalty;
  private double [] _beta;

  private boolean _runAllLambdas = true;

//  @API(help = "Link.", filter = Default.class)
  Link link = Link.identity;

  @API(help = "CaseMode", filter = Default.class)
  CaseMode case_mode = CaseMode.none;
  @API(help = "CaseMode", filter = Default.class)
  double case_val = 0;
  @API(help = "Tweedie variance power", filter = Default.class)
  double tweedie_variance_power;
  double tweedie_link_power;
  @API(help = "alpha", filter = Default.class)
  double [] alpha = new double[]{0.5};
//  @API(help = "lambda", filter = RSeq2.class)
  @API(help = "lambda", filter = Default.class)
  double [] lambda;// = new double[]{1e-5};
  public static final double DEFAULT_BETA_EPS = 1e-4;
  @API(help = "beta_eps", filter = Default.class)
  double beta_epsilon = DEFAULT_BETA_EPS;
  int _lambdaIdx = 0;

  @Override public Key defaultDestKey(){
    return null;
  }
  @Override public Key defaultJobKey() {return null;}

  public GLM2() {}
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
    this.beta_epsilon = betaEpsilon;
    _beta = beta;
    _dinfo = dinfo;
    _glm = glm;
    this.lambda = lambda;
    _beta = beta;
    if((_proximalPenalty = proximalPenalty) != 0)
      _wgiven = beta;
    this.alpha= new double[]{alpha};
    this.n_folds = nfolds;
    source = dinfo._adaptedFrame;
    response = dinfo._adaptedFrame.lastVec();
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

  @Override protected void logStart() {
    Log.info("Starting GLM2 model build...");
    super.logStart();
    Log.info("    max_iter: ", max_iter);
    Log.info("    standardize: ", standardize);
    Log.info("    n_folds: ", n_folds);
    Log.info("    family: ", family);
    Log.info("    wgiven: " + arrayToString(_wgiven));
    Log.info("    proximalPenalty: " + _proximalPenalty);
    Log.info("    runAllLambdas: " + _runAllLambdas);
    Log.info("    link: " + link);
    Log.info("    case_mode: " + case_mode);
    Log.info("    case_val: " + case_val);
    Log.info("    tweedie_variance_power: " + tweedie_variance_power);
    Log.info("    tweedie_link_power: " + tweedie_link_power);
    Log.info("    alpha: " + arrayToString(alpha));
    Log.info("    lambda: " + arrayToString(lambda));
    Log.info("    beta_epsilon: " + beta_epsilon);
    Log.info("    description: " + description);
  }

  public GLM2 setCase(CaseMode cm, double cv){
    case_mode = cm;
    case_val = cv;
    return this;
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

  @Override protected Response serve() {
    init();
    link = family.defaultLink;// TODO
    tweedie_link_power = 1 - tweedie_variance_power;// TODO
    Frame fr = new Frame(source._names.clone(),source.vecs().clone());
    fr.remove(ignored_cols);
    final Vec [] vecs =  fr.vecs();
    ArrayList<Integer> constantOrNAs = new ArrayList<Integer>();
    for(int i = 0; i < vecs.length-1; ++i)// put response to the end
      if(vecs[i] == response){
        fr.add(fr._names[i], fr.remove(i));
        break;
      }
    for(int i = 0; i < vecs.length-1; ++i) // remove constant cols and cols with too many NAs
      if(vecs[i].min() == vecs[i].max() || vecs[i].naCnt() > vecs[i].length()*0.2)constantOrNAs.add(i);
    if(!constantOrNAs.isEmpty()){
      int [] cols = new int[constantOrNAs.size()];
      for(int i = 0; i < cols.length; ++i)cols[i] = constantOrNAs.get(i);
      fr.remove(cols);
    }
    _dinfo = new DataInfo(fr, 1, standardize);
    _glm = new GLMParams(family, tweedie_variance_power, link, tweedie_link_power);
    if(alpha.length > 1) { // grid search
      if(destination_key == null)destination_key = Key.make("GLMGridModel_"+Key.make());
      if(job_key == null)job_key = Key.make("GLMGridJob_"+Key.make());
      Job j = gridSearch(self(),destination_key, _dinfo, _glm, lambda, alpha,n_folds);
      return GLMGridView.redirect(this,j.destination_key);
    } else {
      if(destination_key == null)destination_key = Key.make("GLMModel_"+Key.make());
      if(job_key == null)job_key = Key.make("GLM2Job_"+Key.make());
      fork();
//      return GLMModelView.redirect(this, dest(),job_key);
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
    System.out.println("glm progress = " + progress);
    return progress;
  }

  private class Iteration extends H2OCallback<GLMIterationTask> {
    LSMSolver _solver;
    final DataInfo _dinfo;
    final H2OCountedCompleter _fjt;
//    Key _modelKey;
    GLMModel _model; // latest model
//    GLMModel _oldModel; // model 1 round back (the one being validated)

    public Iteration(GLMModel model, LSMSolver solver, DataInfo dinfo, H2OCountedCompleter fjt){
//      _modelKey = modelKey;
//      _oldModel = model;
      _model = model;
      _solver = solver;
      _dinfo = dinfo;
      _fjt = fjt;
    }

    @Override public Iteration clone(){return new Iteration(_model,_solver,_dinfo,_fjt);}
    @Override public void callback(final GLMIterationTask glmt) {
      if(!cancelled()){
        double [] newBeta = MemoryManager.malloc8d(glmt._xy.length);
        double [] newBetaDeNorm = null;
        _solver.solve(glmt._gram, glmt._xy, glmt._yy, newBeta);
        final boolean diverged = Utils.hasNaNsOrInfs(newBeta);
        if(diverged)
          newBeta = glmt._beta == null?newBeta:glmt._beta;
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
        boolean done = false;
//        _model = _oldModel.clone();
        done = done || _glm.family == Family.gaussian || (glmt._iter+1) == max_iter || beta_diff(glmt._beta, newBeta) < beta_epsilon || cancelled();
        _model.setLambdaSubmodel(_lambdaIdx,newBetaDeNorm == null?newBeta:newBetaDeNorm, newBetaDeNorm==null?null:newBeta, glmt._iter+1);
        if(done){
          H2OCallback fin = new H2OCallback<GLMValidationTask>() {
            @Override public void callback(GLMValidationTask tsk) {
              boolean improved = _model.setAndTestValidation(_lambdaIdx,tsk._res);
              DKV.put(_model._selfKey, _model);
              if(!diverged && (improved || _runAllLambdas) && _lambdaIdx < (lambda.length-1) ){ // continue with next lambda value?
                _solver = new ADMMSolver(lambda[++_lambdaIdx],alpha[0]);
                glmt._val = null;
                Iteration.this.callback(glmt);
              } else    // nope, we're done
                _fjt.tryComplete(); // signal we're done to anyone waiting for the job
            }
            @Override public boolean onExceptionalCompletion(Throwable ex, CountedCompleter cc){
              _fjt.completeExceptionally(ex);
              return true;
            }
          };
          if(GLM2.this.n_folds >= 2) xvalidate(_model, _lambdaIdx, fin);
          else  new GLMValidationTask(_model,_lambdaIdx,fin).dfork(_dinfo._adaptedFrame);
        } else {
          if(glmt._val != null){
            glmt._val.finalize_AIC_AUC();
            _model.setAndTestValidation(_lambdaIdx,glmt._val);//.store();
            UKV.put(_model._selfKey, _model);
          }
          int iter = glmt._iter+1;
          GLMIterationTask nextIter = new GLMIterationTask(GLM2.this, _dinfo,glmt._glm, case_mode, case_val, newBeta,iter,glmt._ymu,glmt._reg);
          nextIter.setCompleter(new Iteration(_model, _solver, _dinfo, _fjt)); // we need to clone here as FJT will set status to done after this method
          nextIter.dfork(_dinfo._adaptedFrame);
        }
      } else throw new JobCancelledException();
    }
    @Override public boolean onExceptionalCompletion(Throwable ex, CountedCompleter caller){
      _fjt.completeExceptionally(ex);
      return false;
    }
  }

  @Override
  public GLM2 fork(){
    start(new JobCompleter(this));
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
    UKV.remove(dest());
    new YMUTask(this, _dinfo, case_mode, case_val, new H2OCallback<YMUTask>() {
      @Override public void callback(final YMUTask ymut){
        if(ymut._ymin == ymut._ymax){
          String msg = case_mode == CaseMode.none
              ?"Attempting to run GLM on column with constant value = " + ymut._ymin
              :"Attempting to run GLM on column with constant value, y " + case_mode + " " + case_val  + " is " + (ymut._ymin == 0?"false":"true") + " for all rows!";
          GLM2.this.cancel(msg);
          GLM2.this._fjtask.completeExceptionally(new JobCancelledException(msg));
        }
        new LMAXTask(GLM2.this, _dinfo, _glm, ymut.ymu(),alpha[0],new H2OCallback<LMAXTask>(){
          @Override public void callback(LMAXTask t){
            final double lmax = t.lmax();
            if(lambda == null){
              lambda = new double[]{lmax,lmax*0.9,lmax*0.75,lmax*0.66,lmax*0.5,lmax*0.33,lmax*0.25,lmax*1e-1,lmax*1e-2,lmax*1e-3,lmax*1e-4,lmax*1e-5,lmax*1e-6,lmax*1e-7,lmax*1e-8}; // todo - make it a sequence of 100 lamdbas
              _runAllLambdas = false;
            }
            else {
              int i = 0; while(i < lambda.length && lambda[i] > lmax)++i;
              if(i > 0)lambda = i == lambda.length?new double[]{lmax}:Arrays.copyOfRange(lambda, i, lambda.length);
            }
            GLMIterationTask firstIter = new GLMIterationTask(GLM2.this,_dinfo,_glm,case_mode, case_val, _beta,0,ymut.ymu(),1.0/ymut.nobs());
            final ADMMSolver solver = new ADMMSolver(lambda[0], alpha[0]);
            solver._proximalPenalty = _proximalPenalty;
            solver._wgiven = _wgiven;
            GLMModel model = new GLMModel(self(),dest(),_dinfo, _glm,beta_epsilon,alpha[0],lambda,ymut.ymu(),GLM2.this.case_mode,GLM2.this.case_val);
            firstIter.setCompleter(new Iteration(model,solver,_dinfo,GLM2.this._fjtask));
            firstIter.dfork(_dinfo._adaptedFrame);
          }
          @Override public boolean onExceptionalCompletion(Throwable ex, CountedCompleter cc){
            if(GLM2.this._fjtask != null)GLM2.this._fjtask.completeExceptionally(ex);
            return true;
          }
        }).dfork(_dinfo._adaptedFrame);
      }
      @Override public boolean onExceptionalCompletion(Throwable ex, CountedCompleter cc){
        if(GLM2.this._fjtask != null)GLM2.this._fjtask.completeExceptionally(ex);
        return true;
      }
    }).dfork(_dinfo._adaptedFrame);
  }

  private void xvalidate(final GLMModel model, int lambdaIxd,final H2OCountedCompleter cmp){
    final Key [] keys = new Key[n_folds];
    H2OCallback callback = new H2OCallback() {
      @Override public void callback(H2OCountedCompleter t) {
        try{
          GLMModel [] models = new GLMModel[keys.length];
          // we got the xval models, now compute their validations...
          for(int i = 0; i < models.length; ++i)models[i] = DKV.get(keys[i]).get();
          new GLMXValidationTask(model,_lambdaIdx,models, cmp).dfork(_dinfo._adaptedFrame);
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
      setCase(case_mode,case_val).
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
      final H2OCountedCompleter fjt = new JobCompleter(this);
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
