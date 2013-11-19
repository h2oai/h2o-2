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
import water.api.RequestServer.API_VERSION;
import water.fvec.Frame;
import water.fvec.Vec;
import water.util.Utils;

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
  private double [] _beta;


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
  double [] lambda = new double[]{1e-5};
  public static final double DEFAULT_BETA_EPS = 1e-4;
  @API(help = "beta_eps", filter = Default.class)
  double beta_epsilon = DEFAULT_BETA_EPS;
  int _lambdaIdx = 0;

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
    this(desc,jobKey,dest,dinfo,glm,lambda,alpha,nfolds,betaEpsilon,parentJob, null);
  }
  public GLM2(String desc, Key jobKey, Key dest, DataInfo dinfo, GLMParams glm, double [] lambda, double alpha, int nfolds, double betaEpsilon, Key parentJob, double [] beta) {
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
    this.alpha= new double[]{alpha};
    this.n_folds = nfolds;
  }

  public static Job gridSearch(Key destinationKey, DataInfo dinfo, GLMParams glm, double [] lambda, double [] alpha, int nfolds){
    return gridSearch(destinationKey, dinfo, glm, lambda, alpha,nfolds,DEFAULT_BETA_EPS);
  }
  public static Job gridSearch(Key destinationKey, DataInfo dinfo, GLMParams glm, double [] lambda, double [] alpha, int nfolds, double betaEpsilon){
    return new GLMGridSearch(4, destinationKey,dinfo,glm,lambda,alpha, nfolds,betaEpsilon).fork();
  }
  private long _startTime;
  @Override protected Response serve() {
    init();
    if(this.destination_key == null){
      this.destination_key = Key.make("GLM<"+ family + ">(" + SOURCE_KEY.toString() + ")" + Key.make().toString());
    }
    link = family.defaultLink;// TODO
    tweedie_link_power = 1 - tweedie_variance_power;// TODO
    _startTime = System.currentTimeMillis();
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
    _dinfo = new DataInfo(fr, true, standardize);
    _glm = new GLMParams(family, tweedie_variance_power, link, tweedie_link_power);
    if(alpha.length > 1) { // grid search
      Job j = gridSearch(destination_key, _dinfo, _glm, lambda, alpha,n_folds);
      return GLMProgressPage2.GLMGrid.redirect(this, j.self(), j.destination_key, API_VERSION.V_2);
    } else {
      start(null);
      return GLMProgressPage2.redirect(this, self(),dest());
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
    return (float)m.iteration()/(float)max_iter; // TODO, do something smarter here
  }

  private class Iteration extends H2OCallback<GLMIterationTask> {
    LSMSolver _solver;
    final DataInfo _dinfo;
    final H2OCountedCompleter _fjt;
    GLMModel _model; // latest model
    GLMModel _oldModel; // model 1 round back (the one being validated)

    public Iteration(GLMModel model, LSMSolver solver, DataInfo dinfo, H2OCountedCompleter fjt){
      _oldModel = model;
      _solver = solver;
      _dinfo = dinfo;
      _fjt = fjt;
    }

    @Override public Iteration clone(){return new Iteration(_model,_solver,_dinfo,_fjt);}
    @Override public void callback(final GLMIterationTask glmt) {
      try {
//        System.out.println(Utils.pprint(glmt._gram.getXX()));
//        System.out.println(Utils.pprint(new double[][]{glmt._xy}));
        if(!cancelled()){
          double [] newBeta = MemoryManager.malloc8d(glmt._xy.length);
          double [] newBetaDeNorm = null;

          _solver.solve(glmt._gram, glmt._xy, glmt._yy, newBeta);
          final boolean diverged = Utils.hasNaNsOrInfs(newBeta);
          if(diverged)
            newBeta = glmt._beta == null?newBeta:glmt._beta;
          if(_dinfo._standardize){
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
          if(glmt._val != null){
            glmt._val.finalize_AIC_AUC();
            _oldModel.setValidation(_lambdaIdx,glmt._val);
          }
          _model = (GLMModel)_oldModel.clone();
          done = done || _glm.family == Family.gaussian || (glmt._iter+1) == max_iter || beta_diff(glmt._beta, newBeta) < beta_epsilon || cancelled();
          _model.setLambdaSubmodel(_lambdaIdx,lambda[_lambdaIdx], newBetaDeNorm == null?newBeta:newBetaDeNorm, newBetaDeNorm==null?null:newBeta, glmt._iter+1);
          if(done){
            H2OCallback fin = new H2OCallback<GLMValidationTask>() {
              @Override public void callback(GLMValidationTask tsk) {
                if(!diverged && tsk._improved && _lambdaIdx < (lambda.length-1) ){ // continue with next lambda value?
                  _oldModel = _model;
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
            DKV.put(dest(),_oldModel);// validation is one iteration behind so put the old (now validated  model into K/V)
            int iter = glmt._iter+1;
            GLMIterationTask nextIter = new GLMIterationTask(GLM2.this, _dinfo,glmt._glm, newBeta,iter,glmt._ymu,glmt._reg);
            nextIter.setCompleter(new Iteration(_model, _solver, _dinfo, _fjt)); // we need to clone here as FJT will set status to done after this method
            nextIter.dfork(_dinfo._adaptedFrame);
          }
        } else throw new JobCancelledException();
      } catch(Throwable ex){
        _fjt.completeExceptionally(ex);
      }
    }
    @Override public boolean onExceptionalCompletion(Throwable ex, CountedCompleter caller){
      _fjt.completeExceptionally(ex);
      return false;
    }
  }

  @Override
  public Job start(final H2OCountedCompleter completer){
    final H2OCountedCompleter fjt = new JobCompleter(this,completer);
    super.start(fjt);
    run(fjt);
    return this;
  }
  public void run(final H2OCountedCompleter completer){
    assert alpha.length == 1;
    UKV.remove(dest());
    new YMUTask(this, _dinfo, new H2OCallback<YMUTask>() {
      @Override public void callback(final YMUTask ymut){
        new LMAXTask(GLM2.this, _dinfo, _glm, ymut.ymu(),alpha[0],new H2OCallback<LMAXTask>(){
          @Override public void callback(LMAXTask t){
            final double lmax = t.lmax();
            if(lambda == null)lambda = new double[]{/*lmax,lmax*0.75,lmax*0.66,lmax*0.5,*/lmax*0.33,lmax*0.25,lmax*0.1,lmax*0.075,lmax*0.05,lmax*0.01}; // todo - make it a sequence of 100 lamdbas
            else {
              int i = 0; while(i < lambda.length && lambda[i] > lmax)++i;
              if(i > 0)lambda = i == lambda.length?new double[]{lmax}:Arrays.copyOfRange(lambda, i, lambda.length);
            }
            GLMIterationTask firstIter = new GLMIterationTask(GLM2.this,_dinfo,_glm,_beta,0,ymut.ymu(),1.0/ymut.nobs());
            final LSMSolver solver = new ADMMSolver(lambda[0], alpha[0]);
            GLMModel model = new GLMModel(dest(),_dinfo._adaptedFrame, _dinfo._catOffsets, _glm,beta_epsilon,alpha[0],lambda,ymut.ymu(),GLM2.this.case_mode,GLM2.this.case_val);
            firstIter.setCompleter(new Iteration(model,solver,_dinfo,completer));
            firstIter.dfork(_dinfo._adaptedFrame);
          }
          @Override public boolean onExceptionalCompletion(Throwable ex, CountedCompleter cc){
            GLM2.this._fjtask.completeExceptionally(ex);
            return true;
          }
        }).dfork(_dinfo._adaptedFrame);
      }
      @Override public boolean onExceptionalCompletion(Throwable ex, CountedCompleter cc){
        GLM2.this._fjtask.completeExceptionally(ex);
        return true;
      }
    }).dfork(_dinfo._adaptedFrame);
  }

  @Override
  public Job fork(){start(null); return this;}

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
    for(int i = 0; i < n_folds; ++i)
      new GLM2(this.description + "xval " + i, self(), keys[i] = Key.make(destination_key + "_xval" + i), _dinfo.getFold(i, n_folds),_glm,new double[]{lambda[_lambdaIdx]},model.alpha,0, model.beta_eps,self(),model.norm_beta(lambdaIxd)).run(callback);
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
    final long _startTime;
//    final Key [] _jobKeys;
    final Key [] _dstKeys;
    final double [] _alphas;

//    final Comparator<GLMModel> _cmp;

//    public GLMGrid (Key [] keys, double [] alphas){
//      this(keys,alphas,null);
//    }
    public GLMGrid (GLM2 [] jobs){
      _alphas = new double [jobs.length];
//      _jobKeys = new Key[jobs.length];
      _dstKeys = new Key[jobs.length];
      for(int i = 0; i < jobs.length; ++i){
//        _jobKeys[i] = jobs[i].job_key;
        _dstKeys[i] = jobs[i].destination_key;
        _alphas[i] = jobs[i].alpha[0];
      }
      _startTime = System.currentTimeMillis();
    }
    public void toHTML(StringBuilder sb){
      ArrayList<GLMModel> models = new ArrayList<GLMModel>(_dstKeys.length);
      for(int i = 0; i < _dstKeys.length; ++i){
        Value v = DKV.get(_dstKeys[i]);
        if(v != null)models.add(v.<GLMModel>get());
      }
      if(models.isEmpty()){
        sb.append("no models computed yet..");
      } else {
        DocGen.HTML.arrayHead(sb);
        sb.append("<tr>");
        sb.append("<th>&alpha;</th>");
        sb.append("<th>&lambda;<sub>max</sub></th>");
        sb.append("<th>&lambda;<sub>min</sub></th>");
        sb.append("<th>&lambda;<sub>best</sub></th>");
        sb.append("<th>nonzeros</th>");
        sb.append("<th>iterations</td>");
        if(models.get(0).glm.family == Family.binomial)
          sb.append("<th>AUC</td>");
        if(models.get(0).glm.family != Family.gamma)
          sb.append("<th>AIC</td>");
        sb.append("<th>Deviance Explained</td>");
        sb.append("<th>Model</th>");
//        sb.append("<th>Progress</th>");
        sb.append("</tr>");
        Collections.sort(models);//, _cmp);
        for(int i = 0; i < models.size();++i){
          GLMModel m = models.get(i);
          sb.append("<tr>");
          sb.append("<td>" + m.alpha + "</td>");
          sb.append("<td>" + m.lambdaMax() + "</td>");
          sb.append("<td>" + m.lambdaMin() + "</td>");
          sb.append("<td>" + m.lambda() + "</td>");
          sb.append("<td>" + (m.rank()-1) + "</td>");
          sb.append("<td>" + m.iteration() + "</td>");
          if(m.glm.family == Family.binomial)
            sb.append("<td>" + aucStr(m.auc()) +  "</td>");
          if(m.glm.family != Family.gamma)
            sb.append("<td>" + aicStr(m.aic()) +  "</td>");
          sb.append("<td>" + devExplainedStr(m.devExplained()) +  "</td>");
          sb.append("<td>" + GLMModelView.link("View Model", m._selfKey) + "</td>");
//          if(job != null && !job.isDone())DocGen.HTML.progress(job.progress(), sb.append("<td>")).append("</td>");
//          else sb.append("<td class='alert alert-success'>" + "DONE" + "</td>");
          sb.append("</tr>");
        }
        DocGen.HTML.arrayTail(sb);
      }
      // now sort the models...
    }
  }
  public static class GLMGridSearch extends Job {
    public final int _maxParallelism;
    transient private AtomicInteger _idx;

    public final GLM2 [] _jobs;
    public GLMGridSearch(int maxP, Key dstKey, DataInfo dinfo, GLMParams glm, double [] lambdas, double [] alphas, int nfolds, double betaEpsilon){
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
      DKV.put(destination_key, new GLMGrid(_jobs));
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
