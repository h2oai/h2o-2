package hex.glm;

import hex.glm.GLMModel.GLMValidationTask;
import hex.glm.GLMParams.CaseMode;
import hex.glm.GLMParams.Family;
import hex.glm.GLMParams.Link;
import hex.glm.GLMTask.GLMIterationTask;
import hex.glm.GLMTask.YMUTask;
import hex.glm.GLMValidation.GLMXValidation;
import hex.glm.LSMSolver.ADMMSolver;

import java.util.ArrayList;
import java.util.Arrays;

import jsr166y.CountedCompleter;
import water.*;
import water.H2O.H2OCallback;
import water.H2O.H2OCountedCompleter;
import water.H2O.H2OEmptyCompleter;
import water.Job.ModelJob;
import water.fvec.Frame;
import water.fvec.Vec;
import water.util.Utils;

public class GLM2 extends ModelJob {
  private GLM2 [] _subjobs;
  @API(help = "max-iterations", filter = Default.class, lmin=1, lmax=1000000)
  int max_iter = 50;
  @API(help = "If true, data will be standardized on the fly when computing the model.", filter = Default.class)
  boolean standardize = true;

  @API(help = "validation folds", filter = Default.class, lmin=0, lmax=100)
  int n_folds;

  @API(help = "Family.", filter = Default.class)
  Family family = Family.gaussian;

  private final int _step;
  private final int _offset;
  private final boolean _complement;
  private double [] _beta;
  private GLMModel _oldModel;

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
  double alpha = 0.5;
  @API(help = "lambda", filter = Default.class)
  double lambda = 1e-5;
  @API(help = "beta_eps", filter = Default.class)
  double beta_epsilon = 1e-4;

  public GLM2 setTweedieVarPower(double d){tweedie_variance_power = d; return this;}

  public GLM2() {_step = 1; _offset = 0; _complement = false;}
  public GLM2(String desc, Key dest, Frame src,Vec response,  boolean standardize, Family family, Link link, double alpha, double lambda){
    this(desc, dest, src, response, standardize, family, link, alpha, lambda, 1,0,false,null,0,CaseMode.none,0);
  }
  public GLM2(String desc, Key dest, Frame src,Vec response, boolean standardize, Family family, Link link, double alpha, double lambda, int step, int offset, boolean complement, double [] beta,int nfold, CaseMode caseMode, double caseVal) {
    description = desc;
    destination_key = dest;
    source = src;
    this.family = family;
    this.link = link;
    this.alpha = alpha;
    this.lambda = lambda;
    this.standardize = standardize;
    _step = step;
    _offset = offset;
    _complement = complement;
    _beta = beta;
    this.n_folds = nfold;
    this.response = response;
    this.case_mode = caseMode;
    this.case_val = caseVal;
  }

  @Override public void cancel(String msg){
    if(_subjobs != null)for(GLM2 g:_subjobs)g.cancel("Parent job cancelled with msg: " + msg);
    super.cancel(msg);
  }

  private long _startTime;
  @Override protected Response serve() {
    link = family.defaultLink;
    _startTime = System.currentTimeMillis();
    run(null);
    return GLMProgressPage2.redirect(this, self(),dest());
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
    return (float)m.iteration/(float)max_iter; // TODO, do something smarter here
  }



  private class Iteration extends H2OCallback<GLMIterationTask> {
    final LSMSolver solver;
    final Frame fr;
    final H2OCountedCompleter fjt;

    public Iteration(LSMSolver solver, Frame fr, H2OCountedCompleter fjt){
      this.solver = solver;
      this.fr = fr;
      this.fjt = fjt;
    }
    @Override public Iteration clone(){return new Iteration(solver,fr,fjt);}

    @Override public void callback(GLMIterationTask glmt) {
      if(!cancelled()){
        double [] newBeta = MemoryManager.malloc8d(glmt._xy.length);
        solver.solve(glmt._gram, glmt._xy, glmt._yy, newBeta);
        boolean done = false;
        if(Utils.hasNaNsOrInfs(newBeta)){
          done = true;
          newBeta = glmt._beta == null?newBeta:glmt._beta;
        }
        done = done || family == Family.gaussian || (glmt._iter+1) == max_iter || beta_diff(glmt._beta, newBeta) < beta_epsilon || cancelled();
        GLMModel res = new GLMModel(dest(),null,glmt._iter+1,fr,glmt,beta_epsilon,alpha,lambda,newBeta,0.5,null,System.currentTimeMillis() - start_time,GLM2.this.case_mode,GLM2.this.case_val);
        if(done){
          // final validation
          GLMValidationTask t = new GLMValidationTask(res,_step,_offset,true);
          t.doAll(fr);
          t._res.finalize_AIC_AUC();
          res.setValidation(t._res);
          DKV.put(dest(),res);
          if(GLM2.this.n_folds < 2){
            remove();
            fjt.tryComplete(); // signal we're done to anyone waiting for the job
          } else
            xvalidate(res, fjt);
        } else {
          glmt._val.finalize_AIC_AUC();
          _oldModel.setValidation(glmt._val);
          DKV.put(dest(),_oldModel);// validation is one iteration behind
          _oldModel = res;
          GLMIterationTask nextIter = new GLMIterationTask(glmt, newBeta);
          nextIter.setCompleter(clone()); // we need to clone here as FJT will set status to done after this method
          nextIter.dfork2(fr);
        }
      } else fjt.onExceptionalCompletion(new RuntimeException("Cancelled!"),null);
    }
    @Override public boolean onExceptionalCompletion(Throwable ex, CountedCompleter caller){
      GLM2.this.cancel(ex);
      fjt.completeExceptionally(ex);
      return false;
    }
  }

  public Job run(final H2OCountedCompleter completer){
    final H2OCountedCompleter fjt = completer == null?new H2OEmptyCompleter():completer;
    start(fjt);
    UKV.remove(dest());
    _oldModel = new GLMModel(dest(),source,new GLMParams(family,tweedie_variance_power,link,1-tweedie_variance_power),beta_epsilon,alpha,lambda,System.currentTimeMillis()-_startTime,GLM2.this.case_mode,GLM2.this.case_val);
    tweedie_link_power = 1 - tweedie_variance_power; // TODO
    Frame fr = new Frame(source._names.clone(),source.vecs().clone());
    System.out.println("Frame = " + Arrays.toString(fr._names));
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
    YMUTask ymut = new YMUTask(this,new GLMParams(family, tweedie_variance_power, link,tweedie_link_power), standardize, case_mode, case_val, fr.anyVec().length());
    fr = ymut.adaptFrame(fr);
    ymut.doIt(fr).getResult();
    GLMIterationTask firstIter = new GLMIterationTask(this,new GLMParams(family, tweedie_variance_power, link,tweedie_link_power),_beta,standardize, 1.0/ymut.nobs(), case_mode, case_val,_step,_offset,_complement);
    firstIter._ymu = ymut.ymu();
    final LSMSolver solver = new ADMMSolver(lambda, alpha);
    firstIter.setCompleter(new Iteration(solver,fr,fjt));
    firstIter.dfork2(fr);
    return this;
  }

  private void xvalidate(final GLMModel model, final H2OCountedCompleter cmp){
    final Key [] keys = new Key[n_folds];
    H2OCallback callback = new H2OCallback() {
      @Override public void callback(H2OCountedCompleter t) {
        model.setValidation(new GLMXValidation(model, keys));
        DKV.put(model._selfKey, model);
        GLM2.this.remove();
      }
      @Override public boolean onExceptionalCompletion(Throwable ex, CountedCompleter caller){
        ex.printStackTrace();
        GLM2.this.cancel(ex);
        return true;
      }
    };
    callback.addToPendingCount(n_folds-1);
    callback.setCompleter(cmp);
    _subjobs = new GLM2[n_folds];
    for(int i = 0; i < n_folds; ++i) // make the jobs objects, we don't want to start jobs before _subjobs is populated
      _subjobs[i] =  new GLM2(this.description + "_xval " + i, keys[i] = Key.make(), source, response, standardize, family, link,alpha,lambda, n_folds, i,false,model.norm_beta,0,case_mode,case_val);
    // now start them
    for(GLM2 g:_subjobs)g.run(callback);
  }
}
