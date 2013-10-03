package hex.glm;

import hex.glm.GLMModel.GLMValidationTask;
import hex.glm.GLMParams.CaseMode;
import hex.glm.GLMParams.Family;
import hex.glm.GLMParams.Link;
import hex.glm.GLMTask.GLMIterationTask;
import hex.glm.GLMTask.YMUTask;
import hex.glm.LSMSolver.ADMMSolver;

import java.util.ArrayList;
import java.util.concurrent.ExecutionException;

import jsr166y.CountedCompleter;
import water.*;
import water.H2O.H2OCallback;
import water.H2O.H2OCountedCompleter;
import water.H2O.H2OEmptyCompleter;
import water.Job.FrameJob;
import water.fvec.Frame;
import water.fvec.Vec;
import water.util.Utils;

public class GLM2 extends FrameJob{

  @API(help="", required=true, filter=GLMResponseVecSelect.class)
  public Vec vresponse;
  class GLMResponseVecSelect extends VecClassSelect { GLMResponseVecSelect() { super("source"); } }

  @API(help="columns to ignore",required=false,filter=GLMMultiVecSelect.class)
  public int [] ignored_cols = new int []{};
  class GLMMultiVecSelect extends MultiVecSelect { GLMMultiVecSelect() { super("source", false);} }

  @API(help = "The GLM Model")
  public GLMModel glm_model;

  @API(help = "max-iterations", filter = Default.class, lmin=1, lmax=1000000)
  int max_iter = 50;
  @API(help = "If true, data will be standardized on the fly when computing the model.", filter = Default.class)
  boolean standardize = true;

  @API(help = "Family.", filter = Default.class)
  Family family = Family.gaussian;

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
  double lambda = 0.0;
  @API(help = "beta_eps", filter = Default.class)
  double beta_eps = 1e-4;

  public GLM2 setTweedieVarPower(double d){tweedie_variance_power = d; return this;}

  public GLM2() {
  }
  public GLM2(String desc, Key dest, Frame src, boolean standardize, Family family, Link link, double alpha, double lambda) {
    description = desc;
    destination_key = dest;
    source = src;
    this.family = family;
    this.link = link;
    this.alpha = alpha;
    this.lambda = lambda;
    this.standardize = standardize;
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

  private long _startTime;
  @Override protected void exec(){
    link = family.defaultLink;
    _startTime = System.currentTimeMillis();
    GLMModel m = new GLMModel(dest(),source,new GLMParams(family,tweedie_variance_power,link,1-tweedie_variance_power),beta_eps,alpha,lambda,System.currentTimeMillis()-_startTime);
    DKV.put(dest(), m);
    try {
      fork(null).get();
    } catch( InterruptedException e ) {
      throw new RuntimeException(e);
    } catch( ExecutionException e ) {
      throw new RuntimeException(e);
    }
  }
  @Override protected Response redirect() {
    return GLMProgressPage2.redirect(this, self(),dest());
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
      double [] newBeta = MemoryManager.malloc8d(glmt._xy.length);
//      System.out.println(glmt._gram.pprint(glmt._gram.getXX()));
      solver.solve(glmt._gram, glmt._xy, glmt._yy, newBeta);
      boolean done = false;
      if(Utils.hasNaNsOrInfs(newBeta)){
        System.out.println("got NaNs in beta after " + glmt._iter + " iterations");
        System.out.println(glmt._gram.pprint(glmt._gram.getXX()));
        done = true;
        newBeta = glmt._beta == null?newBeta:glmt._beta;
      }
      done = done || family == Family.gaussian || (glmt._iter+1) == max_iter || beta_diff(glmt._beta, newBeta) < beta_eps;
      GLMModel res = new GLMModel(dest(),null,glmt._iter+1,fr,glmt,beta_eps,alpha,lambda,newBeta,0.5,null,System.currentTimeMillis() - start_time);
      if(done){
        // final validation
        GLMValidationTask t = new GLMValidationTask(res);
        t.doAll(fr);
        Key valKey = GLMValidation.makeKey();
        DKV.put(valKey,t._res);
        res.validations = new Key[]{valKey};
        DKV.put(dest(),res);
        remove();
        fjt.tryComplete(); // signal we're done to anyone waiting for the job
      } else {
        DKV.put(dest(),res);
        GLMIterationTask nextIter = new GLMIterationTask(glmt, newBeta);
        nextIter.setCompleter(clone()); // we need to clone here as FJT will set status to done after this method
        nextIter.dfork(fr);
      }
    }
    @Override public boolean onExceptionalCompletion(Throwable ex, CountedCompleter caller){
      ex.printStackTrace();
      GLM2.this.cancel("Got exception '" + ex.getClass() + "', with msg '" + ex.getMessage() + "'");
      fjt.onExceptionalCompletion(ex, caller);
      return true;
    }
  }
  public H2OCountedCompleter fork(H2OCountedCompleter completer){
    tweedie_link_power = 1 - tweedie_variance_power; // TODO
    source.remove(ignored_cols);
    final Vec [] vecs =  source.vecs();
    ArrayList<Integer> constantOrNAs = new ArrayList<Integer>();
    for(int i = 0; i < vecs.length-1; ++i)// put response to the end
      if(vecs[i] == vresponse){
        source.add(source._names[i], source.remove(i));
        break;
      }
    for(int i = 0; i < vecs.length-1; ++i) // put response to the end
      if(vecs[i].min() == vecs[i].max() || vecs[i].naCnt() > vecs[i].length()*0.2)constantOrNAs.add(i);
    if(!constantOrNAs.isEmpty()){
      int [] cols = new int[constantOrNAs.size()];
      for(int i = 0; i < cols.length; ++i)cols[i] = constantOrNAs.get(i);
      source.remove(cols);
    }
    final Frame fr = GLMTask.adaptFrame(source);
    YMUTask ymut = new YMUTask(new GLMParams(family, tweedie_variance_power, link,tweedie_link_power), standardize, case_mode, case_val, fr.anyVec().length());
    ymut.doAll(fr);
    GLMIterationTask firstIter = new GLMIterationTask(new GLMParams(family, tweedie_variance_power, link,tweedie_link_power),null,standardize, 1.0/ymut.nobs(), case_mode, case_val);
    firstIter._ymu = ymut.ymu();
    final H2OEmptyCompleter fjt = start(new H2OEmptyCompleter());
    if(completer != null)fjt.setCompleter(completer);
    final LSMSolver solver = new ADMMSolver(lambda, alpha);
    firstIter.setCompleter(new Iteration(solver,fr,fjt));
    firstIter.dfork(fr);
    return fjt;
  }
}
