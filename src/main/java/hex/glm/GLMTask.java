package hex.glm;

import hex.FrameTask;
import hex.glm.GLMParams.Family;
import hex.gram.Gram;

import java.util.ArrayList;

import water.H2O.H2OCountedCompleter;
import water.*;
import water.util.ModelUtils;
import water.util.Utils;

/**
 * Contains all GLM related tasks.
 *
 * @author tomasnykodym
 *
 */

public abstract class GLMTask<T extends GLMTask<T>> extends FrameTask<T> {
  final protected GLMParams _glm;
  public GLMTask(Job job, DataInfo dinfo, GLMParams glm){this(job,dinfo,glm,null);}
  public GLMTask(Job job, DataInfo dinfo, GLMParams glm,H2OCountedCompleter cmp){super(job,dinfo,cmp);_glm = glm;}

  //helper function to compute eta - i.e. beta * row
  protected final double computeEta(final int ncats, final int [] cats, final double [] nums, final double [] beta){
    double res = 0;
    for(int i = 0; i < ncats; ++i)res += beta[cats[i]];
    final int numStart = _dinfo.numStart();
    for(int i = 0; i < nums.length; ++i)res += nums[i]*beta[numStart+i];
    res += beta[beta.length-1]; // intercept
    return res;
  }
  /**
   * Helper task to compute precise mean of response and number of observations.
   * (We skip rows with NAs, so we can't use Vec's mean in general.
   *
   * @author tomasnykodym
   *
   */
  static class YMUTask extends FrameTask<YMUTask>{
    private long   _nobs;
    protected double _ymu;
    public double _ymin = Double.POSITIVE_INFINITY;
    public double _ymax = Double.NEGATIVE_INFINITY;
    public YMUTask(Job job, DataInfo dinfo) {this(job,dinfo,null);}
    public YMUTask(Job job, DataInfo dinfo, H2OCountedCompleter cmp) {
      super(job,dinfo,cmp);
    }
    @Override protected void processRow(long gid, double[] nums, int ncats, int[] cats, double [] responses) {
      double response = responses[0];
      _ymu += response;
      if(response < _ymin)_ymin = response;
      if(response > _ymax)_ymax = response;
      ++_nobs;
    }
    @Override public void reduce(YMUTask t){
      if(t._nobs != 0){
        if(_nobs == 0){
          _ymu = t._ymu;
          _nobs = t._nobs;
          _ymin = t._ymin;
          _ymax = t._ymax;
        } else {
          _ymu = _ymu*((double)_nobs/(_nobs+t._nobs)) + t._ymu*t._nobs/(_nobs+t._nobs);
          _nobs += t._nobs;
          _ymax = Math.max(_ymax,t._ymax);
          _ymin = Math.min(_ymin,t._ymin);
        }
      }
    }
    @Override protected void chunkDone(){_ymu /= _nobs;}
    public double ymu(){return _ymu;}
    public long nobs(){return _nobs;}
  }
  /**
   * Task to compute Lambda Max for the given dataset.
   * @author tomasnykodym
   */
  static class LMAXTask extends GLMIterationTask {
    private double[] _z;
    private final double _gPrimeMu;
    private final double _alpha;

    //public GLMIterationTask(Job job, DataInfo dinfo, GLMParams glm, boolean computeGram, boolean validate, boolean computeGradient, double [] beta, double ymu, double reg, H2OCountedCompleter cmp) {


    public LMAXTask(Job job, DataInfo dinfo, GLMParams glm, double ymu, long nobs, double alpha, float [] thresholds, H2OCountedCompleter cmp) {
      super(job, dinfo, glm, false, true, true, glm.nullModelBeta(dinfo,ymu), ymu, 1.0/nobs, thresholds, cmp);
      _gPrimeMu = glm.linkDeriv(ymu);
      _alpha = alpha;
    }
    @Override public void chunkInit(){
      super.chunkInit();
      _z = MemoryManager.malloc8d(_grad.length);
    }
    @Override public void processRow(long gid, double[] nums, int ncats, int[] cats, double [] responses) {
      double w = (responses[0] - _ymu) * _gPrimeMu;
      for( int i = 0; i < ncats; ++i ) _z[cats[i]] += w;
      final int numStart = _dinfo.numStart();
      for(int i = 0; i < nums.length; ++i)
        _z[i+numStart] += w*nums[i];
      super.processRow(gid, nums, ncats, cats, responses);
    }
    @Override public void reduce(GLMIterationTask git){
      Utils.add(_z, ((LMAXTask)git)._z);
      super.reduce(git);
    }
    public double lmax(){
      double res = Math.abs(_z[0]);
      for( int i = 1; i < _z.length; ++i )
        if(res < _z[i])res = _z[i];
        else if(res < -_z[i])res = -_z[i];
      return _glm.variance(_ymu) * res / (_nobs * Math.max(_alpha,1e-3));
    }
  }

  /**
   * One iteration of glm, computes weighted gram matrix and t(x)*y vector and t(y)*y scalar.
   *
   * @author tomasnykodym
   *
   */
  public static class GLMLineSearchTask extends GLMTask<GLMLineSearchTask> {
    double [][] _betas;
    double []   _objvals;
    double _caseVal = 0;
    final double _l1pen;
    final double _l2pen;
    final double _reg;
    public GLMLineSearchTask(Job job, DataInfo dinfo, GLMParams glm, double [] oldBeta, double [] newBeta, double minStep, long nobs, double alpha, double lambda, H2OCountedCompleter cmp){
      super(job,dinfo,glm,cmp);
      _l2pen = 0.5*(1-alpha)*lambda;
      _l1pen = alpha*lambda;
      _reg = 1.0/nobs;
      ArrayList<double[]> betas = new ArrayList<double[]>();
      double step = 0.5;
      while(step >= minStep){
        double [] b = MemoryManager.malloc8d(oldBeta.length);
        for(int i = 0; i < oldBeta.length; ++i)
          b[i] = 0.5*(oldBeta[i] + newBeta[i]);
        betas.add(b);
        newBeta = b;
        step *= 0.5;
      }
      _betas = new double[betas.size()][];
      betas.toArray(_betas);
    }

    @Override public void chunkInit(){
      _objvals = new double[_betas.length];
    }
    @Override public void chunkDone(){
      for(int i = 0; i < _objvals.length; ++i)
        _objvals[i] *= _reg;
    }
    @Override public void postGlobal(){
      for(int i = 0; i < _objvals.length; ++i)
        for(double d:_betas[i])
          _objvals[i] += d*d*_l2pen + (d>=0?d:-d)*_l1pen;
    }
    @Override public final void processRow(long gid, final double [] nums, final int ncats, final int [] cats, double [] responses){
      for(int i = 0; i < _objvals.length; ++i){
        final double [] beta = _betas[i];
        double y = responses[0];
        _objvals[i] += _glm.deviance(y,_glm.linkInv(computeEta(ncats, cats,nums,beta)));
      }
    }
    @Override
    public void reduce(GLMLineSearchTask git){Utils.add(_objvals,git._objvals);}
  }

  public static final class LMIterationTask extends FrameTask<LMIterationTask>{
    public final int n_folds;
    public long _n;
    Gram [] _gram;
    double [][] _xy;
    public LMIterationTask(Job job, DataInfo dinfo,int nfolds, H2OCountedCompleter cmp){
      super(job, dinfo, cmp);
      n_folds = Math.max(1,nfolds);
    }
    @Override public void chunkInit(){
      _gram = new Gram[n_folds];
      _xy = new double[n_folds][];
      for(int i = 0; i < n_folds; ++i){
        _gram[i] = new Gram(_dinfo.fullN(), _dinfo.largestCat(), _dinfo._nums, _dinfo._cats,true);
        _xy[i] = MemoryManager.malloc8d(_dinfo.fullN()+1); // + 1 is for intercept
      }
    }
    @Override public final void processRow(long gid, final double [] nums, final int ncats, final int [] cats, double [] responses){
      final int fold = n_folds == 1?0:(int)_n % n_folds;
      final double y = responses[0];
      _gram[fold].addRow(nums, ncats, cats, 1);
      for(int i = 0; i < ncats; ++i){
        final int ii = cats[i];
        _xy[fold][ii] += responses[0];
      }
      final int numStart = _dinfo.numStart();
      for(int i = 0; i < nums.length; ++i){
        _xy[fold][numStart+i] += y*nums[i];
      }
      ++_n;
    }

    @Override public void chunkDone(){

    }

    @Override public void reduce(LMIterationTask lmit){
      for(int i = 0; i < n_folds; ++i){
        _gram[i].add(lmit._gram[i]);
        Utils.add(_xy[i],lmit._xy[i]);
      }
    }
  }

  /**
   * One iteration of glm, computes weighted gram matrix and t(x)*y vector and t(y)*y scalar.
   *
   * @author tomasnykodym
   */
  public static class GLMIterationTask extends GLMTask<GLMIterationTask> {
    final double [] _beta;
    protected Gram      _gram;
    double [] _xy;
    protected double [] _grad;
    double    _yy;
    GLMValidation _val; // validation of previous model
    final double _ymu;
    protected final double _reg;
    long _nobs;
    final boolean _validate;
    final float [] _thresholds;
    final boolean _computeGradient;
    final boolean _computeGram;

    public GLMIterationTask(Job job, DataInfo dinfo, GLMParams glm, boolean computeGram, boolean validate, boolean computeGradient, double [] beta, double ymu, double reg, float [] thresholds, H2OCountedCompleter cmp) {
      super(job, dinfo,glm,cmp);
      _beta = beta;
      _ymu = ymu;
      _reg = reg;
      _computeGram = computeGram;
      _validate = validate;
      assert thresholds != null;
      _thresholds = _validate?thresholds:null;

      _computeGradient = computeGradient;
      assert !_computeGradient || validate;
    }

    @Override public void processRow(long gid, final double [] nums, final int ncats, final int [] cats, double [] responses){
      ++_nobs;
      double y = responses[0];
      assert ((_glm.family != Family.gamma) || y > 0) : "illegal response column, y must be > 0  for family=Gamma.";
      assert ((_glm.family != Family.binomial) || (0 <= y && y <= 1)) : "illegal response column, y must be <0,1>  for family=Binomial. got " + y;
      final double w, eta, mu, var, z;
      final int numStart = _dinfo.numStart();
      double d = 1;
      if( _glm.family == Family.gaussian){
        w = 1;
        z = y;
        mu = (_validate || _computeGradient)?computeEta(ncats,cats,nums,_beta):0;
      } else {
        if( _beta == null ) {
          mu = _glm.mustart(y, _ymu);
          eta = _glm.link(mu);
        } else {
          eta = computeEta(ncats, cats,nums,_beta);
          mu = _glm.linkInv(eta);
        }
        var = Math.max(1e-5, _glm.variance(mu)); // avoid numerical problems with 0 variance
        d = _glm.linkDeriv(mu);
        z = eta + (y-mu)*d;
        w = 1.0/(var*d*d);
      }
      if(_validate)
        _val.add(y, mu);
      assert w >= 0 : "invalid weight " + w;
      final double wz = w * z;
      _yy += wz * z;
      if(_computeGradient || _computeGram){
        final double grad = _computeGradient?w*d*(mu-y):0;
        for(int i = 0; i < ncats; ++i){
          final int ii = cats[i];
          if(_computeGradient)_grad[ii] += grad;
          _xy[ii] += wz;
        }
        for(int i = 0; i < nums.length; ++i){
          _xy[numStart+i] += wz*nums[i];
          if(_computeGradient)
            _grad[numStart+i] += grad*nums[i];
        }
        if(_computeGradient)_grad[numStart + _dinfo._nums] += grad;
        _xy[numStart + _dinfo._nums] += wz;
        if(_computeGram)_gram.addRow(nums, ncats, cats, w);
      }
    }
    @Override protected void chunkInit(){
      if(_computeGram)_gram = new Gram(_dinfo.fullN(), _dinfo.largestCat(), _dinfo._nums, _dinfo._cats,true);
      _xy = MemoryManager.malloc8d(_dinfo.fullN()+1); // + 1 is for intercept
      int rank = 0;
      if(_beta != null)for(double d:_beta)if(d != 0)++rank;
      if(_validate)_val = new GLMValidation(null,_ymu, _glm,rank, _thresholds);
      if(_computeGradient)
        _grad = MemoryManager.malloc8d(_dinfo.fullN()+1); // + 1 is for intercept
    }

    @Override protected void chunkDone(){
      if(_computeGram)_gram.mul(_reg);
      for(int i = 0; i < _xy.length; ++i)
        _xy[i] *= _reg;
      if(_grad != null)
        for(int i = 0; i < _grad.length; ++i)
          _grad[i] *= _reg;
      _yy *= _reg;
    }
    @Override
    public void reduce(GLMIterationTask git){
      Utils.add(_xy, git._xy);
      if(_computeGram)_gram.add(git._gram);
      _yy += git._yy;
      _nobs += git._nobs;
      if(_validate) _val.add(git._val);
      if(_computeGradient) Utils.add(_grad,git._grad);
      super.reduce(git);
    }

    @Override public void postGlobal(){
      if(_val != null)_val.finalize_AIC_AUC();
    }
    public double [] gradient(double l2pen){
      final double [] res = _grad.clone();
      for(int i = 0; i < _grad.length-1; ++i) res[i] += l2pen*_beta[i];
      return res;
    }
  }
}
