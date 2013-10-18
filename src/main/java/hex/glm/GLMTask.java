package hex.glm;

import hex.FrameTask;
import hex.glm.GLMParams.CaseMode;
import hex.glm.GLMParams.Family;
import hex.gram.Gram;

import java.util.Arrays;

import water.*;
import water.fvec.*;
import water.util.Utils;

/**
 * Base class for all GLM-related MRTask(2)s.
 *
 * Filters rows with NA(s), handles data-regularization of numeric columns on the fly, and performs on-the fly expansion of categoricals.
 * Users should override processRow method and potentially reduce.
 *
 * @author tomasnykodym
 *
 */
public abstract class GLMTask<T extends GLMTask<T>> extends FrameTask<T>{
  // data-regularization params
  double    _ymu = Double.NaN; // mean of the response

  protected final double [] _beta; // beta vector from previous computation or null if this is the first iteration

  final GLMParams _glm;
  // CaseMode used to turn non-binary response into a binary one (0,1), used only by binomial family
  protected final CaseMode _caseMode;
  // CaseVaue to be used together with the Case predicate to turn the response into binary variable
  protected final double _caseVal;

  // size of the top-left strictly diagonal region of the gram matrix, currently just the size of the largest categorical
  final protected int diagN(){return largestCat() > 1?largestCat():0;}

  public GLMTask(Job job, GLMParams glm, double [] beta, boolean standardize, CaseMode cm, double cv) {this(job,glm, beta, standardize, cm, cv, 1,0,false);}
  public GLMTask(Job job, GLMParams glm, double [] beta, boolean standardize, CaseMode cm, double cv, int step, int offset, boolean complement) {
    super(job, standardize,true, step, offset, complement);
    _caseMode = cm; _caseVal = cv; _beta = beta;
    _glm = glm;
  }
  protected GLMTask(GLMTask gt, double [] beta){
    super(gt);
    _glm = gt._glm;
    _caseMode = gt._caseMode;
    _caseVal = gt._caseVal;
    _beta = beta;
  }

  // inner helper function to compute eta - i.e. beta * row
  protected final double computeEta(int ncats, int [] cats, double [] nums){
    double res = 0;
    for(int i = 0; i < ncats; ++i)res += _beta[cats[i]];
    final int numStart = _catOffsets[_cats];
    for(int i = 0; i < nums.length; ++i)res += nums[i]*_beta[numStart+i];
    res += _beta[_beta.length-1]; // intercept
    return res;
  }
  /**
   * Helper task to compute precise mean of response and number of observations.
   * (We skip rows with NAs, so we can't use Vec's mean in general.
   *
   * @author tomasnykodym
   *
   */
  //TODO do this for all columns
  static class YMUTask extends GLMTask<YMUTask>{
    private long   _nobs;
    private double _ymu;
    private final double _reg;
    public YMUTask(Job job, GLMParams glm, boolean standardize, CaseMode cm, double cv, long nobs) {
      super(job,glm,null,standardize, cm, cv);
      _reg = 1.0/nobs;
    }
    @Override protected void processRow(double[] nums, int ncats, int[] cats, double response) {
      _ymu += response;
      ++_nobs;
    }
    @Override public void reduce(YMUTask t){
      _ymu += t._ymu;
      _nobs += t._nobs;
    }
    @Override protected void chunkDone(){_ymu *= _reg;}

    public double ymu(){return _ymu /(_reg*_nobs);}
    public long nobs(){return _nobs;}
  }
  /**
   * Task to compute Lambda Max for the given dataset.
   * @author tomasnykodym
   *
   */
  static class LMAXTask extends GLMTask<LMAXTask> {
    private final double[] _z;
    private final double _ymu;
    private final double _gPrimeMu;
    private long _nobs;

    public LMAXTask(Job job, GLMParams glm, boolean standardize, CaseMode cm, double cv, double ymu, int P) {
      super(job,glm,null,standardize, cm, cv);
      _z = MemoryManager.malloc8d(P);
      _ymu = ymu;
      _gPrimeMu = glm.linkDeriv(ymu);
    }
    @Override protected void processRow(double[] nums, int ncats, int[] cats, double response) {
      double w = (response - _ymu) * _gPrimeMu;
      for( int i = 0; i < ncats; ++i ) _z[cats[i]] += w;
      final int off = _catOffsets[_cats];
      ++_nobs;
      for(int i = 0; i < nums.length; ++i)
        _z[i+off] += w*nums[i];
    }
    @Override public void reduce(LMAXTask l){Utils.add(_z, l._z); _nobs += l._nobs;}
    public double lmax(){
      double res = Math.abs(_z[0]);
      for( int i = 1; i < _z.length; ++i )
        res = Math.max(res, Math.abs(_z[i]));
      return _glm.variance(_ymu) * res / _nobs;
    }
  }

  /**
   * One iteration of glm, computes weighted gram matrix and t(x)*y vector and t(y)*y scalar.
   *
   * @author tomasnykodym
   *
   */
  public static class GLMIterationTask extends GLMTask<GLMIterationTask> {
    final double _reg;
    final int _iter;
    Gram      _gram;
    double [] _xy;
    double    _yy;
    GLMValidation _val; // validation of previous model

    public GLMIterationTask(Job job, GLMParams glm, double [] beta, boolean standardize, double reg, CaseMode caseMode, double caseVal, int step, int offset, boolean complement) {
      super(job, glm, beta, standardize, caseMode, caseVal, step, offset, complement);
      _iter = 0;
      _reg = reg;
    }

    public GLMIterationTask(GLMIterationTask git, double [] beta) {
      super(git,beta);
      _iter = git._iter+1;
      _reg = git._reg;
    }

    @Override public final void processRow(final double [] nums, final int ncats, final int [] cats, final double y){
      assert ((_glm.family != Family.gamma) || y > 0) : "illegal response column, y must be > 0  for family=Gamma.";
      assert ((_glm.family != Family.binomial) || (0 <= y && y <= 1)) : "illegal response column, y must be <0,1>  for family=Binomial. got " + y;
      final double w, eta, mu, var, z;
      if( _glm.family == Family.gaussian) {
        w = 1;
        z = y;
      } else {
        if( _beta == null ) {
          mu = _glm.mustart(y, _ymu);
          eta = _glm.link(mu);
        } else {
          eta = computeEta(ncats, cats,nums);
          mu = _glm.linkInv(eta);
        }
        _val.add(y, mu);
        var = Math.max(1e-5, _glm.variance(mu)); // avoid numerical problems with 0 variance
        final double d = _glm.linkDeriv(mu);
        z = eta + (y - mu)*d;
        w = 1.0/(var*d*d);
      }
      assert w >= 0 : "invalid weight " + w;
      _yy += 0.5 * w * z * z;
      double wz = w * z;
      for(int i = 0; i < ncats; ++i)_xy[cats[i]] += wz;
      final int numStart = _catOffsets[_cats];
      for(int i = 0; i < nums.length; ++i)_xy[numStart+i] += wz*nums[i];
      _xy[numStart + _nums] += wz;
      _gram.addRow(nums, ncats, cats, w);
    }
    @Override protected void chunkInit(){
      _gram = new Gram(fullN(), diagN(), _nums, _cats,true);
      _xy = MemoryManager.malloc8d(fullN()+1); // + 1 is for intercept
      int rank = 0;
      if(_beta != null)for(double d:_beta)if(d != 0)++rank;
      _val = new GLMValidation(null,_ymu, _glm,rank);
    }
    @Override protected void chunkDone(){
      _gram.mul(_reg);
      _val.regularize(_reg);
      for(int i = 0; i < _xy.length; ++i)
        _xy[i] *= _reg;
    }
    @Override
    public void reduce(GLMIterationTask git){
      Utils.add(_xy, git._xy);
      _yy += git._yy;
      _val.add(git._val);
      super.reduce(git);
    }
  }

}
