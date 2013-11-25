package hex.glm;

import hex.FrameTask;
import hex.glm.GLMParams.Family;
import hex.gram.Gram;
import water.Job;
import water.MemoryManager;
import water.H2O.H2OCountedCompleter;
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

    public YMUTask(Job job, DataInfo dinfo) {this(job,dinfo,null);}
    public YMUTask(Job job, DataInfo dinfo, H2OCountedCompleter cmp) {
      super(job,dinfo,cmp);
    }
    @Override protected void processRow(double[] nums, int ncats, int[] cats, double response) {
      _ymu += response;
      ++_nobs;
    }
    @Override public void reduce(YMUTask t){
      _ymu = _ymu*((double)_nobs/(_nobs+t._nobs)) + t._ymu*t._nobs/(_nobs+t._nobs);
      _nobs += t._nobs;
    }
    @Override protected void chunkDone(){_ymu /= _nobs;}
    public double ymu(){return _ymu;}
    public long nobs(){return _nobs;}
  }
  /**
   * Task to compute Lambda Max for the given dataset.
   * @author tomasnykodym
   */
  static class LMAXTask extends GLMTask<LMAXTask> {
    private double[] _z;
    private final double _ymu;
    private final double _gPrimeMu;
    private long _nobs;
    private final int _n;
    private final double _alpha;


    public LMAXTask(Job job, DataInfo dinfo, GLMParams glm, double ymu, double alpha, H2OCountedCompleter cmp) {
      super(job,dinfo,glm,cmp);
      _ymu = ymu;
      _gPrimeMu = glm.linkDeriv(ymu);
      _n = dinfo.fullN();
      _alpha = alpha;
    }
    @Override public void chunkInit(){
      _z = MemoryManager.malloc8d(_n);
    }
    @Override protected void processRow(double[] nums, int ncats, int[] cats, double response) {
      double w = (response - _ymu) * _gPrimeMu;
      for( int i = 0; i < ncats; ++i ) _z[cats[i]] += w;
      final int numStart = _dinfo.numStart();
      ++_nobs;
      for(int i = 0; i < nums.length; ++i)
        _z[i+numStart] += w*nums[i];
    }
    @Override public void reduce(LMAXTask l){Utils.add(_z, l._z); _nobs += l._nobs;}
    public double lmax(){
      double res = Math.abs(_z[0]);
      for( int i = 1; i < _z.length; ++i )
        res = Math.max(res, Math.abs(_z[i]));
      return _glm.variance(_ymu) * res / (_nobs*Math.max(_alpha,1e-3));
    }
  }

  /**
   * One iteration of glm, computes weighted gram matrix and t(x)*y vector and t(y)*y scalar.
   *
   * @author tomasnykodym
   *
   */
  public static class GLMIterationTask extends GLMTask<GLMIterationTask> {
    int _iter;
    final double [] _beta;
    Gram      _gram;
    double [] _xy;
    double    _yy;
    GLMValidation _val; // validation of previous model
    final double _ymu;
    protected final double _reg;

    public GLMIterationTask(Job job, DataInfo dinfo, GLMParams glm, double [] beta, int iter, double ymu, double reg) {
      super(job, dinfo,glm);
      _iter = iter;
      _beta = beta;
      _ymu = ymu;
      _reg = reg;
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
          eta = computeEta(ncats, cats,nums,_beta);
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
      final int numStart = _dinfo.numStart();
      for(int i = 0; i < nums.length; ++i)_xy[numStart+i] += wz*nums[i];
      _xy[numStart + _dinfo._nums] += wz;
      _gram.addRow(nums, ncats, cats, w);
    }
    @Override protected void chunkInit(){
      _gram = new Gram(_dinfo.fullN(), _dinfo.largestCat(), _dinfo._nums, _dinfo._cats,true);
      _xy = MemoryManager.malloc8d(_dinfo.fullN()+1); // + 1 is for intercept
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
      _gram.add(git._gram);
      _yy += git._yy;
      _val.add(git._val);
      super.reduce(git);
    }
  }
}
