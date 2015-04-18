package hex.glm;

import hex.FrameTask;
import hex.glm.GLMParams.Family;
import hex.glm.GLMParams.Link;
import hex.gram.Gram;

import java.util.Arrays;

import water.H2O.H2OCountedCompleter;
import water.*;
import water.fvec.Chunk;
import water.util.Utils;

/**
 * Contains all GLM related tasks.
 *
 * @author tomasnykodym
 *
 */

public abstract class GLMTask<T extends GLMTask<T>> extends FrameTask<T> {
  final protected GLMParams _glm;

  public GLMTask(Key jobKey, DataInfo dinfo, GLMParams glm){this(jobKey,dinfo,glm,null);}
  public GLMTask(Key jobKey, DataInfo dinfo, GLMParams glm,H2OCountedCompleter cmp){super(jobKey,dinfo,cmp);_glm = glm;}

  //helper function to compute eta - i.e. beta * row
  protected final double computeEta(final int ncats, final int [] cats, final int nnums, final double [] nums, final double [] beta){
    double res = 0;
    for(int i = 0; i < ncats; ++i)res += beta[cats[i]];
    final int numStart = _dinfo.numStart();
    for (int i = 0; i < nnums; ++i) res += nums[i] * beta[numStart + i];
    if(_dinfo._hasIntercept)
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
    private long []  _nobs;
    protected double [] _ymu;
    public double [] _ymin;
    public double [] _ymax;
    final int _nfolds;
    public YMUTask(Key jobKey, DataInfo dinfo,int nfolds) {this(jobKey,dinfo, nfolds, null);}
    public YMUTask(Key jobKey, DataInfo dinfo,int nfolds,  H2OCountedCompleter cmp) {
      super(jobKey,dinfo,cmp);
      _nfolds = nfolds;
    }
    @Override public void chunkInit(){
      super.chunkInit();
      _ymu = new double[_nfolds+1];
      _nobs = new long[_nfolds+1];
      _ymax = new double[_nfolds+1];
      _ymin = new double[_nfolds+1];
      Arrays.fill(_ymax,Double.NEGATIVE_INFINITY);
      Arrays.fill(_ymax,Double.POSITIVE_INFINITY);
    }
    @Override protected void processRow(long gid, double[] nums, int ncats, int[] cats, double [] responses) {
      double response = responses[0];
      _ymu[0] += response;
      ++_nobs[0];
      if(response < _ymin[0])_ymin[0] = response;
      if(response > _ymax[0])_ymax[0] = response;
      for(int i = 1; i < _nfolds+1; ++i) {
        if(gid % _nfolds == (i-1))
          continue;
        _ymu[i] += response;
        ++_nobs[i];
        if(response < _ymin[0])_ymin[i] = response;
        if(response > _ymax[i])_ymax[i] = response;
      }
    }
    @Override public void reduce(YMUTask t){
      if(t._nobs[0] != 0){
        if(_nobs[0] == 0){
          _ymu = t._ymu;
          _nobs = t._nobs;
          _ymin = t._ymin;
          _ymax = t._ymax;
        } else {
          for(int i = 0; i < _nfolds+1; ++i) {
            if(_nobs[i] + t._nobs[i] == 0)continue;
            _ymu[i] = _ymu[i] * ((double) _nobs[i] / (_nobs[i] + t._nobs[i])) + t._ymu[i] * t._nobs[i] / (_nobs[i] + t._nobs[i]);
            _nobs[i] += t._nobs[i];
            if(t._ymax[i] > _ymax[i])
              _ymax[i] = t._ymax[i];
            if(t._ymin[i] < _ymin[i])
              _ymin[i] = t._ymin[i];
          }
        }
      }
    }
    @Override protected void chunkDone(long n){
      for(int i = 0; i < _ymu.length; ++i)
        if(_nobs[i] != 0)_ymu[i] /= _nobs[i];
    }
    public double ymu(){return ymu(-1);}
    public long nobs(){return nobs(-1);}
    public double ymu(int foldId){return _ymu[foldId+1];}
    public long nobs(int foldId){return _nobs[foldId+1];}
  }

  public static class GLMLineSearchTask extends GLMTask<GLMLineSearchTask> {

    public GLMLineSearchTask(int noff, Key jobKey, DataInfo dinfo, GLMParams glm, double[] oldBeta, double[] newBeta,  double [] lowerBounds, double [] upperBounds, double ymu, long nobs, H2OCountedCompleter cmp) {
      super(jobKey, dinfo, glm, cmp);
      double [][] betas = new double[32][];
      double step = GLM2.LS_STEP;
      for(int i = 0; i < betas.length; ++i){
        betas[i] = MemoryManager.malloc8d(newBeta.length);
        for(int j = 0; j < oldBeta.length; ++j) {
          betas[i][j] = oldBeta[j] + step * (newBeta[j] - oldBeta[j]);
          if(lowerBounds != null && betas[i][j] < lowerBounds[j])
            betas[i][j] = lowerBounds[j];
          if(upperBounds != null && betas[i][j] > upperBounds[j])
            betas[i][j] = upperBounds[j];
        }
        step *= GLM2.LS_STEP;
      }
      // public GLMIterationTask(Key jobKey, DataInfo dinfo, GLMParams glm, boolean computeGram, boolean validate, boolean computeGradient, double [] beta, double ymu, double reg, float [] thresholds, H2OCountedCompleter cmp) {
      _glmts = new GLMIterationTask[betas.length];
      for(int i = 0; i < _glmts.length; ++i)
        _glmts[i] = new GLMIterationTask(noff, jobKey,dinfo,glm,false,true,true,betas[i],ymu,1.0/nobs,new float[]{0} /* don't really want CMs!*/,null);
    }

    GLMIterationTask [] _glmts;
    @Override public void chunkInit(){
      _glmts = _glmts.clone();
      for(int i = 0; i < _glmts.length; ++i)
        (_glmts[i] = _glmts[i].clone()).chunkInit();
    }
    @Override public void chunkDone(long n){
      for(int i = 0; i < _glmts.length; ++i)
        _glmts[i].chunkDone(n);
    }
    @Override public void postGlobal(){
      for(int i = 0; i < _glmts.length; ++i)
        _glmts[i].postGlobal();
    }
    @Override public final void processRow(long gid, final double [] nums, final int ncats, final int [] cats, double [] responses){
      for(int i = 0; i < _glmts.length; ++i)
        _glmts[i].processRow(gid,nums,ncats,cats,responses);
    }
    @Override
    public void reduce(GLMLineSearchTask git){
      for(int i = 0; i < _glmts.length; ++i)
        _glmts[i].reduce(git._glmts[i]);
    }
  }


  public static class GLMInterceptTask extends MRTask2<GLMInterceptTask> {
    double _xy;
    double _xx;
    double _icpt;
    final double _sub;
    final double _mul;
    final GLMParams _glm;
    public GLMInterceptTask(GLMParams glm, double sub, double mul, double icpt){
      _glm = glm;
      _sub = sub;
      _mul = mul;
      _icpt = icpt ;
    }
    @Override public void map(Chunk offset, Chunk response){
      for(int i = 0; i < offset._len; ++i){
        if(offset.isNA0(i) || response.isNA0(i)) continue;
        double eta = _icpt;
        double y = response.at0(i);
        double mu = _glm.linkInv(eta+(offset.at0(i)-_sub)*_mul);
        double var = _glm.variance(mu);
        if(var < 1e-5) var = 1e-5; // avoid numerical problems with 0 variance
        double d = _glm.linkDeriv(mu);
        double w = 1.0/(var*d*d);
        double z = eta + (y-mu)*d;
        _xy += w*z;
        _xx += w;
      }
    }
    @Override public void reduce(GLMInterceptTask other){
      _xx += other._xx;
      _xy += other._xy;
    }
    @Override public  void postGlobal(){
      _icpt = _xy/_xx;
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
    protected final double _reg;
    long _nobs;
    final boolean _validate;
    final float [] _thresholds;
    float [][] _newThresholds;
    int [] _ti;
    final boolean _computeGradient;
    final boolean _computeGram;
    public static final int N_THRESHOLDS = 50;
    final int _noffsets;
    final double _ymu;

    public GLMIterationTask(int noff, Key jobKey, DataInfo dinfo, GLMParams glm, boolean computeGram, boolean validate, boolean computeGradient, double [] beta, double ymu, double reg, float [] thresholds, H2OCountedCompleter cmp) {
      super(jobKey, dinfo,glm,cmp);
      assert beta != null;

      _ymu = ymu;
      _noffsets = noff;
      assert beta == null || beta.length == (dinfo.fullN()+(_dinfo._hasIntercept?1:0)-_noffsets):"beta.length != dinfo.fullN(), beta = " + beta.length + " dinfo = " + dinfo.fullN() + ", noffsets = " + _noffsets;
      _beta = beta;
      _reg = reg;
      _computeGram = computeGram;
      _validate = validate;
      assert thresholds != null;
      _thresholds = _validate?thresholds:null;

      _computeGradient = computeGradient;
      assert !_computeGradient || validate;
    }

    private void sampleThresholds(int yi){
      _ti[yi] = (_newThresholds[yi].length >> 2);
      try{ Arrays.sort(_newThresholds[yi]);} catch(Throwable t){
        System.out.println("got AIOOB during sort?! ary = " + Arrays.toString(_newThresholds[yi]));
        return;
      } // sort throws AIOOB sometimes!
      for (int i = 0; i < _newThresholds.length; i += 4)
        _newThresholds[yi][i >> 2] = _newThresholds[yi][i];
    }
    @Override public void processRow(long gid, final double [] nums, final int ncats, final int [] cats, double [] responses /* time...*/){
      ++_nobs;
      double off = 0;
      for(int i = 1; i <= _noffsets; ++i)
        off += nums[nums.length-i];
      final double y = responses[0];
      assert ((_glm.family != Family.gamma) || y > 0) : "illegal response column, y must be > 0  for family = Gamma.";
      assert ((_glm.family != Family.binomial) || (0 <= y && y <= 1)) : "illegal response column, y must be <0,1>  for family=Binomial. got " + y;
      double w, eta, mu, var, z;
      final int numStart = _dinfo.numStart();
      double d = 1;
      if( _glm.family == Family.gaussian && _glm.link == Link.identity){
        w = 1;
        z = _glm.link(y);
        mu =  off + ((_validate || _computeGradient)?computeEta(ncats,cats,nums.length-_noffsets,nums,_beta):0);
      } else {
        eta = computeEta(ncats, cats,nums.length-_noffsets,nums,_beta);
        mu = _glm.linkInv(eta+off);
        var = _glm.variance(mu);
        if(var < 1e-5) var = 1e-5; // avoid numerical problems with 0 variance
        d = _glm.linkDeriv(mu);
        z = eta + (y-mu)*d;
        w = 1.0/(var*d*d);
      }
      if(_validate) {
        _val.add(y, mu);
        if(_glm.family == Family.binomial) {
          int yi = (int) y;
          if (_ti[yi] == _newThresholds[yi].length)
            sampleThresholds(yi);
          _newThresholds[yi][_ti[yi]++] = (float) mu;
        }
      }
      assert w >= 0|| Double.isNaN(w) : "invalid weight " + w; // allow NaNs - can occur if line-search is needed!
      final double wz = w * z;
      _yy += wz * z;
      if(_computeGradient || _computeGram){
        final double grad = _computeGradient?w*d*(mu-y):0;
        for(int i = 0; i < ncats; ++i){
          final int ii = cats[i];
          if(_computeGradient)_grad[ii] += grad;
          _xy[ii] += wz;
        }
        for(int i = 0; i < nums.length - _noffsets; ++i){
          _xy[numStart+i] += wz*nums[i];
          if(_computeGradient)
            _grad[numStart+i] += grad*nums[i];
        }

        if(_dinfo._hasIntercept){
          _xy[_xy.length-1] += wz;
          if(_computeGradient)
            _grad[numStart + _dinfo._nums - _noffsets] += grad;
        }
        if(_computeGram)_gram.addRow(nums, ncats, cats, w);
      }

    }
    @Override protected void chunkInit(){
      if(_computeGram)_gram = new Gram(_dinfo.fullN()-_noffsets, _dinfo.largestCat(), _dinfo._nums-_noffsets, _dinfo._cats, _dinfo._hasIntercept);
      _xy = MemoryManager.malloc8d(_dinfo.fullN()+(_dinfo._hasIntercept?1:0)-_noffsets); // + 1 is for intercept
      int rank = 0;
      if(_beta != null)for(double d:_beta)if(d != 0)++rank;
      if(_validate){
        _val = new GLMValidation(null, _glm,rank, _thresholds);
        if(_glm.family == Family.binomial){
          _ti = new int[2];
          _newThresholds = new float[2][N_THRESHOLDS << 2];
        }
      }
      if(_computeGradient)
        _grad = MemoryManager.malloc8d(_dinfo.fullN()+ (_dinfo._hasIntercept?1:0) - _noffsets);
      if(_glm.family == Family.binomial && _validate){
        _ti = new int[2];
        _newThresholds = new float[2][4*N_THRESHOLDS];
      }
    }

    @Override protected void chunkDone(long n){
      if(_computeGram)_gram.mul(_reg);
      for(int i = 0; i < _xy.length; ++i)
        _xy[i] *= _reg;
      if(_grad != null)
        for(int i = 0; i < _grad.length; ++i)
          _grad[i] *= _reg;
      _yy *= _reg;
      if(_validate && _glm.family == Family.binomial) {
        _newThresholds[0] = Arrays.copyOf(_newThresholds[0],_ti[0]);
        _newThresholds[1] = Arrays.copyOf(_newThresholds[1],_ti[1]);
        Arrays.sort(_newThresholds[0]);
        Arrays.sort(_newThresholds[1]);
      }
    }

    @Override
    public void reduce(GLMIterationTask git){
      if(_jobKey == null || Job.isRunning(_jobKey)) {
        Utils.add(_xy, git._xy);
        if (_computeGram) _gram.add(git._gram);
        _yy += git._yy;
        _nobs += git._nobs;
        if (_validate) _val.add(git._val);
        if (_computeGradient) Utils.add(_grad, git._grad);
        if(_validate && _glm.family == Family.binomial) {
          _newThresholds[0] = Utils.join(_newThresholds[0], git._newThresholds[0]);
          _newThresholds[1] = Utils.join(_newThresholds[1], git._newThresholds[1]);
          if (_newThresholds[0].length >= 2 * N_THRESHOLDS) {
            for (int i = 0; i < 2 * N_THRESHOLDS; i += 2)
              _newThresholds[0][i >> 1] = _newThresholds[0][i];
          }
          if (_newThresholds[0].length > N_THRESHOLDS)
            _newThresholds[0] = Arrays.copyOf(_newThresholds[0], N_THRESHOLDS);
          if (_newThresholds[1].length >= 2 * N_THRESHOLDS) {
            for (int i = 0; i < 2 * N_THRESHOLDS; i += 2)
              _newThresholds[1][i >> 1] = _newThresholds[1][i];
          }
          if (_newThresholds[1].length > N_THRESHOLDS)
            _newThresholds[1] = Arrays.copyOf(_newThresholds[1], N_THRESHOLDS);
        }
        super.reduce(git);
      }
    }

    @Override public void postGlobal(){
      if(_val != null){
        _val.computeAIC();
        _val.computeAUC();
      }

    }
    public double [] gradient(double alpha, double lambda){
      final double [] res = _grad.clone();
      if(_beta != null)
        for(int i = 0; i < res.length-1; ++i) res[i] += (1-alpha)*lambda*_beta[i];
      return res;
    }
  }
}
