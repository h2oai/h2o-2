package hex.glm;


import hex.glm.GLMParams.CaseMode;
import hex.glm.GLMParams.Family;
import hex.gram.Gram;

import java.util.Arrays;

import water.MRTask2;
import water.MemoryManager;
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
public abstract class GLMTask<T extends GLMTask<T>> extends MRTask2<T>{
  protected final boolean _standardize;
  final int _offset;
  final int _step;
  final boolean _complement;

  int _nums = -1;
  int _cats = -1;
  // offsets of categorcial variables
  // each categorical with n levels is virtually expanded into a binary vector of length n -1.
  // _catOffsets basically hold a sum of cardinlities of all preceding categorical variables
  // to be able to address correct elements in the beta vector (which must be fully expanded).
  protected int [] _catOffsets;
  // data-regularization params
  double [] _normSub; // means to be subtracted
  double [] _normMul; // 1/sigma to multiply each numeric param with
  double    _ymu = Double.NaN; // mean of the response

  protected final double [] _beta; // beta vector from previous computation or null if this is the first iteration

  final GLMParams _glm;
  // CaseMode used to turn non-binary response into a binary one (0,1), used only by binomial family
  protected final CaseMode _caseMode;
  // CaseVaue to be used together with the Case predicate to turn the response into binary variable
  protected final double _caseVal;

  // size of the expanded vector of parameters, equal to number of columns + 1(intercept) + sum of categorical levels.
  final protected int fullN(){
    return _nums + _catOffsets[_cats] + 1; // + 1 is for intercept!
  }
  // size of the top-left strictly diagonal region of the gram matrix, currently just the size of the largest categorical
  final protected int diagN(){if(_catOffsets.length < 2)return 0; return _catOffsets[1];}

  public GLMTask(GLMParams glm, double [] beta, boolean standardize, CaseMode cm, double cv) {this(glm, beta, standardize, cm, cv, 1,0,false);}
  public GLMTask(GLMParams glm, double [] beta, boolean standardize, CaseMode cm, double cv, int step, int offset, boolean complement) {
    _standardize = standardize; _caseMode = cm; _caseVal = cv; _beta = beta;
    _glm = glm;
    _step = step;
    _offset = offset;
    _complement = complement;
  }
  protected GLMTask(GLMTask gt, double [] beta){
    _standardize = gt._standardize;
    _glm = gt._glm;
    _caseMode = gt._caseMode;
    _caseVal = gt._caseVal;
    _beta = beta;
    _ymu = gt._ymu;
    _nums = gt._nums;
    _cats = gt._cats;
    _catOffsets = gt._catOffsets;
    _normMul = gt._normMul;
    _normSub = gt._normSub;
    _step = gt._step;
   _offset = gt._offset;
   _complement = gt._complement;
  }

  /**
   * Method to process one row of the data for GLM functions.
   * Numeric and categorical values are passed separately, as is reponse.
   * Categoricals are passed as absolute indexes into the expanded beta vector, 0-levels are skipped
   * (so the number of passed categoricals will not be the same for every row).
   *
   * Categorical expansion/indexing:
   *   Categoricals are placed in the beginning of the beta vector.
   *   Each cat variable with n levels is expanded into n-1 independent binary variables.
   *   Indexes in cats[] will point to the appropriate coefficient in the beta vector, so e.g.
   *   assume we have 2 categorical columns both with values A,B,C, then the following rows will have following indexes:
   *      A,A - ncats = 0, we do not pass any categorical here
   *      A,B - ncats = 1, indexes = [2]
   *      B,B - ncats = 2, indexes = [0,2]
   *      and so on
   *
   * @param nums     - numeric values of this row
   * @param ncats    - number of passed (non-zero) categoricals
   * @param cats     - indexes of categoricals into the expanded beta-vector.
   * @param response - numeric value for the response
   */
  protected abstract void processRow(double [] nums, int ncats, int [] cats, double response);

  /**
   * Reorder the frame's columns so that numeric columns come first followed by categoricals ordered by cardinality in decreasing order and
   * the response is the last.
   * @param fr
   * @return
   */
  public static Frame adaptFrame(Frame fr){
    final Vec [] vecs = fr.vecs();
    final int n = vecs.length-1; // -1 for response
    int [] nums = MemoryManager.malloc4(n);
    int [] cats = MemoryManager.malloc4(n);
    int nnums = 0, ncats = 0;
    for(int i = 0; i < n; ++i){
      if(vecs[i].isEnum())
        cats[ncats++] = i;
      else
        nums[nnums++] = i;
    }
    // sort the cats in the decreasing order according to their size
    for(int i = 0; i < ncats; ++i)
      for(int j = i+1; j < ncats; ++j)
        if(vecs[cats[i]].domain().length < vecs[cats[j]].domain().length){
          int x = cats[i];
          cats[i] = cats[j];
          cats[j] = x;
        }
    Vec [] vecs2 = vecs.clone();
    String [] names = fr._names.clone();
    for(int i = 0; i < ncats; ++i){
      vecs2[i] = vecs[cats[i]];
      names[i] = fr._names[cats[i]];
    }
    for(int i = 0; i < nnums; ++i){
      vecs2[i+ncats]  = vecs [nums[i]];
      names[i+ncats] = fr._names[nums[i]];
    }
    return new Frame(names,vecs2);
  }

  // TODO change this to extract mean, sigma, nobs for the actual dataset
  // (NAs being skipped) as I did for the response variable
  // and move it from dfork!
  @Override public T dfork(Frame fr){
    if(_cats == -1 && _nums == -1 ){
      assert _normMul == null;
      assert _normSub == null;
      int i = 0;
      final Vec [] vecs = fr.vecs();
      final int n = vecs.length-1; // -1 for response
      while(i < n && vecs[i].isEnum())++i;
      _cats = i;
      while(i < n && !vecs[i].isEnum())++i;
      _nums = i-_cats;
      if(i != n)
        throw new RuntimeException("Incorrect format of the input frame. Frame is asusmed to be ordered so that categorical columns come before numerics.");
      _normSub = MemoryManager.malloc8d(_nums);
      _normMul = MemoryManager.malloc8d(_nums); Arrays.fill(_normMul, 1);
      if(_standardize) for(i = 0; i < _nums; ++i){
        _normSub[i] = vecs[i+_cats].mean();
        _normMul[i] = 1.0/vecs[i+_cats].sigma();
      }
      _catOffsets = MemoryManager.malloc4(_cats+1);
      int len = _catOffsets[0] = 0;
      for(i = 0; i < _cats; ++i)
        _catOffsets[i+1] = (len += vecs[i].domain().length - 1);
      // get mean of response...
      if(Double.isNaN(_ymu))_ymu = vecs[vecs.length-1].mean();
    }
    return super.dfork(fr);
  }

  /**
   * Extracts the values, applies regularization to numerics, adds appropriate offsets to categoricals,
   * and adapts response according to the CaseMode/CaseValue if set.
   */
  @Override public void map(Chunk [] chunks){
    final int nrows = chunks[0]._len;
    double [] nums = MemoryManager.malloc8d(_nums);
    int    [] cats = MemoryManager.malloc4(_cats);
    final int step = _complement?_step:1;
    final int start = _complement?_offset:0;

    OUTER:
    for(int r = start; r < nrows; r += step){
      if(_step > step && (r % _step) == _offset)continue;
      for(Chunk c:chunks)if(c.isNA0(r))continue OUTER; // skip rows with NAs!
      int i = 0, ncats = 0;
      for(; i < _cats; ++i){
        int c = (int)chunks[i].at80(r);
        if(c != 0)cats[ncats++] = c + _catOffsets[i] - 1;
      }
      for(;i < chunks.length-1;++i)
        nums[i-_cats] = (chunks[i].at0(r) - _normSub[i-_cats])*_normMul[i-_cats];
      processRow(nums, ncats, cats, chunks[chunks.length-1].at0(r));
    }
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
    public YMUTask(GLMParams glm, boolean standardize, CaseMode cm, double cv, long nobs) {
      super(glm,null,standardize, cm, cv);
      _reg = 1.0/nobs;
    }
    @Override protected void processRow(double[] nums, int ncats, int[] cats, double response) {
      _ymu += response;
      ++_nobs;
    }
    @Override public void map(Chunk [] chunks){
      super.map(chunks);
      _ymu *= _reg;
    }
    public double ymu(){return _ymu * (_nobs/_reg);}
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

    public LMAXTask(GLMParams glm, boolean standardize, CaseMode cm, double cv, double ymu, int P) {
      super(glm,null,standardize, cm, cv);
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

    public GLMIterationTask(GLMParams glm, double [] beta, boolean standardize, double reg, CaseMode caseMode, double caseVal, int step, int offset, boolean complement) {
      super(glm, beta, standardize, caseMode, caseVal, step, offset, complement);
      _iter = 0;
      _reg = reg;
    }

    public GLMIterationTask(GLMIterationTask git, double [] beta) {
      super(git,beta);
      _iter = git._iter+1;
      _reg = git._reg;
    }

    @Override public final void processRow(double [] nums, int ncats, int [] cats, double y){
      assert ((_glm.family != Family.gamma) || y > 0) : "illegal response column, y must be > 0  for family=Gamma.";
      if( _caseMode != CaseMode.none ) y = (_caseMode.isCase(y, _caseVal)) ? 1 : 0;
      double w = 1;
      double eta = 0, mu = 0, var = 1;
      if( _glm.family != Family.gaussian) {
        if( _beta == null ) {
          mu = _glm.mustart(y);
          eta = _glm.link(mu);
        } else {
          eta = computeEta(ncats, cats,nums);
          mu = _glm.linkInv(eta);
        }
        if(Double.isNaN(mu)){
          System.out.println("got NaN mu from: beta=" + Arrays.toString(_beta) + ", row = " + Arrays.toString(nums) + ", cats = " + Arrays.toString(cats) + ", ncats = " + ncats);
        }
        _val.add(y, mu);

        var = Math.max(1e-5, _glm.variance(mu)); // avoid numerical problems with 0 variance
        if( _glm.family == Family.binomial || _glm.family == Family.poisson ) {
          w = var;
          y = eta + (y - mu) / var;
        } else {
          double dp = _glm.linkInvDeriv(eta);
          w = dp * dp / var;
          y = eta + (y - mu) / dp;
        }
      }
      assert w >= 0 : "invalid weight " + w;
      _yy += 0.5 * w * y * y;
      double wy = w * y;
      for(int i = 0; i < ncats; ++i)_xy[cats[i]] += wy;
      final int numStart = _catOffsets[_cats];
      for(int i = 0; i < nums.length; ++i)_xy[numStart+i] += wy*nums[i];
      _xy[numStart + _nums] += wy;
      _gram.addRow(nums, ncats, cats, w);
    }
    @Override public void map(Chunk [] chunks){
      _gram = new Gram(fullN(), diagN(), _nums, _cats);
      _xy = MemoryManager.malloc8d(fullN());
      int rank = 0;
      if(_beta != null)for(double d:_beta)if(d != 0)++rank;
      _val = new GLMValidation(null,_ymu, _glm,rank);
      super.map(chunks);
      _gram.mul(_reg);
      if(_val.nobs > 0)_val.avg_err /= _val.nobs;
      for(int i = 0; i < _xy.length; ++i)
        _xy[i] *= _reg;
    }
    @Override
    public void reduce(GLMIterationTask git){
      _gram.add(git._gram);
      Utils.add(_xy, git._xy);
      _yy += git._yy;
      _val.add(git._val);
    }
  }

}
