package hex;

import hex.glm.GLMParams.CaseMode;

import java.util.Arrays;

import water.*;
import water.H2O.H2OCountedCompleter;
import water.Job.JobCancelledException;
import water.fvec.*;

public abstract class FrameTask<T extends FrameTask<T>> extends MRTask2<T>{
  final protected DataInfo _dinfo;
  final Job _job;
  double    _ymu = Double.NaN; // mean of the response
  // size of the expanded vector of parameters

  public FrameTask(Job job, DataInfo dinfo) {
    this(job,dinfo,null);
  }
  public FrameTask(Job job, DataInfo dinfo, H2OCountedCompleter cmp) {
    super(cmp);
    _job = job;
    _dinfo = dinfo;
  }
  protected FrameTask(FrameTask ft){
    _dinfo = ft._dinfo;
    _job = ft._job;
  }
  public double [] normMul(){return _dinfo._normMul;}
  public double [] normSub(){return _dinfo._normSub;}

  /**
   * Method to process one row of the data for GLM functions.
   * Numeric and categorical values are passed separately, as is response.
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
  protected void processRow(double [] nums, int ncats, int [] cats, double response){throw new RuntimeException("should've been overriden!");}
  protected void processRow(double [] nums, int ncats, int [] cats){throw new RuntimeException("should've been overriden!");}
  protected void processRow(double [] nums, int ncats, int [] cats, double response, NewChunk [] outputs){throw new RuntimeException("should've been overriden!");}
  protected void processRow(double [] nums, int ncats, int [] cats, NewChunk [] outputs){throw new RuntimeException("should've been overriden!");}


  public static class DataInfo extends Iced {
    public final Frame _adaptedFrame;
    public final boolean _hasResponse;
    public final boolean _standardize;
    public final int _nums;
    public final int _cats;
    public final int [] _catOffsets;
    public final double [] _normMul;
    public final double [] _normSub;
    public final int _foldId;
    public final int _nfolds;

    private DataInfo(DataInfo dinfo,int foldId, int nfolds){
      _standardize = dinfo._standardize;
      _hasResponse = dinfo._hasResponse;
      _nums = dinfo._nums;
      _cats = dinfo._cats;
      _adaptedFrame = dinfo._adaptedFrame;
      _catOffsets = dinfo._catOffsets;
      _normMul = dinfo._normMul;
      _normSub = dinfo._normSub;
      _foldId = foldId;
      _nfolds = nfolds;
    }
    public DataInfo(Frame fr, boolean hasResponse, double [] normSub, double [] normMul){
      this(fr,hasResponse,normSub != null && normMul != null);
      assert (normSub == null) == (normMul == null);
      if(normSub != null && normMul != null){
        System.arraycopy(normSub, 0, _normSub, 0, normSub.length);
        System.arraycopy(normMul, 0, _normMul, 0, normMul.length);
      }
    }
    public DataInfo(Frame fr, boolean hasResponse, boolean standardize){
      _nfolds = _foldId = 0;
      _standardize = standardize;
      _hasResponse = hasResponse;
      final Vec [] vecs = fr.vecs();
      final int n = vecs.length-(hasResponse?1:0); // -1 for response
      int [] nums = MemoryManager.malloc4(n);
      int [] cats = MemoryManager.malloc4(n);
      int nnums = 0, ncats = 0;
      for(int i = 0; i < n; ++i){
        if(vecs[i].isEnum())
          cats[ncats++] = i;
        else
          nums[nnums++] = i;
      }
      _nums = nnums;
      _cats = ncats;
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
      _catOffsets = MemoryManager.malloc4(ncats+1);
      int len = _catOffsets[0] = 0;

      for(int i = 0; i < ncats; ++i){
        Vec v = (vecs2[i] = vecs[cats[i]]);
        names[i] = fr._names[cats[i]];
        _catOffsets[i+1] = (len += v.domain().length - 1);
      }
      if(standardize){
        _normSub = MemoryManager.malloc8d(nnums);
        _normMul = MemoryManager.malloc8d(nnums); Arrays.fill(_normMul, 1);
      } else
        _normSub = _normMul = null;
      for(int i = 0; i < nnums; ++i){
        Vec v = (vecs2[i+ncats]  = vecs [nums[i]]);
        names[i+ncats] = fr._names[nums[i]];
        if(standardize){
          _normSub[i] = v.mean();
          _normMul[i] = 1.0/v.sigma();
        }
      }
      _adaptedFrame = new Frame(names,vecs2);
    }
    public DataInfo getFold(int foldId, int nfolds){
      return new DataInfo(this, foldId, nfolds);
    }
    public final int fullN(){return _nums + _catOffsets[_cats];}
    public final int largestCat(){return _cats > 0?_catOffsets[1]:0;}
    public final int numStart(){return _catOffsets[_cats];}
  }

  @Override
  public T dfork(Frame fr){
    assert fr == _dinfo._adaptedFrame;
    return super.dfork(fr);
  }

  /**
   * Override this to initialize at the beginning of chunk processing.
   */
  protected void chunkInit(){}
  /**
   * Override this to do post-chunk processing work.
   */
  protected void chunkDone(){}

  protected CaseMode _caseMode;
  protected double _caseVal;

  private double response(double d){
    if(_caseMode == CaseMode.none)return d;
    return _caseMode.isCase(d, _caseVal)?1:0;
  }
  /**
   * Extracts the values, applies regularization to numerics, adds appropriate offsets to categoricals,
   * and adapts response according to the CaseMode/CaseValue if set.
   */
  @Override public final void map(Chunk [] chunks, NewChunk [] outputs){
    if(_job != null && _job.cancelled())throw new JobCancelledException();
    chunkInit();
    final int nrows = chunks[0]._len;
    double [] nums = MemoryManager.malloc8d(_dinfo._nums);
    int    [] cats = MemoryManager.malloc4(_dinfo._cats);

    OUTER:
    for(int r = 0; r < nrows; ++r){
      if(_dinfo._nfolds > 0 && (r % _dinfo._nfolds) == _dinfo._foldId)continue;
      for(Chunk c:chunks)if(c.isNA0(r))continue OUTER; // skip rows with NAs!
      int i = 0, ncats = 0;
      for(; i < _dinfo._cats; ++i){
        int c = (int)chunks[i].at80(r);
        if(c != 0)cats[ncats++] = c + _dinfo._catOffsets[i] - 1;
      }
      final int n = _dinfo._hasResponse?chunks.length-1:chunks.length;
      for(;i < n;++i){
        double d = chunks[i].at0(r);
        if(_dinfo._normMul != null) d = (d - _dinfo._normSub[i-_dinfo._cats])*_dinfo._normMul[i-_dinfo._cats];
        nums[i-_dinfo._cats] = d;
      }
      if(outputs != null && outputs.length > 0){
        if(!_dinfo._hasResponse) processRow(nums, ncats, cats,outputs);
        else processRow(nums, ncats, cats,chunks[chunks.length-1].at0(r),outputs);
      } else {
        if(!_dinfo._hasResponse) processRow(nums, ncats, cats);
        else processRow(nums, ncats, cats,chunks[chunks.length-1].at0(r));
      }
    }
    chunkDone();
  }
}
