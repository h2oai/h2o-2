package hex;

import java.util.Arrays;

import water.*;
import water.fvec.*;

public abstract class FrameTask<T extends FrameTask<T>> extends MRTask2<T>{
  protected final boolean _standardize;
  public final boolean standardize(){return _standardize;}
  final int _offset;
  final int _step;
  final boolean _complement;
  final boolean _hasResponse;
  final Job _job;

  protected int _nums = -1;
  public final int nums(){return _nums;}
  protected int _cats = -1;
  public final int cats(){return _cats;}
  // offsets of categorcial variables
  // each categorical with n levels is virtually expanded into a binary vector of length n -1.
  // _catOffsets basically hold a sum of cardinlities of all preceding categorical variables
  // to be able to address correct elements in the beta vector (which must be fully expanded).
  protected int [] _catOffsets;
  public final int [] catOffsets(){return _catOffsets;}
  // data-regularization params
  double [] _normSub; // means to be subtracted
  public final double [] normSub(){return _normSub;}
  double [] _normMul; // 1/sigma to multiply each numeric param with
  public final double [] normMul(){return _normMul;}
  double    _ymu = Double.NaN; // mean of the response
  // size of the expanded vector of parameters
  final protected int fullN(){return _nums + _catOffsets[_cats];}
  final protected int largestCat(){return _cats > 0?_catOffsets[1]:0;}

  public FrameTask(Job job, boolean standardize, boolean hasResponse) {this(job,standardize,hasResponse, 1,0,false);}
  public FrameTask(Job job, boolean standardize, boolean hasResponse, int step, int offset, boolean complement) {
    _hasResponse = hasResponse;
    _standardize = standardize;
    _job = job;
    _step = step;
    _offset = offset;
    _complement = complement;
  }
  protected FrameTask(FrameTask ft){
    _standardize = ft._standardize;
    _hasResponse = ft._hasResponse;
    _nums = ft._nums;
    _cats = ft._cats;
    _catOffsets = ft._catOffsets;
    _normMul = ft._normMul;
    _normSub = ft._normSub;
    _step = ft._step;
   _offset = ft._offset;
   _complement = ft._complement;
   _job = ft._job;
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
  protected void processRow(double [] nums, int ncats, int [] cats, double response){throw new RuntimeException("should've been overriden!");}
  protected void processRow(double [] nums, int ncats, int [] cats){throw new RuntimeException("should've been overriden!");}

  /**
   * Reorder the frame's columns so that numeric columns come first followed by categoricals ordered by cardinality in decreasing order and
   * the response is the last.
   * @param fr
   * @return
   */
  public Frame adaptFrame(Frame fr){
    final Vec [] vecs = fr.vecs();
    final int n = vecs.length-(_hasResponse?1:0); // -1 for response
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
  public T doIt(Frame fr){return dfork2(fr).getResult();}
  public T dfork2(Frame fr){
    if(_cats == -1 && _nums == -1 ){
      fr = adaptFrame(fr);
      assert _normMul == null;
      assert _normSub == null;
      int i = 0;
      final Vec [] vecs = fr.vecs();
      final int n = _hasResponse?vecs.length-1:vecs.length; // -1 for response
      while(i < n && vecs[i].isEnum())++i;
      _cats = i;
      while(i < n && !vecs[i].isEnum())++i;
      _nums = i-_cats;
      if(i != n)
        throw new RuntimeException("Incorrect format of the input frame. Frame is assumed to be ordered so that categorical columns come before numerics.");
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
    return dfork(fr);
  }
  /**
   * Override this to initialize at the beginning of chunk processing.
   */
  protected void chunkInit(){}
  /**
   * Override this to do post-chunk processing work.
   */
  protected void chunkDone(){}
  /**
   * Extracts the values, applies regularization to numerics, adds appropriate offsets to categoricals,
   * and adapts response according to the CaseMode/CaseValue if set.
   */
  @Override public final void map(Chunk [] chunks){
    if(_job != null && _job.cancelled())throw new RuntimeException("Cancelled");
    chunkInit();
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
      final int n = _hasResponse?chunks.length-1:chunks.length;
      for(;i < n;++i)
        nums[i-_cats] = (chunks[i].at0(r) - _normSub[i-_cats])*_normMul[i-_cats];
      if(!_hasResponse) processRow(nums, ncats, cats);
      else processRow(nums, ncats, cats,chunks[chunks.length-1].at0(r));
    }
    chunkDone();
  }
}
