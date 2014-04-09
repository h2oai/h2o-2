package hex;

import water.H2O.H2OCountedCompleter;
import water.Iced;
import water.Job;
import water.Job.JobCancelledException;
import water.MRTask2;
import water.MemoryManager;
import water.fvec.Chunk;
import water.fvec.Frame;
import water.fvec.NewChunk;
import water.fvec.Vec;
import water.util.Log;
import water.util.Utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;

public abstract class FrameTask<T extends FrameTask<T>> extends MRTask2<T>{
  final protected DataInfo _dinfo;
  final Job _job;
  double    _ymu = Double.NaN; // mean of the response
  // size of the expanded vector of parameters

  protected float _useFraction = 1.0f;
  protected boolean _shuffle = false;

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
    _useFraction = ft._useFraction;
    _shuffle = ft._shuffle;
  }
  public final double [] normMul(){return _dinfo._normMul;}
  public final double [] normSub(){return _dinfo._normSub;}
  public final double [] normRespMul(){return _dinfo._normMul;}
  public final double [] normRespSub(){return _dinfo._normSub;}

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
   * @param gid      - global id of this row, in [0,_adaptedFrame.numRows())
   * @param nums     - numeric values of this row
   * @param ncats    - number of passed (non-zero) categoricals
   * @param cats     - indexes of categoricals into the expanded beta-vector.
   * @param response - numeric value for the response
   */
  protected void processRow(long gid, double [] nums, int ncats, int [] cats, double [] response){throw new RuntimeException("should've been overriden!");}
  protected void processRow(long gid, double [] nums, int ncats, int [] cats, double [] response, NewChunk [] outputs){throw new RuntimeException("should've been overriden!");}


  public static class DataInfo extends Iced {
    public Frame _adaptedFrame;
    public final int _responses; // number of responses
    public final boolean _standardize;
    public final boolean _standardize_response;
    public final int _nums;
    public final int _cats;
    public final int [] _catOffsets;
    public final double [] _normMul;
    public final double [] _normSub;
    public final double [] _normRespMul;
    public final double [] _normRespSub;
    public final int _foldId;
    public final int _nfolds;

    private DataInfo(DataInfo dinfo, int foldId, int nfolds){
      _standardize = dinfo._standardize;
      _standardize_response = dinfo._standardize_response;
      _responses = dinfo._responses;
      _nums = dinfo._nums;
      _cats = dinfo._cats;
      _adaptedFrame = dinfo._adaptedFrame;
      _catOffsets = dinfo._catOffsets;
      _normMul = dinfo._normMul;
      _normSub = dinfo._normSub;
      _normRespMul = dinfo._normRespMul;
      _normRespSub = dinfo._normRespSub;
      _foldId = foldId;
      _nfolds = nfolds;
    }
    public DataInfo(Frame fr, int hasResponses, double [] normSub, double [] normMul) {
      this(fr,hasResponses,normSub,normMul,null,null);
    }
    public DataInfo(Frame fr, int hasResponses, double [] normSub, double [] normMul, double [] normRespSub, double [] normRespMul){
      this(fr,hasResponses,normSub != null && normMul != null, normRespSub != null && normRespMul != null);
      assert (normSub == null) == (normMul == null);
      assert (normRespSub == null) == (normRespMul == null);
      if(normSub != null && normMul != null){
        System.arraycopy(normSub, 0, _normSub, 0, normSub.length);
        System.arraycopy(normMul, 0, _normMul, 0, normMul.length);
      }
      if(normRespSub != null && normRespMul != null){
        System.arraycopy(normRespSub, 0, _normRespSub, 0, normRespSub.length);
        System.arraycopy(normRespMul, 0, _normRespMul, 0, normRespMul.length);
      }
    }

    /**
     * Prepare a Frame (with a single response) to be processed by the FrameTask
     * 1) Place response at the end
     * 2) (Optionally) Remove columns with constant values or with >20% NaNs
     * 3) Possibly turn integer categoricals into enums
     *
     * @param source A frame to be expanded and sanity checked
     * @param response (should be part of source)
     * @param toEnum Whether or not to turn categoricals into enums
     * @param dropConstantCols Whether or not to drop constant columns
     * @return Frame to be used by FrameTask
     */
    public static Frame prepareFrame(Frame source, Vec response, int[] ignored_cols, boolean toEnum, boolean dropConstantCols, boolean dropNACols) {
      Frame fr = new Frame(source._names.clone(), source.vecs().clone());
      if (ignored_cols != null) fr.remove(ignored_cols);
      final Vec[] vecs =  fr.vecs();
      // put response to the end (if not already)
      for(int i = 0; i < vecs.length-1; ++i) {
        if(vecs[i] == response){
          final String n = fr._names[i];
          if (toEnum && !vecs[i].isEnum()) fr.add(n, fr.remove(i).toEnum()); //convert int classes to enums
          else fr.add(n, fr.remove(i));
          break;
        }
      }
      // special case for when response was at the end already
      if (toEnum && !response.isEnum() && vecs[vecs.length-1] == response) {
        final String n = fr._names[vecs.length-1];
        fr.add(n, fr.remove(vecs.length-1).toEnum());
      }

      ArrayList<Integer> constantOrNAs = new ArrayList<Integer>();
      {
        ArrayList<Integer> constantCols = new ArrayList<Integer>();
        ArrayList<Integer> NACols = new ArrayList<Integer>();
        for(int i = 0; i < vecs.length-1; ++i) {
          // remove constant cols and cols with too many NAs
          final boolean dropconstant = dropConstantCols && vecs[i].min() == vecs[i].max();
          final boolean droptoomanyNAs = dropNACols && vecs[i].naCnt() > vecs[i].length()*0.2;
          if(dropconstant) {
            constantCols.add(i);
          } else if (droptoomanyNAs) {
            NACols.add(i);
          }
        }
        constantOrNAs.addAll(constantCols);
        constantOrNAs.addAll(NACols);

        // Report what is dropped
        String msg = "";
        if (constantCols.size() > 0) msg += "Dropping constant column(s): ";
        for (int i : constantCols) msg += fr._names[i] + " ";
        if (NACols.size() > 0) msg += "Dropping column(s) with too many missing values: ";
        for (int i : NACols) msg += fr._names[i] + " (" + String.format("%.2f", vecs[i].naCnt() * 100. / vecs[i].length()) + "%) ";
        for (String s : msg.split("\n")) Log.info(s);
      }
      if(!constantOrNAs.isEmpty()){
        int [] cols = new int[constantOrNAs.size()];
        for(int i = 0; i < cols.length; ++i)
          cols[i] = constantOrNAs.get(i);
        fr.remove(cols);
      }
      return fr;
    }
    public static Frame prepareFrame(Frame source, Vec response, int[] ignored_cols, boolean toEnum, boolean dropConstantCols) {
      return prepareFrame(source, response, ignored_cols, toEnum, dropConstantCols, false);
    }
    public DataInfo(Frame fr, int nResponses, boolean standardize) {
      this(fr, nResponses, standardize, false);
    }
    public DataInfo(Frame fr, int nResponses, boolean standardize, boolean standardize_response){
      _nfolds = _foldId = 0;
      _standardize = standardize;
      _standardize_response = standardize_response;
      _responses = nResponses;
      final Vec [] vecs = fr.vecs();
      final int n = vecs.length-_responses;
      if (n < 1) throw new IllegalArgumentException("Training data must have at least one column.");
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
      } else _normSub = _normMul = null;
      for(int i = 0; i < nnums; ++i){
        Vec v = (vecs2[i+ncats] = vecs[nums[i]]);
        names[i+ncats] = fr._names[nums[i]];
        if(standardize){
          _normSub[i] = v.mean();
          _normMul[i] = v.sigma() != 0 ? 1.0/v.sigma() : 1.0;
        }
      }

      if(standardize_response){
        _normRespSub = MemoryManager.malloc8d(_responses);
        _normRespMul = MemoryManager.malloc8d(_responses); Arrays.fill(_normRespMul, 1);
      } else _normRespSub = _normRespMul = null;
      for(int i = 0; i < _responses; ++i){
        Vec v = (vecs2[nnums+ncats+i] = vecs[nnums+ncats+i]);
        if(standardize_response){
          _normRespSub[i] = v.mean();
          _normRespMul[i] = v.sigma() != 0 ? 1.0/v.sigma() : 1.0;
//          Log.info("normalization for response[" + i + ": mul " + _normRespMul[i] + ", sub " + _normRespSub[i]);
        }
      }
      _adaptedFrame = new Frame(names,vecs2);
      _adaptedFrame.reloadVecs();
    }
    public String toString(){
      return "";
    }
    public DataInfo getFold(int foldId, int nfolds){
      return new DataInfo(this, foldId, nfolds);
    }
    public final int fullN(){return _nums + _catOffsets[_cats];}
    public final int largestCat(){return _cats > 0?_catOffsets[1]:0;}
    public final int numStart(){return _catOffsets[_cats];}
    public final String [] coefNames(){
      int k = 0;
      final int n = fullN();
      String [] res = new String[n];
      final Vec [] vecs = _adaptedFrame.vecs();
      for(int i = 0; i < _cats; ++i)
        for(int j = 1; j < vecs[i]._domain.length; ++j)
          res[k++] = _adaptedFrame._names[i] + "." + vecs[i]._domain[j];
      final int nums = n-k;
      for(int i = 0; i < nums; ++i)
        res[k+i] = _adaptedFrame._names[_cats+i];
      return res;
    }
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


  /**
   * Extracts the values, applies regularization to numerics, adds appropriate offsets to categoricals,
   * and adapts response according to the CaseMode/CaseValue if set.
   */
  @Override public final void map(Chunk [] chunks, NewChunk [] outputs){
    long t0 = System.nanoTime(), t2 = 0;
    if(_job != null && _job.self() != null && !Job.isRunning(_job.self()))throw new JobCancelledException();
    final int nrows = chunks[0]._len;
    final long offset = chunks[0]._start;
    chunkInit();
    double [] nums = MemoryManager.malloc8d(_dinfo._nums);
    int    [] cats = MemoryManager.malloc4(_dinfo._cats);
    double [] response = MemoryManager.malloc8d(_dinfo._responses);
    int start = 0;
    int end = nrows;

    boolean contiguous = false;
    Random skip_rng = null; //random generator for skipping rows
    if (_useFraction < 1.0) {
      skip_rng = water.util.Utils.getDeterRNG(new Random().nextLong());
      if (contiguous) {
        final int howmany = (int)Math.ceil(_useFraction*nrows);
        if (howmany > 0) {
          start = skip_rng.nextInt(nrows - howmany);
          end = start + howmany;
        }
        assert(start < nrows);
        assert(end <= nrows);
      }
    }

    long[] shuf_map = null;
    if (_shuffle) {
      shuf_map = new long[end-start];
      for (int i=0;i<shuf_map.length;++i)
        shuf_map[i] = start + i;
      Utils.shuffleArray(shuf_map, new Random().nextLong());
    }
    long nanos = 0;
    long t1 = System.nanoTime();
    OUTER:
    for(int rr = start; rr < end; ++rr){
      final int r = shuf_map != null ? (int)shuf_map[rr-start] : rr;
      if ((_dinfo._nfolds > 0 && (r % _dinfo._nfolds) == _dinfo._foldId)
              || (skip_rng != null && skip_rng.nextFloat() > _useFraction))continue;
      for(Chunk c:chunks)if(c.isNA0(r))continue OUTER; // skip rows with NAs!
      int i = 0, ncats = 0;
      for(; i < _dinfo._cats; ++i){
        int c = (int)chunks[i].at80(r);
        if(c != 0)cats[ncats++] = c + _dinfo._catOffsets[i] - 1;
      }
      final int n = chunks.length-_dinfo._responses;
      for(;i < n;++i){
        double d = chunks[i].at0(r);
        if(_dinfo._normMul != null) d = (d - _dinfo._normSub[i-_dinfo._cats])*_dinfo._normMul[i-_dinfo._cats];
        nums[i-_dinfo._cats] = d;
      }
      for(i = 0; i < _dinfo._responses; ++i) {
        response[i] = chunks[chunks.length-_dinfo._responses + i].at0(r);
        if (_dinfo._normRespMul != null) response[i] = (response[i] - _dinfo._normRespSub[i])*_dinfo._normRespMul[i];
      }
      if(outputs != null && outputs.length > 0)
        processRow(offset+r, nums, ncats, cats, response, outputs);
      else
        processRow(offset+r, nums, ncats, cats, response);
      t2 = System.nanoTime();
      nanos += (t2-t1);
      t1 = t2;
    }
    chunkDone();
    System.out.println("FrameTask.map done, processRow took " + nanos + "ns, while while map took " + (t2 - t0) + "ns and compute2 took " + (t2-_t0)+"ns");
    System.out.println();
  }
}
