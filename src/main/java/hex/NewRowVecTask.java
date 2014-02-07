package hex;

//import hex.DGLM.GramMatrixFactory;
//import hex.DGLM.GramMatrixFunc;
import hex.RowVecTask.Sampling;

import java.util.*;

import water.*;
import water.Job.ChunkProgressJob;
import water.Job.JobCancelledException;
import water.ValueArray.Column;
import water.fvec.Frame;
import water.fvec.Vec;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

public class NewRowVecTask<T extends Iced> extends MRTask {
  // These fields are all *shared* *read-only*.
  final RowFunc<T> _func;
  final DataFrame  _data;
  final Job _job;
  //-----------------------------------------------------
  T _result; // result of the computation is stored here

  public static abstract class RowFunc<T extends Iced> extends Iced  {
    protected boolean _expandCats = true;
    protected boolean _standardize = true;
    public static final BiMap<Class<? extends RowFunc>, Integer> TYPE;
    static {
        TYPE = HashBiMap.create();
        TYPE.put(null, -1);
        //TYPE.put(GramMatrixFunc.class, 0);
    }
    public abstract T newResult();
    public abstract void processRow(T res, double [] x, int [] indexes);

    public long memReq(){return 0;}

    public abstract T reduce(T x, T y);

    public T apply(Job j, DataFrame data) throws JobCancelledException{
      NewRowVecTask<T> tsk = new NewRowVecTask<T>(j,this, data);
      tsk.invoke(data._ary._key);
      if(j != null && !Job.isRunning(j.self()))throw new JobCancelledException();
      return tsk._result;
    }

    public T result(T res){
      return res;
    }

  }

  /**
   * Struct to keep info about our data. Contains column ids and info about data preprocessing and expansion of vategoricals.
   *
   * The last element is ALWAYS treated as response variable and is never normalized nor expanded (if categorical).
   *
   * @author tomasnykodym
   *
   */
  public static final class DataFrame extends Iced {
    final ValueArray _ary;
    final private Sampling _s;
    final int[] _modelDataMap;
    final int[] _colCatMap;
    final double [] _normSub;
    final double [] _normMul;
    final int _dense;
    final int _response;
    final long _nobs;
    public final boolean _standardized;

    public static DataFrame makePCAData(ValueArray ary, int [] colIds, boolean standardize){
      for(int i = 0; i < colIds.length-1; ++i){
        int c = colIds[i];
        if(ary._cols[c]._domain != null)
          throw new RuntimeException("categorical columns can not be used with PCA!");
      }
      return new DataFrame(ary,colIds,standardize);
    }
    private DataFrame(ValueArray ary, int [] colIds, boolean standardize){
      _ary = ary;
      _s = null;
      _response = -1;
      _colCatMap = new int[colIds.length+1];
      for(int i = 0; i < _colCatMap.length;++i)
        _colCatMap[i] = i;
      _modelDataMap = colIds;
      _dense = colIds.length;
      _normSub = new double[colIds.length];
      _normMul = new double[colIds.length];
      Arrays.fill(_normMul, 1);
      if(standardize) for(int i = 0; i < colIds.length; ++i){
        Column col = ary._cols[colIds[i]];
        if(col._domain == null){
          int ii = _colCatMap[i];
          _normSub[ii] = col._mean;
          _normMul[ii] = 1.0/col._sigma;
        }
      }
      _standardized = standardize;
      _nobs = ary._numrows;
    }

    public DataFrame(ValueArray ary, int [] colIds, Sampling s, boolean standardize, boolean expandCat){
      ArrayList<Integer> numeric = new ArrayList<Integer>();
      ArrayList<Integer> categorical = new ArrayList<Integer>();
      for(int i = 0; i < colIds.length-1; ++i){
        int c = colIds[i];
        if(ary._cols[c]._domain != null)
          categorical.add(c);
        else
          numeric.add(c);
      }
      _dense = numeric.size()+1; // numeric + 1 for response/intercept
      final Column [] cols = ary._cols;
      Collections.sort(categorical, new Comparator<Integer>() {
        @Override public int compare(Integer o1, Integer o2) {
          return cols[o2]._domain.length-cols[o1]._domain.length;
        }
      });

      int idx = 0;
      for(int i:categorical)colIds[idx++] = i;
      for(int i:numeric)colIds[idx++] = i;
      colIds[idx] = colIds[colIds.length-1];
      _response = idx;
      _ary = ary;
      _modelDataMap = colIds;
      _s = s;
      _colCatMap = new int[colIds.length+1];
      int len=0;
      for( int i=0; i<colIds.length; i++ ) {
        _colCatMap[i] = len;
        if(i == _response){
        ++len;
        } else {
          ValueArray.Column C = ary._cols[colIds[i]];
          len += ( expandCat && C._domain != null )?C._domain.length-1:1;
        }
      }
      _colCatMap[colIds.length] = len;
      _normSub = new double[len];
      _normMul = new double[len];
      Arrays.fill(_normMul, 1);
      boolean standardized = false;
      if(standardize )for(int i = 0; i < colIds.length; ++i){
        if(i == _response)continue;
        standardized = true;
        Column col = ary._cols[colIds[i]];
        if(col._domain == null){
          int ii = _colCatMap[i];
          _normSub[ii] = col._mean;
          _normMul[ii] = 1.0/col._sigma;
        }
      }
      _standardized = standardized;
      _nobs = (s != null)?(long)(ary._numrows*s.ratio()):ary._numrows;
    }
    public int largestCatSz(){
      return _colCatMap[1] - _colCatMap[0];
    }
    public Sampling getSampling() {return (_s != null)?_s.clone():null;}
    public Sampling getSamplingComplement() {return (_s != null)?_s.complement():null;}
    public int expandedSz() {return _colCatMap[_colCatMap.length-1];}
    public int compactSz(){return _modelDataMap.length;}
    public int dense(){return _dense;}
    public int [] betaColMap(){
      int [] res = _modelDataMap.clone();
      System.arraycopy(_modelDataMap, 0, res, _modelDataMap.length-_dense, _dense);
      System.arraycopy(_modelDataMap, _dense, res, 0, _modelDataMap.length-_dense);
      return res;
    }
    public double [] denormalizeBeta(double [] beta) {
      if(!_standardized)
        return beta;
      double [] newBeta = beta.clone();
      double norm = 0.0;        // Reverse any normalization on the intercept
      for( int i=0; i<newBeta.length-1; i++ ) {
        double b = newBeta[i]*_normMul[i];
        norm += b*_normSub[i]; // Also accumulate the intercept adjustment
        newBeta[i] = b;
      }
      newBeta[newBeta.length-1] -= norm;
      return newBeta;
    }
    // Returns frame with only columns specified in model
    public Frame modelAsFrame() {
      Frame temp = _ary.asFrame();
      Vec[] vecs = new Vec[_modelDataMap.length];
      String[] names = new String[_modelDataMap.length];
      for(int i = 0; i < _modelDataMap.length; i++) {
        vecs[i] = temp.vecs()[_modelDataMap[i]];
        names[i] = temp._names[_modelDataMap[i]];
      }
      return new Frame(names, vecs);
    }
  }

  public NewRowVecTask(Job job,RowFunc<T> f, DataFrame data){
    _job = job;
    _func = f;
    _data = data;
  }

  @Override
  public long memOverheadPerChunk(){
    return _func.memReq();
  }

  @Override
  public void map(Key key) {
    if(_job != null && !Job.isRunning(_job.self()))return;
    T result = _func.newResult();
    Sampling s = _data.getSampling();
    AutoBuffer bits = _data._ary.getChunk(key);
    final int rows = bits.remaining()/_data._ary._rowsize;

    // Array to hold a model's worth of training data from the dataset, plus
    // the response column last.
    final int n = _data._modelDataMap.length;
    double [] x = new double[n];
    // Mapping from the dense 'x' model data to the categorically expanded
    // columns.  This is basically _colCatMap, except that categorical columns
    // have the expanded column number in the index.
    int [] indexes = new int [n];


    // Build a dense row of doubles for processing from the dataset.
    // Do all the mappings from model columns to dataset columns.
ROW:
    for( int r=0; r<rows; r++ ) {
      if( s != null && s.skip(r) ) continue;
      // For all the model's columns
      for( int i = 0; i<n; i++ ) {
        int dataColIdx = _data._modelDataMap[i]; // Column in the dataset
        // Ignore missing data
        if( _data._ary.isNA(bits,r,dataColIdx) ) continue ROW;
        // Get the dataset data - not yet done any categorical expansion

        // Is it categorical?
        int idx = _data._colCatMap[i];
        if(idx+1 == _data._colCatMap[i+1] ) { // No room for categories ==> numerical
          double d = _data._ary.datad(bits,r,dataColIdx);
          x[i] = (_data._normSub != null)?(d - _data._normSub[idx]) * _data._normMul[idx]:d;
          indexes[i] = idx;
        } else {                // Else categorical
          long l = _data._ary.data(bits, r,dataColIdx);

          // Size of category (number of factors/enum elements) is also the
          // number of expanded columns.  Make sure that the enum/factor 'd' is
          // in range.
          if(l == 0){
            x[i] = 0; // we skip the first categorical (so that the sum of the categorical columns is not always 1!)
          } else {
            x[i] = 1.0;
            --l;
          }
          assert 0 <= l && (int)l < _data._colCatMap[i+1]-idx;
          indexes[i] = idx+(int)l; // Which expanded column to use
        }
      }
      // At this point 'x' contains the normalized compacted feature data from
      // the training dataset, and 'indexes' contains enum/categorical expanded
      // column number.
      _func.processRow(result, x, indexes);
    }
    _result = _func.result(result);
    if(_job instanceof ChunkProgressJob)
      ((ChunkProgressJob)_job).updateProgress(1);
  }

  @Override public void reduce(DRemoteTask drt) {
    if(_job != null && !Job.isRunning(_job.self())) return;
    NewRowVecTask<T> rv = (NewRowVecTask<T>)drt;
    assert _result != rv._result;
    _result = (_result != null)?_func.reduce(_result, rv._result):rv._result;
    rv._result = null;
  }

  /** Dial back default logging; these passes are common and rarely cause issues. */
  @Override public boolean logVerbose() { return false; }
}
