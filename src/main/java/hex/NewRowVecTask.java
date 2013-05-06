package hex;

//import hex.DGLM.GramMatrixFactory;
//import hex.DGLM.GramMatrixFunc;
import hex.RowVecTask.Sampling;

import java.util.Arrays;

import javax.swing.ProgressMonitor;

import water.*;
import water.Job.ChunkProgress;
import water.Job.ChunkProgressJob;
import water.ValueArray.Column;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

public class NewRowVecTask<T extends Iced> extends MRTask {
  // These fields are all *shared* *read-only*.
  final RowFunc<T> _func;
  final DataFrame  _data;
  final Job _job;
  //-----------------------------------------------------
  T _result; // result of the computation is stored here

  public static class JobCancelledException extends Exception {}


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
      if(j.cancelled())throw new JobCancelledException();
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
    final long _nobs;
    public final boolean _standardized;

    public DataFrame(ValueArray ary, int [] colIds, Sampling s, boolean standardize, boolean expandCat){
      _ary = ary;
      _modelDataMap = colIds;
      _s = s;
      _colCatMap = new int[colIds.length+1];
      int len=0;
      for( int i=0; i<colIds.length-1; i++ ) {
        _colCatMap[i] = len;
        ValueArray.Column C = ary._cols[colIds[i]];
        len += ( expandCat && C._domain != null )?C._domain.length-1:1;
      }
      // the last element (response variable) is NEVER expanded
      _colCatMap[colIds.length-1] = len++;
      _colCatMap[colIds.length] = len;
      _normSub = new double[len];
      _normMul = new double[len];
      Arrays.fill(_normMul, 1);
      boolean standardized = false;
      if(standardize )for(int i = 0; i < colIds.length-1; ++i){
        standardized = true;
        Column col = ary._cols[colIds[i]];
        if(col._domain == null){
          int idx = _colCatMap[i];
          _normSub[idx] = col._mean;
          _normMul[idx] = 1.0/col._sigma;
        }
      }
      _standardized = standardized;
      _nobs = (s != null)?(long)(ary._numrows*s.ratio()):ary._numrows;
    }
    public Sampling getSampling() {return (_s != null)?_s.clone():null;}
    public Sampling getSamplingComplement() {return (_s != null)?_s.complement():null;}
    public int expandedSz() {return _colCatMap[_colCatMap.length-1];}
    public int compactSz(){return _modelDataMap.length;}
    public int dense(){
      for(int i = 0; i < _modelDataMap.length; ++i)
        if(_colCatMap[i] != i)return i-1;
      return _modelDataMap.length;
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
    if(_job != null && _job.cancelled())return;
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

  @Override
  public void reduce(DRemoteTask drt) {
    if(_job != null && _job.cancelled()) return;
    NewRowVecTask<T> rv = (NewRowVecTask<T>)drt;
    assert _result != rv._result;
    _result = (_result != null)?_func.reduce(_result, rv._result):rv._result;
    rv._result = null;
  }
}
