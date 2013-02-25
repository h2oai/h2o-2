package hex;

import java.util.Arrays;
import water.*;

public abstract class RowVecTask extends MRTask {
  // These fields are all *shared* *read-only*.
  final ValueArray _ary;
  final Sampling _s;
  final int[] _modelDataMap;
  final int[] _colCatMap;
  final double [] _normSub;
  final double [] _normMul;

  public enum DataPreprocessing {
    NONE,        // x_new = x
    NORMALIZE,   // x_new = (x - x_min)/(x_max - x_min)  /// scales numeric values to 0 - 1 range
    STANDARDIZE, // x_new = (x - x_mu)/x_sigma  /// transforms data to have zero mean and unit variance
    AUTO
  };

  public RowVecTask(ValueArray ary, Sampling s, int[] modelDataMap, int[] colCatMap, double[] normSub, double[] normMul ) {
    assert modelDataMap.length+1 == colCatMap.length; // same as the model feature count
    _ary = ary;
    _s = s;
    _modelDataMap = modelDataMap;
    _colCatMap = colCatMap;
    _normSub = normSub;
    _normMul = normMul;
  }

  public static class Sampling extends Iced implements Cloneable{
    private final int _step;
    private final int _offset;
    private final boolean _complement;
    private int _next;

    public Sampling(int offset, int step, boolean complement) {
      _step = step;
      _complement = complement;
      _offset = offset;
    }
    public Sampling(Sampling other) {
      this(other._offset,other._step,other._complement);
    }

    Sampling complement(){
      return new Sampling(_offset,_step, !_complement);
    }

    public Sampling reset() {
      return new Sampling(_offset,_step,_complement);
    }

    boolean skip(int row) {
      if( row < _next+_offset ) return _complement;
      _next += _step;
      return !_complement;
    }
    public String toString(){
      return "Sampling(step="+_step + ",offset=" + _offset + ",complement=" + _complement + ")";
    }

    public double ratio() {
      double res = 1.0/_step;
      if(!_complement) res = 1.0-res;
      return res;
    }


    public Sampling clone(){
      return new Sampling(this);
    }
  }

  @Override
  public void map(Key key) {
    Sampling s = (_s != null)?_s.clone():null;
    init2();                    // Specialized subtask per-chunk init
    AutoBuffer bits = _ary.getChunk(key);
    final int rows = bits.remaining()/_ary._rowsize;

    // Array to hold a model's worth of training data from the dataset, plus
    // the response column last.
    double [] x = new double[_modelDataMap.length];
    Arrays.fill(x, 1.0);
    // Mapping from the dense 'x' model data to the categorically expanded
    // columns.  This is basically _colCatMap, except that categorical columns
    // have the expanded column number in the index.
    int [] indexes = _colCatMap.clone();

    // Build a dense row of doubles for processing from the dataset.
    // Do all the mappings from model columns to dataset columns.
ROW:
    for( int r=0; r<rows; r++ ) {
      if( s != null && s.skip(r) ) continue;
      // For all the model's columns
      for( int i = 0; i<_modelDataMap.length; i++ ) {
        int dataColIdx = _modelDataMap[i]; // Column in the dataset
        // Ignore missing data
        if( _ary.isNA(bits,r,dataColIdx) ) continue ROW;
        // Get the dataset data - not yet done any categorical expansion
        double d = _ary.datad(bits,r,dataColIdx);
        // Is it categorical?
        int idx = _colCatMap[i];
        if( i == _modelDataMap.length-1 || // Last column is response-col, not categorical
            idx+1 == _colCatMap[i+1] ) { // No room for categories ==> numerical
          x[i] = (d - _normSub[idx]) * _normMul[idx];
        } else {                // Else categorical
          // TODO: Add normalization
          x[i] = 1.0;           // Always a 1.0
          // Size of category (number of factors/enum elements) is also the
          // number of expanded columns.  Make sure that the enum/factor 'd' is
          // in range.
          assert 0 <= d && (int)d < _colCatMap[i+1]-idx;
          indexes[i] = idx+(int)d; // Which expanded column to use
        }
      }
      // At this point 'x' contains the normalized compacted feature data from
      // the training dataset, and 'indexes' contains enum/categorical expanded
      // column number.
      processRow(x, indexes);
    }
  }
  abstract void processRow(double [] x, int[] indexes);
  protected void init2(){}
}
