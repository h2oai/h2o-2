package hex.singlenoderf;


import jsr166y.ForkJoinTask;
import jsr166y.RecursiveAction;
import water.*;
import water.fvec.Frame;
import water.fvec.Vec;
import water.util.Log;
import water.util.Log.Tag.Sys;

import java.text.DecimalFormat;
import java.util.Arrays;

/**A DataAdapter maintains an encoding of the original data. Every raw value (of type float)
 * is represented by a short value. When the number of unique raw value is larger that binLimit,
 * the DataAdapter will perform binning on the data and use the same short encoded value to
 * represent several consecutive raw values.
 *
 * Missing values, NaNs and Infinity are treated as BAD data. */
final class DataAdapter  {

  /** Place holder for missing data, NaN, Inf in short encoding.*/
  static final short BAD = Short.MIN_VALUE;

  /** Number of classes. */
  private final int _numClasses;
  /** Columns. */
  final Col[]    _c;
  /** Seed for sampling */
  private final long     _seed;
  /** Number of rows */
  public  final int      _numRows;
  /** Class weights */
  public  final double[] _classWt;
  /** Use regression */
  public final boolean _regression;

  public Key _jobKey;

  DataAdapter(Frame fr, SpeeDRFModel model, int[] modelDataMap, int rows,
              long unique, long seed, int binLimit, double[] classWt) {
//    assert model._dataKey == fr._key;
    _seed       = seed+(unique<<16); // This is important to preserve sampling selection!!!
    /* Maximum arity for a column (not a hard limit) */
    _numRows    = rows;
    _jobKey     = model.jobKey;
    _numClasses = model.regression ? 1 : model.classes();
    _regression = model.regression;
    _c = new Col[fr.numCols()];
    for( int i = 0; i < _c.length; i++ ) {
      if(model.jobKey != null && !Job.isRunning(model.jobKey)) throw new Job.JobCancelledException();
      assert fr._names[modelDataMap[i]].equals(fr._names[i]);
      Vec v = fr.vecs()[i];
      if( isByteCol(v,rows, i == _c.length-1, _regression) ) // we do not bin for small values
        _c[i] = new Col(fr._names[i], rows, i == _c.length-1);
      else
        _c[i] = new Col(fr._names[i], rows, i == _c.length-1, binLimit, !(v.isEnum() || v.isInt()));
    }
    boolean trivial = true;
    if (classWt != null) for(double f: classWt) if (f != 1.0) trivial = false;
    _classWt = trivial ?  null : classWt;
  }

  static boolean isByteCol( Vec C, int rows, boolean isClass, boolean regression) {
    if (regression) {
      return !isClass && (C.isInt() || C.isEnum()) && C.min() >= 0 && C.length() == rows && (C.max() < 255 || C.max() < 256 && C.length() == rows);
    }
    return (C.isInt() || C.isEnum()) && !isClass && C.min() >= 0 && C.length()==rows &&
            (C.max()<255 || C.max() <256 && C.length()==rows);
  }

  /** Given a value in enum format, returns:  the value in the original format if no
   * binning was applied,  or if binning was applied a value that is inbetween
   * the idx and the next value.  If the idx is the last value return (2*idx+1)/2. */
  public float unmap(int col, int idx){ return _c[col].rawSplit(idx); }
  public boolean isFloat(int col) { return _c[col].isFloat(); }
  public long seed()          { return _seed; }
  public int columns()        { return _c.length;}
  public int classOf(int idx) { return _c[_c.length-1].get(idx); }
  /** The number of possible prediction classes. */
  public int classes()        { return _numClasses; }
  /** Transforms given binned index (short) from class column into a value from interval [0..N-1]
   * corresponding to a particular predictor class */
  public int unmapClass(int clazz) {
    Col c = _c[_c.length-1];
    if (c._isByte)
      return clazz;
    else {
      // OK, this is not fully correct bad handle corner-cases like for example dataset uses classes only
      // with 0 and 3. Our API reports that there are 4 classes but in fact there are only 2 classes.
      if (clazz >= c._binned2raw.length) clazz = c._binned2raw.length - 1;
      return (int) (c.raw(clazz) - c._min);
    }
  }

  /** Returns the number of bins, i.e. the number of distinct values in the column.  */
  public int columnArity(int col) { return _c[col].arity(); }
  public int columnArityOfClassCol() { return _c[_c.length - 1].arity(); }

  /** Return a short that represents the binned value of the original row,column value.  */
  public short getEncodedColumnValue(int row, int col) { return _c[col].get(row); }
  public short getEncodedClassColumnValue(int row) { return _c[_c.length-1].get(row); }
  public double getRawColumnValue(int row, int col) { return _c[col].getRaw(row); }
  public float getRawClassColumnValueFromBin(int row) {
    int idx = _c.length-1;
    short btor = _c[idx].get(row);
    if (_c[idx]._binned == null) {
      return (float)(0xFF & _c[idx]._rawB[row]);
    }
    return _c[_c.length-1]._binned2raw[btor];
  }

  public void shrink() {
    if(_jobKey != null && !Job.isRunning(_jobKey)) throw new Job.JobCancelledException();
//    for ( Col c: _c) c.shrink();
    // sort columns in parallel: c.shrink() calls single-threaded Arrays.sort()
    RecursiveAction [] ras = new RecursiveAction[_c.length];
    int i=0;
    for ( final Col c: _c) {
      ras[i++] = new RecursiveAction() {
        @Override public void compute() { c.shrink(); }
      };
    }
    ForkJoinTask.invokeAll(ras);
  }

  public String columnName(int i) { return _c[i].name(); }

  public boolean isValid(int col, float f) {
    return !_c[col].isFloat() || !Float.isInfinite(f);
  }

  public final void    add(float v, int row, int col) {
    _c[col].add (row,v); }
  public final void    add1(int  v, int row, int col) {
    _c[col].add1(row,v); }
  public final void    addBad(int row, int col)       { _c[col].addBad(row); }
  public final boolean hasBadValue(int row, int col)  { return _c[col].isBad(row); }
  public final boolean isBadRow(int row)              { return _c[_c.length-1].isBad(row); }
  public final boolean isBadRowRaw(int row)           { return _c[_c.length-1].isBadRaw(row); }
  public final boolean isIgnored(int col)             { return _c[col].isIgnored(); }
  public final void    markIgnoredRow(int row)        { _c[_c.length-1].addBad(row);  }
  public final int     classColIdx()                  { return _c.length - 1; }
  public final boolean hasAnyInvalid(int col)         { return _c[col]._invalidValues!=0; }

  static class Col {
    /** Encoded values*/
    short[] _binned;
    /** Original values, kept only during inhale*/
    float[] _raw;
    /** Original values which we do not want to bin */
    byte[]  _rawB;
    /** Map from binned to original*/
    float[] _binned2raw;
    final boolean _isClass, _isFloat, _isByte;
    final int _colBinLimit;
    final String _name;
    /** Total number of bad values in the column. */
    int _invalidValues;
    float _min, _max;
    int _arity;

    static final DecimalFormat df = new  DecimalFormat ("0.##");
    boolean _ignored;

    Col(String s, int rows, boolean isClass) {
      _name = s; _isClass = isClass;
      _rawB = MemoryManager.malloc1(rows);
      _isFloat = false;
      _isByte  = true;
      _colBinLimit = 0;
    }

    Col(String s, int rows, boolean isClass, int binLimit, boolean isFloat) {
      _name = s; _isFloat = isFloat; _isClass = isClass; _colBinLimit = binLimit; _isByte = false;
      _raw = MemoryManager.malloc4f(rows);
      _ignored = false;
    }

    boolean isFloat()      { return _isFloat; }
    boolean isIgnored()    { return _ignored; }
    int arity()            { return _ignored ? -1 : _arity; }
    String name()          { return _name;        }
    short get(int row)     { return (short) (_isByte ? (_rawB[row]&0xFF) : _binned[row]); }
    double getRaw(int row) { return (double)(_isByte ? (_rawB[row]&0xFF) : _binned2raw[_binned[row]]);}

    void add(int row, float val) {
      _raw [row] = val; }
    void add1(int row, int  val) {
      _rawB[row] = (byte)val; }
    void addBad(int row)         { if (!_isByte) _raw[row] = Float.NaN; else _rawB[row] = (byte)255; }

    private boolean isBadRaw(float f) { return Float.isNaN(f); }
    boolean isBad(int row)            {
      return _isByte ? (_rawB[row]&0xFF)==255 : _binned[row] == BAD;
    }

    /** For all columns - encode all floats as unique shorts. */
    void shrink() {
      if (_isByte) {
        _arity = 256;
        return ; // do not shrink byte columns
      }
      float[] vs = _raw.clone();
      Arrays.sort(vs); // Sort puts all Float.NaN at the end of the array (according Float.NaN doc)
      int ndups = 0, i = 0, nans = 0; // Counter of all NaNs
      while(i < vs.length-1) {      // count dups
        int j = i+1;
        if (isBadRaw(vs[i]))  { nans = vs.length - i; break; } // skip all NaNs
        if (isBadRaw(vs[j]))  { nans = vs.length - j; break; } // there is only one remaining NaN (do not forget on it)
        while(j < vs.length && vs[i] == vs[j]){  ++ndups; ++j; }
        i = j;
      }
      _invalidValues = nans;
      if ( vs.length <= nans) {
        // to many NaNs in the column => ignore it
        _ignored = true;
        _raw     = null;
        Log.info(Sys.RANDF, "Ignore column: " + this);
        return;
      }
      int n = vs.length - ndups - nans;
      int rem = n % _colBinLimit;
      int maxBinSize = (n > _colBinLimit) ? (n / _colBinLimit + Math.min(rem,1)) : 1;
      // Assign shorts to floats, with binning.
      _binned2raw = MemoryManager.malloc4f(Math.min(n, _colBinLimit)); // if n is smaller than bin limit no need to compact
      int smax = 0, cntCurBin = 1;
      i = 0;
      _binned2raw[0] = vs[i];
      for(; i < vs.length; ++i) {
        if(isBadRaw(vs[i])) break; // the first NaN, there are only NaN in the rest of vs[] array
        if(vs[i] == _binned2raw[smax]) continue; // remove dups
        if( ++cntCurBin > maxBinSize ) {
          if(rem > 0 && --rem == 0)--maxBinSize; // check if we can reduce the bin size
          ++smax;
          cntCurBin = 1;
        }
        _binned2raw[smax] = vs[i];
      }
      ++smax;
//      for(i = 0; i< vs.length; i++) if (!isBadRaw(vs[i])) break;
      // All Float.NaN are at the end of vs => min is stored in vs[0]
      _min = vs[0];
      for(i = vs.length -1; i>= 0; i--) if (!isBadRaw(vs[i])) break;
      _max = vs[i];
      vs = null; // GCed
      _binned = MemoryManager.malloc2(_raw.length);
      // Find the bin value by lookup in bin2raw array which is sorted so we can do binary lookup.
      for(i = 0; i < _raw.length; i++)
        if (isBadRaw(_raw[i]))
          _binned[i] = BAD;
        else {
          short idx = (short) Arrays.binarySearch(_binned2raw, _raw[i]);
          if (idx >= 0) _binned[i] = idx;
          else _binned[i] = (short) (-idx - 1); // this occurs when we are looking for a binned value, we return the smaller value in the array.
          assert _binned[i] < _binned2raw.length;
        }
      if( n > _colBinLimit )   Log.info(Sys.RANDF,this+" this column's arity was cut from "+n+" to "+smax);
      _arity = _binned2raw.length;
      _raw = null; // GCced
    }

    /**Given an encoded short value, return the original float*/
    public float raw(int idx) { return _binned2raw[idx]; }

    /**Given an encoded short value, return the float that splits that value with the next.*/
    public float rawSplit(int idx){
      if (_isByte) return idx; // treat index as value
      if (idx == BAD) return Float.NaN;
      float flo = _binned2raw[idx]; // Convert to the original values
      float fhi = (idx+1 < _binned2raw.length)? _binned2raw[idx+1] : flo+1.f;
      //assert flo < fmid && fmid < fhi : "Values " + flo +","+fhi ; // Assert that the float will properly split
      return (flo+fhi)/2.0f;
    }

    int rows() { return _isByte ? _rawB.length : _binned.length; }

    @Override public String toString() {
      String res = "Column("+_name+"){";
      if (_ignored) res+="IGNORED";
      else {
        res+= " ["+df.format(_min) +","+df.format(_max)+"]";
        res+=",bad values=" + _invalidValues + "/" + rows();
        if (_isClass) res+= " CLASS ";
      }
      res += "}";
      return res;
    }
  }
}
