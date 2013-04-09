package hex.rf;
import java.text.DecimalFormat;
import java.util.Arrays;

import water.MemoryManager;
import water.ValueArray;
import water.util.Utils;

/**A DataAdapter maintains an encoding of the original data. Every raw value (of type float)
 * is represented by a short value. When the number of unique raw value is larger that binLimit,
 * the DataAdapter will perform binning on the data and use the same short encoded value to
 * represent several consecutive raw values.
 *
 * Missing values, NaNs and Infinity are treated as BAD data. */
final class DataAdapter  {

  /** Place holder for missing data, NaN, Inf in short encoding.*/
  static final short BAD = Short.MIN_VALUE;

  /** Data to load */
  public final ValueArray _ary;
  /** Number of classes. */
  private final int _numClasses;
  /** ??? */
  private int [] _intervalsStarts;
  /** Columns. */
  private final Col[]    _c;
  /** Unique cookie identifying this dataset*/
  private final long     _dataId;
  /** Seed for sampling */
  private final long     _seed;
  /** Number of rows */
  public  final int      _numRows;
  /** Class weights */
  public  final double[] _classWt;
  /** Maximum arity for a column (not a hard limit) */
  private final int      _binLimit;

  DataAdapter(ValueArray ary, RFModel model, int[] modelDataMap, int rows,
              long unique, long seed, int binLimit, double[] classWt) {
    assert model._dataKey == ary._key;
    _ary = ary;
    _seed = seed+(unique<<16); // This is important to preserve sampling selection!!!
    _binLimit = binLimit;
    _dataId = unique;
    _numRows = rows;
    _numClasses = model.classes();

    _c = new Col[model._va._cols.length];
    for( int i = 0; i < _c.length; i++ ) {
      assert ary._cols[modelDataMap[i]]._name.equals(model._va._cols[i]._name);
      _c[i]= new Col(model._va._cols[i]._name, rows, i == _c.length-1,_binLimit, model._va._cols[i].isFloat());
    }
    boolean trivial = true;
    if (classWt != null) for(double f: classWt) if (f != 1.0) trivial = false;
    _classWt = trivial ?  null : classWt;
  }

  public void initIntervals(int n){
    _intervalsStarts = new int[n+1];
    _intervalsStarts[n] = _numRows;
  }
  public void setIntervalStart(int i, int S){
    if(_intervalsStarts == null)_intervalsStarts = new int[i+1];
    if(_intervalsStarts.length <= i)_intervalsStarts = Arrays.copyOf(_intervalsStarts, i+1);
    _intervalsStarts[i] = S;
  }

  final int[] getIntervalsStarts() { return _intervalsStarts; }

  /** Given a value in enum format, returns:  the value in the original format if no
   * binning was applied,  or if binning was applied a value that is inbetween
   * the idx and the next value.  If the idx is the last value return (2*idx+1)/2. */
  public float unmap(int col, int idx){ return _c[col].rawSplit(idx); }

  public void computeBins(int col){_c[col].shrink();}

  public boolean isFloat(int col) { return _c[col].isFloat(); }
  public long seed()          { return _seed; }
  public int columns()        { return _c.length;}
  public int classOf(int idx) { return _c[_c.length-1].get(idx); }
  /** Transforms given binned index (short) from class column into a value from interval [0..N-1]
   * corresponding to a particular predictor class */
  public int unmapClass(int clazz) { Col c = _c[_c.length-1]; return (int) (c.raw(clazz) - c.min); }
  /**Returns true if the row has missing data. */
  public long dataId()         { return _dataId; }
  /** The number of possible prediction classes. */
  public int classes()        { return _numClasses; }

  /** Returns the number of bins, i.e. the number of distinct values in the column.  */
  public int columnArity(int col) { return _c[col].arity(); }

  /** Return a short that represents the binned value of the original row,column value.  */
  public short getEncodedColumnValue(int row, int col) { return _c[col].get(row); }

  public void shrink() {
    for ( Col c: _c) c.shrink();
  }

  public String columnName(int i) { return _c[i].name(); }

  public boolean isValid(int col, float f) {
    if (!_c[col].isFloat()) return true;
    if (Float.isInfinite(f)) return false;
    return true;
  }

  public void add(float v, int row, int col){ _c[col].add(row,v); }

  public final void addBad(int row, int col) { _c[col].addBad(row); }

  public boolean hasBadValue(int row, int col) { return _c[col].isBad(row); }

  public boolean isBadRow(int row) { return _c[_c.length-1].isBad(row); }

  public void markIgnoredRow(int row) {
    _c[_c.length-1].addBad(row);
  }

  private static class Col {
    /** Encoded values*/
    short[] binned;
    /** Original values, kept only during inhale*/
    float[] raw;
    /** Map from binned to original*/
    float[] binned2raw;
    boolean isClass, isFloat;
    int binLimit;
    String name;
    static final DecimalFormat df = new  DecimalFormat ("0.##");
    /** Total number of bad values in the column. */
    int invalidValues;
    float min, max;

    Col(String s, int rows, boolean isClass_, int binLimit_, boolean isFloat_) {
      name = s; isFloat = isFloat_; isClass = isClass_; binLimit = binLimit_;
      raw = MemoryManager.malloc4f(rows);
    }

    boolean isFloat() { return isFloat; }
    boolean isClass() { return isClass; }
    int arity() { return binned2raw.length; }
    String name() { return name; }
    short get(int row) { return binned[row]; }
    void add(int row, float val) { raw[row] = val; }

    void addBad(int row) { raw[row] = Float.NaN; }

    private boolean isBadRaw(float f) { return Float.isNaN(f); }
    boolean isBad(int row) { return binned[row] == BAD; }

    /** For all columns - encode all floats as unique shorts. */
    void shrink() {
      float[] vs = raw.clone();
      Arrays.sort(vs); // Sort puts all Float.NaN at the end of the array (according Float.NaN doc)
      int ndups = 0, i = 0, nans = 0; // Counter of all NaNs
      while(i < vs.length-1){      // count dups
        int j = i+1;
        if (isBadRaw(vs[i]))  { nans = vs.length - i; break; } // skipe all NaNs
        if (isBadRaw(vs[j]))  { nans = vs.length - j; break; } // there is only one remaining NaN (do not forget on it)
        while(j < vs.length && vs[i] == vs[j]){  ++ndups; ++j; }
        i = j;
      }
      invalidValues = nans;
      assert vs.length > nans : "Nothing but missing values, should ignore column " + name;
      int n = vs.length - ndups - nans;
      int rem = n % binLimit;
      int maxBinSize = (n > binLimit) ? (n / binLimit + Math.min(rem,1)) : 1;
      // Assign shorts to floats, with binning.
      binned2raw = MemoryManager.malloc4f(Math.min(n, binLimit)); // if n is smaller than bin limit no need to compact
      int smax = 0, cntCurBin = 1;
      i = 0;
      binned2raw[0] = vs[i];
      for(; i < vs.length; ++i) {
        if(isBadRaw(vs[i])) break; // the first NaN, there are only NaN in the rest of vs[] array
        if(vs[i] == binned2raw[smax]) continue; // remove dups
        if( ++cntCurBin > maxBinSize ) {
          if(rem > 0 && --rem == 0)--maxBinSize; // check if we can reduce the bin size
          ++smax;
          cntCurBin = 1;
        }
        binned2raw[smax] = vs[i];
      }
      ++smax;
//      for(i = 0; i< vs.length; i++) if (!isBadRaw(vs[i])) break;
      // All Float.NaN are at the end of vs => min is stored in vs[0]
      min = vs[0];
      for(i = vs.length -1; i>= 0; i--) if (!isBadRaw(vs[i])) break;
      max = vs[i];
      vs = null; // GCed
      binned = MemoryManager.malloc2(raw.length);
      // Find the bin value by lookup in bin2raw array which is sorted so we can do binary lookup.
      for(i = 0; i < raw.length; i++)
        if (isBadRaw(raw[i]))
          binned[i] = BAD;
        else {
          short idx = (short) Arrays.binarySearch(binned2raw, raw[i]);
          if (idx >= 0) binned[i] = idx;
          else binned[i] = (short) (-idx - 1); // this occurs when we are looking for a binned value, we return the smaller value in the array.
          assert binned[i] < binned2raw.length;
        }
      if( n > binLimit )  Utils.pln(this+" this column's arity was cut from "+n+" to "+smax);
      raw = null; // GCced
    }

    /**Given an encoded short value, return the original float*/
    public float raw(int idx) { return binned2raw[idx]; }

    /**Given an encoded short value, return the float that splits that value with the next.*/
    public float rawSplit(int idx){
      if (idx == BAD) return Float.NaN;
      float flo = binned2raw[idx+0]; // Convert to the original values
      float fhi = (idx+1 < binned2raw.length)? binned2raw[idx+1] : flo+1.f;
      float fmid = (flo+fhi)/2.0f; // Compute a split-value
      //assert flo < fmid && fmid < fhi : "Values " + flo +","+fhi ; // Assert that the float will properly split
      return fmid;
    }

    int rows() { return binned.length; }

    public String toString() {
      String res = "Column("+name+"){";
      res+= " ["+df.format(min) +","+df.format(max)+"]";
      res+=",bad values=" + invalidValues + "/" + rows();
      if (isClass) res+= " CLASS ";
      res += "}";
      return res;
    }
  }
}
