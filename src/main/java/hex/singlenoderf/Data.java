package hex.singlenoderf;

import water.MemoryManager;
import water.util.Utils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;


public class Data implements Iterable<Data.Row> {
  /** Use stratified sampling */
  boolean _stratify;

  /** Random generator to make decision about missing data. */
  final Random _rng;

  public final class Row {
    int _index;
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append(_index).append(" [").append(classOf()).append("]:");
      for( int i = 0; i < _dapt.columns(); ++i ) sb.append(_dapt.hasBadValue(_index, i) ? "NA" : _dapt.getEncodedColumnValue(_index, i)).append(',');
      return sb.toString();
    }
    public int classOf()    { return _dapt.classOf(_index); }
    public final short getEncodedColumnValue(int colIndex) {
      return _dapt.getEncodedColumnValue(_index, colIndex); }
    public final short getEncodedClassColumnValue() {
      return _dapt.getEncodedClassColumnValue(_index);
    }
    public final float getRawClassColumnValueFromBin() {
      return _dapt.getRawClassColumnValueFromBin(_index);
    }
    public final boolean hasValidValue(int colIndex) { return !_dapt.hasBadValue(_index, colIndex); }
    public final boolean isValid() { return !_dapt.isBadRow(_index); }
    public final boolean isValidRaw() { return !_dapt.isBadRowRaw(_index); }
    public final double getRawColumnValue(int colIndex) { return _dapt.getRawColumnValue(_index, colIndex); }
  }

  protected final DataAdapter _dapt;

  /** Returns new Data object that stores all adapter's rows unchanged.   */
  public static Data make(DataAdapter da) { return new Data(da); }

  protected Data(DataAdapter dapt) {
    _dapt = dapt;
    _rng  =  Utils.getDeterRNG(0x7b85dfe19122f0d5L);
    _columnInfo = new ColumnInfo[_dapt.columns()];
    for(int i = 0; i<_columnInfo.length; i++)
      _columnInfo[i] = new ColumnInfo(i);
  }

  protected int start()          { return 0;                   }
  protected int end()            { return _dapt._numRows;      }
  public final int    rows()           { return end() - start();     }
  public final int    columns()        { return _dapt.columns();     }
  public final int    classes()        { return _dapt.classes();     }
  public final long   seed()           { return _dapt.seed();        }
  public final String colName(int i)   { return _dapt.columnName(i); }
  public final float  unmap(int col, int split) { return _dapt.unmap(col, split); }
  public final int    columnArity(int colIndex) { return _dapt.columnArity(colIndex); }
  public final int columnArityOfClassCol() { return _dapt.columnArityOfClassCol(); }
  /** Transforms given binned index (short) into 0..N-1 corresponding to predictor class */
  public final int      unmapClass(int clazz) {return _dapt.unmapClass(clazz); }
  public final boolean  isFloat(int col)      { return _dapt.isFloat(col);     }
  public final double[] classWt()             { return _dapt._classWt;         }
  public final boolean  isIgnored(int col)    { return _dapt.isIgnored(col);   }
  public final float computeAverage() {
    float av = 0.f;
    int nobs = 0;
    for (Row r: this) {
      if (r.isValid()) {
        av += r.getRawClassColumnValueFromBin();
      }
      nobs++;
    }
    return nobs == 0 ? 0 : av / (float)(nobs);
  }

  public double[] unpackRow(Row r) {
    double[] res = new double[_dapt._c.length-1];
    for (int i = 0; i < _dapt._c.length-1; ++i)  res[i] = r.getRawColumnValue(i);
    return res;
  }

  public Row at(int i) { Row _r = new Row(); _r._index = permute(i); return _r;}

  public final Iterator<Row> iterator() { return new RowIter(start(), end()); }
  private class RowIter implements Iterator<Row> {
    final Row _r = new Row();
    int _pos = 0; final int _end;
    public RowIter(int start, int end) { _pos = start; _end = end;       }
    public boolean hasNext()           { return _pos < _end;             }
    public Row     next()              { _r._index = permute(_pos++); return _r; }
    public void    remove()            { throw new RuntimeException("Unsupported"); }
  }

  // ----------------------
  private int filterInv(Tree.SplitNode node, int[] permutation, Statistic ls, Statistic rs) {
    final Row row = new Row();
    int l = start(), r = end() - 1;
    while (l <= r) {
      int permIdx = row._index = permutation[l];
      boolean putToLeft;
      if (node.canDecideAbout(row)) { // are we splitting over existing value
        putToLeft = node.isIn(row);
      } else { // make a random choice about non
        putToLeft = _rng.nextBoolean();
      }

      if (putToLeft) {
        ls.addQ(row, ls._regression);
        ++l;
      } else {
        rs.addQ(row, rs._regression);
        permutation[l] = permutation[r];
        permutation[r--] = permIdx;
      }
    }
    return l;
  }

  public long[] nonOOB() {
    ArrayList<Integer> res = new ArrayList<Integer>();
    for (Row r : this) res.add(r._index);
    long[] rr = new long[res.size()];
    for (int i = 0; i < rr.length; ++i) rr[i] = res.get(i);
    return rr;
  }

  // Filter a column, with all valid data.  i.e., skip the invalid check
  private int filterVal(Tree.SplitNode node, int[] permutation, Statistic ls, Statistic rs) {
    final int l =filterVal1(node,permutation);
    filterVal3(permutation,ls,start(),l);
    filterVal3(permutation,rs,l,end());
    return l;
  }

  // Hand-inlining for performance... CNC
  private int filterVal1(Tree.SplitNode node, int[] permutation) {
    int cidx = node._column;    // Decision column guiding the split
    DataAdapter.Col cs[] = _dapt._c;
    short bins[] = cs[cidx]._binned; // Bin#'s for each row
    byte  binb[] = cs[cidx]._rawB;   // Bin#'s for each row
    int split = node._split;          // Value to split on

    // Move the data into left/right halves
    int l = start(), r = end() - 1;
    while (l <= r) {
      int permIdx = permutation[l];
      int val = bins==null ? (0xFF&binb[permIdx]) : bins[permIdx];
      if( val <= split ) {
        ++l;
      } else {
        permutation[l] = permutation[r];
        permutation[r--] = permIdx;
      }
    }
    return l;
  }

  // Update the histogram
  private void filterVal3(int[] permutation, Statistic s, final int lo, final int hi) {
    if (!s._regression) {
      DataAdapter.Col cs[] = _dapt._c;
      short classs[]= cs[_dapt.classColIdx()]._binned;
      int cds[][][] = s._columnDists;
      int fs[] = s._features;

      // Run this loop by-feature instead of by-row - so that the updates in the
      // inner loops do not need to start from loading the feature array.
      for (int f : fs) {
        if (f == -1) break;       // Short features.
        int cdsf[][] = cds[f];    // Histogram per-column (by value & class)
        short[] bins = cs[f]._binned; // null if byte col, otherwise bin#

        if (bins != null) {              // binned?
          for (int i = lo; i < hi; i++) {    // Binned-loop
            int permIdx = permutation[i]; // Get the row
            int val = bins[permIdx];      // Bin-for-row
            if (val == DataAdapter.BAD) continue; // ignore bad rows
            int cls = classs[permIdx];    // Class-for-row
            if (cls == DataAdapter.BAD) continue; // ignore rows with NA in response column
            cdsf[val][cls]++;             // Bump histogram
          }

        } else {                          // not binned?
          byte[] raw = cs[f]._rawB;       // Raw unbinned byte array
          for (int i = lo; i < hi; i++) {    // not-binned loop
            int permIdx = permutation[i]; // Get the row
            int val = (0xFF & raw[permIdx]);// raw byte value, has no bad rows
            int cls = classs[permIdx] & 0xFF;    // Class-for-row
            cdsf[val][cls]++;             // Bump histogram
          }
        }
      }
    } else {
      DataAdapter.Col cols[] = _dapt._c;
      float[] response;
      if (cols[_dapt.classColIdx()]._binned == null) {
        response = new float[cols[_dapt.classColIdx()]._rawB.length];
        for (int b = 0; b < response.length; ++b)
          response[b] = (float)(0xFF & cols[_dapt.classColIdx()]._rawB[b]);
      } else {
        response = new float[cols[_dapt.classColIdx()]._binned.length];
        for (int f = 0; f < response.length; ++f)
          response[f] = cols[_dapt.classColIdx()]._binned2raw[cols[_dapt.classColIdx()]._binned[f]];
      }
      int cds[][][] = s._columnDistsRegression;
      int fs[] = s._features;

      for (int f: fs) {
        if (f == -1) break;
        int cdsf[][] = cds[f];
        short[] bins = cols[f]._binned;

        if (bins != null) {
          for (int i = lo; i < hi; i++) {
            int permIdx = permutation[i];
            int val = bins[permIdx];
            if (val == DataAdapter.BAD) continue; // ignore bad rows
            float resp = response[permIdx];    // Class-for-row
            int response_bin = _dapt.getEncodedClassColumnValue(permIdx); //cols[cols.length-1]._binned[permIdx]; //cols[_dapt.classColIdx()]._binned == null ? (cols[_dapt.classColIdx()]._rawB[permIdx] & 0xFF) : cols[_dapt.classColIdx()]._binned[permIdx];
            if (resp == DataAdapter.BAD) continue; // ignore rows with NA in response column
            cdsf[val][response_bin]++; // = resp;             // Bump histogram
          }
        } else {
          byte[] raw = cols[f]._rawB;
          for (int i = lo; i < hi; i++) {
            int permIdx = permutation[i];
            int val = raw[permIdx]&0xFF;
            if (val == DataAdapter.BAD) continue;
            short resp = cols[cols.length-1]._binned[permIdx];
            if (resp == DataAdapter.BAD) continue;
            int response_bin = _dapt.getEncodedClassColumnValue(permIdx); //cols[cols.length-1]._binned[permIdx]; //cols[_dapt.classColIdx()]._binned == null ? (cols[_dapt.classColIdx()]._rawB[permIdx] & 0xFF) : cols[_dapt.classColIdx()]._binned[permIdx];
            cdsf[val][response_bin]++; // = resp;
          }
        }
      }
    }
  }

  public void filter(Tree.SplitNode node, Data[] result, Statistic ls, Statistic rs) {
    int[] permutation = getPermutationArray();
    int cidx = node._column;
    int l =  _dapt.hasAnyInvalid(cidx) || _dapt.hasAnyInvalid(_dapt.columns()-1)
            ? filterInv(node,permutation,ls,rs)
            : filterVal(node,permutation,ls,rs);
    ColumnInfo[] linfo = _columnInfo.clone();
    ColumnInfo[] rinfo = _columnInfo.clone();
    linfo[node._column]= linfo[node._column].left(node._split);
    rinfo[node._column]= rinfo[node._column].right(node._split);
    result[0]= new Subset(this, permutation, start(), l);
    result[1]= new Subset(this, permutation, l,   end());
    result[0]._columnInfo = linfo;
    result[1]._columnInfo = rinfo;
  }

  public Data sampleWithReplacement(double bagSizePct, short[] complement) {
    // Make sure that values come in order
    int size = (int)(rows() * bagSizePct);
    /* NOTE: Before changing used generator think about which kind of random generator you need:
     * if always deterministic or non-deterministic version - see hex.speedrf.Utils.get{Deter}RNG */
    Random r = Utils.getRNG(seed());
    for( int i = 0; i < size; ++i)
      complement[permute(r.nextInt(rows()))]++;
    int[] sample = MemoryManager.malloc4(size);
    for( int i = 0, j = 0; i < sample.length;) {
      while(complement[j]==0) j++;
      for (int k = 0; k < complement[j]; k++) sample[i++] = j;
      j++;
    }
    return new Subset(this, sample, 0, sample.length);
  }

  public Data complement(Data parent, short[] complement) { throw new RuntimeException("Only for subsets."); }
  @Override public Data clone() throws CloneNotSupportedException { return this; }
  protected int permute(int idx) { return idx; }
  protected int[] getPermutationArray() {
    int[] perm = MemoryManager.malloc4(rows());
    for( int i = 0; i < perm.length; ++i ) perm[i] = i;
    return perm;
  }

  public int colMinIdx(int i) { return _columnInfo[i].min; }
  public int colMaxIdx(int i) { return _columnInfo[i].max; }

  class ColumnInfo {
    private final int col;
    int min, max;
    ColumnInfo(int col_) { col=col_; max = _dapt.columnArity(col_) - 1; }
    ColumnInfo left(int idx) {
      ColumnInfo res = new ColumnInfo(col);
      res.max = idx < max ? idx : max;
      res.min = min;
      return res;
    }
    ColumnInfo right(int idx) {
      ColumnInfo res = new ColumnInfo(col);
      res.min = idx >= min ? (idx+1) : min;
      res.max = max;
      return res;
    }

    public String toString() { return  col +  "["+ min +","+ max + "]"; }
  }

  ColumnInfo[] _columnInfo;
}

class Subset extends Data {
  private final int[] _permutation;
  private final int _start, _end;

  @Override protected int[] getPermutationArray() { return _permutation;      }
  @Override protected int permute(int idx)        { return _permutation[idx]; }
  @Override protected int start()                 { return _start;            }
  @Override protected int end()                   { return _end;              }
  @Override public Subset clone() throws CloneNotSupportedException { return new Subset(this,_permutation.clone(),_start,_end); }

  /** Creates new subset of the given data adapter. The permutation is an array
   * of original row indices of the DataAdapter object that will be used.  */
  public Subset(Data data, int[] permutation, int start, int end) {
    super(data._dapt);
    _start       = start;
    _end         = end;
    _permutation = permutation;
  }

  @Override public Data complement(Data parent, short[] complement) {
    int size= 0;
    for (short aComplement : complement) if (aComplement == 0) size++;
    int[] p = MemoryManager.malloc4(size);
    int pos = 0;
    for(int i=0;i<complement.length; i++) if (complement[i]==0) p[pos++] = i;
    return new Subset(this, p, 0, p.length);
  }

}
