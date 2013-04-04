package hex.rf;

import hex.rf.Data.Row;
import hex.rf.Tree.SplitNode;

import java.util.*;

import water.MemoryManager;
import water.util.Utils;

public class Data implements Iterable<Row> {
  /** Use stratified sampling */
  boolean _stratify;

  /** Random generator to make decision about missing data. */
  final Random _rng;

  public final class Row {
    int _index;
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append(_index).append(" ["+classOf()+"]:");
      for( int i = 0; i < _dapt.columns(); ++i ) sb.append(_dapt.hasBadValue(_index, i) ? "NA" : _dapt.getEncodedColumnValue(_index, i)).append(',');
      return sb.toString();
    }
    public int classOf()    { return _dapt.classOf(_index); }
    public final short getEncodedColumnValue(int colIndex) { return _dapt.getEncodedColumnValue(_index, colIndex); }
    public final boolean hasValidValue(int colIndex) { return !_dapt.hasBadValue(_index, colIndex); }
    public final boolean isValid() { return !_dapt.isBadRow(_index); }
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
  public int rows()              { return end() - start();     }
  public int columns()           { return _dapt.columns();     }
  public int classes()           { return _dapt.classes();     }
  public long seed()             { return _dapt.seed();        }
  public long dataId()           { return _dapt.dataId();      }
  public String colName(int i)   { return _dapt.columnName(i); }
  public float unmap(int col, int split) { return _dapt.unmap(col, split); }
  public int columnArity(int colIndex) { return _dapt.columnArity(colIndex); }
  /** Transforms given binned index (short) into 0..N-1 corresponding to predictor class */
  public int unmapClass(int clazz) {return _dapt.unmapClass(clazz); }
  public boolean isFloat(int col){ return _dapt.isFloat(col); }
  public double[] classWt()      { return _dapt._classWt; }

  public final Iterator<Row> iterator() { return new RowIter(start(), end()); }
  private class RowIter implements Iterator<Row> {
    final Row _r = new Row();
    int _pos = 0; final int _end;
    public RowIter(int start, int end) { _pos = start; _end = end;       }
    public boolean hasNext()           { return _pos < _end;             }
    public Row next()                  { _r._index = permute(_pos++); return _r; }
    public void remove()               { throw new Error("Unsupported"); }
  }

  public void filter(SplitNode node, Data[] result, Statistic ls, Statistic rs) {
    final Row row = new Row();
    int[] permutation = getPermutationArray();
    int l = start(), r = end() - 1;

    while (l <= r) {
      int permIdx = row._index = permutation[l];
      boolean putToLeft = true;
      if (node.canDecideAbout(row)) { // are we splitting over existing value
        putToLeft = node.isIn(row);
      } else { // make a random choice about non
        putToLeft = _rng.nextBoolean();
      }

      if (putToLeft) {
        ls.addQ(row);
        ++l;
      } else {
        rs.addQ(row);
        permutation[l] = permutation[r];
        permutation[r--] = permIdx;
      }
    }
    assert r+1 == l;
    ls.applyClassWeights();     // Weight the distributions
    rs.applyClassWeights();     // Weight the distributions
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
    short[] in = complement;
    int size = (int)(rows() * bagSizePct);
    /* NOTE: Before changing used generator think about which kind of random generator you need:
     * if always deterministic or non-deterministic version - see hex.rf.Utils.get{Deter}RNG */
    Random r = Utils.getRNG(seed());
    for( int i = 0; i < size; ++i)
      in[permute(r.nextInt(rows()))]++;
    int[] sample = MemoryManager.malloc4(size);
    for( int i = 0, j = 0; i < sample.length;) {
      while(in[j]==0) j++;
      for (int k = 0; k < in[j]; k++) sample[i++] = j;
      j++;
    }
    return new Subset(this, sample, 0, sample.length);
  }


  /** Roll a fair die for sampling, resetting the random die every numrows. */
  private int[] sampleFair(double bagSizePct, long seed, int rowsPerChunk ) {
    // preconditions
    assert rowsPerChunk != 0 : "RowsPerChunk contains 0! Not able to assure deterministic sampling!";
    // init
    Random rand = null;
    int   rows   = rows();
    int   size   = bagsz(rows,bagSizePct);
    int[] sample = MemoryManager.malloc4((int)(size*1.10));
    float f      = (float)bagSizePct;
    int   cnt    = 0;  // Counter for resetting Random
    int   j      = 0;  // Number of selected samples
    // compute
    for( int i=0; i<rows; i++ ) {
      if( cnt--==0 ) {
        /* NOTE: Before changing used generator think about which kind of random generator you need:
         * if always deterministic or non-deterministic version - see hex.rf.Utils.get{Deter}RNG */
        long chunkSamplingSeed = chunkSampleSeed(seed, i);
        rand = Utils.getDeterRNG(chunkSamplingSeed);
        cnt  = rowsPerChunk-1;
        if( i+2*rowsPerChunk > rows ) cnt = rows; // Last chunk is big
      }
      float randFloat = rand.nextFloat();
      if( randFloat < f ) {
        if( j == sample.length ) sample = Arrays.copyOfRange(sample,0,(int)(sample.length*1.2));
        sample[j++] = i;
      }
    }
    return Arrays.copyOf(sample,j); // Trim out bad rows
  }

  /** This method returns the correct seed based on initial seed and row index.
   *  WARNING : this method is crucial for correct replay of sampling.
   */
  static long chunkSampleSeed(long seed, int rowIdx) { return seed + ((long)rowIdx<<16); }

  /** Strata is a dataset group which corresponds to a class.
   * Sampling is specified per strata group -
   *
   * Stratified sampling look only at local data.
   */
  private int[] sampleStratified(float[] samplePerStrata, long seed, int rowsPerChunk) {
    // preconditions
    assert samplePerStrata.length == _dapt.classes() : "There is not enought number of samples for individual stratas!";
    // precomputing - find the largest sample and compute the bag size for it
    float largestSample = 0f;
    for (float sample : samplePerStrata) if (sample > largestSample) largestSample = sample;
    // compute
    Random rand   = null;
    int    rows   = rows();
    int[]  sample = new int[(int) (largestSample*rows)]; // be little bit more pessimistic
    int    j      = 0;
    int    cnt    = 0;
    // collect samples per strata
    for (int i=0; i<rows; i++) {
      if( cnt--==0 ) {
        long chunkSamplingSeed = chunkSampleSeed(seed, i);
        rand = Utils.getDeterRNG(chunkSamplingSeed);
        cnt  = rowsPerChunk-1;
        if( i+2*rowsPerChunk > rows ) cnt = rows; // Last chunk is big
      }
      float randFloat = rand.nextFloat();
      int strata = _dapt.classOf(i); // strata groups are represented by response classes
      if (randFloat < samplePerStrata[strata]) {
        if( j == sample.length ) sample = Arrays.copyOfRange(sample,0,(int)(sample.length*1.2));
        sample[j++] = i;
      }
    }
    return Arrays.copyOf(sample,j);
  }

  /** added for stratified sampling, uniformly picks sample of n elements from the given interval */
  private int sampleFromClass(int c, int n, int startIdx, int sample [], Random r) {
    int iStart = _dapt.getIntervalsStarts()[c];
    int iEnd = _dapt.getIntervalsStarts()[c+1];
    int iWidth = iEnd - iStart;
    for(int i = 0; i < n; ++i){
      int candidate = iStart + r.nextInt(iWidth);
 //FIXME     while(_dapt.badRow(candidate)){
//        if(candidate == iStart)candidate = iStart + iWidth;
        //--candidate;
//      }
      sample[startIdx++] = candidate;
    }
    return startIdx;
  }

  public Data sample(float[] samplePerStrata, long seed, int rowsPerChunk) {

    int sample[] = sampleStratified(samplePerStrata, seed, rowsPerChunk);
    Arrays.sort(sample);
    // -debug
    System.out.println("Data.sample(): strata = " + Arrays.toString(samplePerStrata));
    int   sumHisto = 0;
    int[] histo = new int[_dapt.classes()];
    for (int i = 0; i < rows(); i++) histo[_dapt.classOf(i)]++;
    for (int h : histo) sumHisto += h;

    int sumSampledHisto = 0;
    int[] sampledHisto = new int[_dapt.classes()];
    for (int i : sample) sampledHisto[_dapt.classOf(i)]++;
    for (int h : sampledHisto) sumSampledHisto += h;

    System.out.println("Total: " + sumSampledHisto + "/" + sumHisto + " (" + 100*sumSampledHisto/sumHisto );
    for (int i = 0; i < histo.length; i++) {
      System.out.println("Class " + i + " " + sampledHisto[i] + "/" + histo[i] + " (" + 100*sampledHisto[i]/histo[i]);
    }
    // -end of debug
    return new Subset(this, sample, 0, sample.length);
  }

  public Data sample(int [] strata, long seed) {
    int sz = 0;
    for(int s:strata)sz += s;
    int [] sample = new int[sz];
    int idx = 0;
    /* NOTE: Before changing used generator think about which kind of random generator you need:
     * if always deterministic or non-deterministic version - see hex.rf.Utils.get{Deter}RNG */
    Random r = Utils.getRNG(seed);
    for(int i = 0; i < strata.length; ++i){
      idx = sampleFromClass(i, strata[i], idx, sample,r);
    }
    Arrays.sort(sample); // we want an ordered sample
    return new Subset(this, sample, 0, sample.length);
  }

  // Deterministically sample the 'this' Data at the bagSizePct.  Toss out
  // invalid rows (as-if not sampled), but maintain the sampling rate.
  public Data sample(double bagSizePct, long seed, int numrows) {
    assert getClass()==Data.class; // No subclassing on this method
    int [] sample;
    sample = sampleFair(bagSizePct,seed,numrows);
    // add the remaining rows
    Arrays.sort(sample); // we want an ordered sample
    return new Subset(this, sample, 0, sample.length);
  }
  private int bagsz( int rows, double bagSizePct ) {
    int size = (int)(rows * bagSizePct);
    return (size>0 || rows==0) ? size : 1;
  }

  public Data complement(Data parent, short[] complement) { throw new Error("Only for subsets."); }
  @Override public Data clone() { return this; }
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
    int min() { return min; }
    int max() { return max; }

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
  @Override public Subset clone()                 { return new Subset(this,_permutation.clone(),_start,_end); }

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
    for(int i=0;i<complement.length; i++) if (complement[i]==0) size++;
    int[] p = MemoryManager.malloc4(size);
    int pos = 0;
    for(int i=0;i<complement.length; i++) if (complement[i]==0) p[pos++] = i;
    return new Subset(this, p, 0, p.length);
  }

}
