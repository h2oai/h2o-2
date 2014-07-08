package hex.singlenoderf;

import hex.singlenoderf.Data.Row;
import water.util.Utils;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;

/** Keeps track of the column distributions and analyzes the column splits in the
 * end producing the single split that will be used for the node. */
abstract class Statistic {
  /** Column distributions:  column  x  arity x classes
   *  Remembers the number of rows of the given column index, encodedValue, class.  */
  protected final int[][][] _columnDists;
  protected final int[] _features;         // Columns/features that are currently used.
  protected Random _random;                // Pseudo random number generator
  private long _seed;                      // Seed for prng
  private HashSet<Integer> _remembered;    // Features already used
  final double[] _classWt;                 // Class weights
  private int _exclusiveSplitLimit;
  protected final int[/*num_features*/][/*column_bins*/][/*response_bins*/] _columnDistsRegression;
  boolean _regression;

  /** Returns the best split for a given column   */
  protected abstract Split ltSplit(int colIndex, Data d, int[] dist, int distWeight, Random rand);
  protected abstract Split eqSplit(int colIndex, Data d, int[] dist, int distWeight, Random rand);
  protected abstract Split ltSplit(int colIndex, Data d, float[] dist, float distWeight, Random rand);
  protected abstract Split eqSplit(int colIndex, Data d, float[] dist, float distWeight, Random rand);

  /** Split descriptor for a particular column.
   * Holds the column name and the split point, which is the last column class
   * that will go to the left tree. If the column index is -1 then the split
   * value indicates the return value of the node.
   */
  static class Split {
    final int _column, _split;
    final float _splitReg;
    final double _fitness;

    Split(int column, int split, double fitness) {
      _column = column;  _split = split;  _fitness = fitness; _splitReg = -1.f;
    }
    /** A constant split used for true leaf nodes where all rows are of the same class.  */
    static Split constant(int result) {  return new Split(-1, result, -1); }
    /** An impossible split, which behaves like a constant split. However impossible split
     * occurs when there are different row classes present, but they all have
     * the same column value and therefore no split can be made.
     */
    static Split impossible(int result) { return new Split(-2, result, -1);  }
    /** Classic split. All lower or equal than split value go left, all greater go right.  */
    static Split split(int column, int split, double fitness) { return new Split(column, split,fitness); }
    /** Return an impossible split that has the best fitness */
    static Split defaultSplit() { return new Split(-2,0,-Double.MAX_VALUE); }
    /** Exclusion split. All equal to split value go left, all different go right.  */
    static Split exclusion(int column, int split, double fitness) { return new ExclusionSplit(column,split,fitness); }
    final boolean isLeafNode()   { return _column < 0; }
    final boolean isConstant()   { return _column == -1; }
    final boolean isImpossible() { return _column == -2;  }
    final boolean betterThan(Split other) { return _fitness > other._fitness; }
    final boolean isExclusion()  { return this instanceof ExclusionSplit; }
  }

  /** An exclusion split.  */
  static class ExclusionSplit extends Split {
    ExclusionSplit(int column, int split, double fitness) {  super(column, split,fitness);  }
  }

  /** Aggregates the given column's distribution to the provided array and
   * returns the sum of weights of that array.  */
  private int aggregateColumn(int colIndex, int[] dist) {
    int sum = 0;
    for (int j = 0; j < _columnDists[colIndex].length; ++j) {
      for (int i = 0; i < dist.length; ++i) {
        int tmp = _columnDists[colIndex][j][i];
        sum     += tmp;
        dist[i] += tmp;
      }
    }
    return sum;
  }

  /** Sum up the target responses and return that value (this will be the unweighted "mean"
   *
   * @param colIndex: The column we're summing over
   * @param dist: The *raw* response value for each bin.
   * @return The unweighted mean.
   */
  private float aggregateColumn(int colIndex, float[] dist) {
    float sum = 0.f;
    for (int j = 0; j < _columnDistsRegression[colIndex].length; ++j) {
      for (int i = 0; i < dist.length; ++i) {
        float tmp = _columnDistsRegression[colIndex][j][i];
        sum += tmp;
        dist[i] += tmp;
      }
    }
    return sum;
  }

  Statistic(Data data, int featuresPerSplit, long seed, int exclusiveSplitLimit, boolean regression) {
    _columnDistsRegression = new int[data.columns() - 1][][];
    _columnDists = new int[data.columns()-1][][];
    _regression = regression;
    if (!regression) {
      _random = Utils.getRNG(seed);
      // first create the column distributions
      for (int i = 0; i < _columnDists.length; ++i)
        if (!data.isIgnored(i))
          _columnDists[i] = new int[data.columnArity(i)+1][data.classes()];
      // create the columns themselves
      _features = new int[featuresPerSplit];
      _remembered = null;
      _classWt = data.classWt();  // Class weights
      _exclusiveSplitLimit = exclusiveSplitLimit;
    } else {
      _random = Utils.getRNG(seed);
      for (int i = 0; i < _columnDistsRegression.length; ++i)
        if(!data.isIgnored(i)) {
          DataAdapter.Col c = data._dapt._c[i];
          int colBins = c._isByte ? Utils.maxValue(c._rawB) : c._binned.length;
          _columnDistsRegression[i] = new int[colBins + 1][ data.columnArityOfClassCol()];
        }
      _features = new int[featuresPerSplit];
      _remembered = null;
      _classWt = data.classWt();
      _exclusiveSplitLimit = exclusiveSplitLimit;
    }
  }

  /** Remember features used for this split so we can grab different features
   * and avoid these useless ones. Returns false if no more features are left. */
  boolean rememberFeatures(Data data) {
    if( _remembered == null ) _remembered = new HashSet<Integer>();
    for(int f : _features) if ( f != -1 ) _remembered.add(f);
    for(int i=0;i<data.columns()-1;i++) if(isColumnUsable(data,i)) return true;
    return false;
  }
  /**We are done with this particular split and can forget the features we have
   * used to compute it.*/
  void forgetFeatures() { _remembered = null; }
  /**Features can be used in a split if they are not already used. */
  private boolean isColumnUsable(Data d, int i) {
    assert i < d.columns()-1;   // Last column is class
    return !d.isIgnored(i) && (_remembered == null || !_remembered.contains(i)) && d.colMaxIdx(i) != d.colMinIdx(i);
  }

  /** Resets the statistic for the next split. Pick a subset of the features and zero out
   * distributions. Implementation uses reservoir sampling (http://en.wikipedia.org/wiki/Reservoir_sampling)
   * to select features.  Features that (a) have been marked as ignore, (b) that have already been
   * tried at this split, (c) the class feature, will not be selected. */
  void reset(Data data, long seed, boolean regression) {
    if (!regression) {
        _random = Utils.getRNG(_seed = seed);
        int i = 0, j = 0, featuresPerSplit = _features.length;
        Arrays.fill(_features, -1);
        for( ; j < featuresPerSplit && i < data.columns()-1; i++) if (isColumnUsable(data, i)) _features[j++] = i;
        for( ; i < data.columns()-1; i++ ) {
          if( !isColumnUsable(data, i) ) continue;
          int k = _random.nextInt(j+1); // Reservoir sampling: take a random number in the interval [0,index] (inclusive)
          if( k < featuresPerSplit ) _features[k] = i;
          j++;
        }
        for( int f : _features) if (f != -1) for( int[] d: _columnDists[f]) Arrays.fill(d,0);  // reset the column distributions
    } else {
      _random = Utils.getRNG(_seed = seed);
      int i = 0, j = 0, featuresPerSplit = _features.length;
      Arrays.fill(_features, -1);
      for( ; j < featuresPerSplit && i < data.columns()-1; i++) if (isColumnUsable(data, i)) _features[j++] = i;
      for( ; i < data.columns()-1; i++ ) {
        if( !isColumnUsable(data, i) ) continue;
        int k = _random.nextInt(j+1); // Reservoir sampling: take a random number in the interval [0,index] (inclusive)
        if( k < featuresPerSplit ) _features[k] = i;
        j++;
      }
      for( int f : _features) if (f != -1) for( int[] d: _columnDistsRegression[f]) Arrays.fill(d,0);  // reset the column distributions
    }
  }

  /** Adds the given row to the statistic. Updates the column distributions for
   * the analyzed columns. */
  void addQ(Row row, boolean regression) {
    final int cls = row.classOf(); //regression ? -1 : row.classOf();
    for (int f : _features)
      if ( f != -1) {
        if (row.isValid() && row.hasValidValue(f)) {
          if (!regression) {
            short val = row.getEncodedColumnValue(f);
            _columnDists[f][val][cls]++;
          } else {
            short val = row.getEncodedColumnValue(f);
            if (val == DataAdapter.BAD) continue;
            int resp = row.getEncodedClassColumnValue();
            if (resp == DataAdapter.BAD) continue;
//            short val2 = row.getEncodedClassColumnValue();
            _columnDistsRegression[f][val][resp]++; // = row.getRawClassColumnValueFromBin();
          }
        }
      }
  }

  /** Adds the given row to the statistic.  Updates the column distributions for
   * the analyzed columns.  This version knows the row is always valid (always
   * has a valid class), and is hand-inlined.  */
//  void addQValid( final int cls, final int ridx, final DataAdapter.Col cs[]) {
//    for (int f : _features) {
//      if (f == -1) break;
//      short[] bins = cs[f]._binned; // null if byte col, otherwise bin#
//      int val;
//      if (bins != null) {      // binned?
//        val = bins[ridx];       // Grab bin#
//        if (val == DataAdapter.BAD) continue; // ignore bad rows
//      } else {                  // not binned?
//        val = (0xFF & cs[f]._rawB[ridx]); // raw byte value, has no bad rows
//      }
//      _columnDists[f][val][cls]++;
//    }
//  }

  /** Apply any class weights to the distributions.*/
  void applyClassWeights() {
    if( _classWt == null ) return;
    if (_regression) return;
    for( int f : _features ) // For all columns, get the distribution
      if ( f != -1)
        for( int[] clss : _columnDists[f] ) // For all distributions, get the class distribution
          for( int cls=0; cls<clss.length; cls++ )
            clss[cls] = (int)(clss[cls]*_classWt[cls]); // Scale by the class weights
  }

  /** Calculates the best split and returns it. The split can be either a split
   * which is a node where all rows with given column value smaller or equal to
   * the split value will go to the left and all greater will go to the right.
   * Or it can be an exclusion split, where all rows with column value equal to
   * split value go to the left and all others go to the right.
   */
  Split split(Data d, boolean expectLeaf) {
    if(!_regression) {
      int[] dist = new int[d.classes()];
      boolean valid = false;
      for(int f : _features) valid |= f != -1;
      if (!valid) return Split.defaultSplit(); // there are no features left...
      int distWeight = aggregateColumn(_features[0], dist);    // initialize the distribution array
      int m = Utils.maxIndex(dist, _random);
      if( expectLeaf || (dist[m] == distWeight ))  return Split.constant(m); // check if we are leaf node
      Split bestSplit = Split.defaultSplit();
      for( int f : _features ) {  // try the splits
        if ( f == -1 ) continue;
        Split s = pickAndSplit(d,f, dist, distWeight, _random);
        if( s.betterThan(bestSplit) ) bestSplit = s;
      }
      if( !bestSplit.isImpossible() ) return bestSplit;
      if( !rememberFeatures(d) ) return bestSplit;  // Enough features to try again?
      reset(d,_seed+(1L<<16), _regression);  // Reset with new features
      for(Row r: d)  addQ(r, _regression);   // Reload the distributions
      applyClassWeights();      // Weight the distributions
      return split(d,expectLeaf);
    } else {
      float[] dist = new float[d.columnArityOfClassCol()];
      boolean valid = false;
      for(int f: _features) valid |= f != -1;
      if(!valid) return Split.defaultSplit();
      float unweightedMean = aggregateColumn(_features[0], dist);
      int m = Utils.maxIndex(dist, _random);
      if(expectLeaf || (dist[m] == unweightedMean)) return Split.constant(m);
      Split bestSplit = Split.defaultSplit();
      for (int f: _features) {
        if (f == -1) continue;
        Split s = pickAndSplit(d,f,dist,unweightedMean,_random);
        if (s.betterThan(bestSplit)) bestSplit = s;
      }
      if (!bestSplit.isImpossible()) return bestSplit;
      if (!rememberFeatures(d)) return bestSplit;
      reset(d, _seed+(1L<<16), _regression);
      for(Row r: d) addQ(r,_regression);
      return split(d, expectLeaf);
    }
  }

  private Split pickAndSplit(Data d, int col, int[] dist, int distWeight, Random rand) {
    boolean isBool = d.columnArity(col) == 1; //screwed up api, 1 means 2.
    boolean isBig = d.columnArity(col) > _exclusiveSplitLimit;
    boolean isFloat = d.isFloat(col);
    if (isBool) return eqSplit(col,d,dist,distWeight,_random);
    else if (isBig || isFloat) return ltSplit(col,d, dist, distWeight, _random);
    else {
      Split s1 = eqSplit(col,d,dist,distWeight,_random);
      if (s1.isImpossible()) return s1;
      Split s2 = ltSplit(col,d, dist, distWeight, _random);
      return s1.betterThan(s2) ? s1 : s2;
    }
  }

  private Split pickAndSplit(Data d, int col, float[] dist, float distWeight, Random rand) {
    boolean isBool = d.columnArity(col) == 1; //screwed up api, 1 means 2.
    boolean isBig = d.columnArity(col) > _exclusiveSplitLimit;
    boolean isFloat = d.isFloat(col);
    if (isBool) return eqSplit(col,d,dist,distWeight,_random);
    else if (isBig || isFloat) return ltSplit(col,d, dist, distWeight, _random);
    else {
      Split s1 = eqSplit(col,d,dist,distWeight,_random);
      if (s1.isImpossible()) return s1;
      Split s2 = ltSplit(col,d, dist, distWeight, _random);
      return s1.betterThan(s2) ? s1 : s2;
    }
  }

}
