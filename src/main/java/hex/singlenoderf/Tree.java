package hex.singlenoderf;

import hex.singlenoderf.Data.Row;
import hex.singlenoderf.Tree.SplitNode.SplitInfo;
import jsr166y.CountedCompleter;
import jsr166y.RecursiveTask;
import water.*;
import water.H2O.H2OCountedCompleter;
import water.fvec.Chunk;
import water.fvec.Frame;
import water.util.Log;
import water.util.Log.Tag.Sys;
import water.util.Utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

public class Tree extends H2OCountedCompleter {
  static public enum SelectStatType {ENTROPY, GINI};
  static public enum StatType { ENTROPY, GINI, MSE};

  /** Left and right seed initializer number for statistics */
  public static final long LTSS_INIT = 0xe779aef0a6fd0c16L;
  public static final long RTSS_INIT = 0x5e63c6377e5297a7L;
  /** Left and right seed initializer number for subtrees */
  public static final long RTS_INIT = 0xa7a34721109d3708L;
  public static final long LTS_INIT = 0x264ccf88cf4dec32L;
  /** If the number of rows is higher then given number, fork-join is used to build
   * subtrees, else subtrees are built sequentially
   */
  public static final int ROWS_FORK_TRESHOLD = 1<<11;

  final StatType _type;         // Flavor of split logic
  final Data _data;         // Data source
  final hex.singlenoderf.Sampling _sampler;      // Sampling strategy
  final int      _data_id;      // Data-subset identifier (so trees built on this subset are not validated on it)
  final int      _maxDepth;     // Tree-depth cutoff
  final int      _numSplitFeatures;  // Number of features to check at each splitting (~ split features)
  INode          _tree;         // Root of decision tree
  ThreadLocal<hex.singlenoderf.Statistic>[] _stats  = new ThreadLocal[2];
  final Job      _job;          // DRF job building this tree
  final long     _seed;         // Pseudo random seed: used to playback sampling
  int            _exclusiveSplitLimit;
  int            _verbose;
  final byte     _producerId;   // Id of node producing this tree
  final boolean  _regression; // If true, will build regression tree.

  /**
   * Constructor used to define the specs when building the tree from the top.
   */
  public Tree(final Job job, final Data data, byte producerId, int maxDepth, StatType stat,
              int numSplitFeatures, long seed, int treeId, int exclusiveSplitLimit,
              final hex.singlenoderf.Sampling sampler, int verbose, boolean regression) {
    _job              = job;
    _data             = data;
    _type             = stat;
    _data_id          = treeId;
    _maxDepth         = maxDepth-1;
    _numSplitFeatures = numSplitFeatures;
    _seed             = seed;
    _sampler          = sampler;
    _exclusiveSplitLimit = exclusiveSplitLimit;
    _verbose          = verbose;
    _producerId       = producerId;
    _regression       = regression;
  }

  // Oops, uncaught exception
  public boolean onExceptionalCompletion( Throwable ex, CountedCompleter _) {
    ex.printStackTrace();
    return true;
  }

  private hex.singlenoderf.Statistic getStatistic(int index, Data data, long seed, int exclusiveSplitLimit) {
    hex.singlenoderf.Statistic result = _stats[index].get();
    if (_type == StatType.MSE) {
      if(!_regression) {
        throw H2O.unimpl();
      }
      if (result == null) {
      result = new MSEStatistic(data, _numSplitFeatures, _seed, exclusiveSplitLimit);
      _stats[index].set(result);
      }
      result.forgetFeatures();
    } else {
    if( result==null ) {
      result  = _type == StatType.GINI ?
              new GiniStatistic(data,_numSplitFeatures, _seed, exclusiveSplitLimit) :
              new EntropyStatistic(data,_numSplitFeatures, _seed, exclusiveSplitLimit);
      _stats[index].set(result);
    }
    }
    result.forgetFeatures();   // All new features
    result.reset(data, seed, _regression);
    return result;
  }

  private StringBuffer computeStatistics() {
    StringBuffer sb = new StringBuffer();
    ArrayList<SplitInfo>[] stats = new ArrayList[_data.columns()];
    for (int i = 0; i < _data.columns()-1; i++) stats[i] = new ArrayList<SplitInfo>();
    _tree.computeStats(stats);
    for (int i = 0; i < _data.columns()-1; i++) {
      String colname = _data.colName(i);
      ArrayList<SplitInfo> colSplitStats = stats[i];
      Collections.sort(colSplitStats);
      int usage = 0;
      for (SplitInfo si : colSplitStats) {
        usage += si._used;
      }
      sb.append(colname).append(':').append(usage).append("x");
      for (SplitInfo si : colSplitStats) {
        sb.append(", <=").append(Utils.p2d(si.splitNode().split_value())).append('{').append(si.affectedLeaves()).append("}x"+si._used+" ");
      }

      sb.append('\n');
    }
    return sb;
  }

  // Actually build the tree
  @Override public void compute2() {
    if(Job.isRunning(_job.self())) {
      Timer timer    = new Timer();
      _stats[0]      = new ThreadLocal<hex.singlenoderf.Statistic>();
      _stats[1]      = new ThreadLocal<hex.singlenoderf.Statistic>();
      Data d = _sampler.sample(_data, _seed);
      hex.singlenoderf.Statistic left = getStatistic(0, d, _seed, _exclusiveSplitLimit);
      // calculate the split
      for( Row r : d ) left.addQ(r, _regression);
      if (!_regression)
        left.applyClassWeights();   // Weight the distributions
      hex.singlenoderf.Statistic.Split spl = left.split(d, false);
      if(spl.isLeafNode()) {
        if(_regression) {
          float av = d.computeAverage();
          _tree = new LeafNode(-1, d.rows(), av);
        } else {
          _tree =  new LeafNode(_data.unmapClass(spl._split), d.rows(),-1);
        }
      } else {
        _tree = new FJBuild (spl, d, 0, _seed).compute();
      }

//      if (_verbose > 1)  Log.info(Sys.RANDF,computeStatistics().toString());
      _stats = null; // GC

      // Atomically improve the Model as well
      appendKey(_job.dest(),toKey(), _verbose > 10 ? _tree.toString(new StringBuilder(""), Integer.MAX_VALUE).toString() : "");
      StringBuilder sb = new StringBuilder("[RF] Tree : ").append(_data_id+1);
      sb.append(" d=").append(_tree.depth()).append(" leaves=").append(_tree.leaves()).append(" done in ").append(timer).append('\n');
      Log.info(sb.toString());
      if (_verbose > 10) {
        Log.info(Sys.RANDF, _tree.toString(sb, Integer.MAX_VALUE).toString());
      }
    }
    // Wait for completation
    tryComplete();
  }

  // Stupid static method to make a static anonymous inner class
  // which serializes "for free".
  static void appendKey(Key model, final Key tKey, final String tString) {
    final int selfIdx = H2O.SELF.index();
    new TAtomic<SpeeDRFModel>() {
      @Override public SpeeDRFModel atomic(SpeeDRFModel old) {
        if(old == null) return null;
        return SpeeDRFModel.make(old, tKey, selfIdx, tString);
      }
    }.invoke(model);
  }

  private class FJBuild extends RecursiveTask<INode> {
    final hex.singlenoderf.Statistic.Split _split;
    final Data _data;
    final int _depth;
    final long _seed;

    FJBuild(hex.singlenoderf.Statistic.Split split, Data data, int depth, long seed) {
      _split = split;  _data = data; _depth = depth; _seed = seed;
    }

    @Override public INode compute() {
      hex.singlenoderf.Statistic left = getStatistic(0,_data, _seed + LTSS_INIT, _exclusiveSplitLimit); // first get the statistics
      hex.singlenoderf.Statistic rite = getStatistic(1,_data, _seed + RTSS_INIT, _exclusiveSplitLimit);
      Data[] res = new Data[2]; // create the data, node and filter the data
      int c = _split._column, s = _split._split;
      assert c != _data.columns()-1; // Last column is the class column
      SplitNode nd = _split.isExclusion() ?
              new ExclusionNode(c, s, _data.colName(c), _data.unmap(c,s)) :
              new SplitNode    (c, s, _data.colName(c), _data.unmap(c,s));
      _data.filter(nd,res,left,rite);
      FJBuild fj0 = null, fj1 = null;
      hex.singlenoderf.Statistic.Split ls = left.split(res[0], _depth >= _maxDepth); // get the splits
      hex.singlenoderf.Statistic.Split rs = rite.split(res[1], _depth >= _maxDepth);
      if (ls.isLeafNode() || ls.isImpossible()) {
        if (_regression) {
          float av = res[0].computeAverage();
          nd._l = new LeafNode(-1, res[0].rows(), av);
        } else {
          nd._l = new LeafNode(_data.unmapClass(ls._split), res[0].rows(),-1); // create leaf nodes if any
        }
      }
      else  fj0 = new FJBuild(ls,res[0],_depth+1, _seed + LTS_INIT);
      if (rs.isLeafNode() || rs.isImpossible()) {
        if (_regression) {

          float av = res[1].computeAverage();
          nd._r = new LeafNode(-1, res[1].rows(), av);
        } else {
        nd._r = new LeafNode(_data.unmapClass(rs._split), res[1].rows(),-1);
        }
      }
      else  fj1 = new  FJBuild(rs,res[1],_depth+1, _seed - RTS_INIT);
      // Recursively build the splits, in parallel
      if (_data.rows() > ROWS_FORK_TRESHOLD) {
        if( fj0 != null &&        (fj1!=null ) ) fj0.fork();
        if( fj1 != null ) nd._r = fj1.compute();
        if( fj0 != null ) nd._l = (fj1!=null ) ? fj0.join() : fj0.compute();
      } else {
        if( fj1 != null ) nd._r = fj1.compute();
        if( fj0 != null ) nd._l = fj0.compute();
      }
      /* Degenerate trees such as the following  can occur when an impossible split was found.
              y<=1.1        This is unusual enough to ignore.
              /    \
           y<=1.0   0
           / \
          1   1   */
      // FIXME there is still issue with redundant trees!
      return nd;
    }
  }

  public static abstract class INode {
    abstract float classify(Row r);
    abstract int depth();       // Depth of deepest leaf
    abstract int leaves();      // Number of leaves
    abstract void computeStats(ArrayList<SplitInfo>[] stats);
    abstract StringBuilder toString( StringBuilder sb, int len );
    final boolean isLeaf() { return depth() == 0; }

    public abstract void print(TreePrinter treePrinter) throws IOException;
    abstract void write( AutoBuffer bs );
    int _size;                  // Byte-size in serialized form
    final int size( ) { return _size==0 ? (_size=size_impl()) : _size;  }
    abstract int size_impl();
  }

  /** Leaf node that for any row returns its the data class it belongs to. */
  static class LeafNode extends INode {
    final float _c;      // The continuous response
    final int _class;    // A category reported by the inner node
    final int _rows;     // A number of classified rows (only meaningful for training)
    /**
     * Construct a new leaf node.
     * @param c - a particular value of class predictor from interval [0,N-1]
     *                OR possibly -1 for regression
     * @param r - A continous response.
     * @param rows - numbers of rows with the predictor value
     */
    LeafNode(int c, int rows, float r) {
      assert -1 <= c && c <= 254; // sanity check
      _class = c;               // Class from 0 to _N-1
      _rows  = rows;
      _c = r;
    }

    @Override public int depth()  { return 0; }
    @Override public int leaves() { return 1; }
    @Override public void computeStats(ArrayList<SplitInfo>[] stats) { /* do nothing for leaves */ }
    @Override public float classify(Row r) { if (_class == -1) { return _c; } else return (float)_class; }
    @Override public StringBuilder toString(StringBuilder sb, int n ) { return sb.append('[').append(_class).append(']').append('{').append(_rows).append('}'); }
    @Override public void print(TreePrinter p) throws IOException { p.printNode(this); }
    @Override void write( AutoBuffer bs ) {
      bs.put1('[');             // Leaf indicator
      if (_class == -1) {
        bs.put4f(_c);
      } else {
        bs.put1(_class);
      }
    }
    @Override int size_impl( ) {
      if (_class == -1) {
        return 5;
      }
      return 2; } // 2 bytes in serialized form
  }

  /** Gini classifier node. */
  static class SplitNode extends INode {
    final int _column;
    final int _split;
    INode _l, _r;
    int _depth, _leaves, _size;
    String _name;
    float _originalSplit;

    public SplitNode(int column, int split, String columnName, float originalSplit) {
      _name = columnName;
      _column = column;
      _split = split;
      _originalSplit = originalSplit;
    }

    static class SplitInfo implements Comparable<SplitInfo> {
      /**first node which introduce split*/
      final SplitNode _splitNode;
      int _affectedLeaves;
      int _used;
      SplitInfo (SplitNode splitNode, int affectedLeaves) { _splitNode = splitNode; _affectedLeaves = affectedLeaves; _used = 1; }
      final SplitNode splitNode() { return _splitNode; }
      final int affectedLeaves()  { return _affectedLeaves; }

      static SplitInfo info(SplitNode splitNode, int leavesAffected) {
        return new SplitInfo(splitNode, leavesAffected);
      }

      @Override public int compareTo(SplitInfo o) {
        if (o._affectedLeaves == _affectedLeaves)  return 0;
        else if (_affectedLeaves < o._affectedLeaves) return 1;
        else return -1;
      }
    }

    @Override float classify(Row r) { return r.getEncodedColumnValue(_column) <= _split ? _l.classify(r) : _r.classify(r);  }
    @Override public int depth() { return  _depth != 0 ? _depth : (_depth = Math.max(_l.depth(), _r.depth()) + 1); }
    @Override public int leaves() { return  _leaves != 0 ? _leaves : (_leaves=_l.leaves() + _r.leaves()); }
    @Override void computeStats(ArrayList<SplitInfo>[] stats) {
      SplitInfo splitInfo = null;
      // Find the same split
      for (SplitInfo si : stats[_column]) {
        if (si.splitNode()._split == _split) {
          splitInfo = si;
          break;
        }
      }
      if (splitInfo == null) {
        stats[_column].add(SplitInfo.info(this, _leaves));
      } else {
        splitInfo._affectedLeaves += leaves();
        splitInfo._used += 1;
      }
      _l.computeStats(stats);
      _r.computeStats(stats);
    }
    // Computes the original split-value, as a float.  Returns a float to keep
    // the final size small for giant trees.
    protected final float split_value() { return _originalSplit; }
    @Override public void print(TreePrinter p) throws IOException { p.printNode(this); }
    @Override public String toString() { return "S "+_column +"<=" + _originalSplit + " ("+_l+","+_r+")";  }
    @Override public StringBuilder toString( StringBuilder sb, int n ) {
      sb.append(_name).append("<=").append(Utils.p2d(split_value())).append('@').append(leaves()).append(" (");
      if( sb.length() > n ) return sb;
      sb = _l.toString(sb,n).append(',');
      if( sb.length() > n ) return sb;
      sb = _r.toString(sb,n).append(')');
      return sb;
    }

    @Override void write( AutoBuffer bs ) {
      bs.put1('S');             // Node indicator
      assert Short.MIN_VALUE <= _column && _column < Short.MAX_VALUE;
      bs.put2((short) _column);
      bs.put4f(split_value());
      int skip = _l.size(); // Drop down the amount to skip over the left column
      if( skip <= 254 )  bs.put1(skip);
      else { bs.put1(0); bs.put3(skip); }
      _l.write(bs);
      _r.write(bs);
    }
    @Override public int size_impl( ) {
      // Size is: 1 byte indicator, 2 bytes col, 4 bytes val, the skip, then left, right
      return _size=(1+2+4+(( _l.size() <= 254 ) ? 1 : 4)+_l.size()+_r.size());
    }
    public boolean isIn(final Row row) {  return row.getEncodedColumnValue(_column) <= _split; }
    public final boolean canDecideAbout(final Row row) { return row.hasValidValue(_column); }
  }

  /** Node that classifies one column category to the left and the others to the right. */
  static class ExclusionNode extends SplitNode {

    public ExclusionNode(int column, int val, String cname, float origSplit) { super(column,val,cname,origSplit);  }

    @Override float classify(Row r) { return r.getEncodedColumnValue(_column) == _split ? _l.classify(r) : _r.classify(r); }
    @Override public void print(TreePrinter p) throws IOException { p.printNode(this); }
    @Override public String toString() { return "E "+_column +"==" + _split + " ("+_l+","+_r+")"; }
    @Override public StringBuilder toString( StringBuilder sb, int n ) {
      sb.append(_name).append("==").append(_split).append('@').append(leaves()).append(" (");
      if( sb.length() > n ) return sb;
      sb = _l.toString(sb,n).append(',');
      if( sb.length() > n ) return sb;
      sb = _r.toString(sb,n).append(')');
      return sb;
    }
    public int size_impl( ) {
      // Size is: 1 byte indicator, 2 bytes col, 4 bytes val, the skip, then left, right
      return _size=(1+2+4+(( _l.size() <= 254 ) ? 1 : 4)+_l.size()+_r.size());
    }

    @Override void write( AutoBuffer bs ) {
      bs.put1('E');             // Node indicator
      assert Short.MIN_VALUE <= _column && _column < Short.MAX_VALUE;
      bs.put2((short)_column);
      bs.put4f(split_value());
      int skip = _l.size(); // Drop down the amount to skip over the left column
      if( skip <= 254 )  bs.put1(skip);
      else { bs.put1(0); bs.put3(skip); }
      _l.write(bs);
      _r.write(bs);
    }
    public boolean isIn(Row row) { return row.getEncodedColumnValue(_column) == _split; }
  }

  public float classify(Row r) { return _tree.classify(r); }
  public String toString()   { return _tree.toString(); }
  public int leaves() { return _tree.leaves(); }
  public int depth() { return _tree.depth(); }

  // Write the Tree to a random Key homed here.
  public Key toKey() {
    AutoBuffer bs = new AutoBuffer();
    bs.put4(_data_id);
    bs.put8(_seed);
    bs.put1(_producerId);
    _tree.write(bs);
    Key key = Key.make((byte)1,Key.DFJ_INTERNAL_USER, H2O.SELF);
    DKV.put(key,new Value(key, bs.buf()));
    return key;
  }

  /** Classify this serialized tree - withOUT inflating it to a full tree.
   Use row 'row' in the dataset 'ary' (with pre-fetched bits 'databits')
   Returns classes from 0 to N-1*/
  public static float classify( AutoBuffer ts, Frame fr, Chunk[] chks, int row, int modelDataMap[], short badData, boolean regression ) {
    ts.get4();    // Skip tree-id
    ts.get8();    // Skip seed
    ts.get1();    // Skip producer id
    byte b;

    while( (b = (byte) ts.get1()) != '[' ) { // While not a leaf indicator
      assert b == '(' || b == 'S' || b == 'E';
      int col = modelDataMap[ts.get2()]; // Column number in model-space mapped to data-space
      float fcmp = ts.get4f();  // Float to compare against
      if( chks[col].isNA0(row) ) return badData;
      float fdat = (float)chks[col].at0(row);
      int skip = (ts.get1()&0xFF);
      if( skip == 0 ) skip = ts.get3();
      if (b == 'E') {
        if (fdat != fcmp)
          ts.position(ts.position() + skip);
      } else {
        // Picking right subtree? then skip left subtree
        if( fdat > fcmp ) ts.position(ts.position() + skip);
      }
    }
    if (regression) {
      return ts.get4f();
    }
    return (float)((short) ( ts.get1()&0xFF ));      // Return the leaf's class
  }

  // Classify on the compressed tree bytes, from the pre-packed double data
  public static double classify( AutoBuffer ts, double[] ds, double badat, boolean regression ) {
    ts.get4();    // Skip tree-id
    ts.get8();    // Skip seed
    ts.get1();    // Skip producer id
    byte b;

    while( (b = (byte) ts.get1()) != '[' ) { // While not a leaf indicator
      assert b == '(' || b == 'S' || b == 'E';
      int col = ts.get2(); // Column number in model-space
      float fcmp = ts.get4f();  // Float to compare against
      if( Double.isNaN(ds[col]) )
      {
        return badat;
      }
      float fdat = (float)ds[col];
      int skip = (ts.get1()&0xFF);
      if( skip == 0 ) skip = ts.get3();
      if (b == 'E') {
        if (fdat != fcmp)
          ts.position(ts.position() + skip);
      } else {
        // Picking right subtree? then skip left subtree
        if( fdat > fcmp ) ts.position(ts.position() + skip);
      }
    }
    if(regression) return ts.get4f();
    return ts.get1()&0xFF;      // Return the leaf's class
  }

  public static int dataId( byte[] bits) { return UDP.get4(bits, 0); }
  public static long seed ( byte[] bits) { return UDP.get8(bits, 4); }
  public static byte producerId( byte[] bits) { return bits[0+4+8]; }

  /** Abstract visitor class for serialized trees.*/
  public static abstract class TreeVisitor<T extends Exception> {
    protected TreeVisitor<T> leaf( int tclass ) throws T { return this; }
    protected TreeVisitor<T>  pre( int col, float fcmp, int off0, int offl, int offr ) throws T { return this; }
    protected TreeVisitor<T>  mid( int col, float fcmp ) throws T { return this; }
    protected TreeVisitor<T> post( int col, float fcmp ) throws T { return this; }
    protected TreeVisitor<T> leafFloat(float fl) throws T { return this; }
    long  result( ) { return 0; }
    protected final AutoBuffer _ts;
    protected final boolean _regression;
    public TreeVisitor( AutoBuffer tbits, boolean regression ) {
      _ts = tbits;
      _ts.get4(); // Skip tree ID
      _ts.get8(); // Skip seed
      _ts.get1(); // Skip producer id
      _regression = regression;
    }

    public final TreeVisitor<T> visit() throws T {
      byte b = (byte) _ts.get1();
      if( b == '[' ) {
        if (_regression) return leafFloat(_ts.get4f());
        return leaf(_ts.get1()&0xFF);
      }
      assert b == '(' || b == 'S' || b =='E' : b;
      int off0 = _ts.position()-1;    // Offset to start of *this* node
      int col = _ts.get2();     // Column number
      float fcmp = _ts.get4f(); // Float to compare against
      int skip = (_ts.get1()&0xFF);
      if( skip == 0 ) skip = _ts.get3();
      int offl = _ts.position();      // Offset to start of *left* node
      int offr = _ts.position()+skip; // Offset to start of *right* node
      return pre(col,fcmp,off0,offl,offr).visit().mid(col,fcmp).visit().post(col,fcmp);
    }
  }

  /** Return (depth<<32)|(leaves), in 1 pass. */
  public static long depth_leaves( AutoBuffer tbits, boolean regression ) {
    return new TreeVisitor<RuntimeException>(tbits, regression) {
      int _maxdepth, _depth, _leaves;
      protected TreeVisitor leafFloat(float fl) { _leaves++; if(_depth > _maxdepth) _maxdepth = _depth; return this; }
      protected TreeVisitor leaf(int tclass ) { _leaves++; if( _depth > _maxdepth ) _maxdepth = _depth; return this; }
      protected TreeVisitor pre (int col, float fcmp, int off0, int offl, int offr ) { _depth++; return this; }
      protected TreeVisitor post(int col, float fcmp ) { _depth--; return this; }
      long result( ) {return ((long)_maxdepth<<32) | _leaves; }
    }.visit().result();
  }
}
