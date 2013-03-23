package hex.rf;

import hex.rf.Data.Row;
import hex.rf.Tree.SplitNode.SplitInfo;

import java.io.IOException;
import java.util.*;

import jsr166y.CountedCompleter;
import jsr166y.RecursiveTask;
import water.*;
import water.Timer;
import water.util.Utils;

public class Tree extends CountedCompleter {
  static public enum StatType { ENTROPY, GINI };

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
  final Data _data;             // Data source
  final int _data_id;           // Data-subset identifier (so trees built on this subset are not validated on it)
  final int _max_depth;         // Tree-depth cutoff
  final int _numSplitFeatures;  // Number of features to check at each splitting (~ split features)
  INode _tree;                  // Root of decision tree
  ThreadLocal<Statistic>[] _stats  = new ThreadLocal[2];
  final Job _job;
  final int _alltrees;          // Number of trees expected to build a complete model
  final long _seed;             // Pseudo random seed: used to playback sampling
  final int _numrows;           // Used to playback sampling
  final float _sample;          // Sample rate
  boolean _stratify;
  int [] _strata;
  transient int _verbose ;
  int _exclusiveSplitLimit;

  // Constructor used to define the specs when building the tree from the top
  public Tree( Data data, int max_depth, double min_error_rate, StatType stat, int numSplitFeatures, long seed, Job job, int treeId, int alltrees, float sample, int rowsize, boolean stratify, int [] strata, int verbose, int exclusiveSplitLimit) {
    _type             = stat;
    _data             = data;
    _data_id          = treeId; //data.dataId();
    _max_depth        = max_depth-1;
    _numSplitFeatures = numSplitFeatures;
    _job              = job;
    _alltrees         = alltrees;
    _seed             = seed;
    assert sample <= 1.0f : "Stratify sampling should in interval (0,1] but it is " + sample;
    _sample           = sample;
    _numrows          = rowsize;
    _stratify         = stratify;
    _strata           = strata;
    _verbose          = verbose;
    _exclusiveSplitLimit = exclusiveSplitLimit;
  }

  // Oops, uncaught exception
  public boolean onExceptionalCompletion( Throwable ex, CountedCompleter _) {
    ex.printStackTrace();
    return true;
  }

  private Statistic getStatistic(int index, Data data, long seed, int exclusiveSplitLimit) {
    Statistic result = _stats[index].get();
    if( result==null ) {
      result  = _type == StatType.GINI ?
          new GiniStatistic   (data,_numSplitFeatures, _seed, exclusiveSplitLimit) :
          new EntropyStatistic(data,_numSplitFeatures, _seed, exclusiveSplitLimit);
      _stats[index].set(result);
    }
    result.forgetFeatures();   // All new features
    result.reset(data, seed);
    return result;
  }

  StringBuffer computeStatistics() {
    StringBuffer sb = new StringBuffer();
    ArrayList<SplitInfo>[] stats = new ArrayList[_data.columns()];
    for (int i = 0; i < _data.columns()-1; i++) stats[i] = new ArrayList<Tree.SplitNode.SplitInfo>();
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
  public void compute() {
    if(!_job.cancelled()) {
      Timer timer = new Timer();
      _stats[0] = new ThreadLocal<Statistic>();
      _stats[1] = new ThreadLocal<Statistic>();
      Data d = (true && _stratify)?_data.sample(_strata,_seed):_data.sample(_sample,_seed,_numrows);
      Statistic left = getStatistic(0, d, _seed, _exclusiveSplitLimit);
      // calculate the split
      for( Row r : d ) left.addQ(r);
      left.applyClassWeights();   // Weight the distributions
      Statistic.Split spl = left.split(d, false);
      _tree = spl.isLeafNode()
        ? new LeafNode(_data.unmapClass(spl._split), d.rows())
        : new FJBuild (spl, d, 0, _seed).compute();

      if (_verbose > 1)
        Utils.pln(computeStatistics().toString());
      _stats = null; // GC

      // Atomically improve the Model as well
      appendKey(_job.dest(),toKey());
      StringBuilder sb = new StringBuilder("[RF] Tree : ").append(_data_id+1);
      sb.append(" d=").append(_tree.depth()).append(" leaves=").append(_tree.leaves()).append(" done in ").append(timer).append('\n');
      Utils.pln(_tree.toString(sb,  _verbose > 0 ? Integer.MAX_VALUE : 200).toString());
    }
    // Wait for completation
    tryComplete();
  }

  // Stupid static method to make a static anonymous inner class
  // which serializes "for free".
  static void appendKey(Key model, final Key tKey) {
    new TAtomic<RFModel>() {
      @Override public RFModel atomic(RFModel old) {
        if(old == null) return null;
        return RFModel.make(old,tKey);
      }
    }.invoke(model);
  }

  private class FJBuild extends RecursiveTask<INode> {
    final Statistic.Split _split;
    final Data _data;
    final int _depth;
    final long _seed;

    FJBuild(Statistic.Split split, Data data, int depth, long seed) {
      _split = split;  _data = data; _depth = depth; _seed = seed;
    }

    @Override public INode compute() {
      Statistic left = getStatistic(0,_data, _seed + LTSS_INIT, _exclusiveSplitLimit); // first get the statistics
      Statistic rite = getStatistic(1,_data, _seed + RTSS_INIT, _exclusiveSplitLimit);
      Data[] res = new Data[2]; // create the data, node and filter the data
      int c = _split._column, s = _split._split;
      assert c != _data.columns()-1; // Last column is the class column
      SplitNode nd = _split.isExclusion() ?
        new ExclusionNode(c, s, _data.colName(c), _data.unmap(c,s)) :
        new SplitNode    (c, s, _data.colName(c), _data.unmap(c,s));
      _data.filter(nd,res,left,rite);
      FJBuild fj0 = null, fj1 = null;
      Statistic.Split ls = left.split(res[0], _depth >= _max_depth); // get the splits
      Statistic.Split rs = rite.split(res[1], _depth >= _max_depth);
      if (ls.isLeafNode() || ls.isImpossible())
            nd._l = new LeafNode(_data.unmapClass(ls._split), res[0].rows()); // create leaf nodes if any
      else  fj0 = new FJBuild(ls,res[0],_depth+1, _seed + LTS_INIT);
      if (rs.isLeafNode() || rs.isImpossible())
            nd._r = new LeafNode(_data.unmapClass(rs._split), res[1].rows());
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
    abstract int classify(Row r);
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
    final int _class;    // A category reported by the inner node
    final int _rows;     // A number of classified rows (only meaningful for training)
    /**
     * Construct a new leaf node.
     * @param c - a particular value of class predictor from interval [0,N-1]
     * @param rows - numbers of rows with the predictor value
     */
    LeafNode(int c, int rows) {
      assert 0 <= c && c <= 254; // sanity check
      _class = c;               // Class from 0 to _N-1
      _rows  = rows;
    }
    @Override public int depth()  { return 0; }
    @Override public int leaves() { return 1; }
    @Override public void computeStats(ArrayList<SplitInfo>[] stats) { /* do nothing for leaves */ }
    @Override public int classify(Row r) { return _class; }
    @Override public StringBuilder toString(StringBuilder sb, int n ) { return sb.append('[').append(_class).append(']').append('{').append(_rows).append('}'); }
    @Override public void print(TreePrinter p) throws IOException { p.printNode(this); }
    @Override void write( AutoBuffer bs ) {
      bs.put1('[');             // Leaf indicator
      bs.put1(_class);
    }
    @Override int size_impl( ) { return 2; } // 2 bytes in serialized form
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

    @Override int classify(Row r) { return r.getEncodedColumnValue(_column) <= _split ? _l.classify(r) : _r.classify(r);  }
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

    @Override int classify(Row r) { return r.getEncodedColumnValue(_column) == _split ? _l.classify(r) : _r.classify(r); }
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

  public int classify(Row r) { return _tree.classify(r); }
  public String toString()   { return _tree.toString(); }
  public int leaves() { return _tree.leaves(); }
  public int depth() { return _tree.depth(); }

  // Write the Tree to a random Key homed here.
  public Key toKey() {
    AutoBuffer bs = new AutoBuffer();
    bs.put4(_data_id);
    bs.put8(_seed);
    _tree.write(bs);
    Key key = Key.make(UUID.randomUUID().toString(),(byte)1,Key.DFJ_INTERNAL_USER, H2O.SELF);
    DKV.put(key,new Value(key, bs.buf()));
    return key;
  }

  /** Classify this serialized tree - withOUT inflating it to a full tree.
     Use row 'row' in the dataset 'ary' (with pre-fetched bits 'databits')
     Returns classes from 0 to N-1*/
  public static short classify( AutoBuffer ts, ValueArray ary, AutoBuffer databits, int row, int modelDataMap[], short badData ) {
    ts.get4();    // Skip tree-id
    ts.get8();    // Skip seed
    byte b;

    while( (b = (byte) ts.get1()) != '[' ) { // While not a leaf indicator
      assert b == '(' || b == 'S' || b == 'E';
      int col = modelDataMap[ts.get2()]; // Column number in model-space mapped to data-space
      float fcmp = ts.get4f();  // Float to compare against
      if( ary.isNA(databits, row, col) ) return badData;
      float fdat = (float)ary.datad(databits, row, col);
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
    return (short) ( ts.get1()&0xFF );      // Return the leaf's class
  }

  // Classify on the compressed tree bytes, from the pre-packed double data
  public static double classify( AutoBuffer ts, double[] ds, double badat ) {
    ts.get4();    // Skip tree-id
    ts.get8();    // Skip seed
    byte b;

    while( (b = (byte) ts.get1()) != '[' ) { // While not a leaf indicator
      assert b == '(' || b == 'S' || b == 'E';
      int col = ts.get2(); // Column number in model-space
      float fcmp = ts.get4f();  // Float to compare against
      if( Double.isNaN(ds[col]) ) return badat;
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
    return ts.get1()&0xFF;      // Return the leaf's class
  }

  public static int dataId( byte[] bits) { return UDP.get4(bits, 0); }
  public static long seed ( byte[] bits) { return UDP.get8(bits, 4); }

  /** Abstract visitor class for serialized trees.*/
  public static abstract class TreeVisitor<T extends Exception> {
    protected TreeVisitor<T> leaf( int tclass ) throws T { return this; }
    protected TreeVisitor<T>  pre( int col, float fcmp, int off0, int offl, int offr ) throws T { return this; }
    protected TreeVisitor<T>  mid( int col, float fcmp ) throws T { return this; }
    protected TreeVisitor<T> post( int col, float fcmp ) throws T { return this; }
    long  result( ) { return 0; }
    protected final AutoBuffer _ts;
    public TreeVisitor( AutoBuffer tbits ) {
      _ts = tbits;
      _ts.get4();               // Skip tree ID
      _ts.get8();               // Skip seed
    }

    public final TreeVisitor<T> visit() throws T {
      byte b = (byte) _ts.get1();
      if( b == '[' ) return leaf(_ts.get1()&0xFF);
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
  public static long depth_leaves( AutoBuffer tbits ) {
    return new TreeVisitor<RuntimeException>(tbits) {
      int _maxdepth, _depth, _leaves;
      protected TreeVisitor leaf(int tclass ) { _leaves++; if( _depth > _maxdepth ) _maxdepth = _depth; return this; }
      protected TreeVisitor pre (int col, float fcmp, int off0, int offl, int offr ) { _depth++; return this; }
      protected TreeVisitor post(int col, float fcmp ) { _depth--; return this; }
      long result( ) {return ((long)_maxdepth<<32) | _leaves; }
    }.visit().result();
  }
}
