package hex.singlenoderf;

import hex.gbm.DTree.TreeModel;
import hex.singlenoderf.Data.Row;
import hex.singlenoderf.Tree.SplitNode.SplitInfo;
import jsr166y.CountedCompleter;
import jsr166y.RecursiveTask;
import org.apache.commons.lang.NotImplementedException;
import water.*;
import water.H2O.H2OCountedCompleter;
import water.fvec.Chunk;
import water.util.Log;
import water.util.SB;
import water.util.Utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Tree extends H2OCountedCompleter {

  static public enum SelectStatType {ENTROPY, GINI, TWOING};
  static public enum StatType { ENTROPY, GINI, MSE, TWOING};

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

  final Key      _jobKey;          // DRF job building this tree
  final Key      _modelKey; // A model key of the forest

  final StatType _type;         // Flavor of split logic
  final Data _data;         // Data source
  final hex.singlenoderf.Sampling _sampler;      // Sampling strategy
  final int      _data_id;      // Data-subset identifier (so trees built on this subset are not validated on it)
  final int      _maxDepth;     // Tree-depth cutoff
  final int      _numSplitFeatures;  // Number of features to check at each splitting (~ split features)
  INode          _tree;         // Root of decision tree
  ThreadLocal<hex.singlenoderf.Statistic>[] _stats  = new ThreadLocal[2];
  final long     _seed;         // Pseudo random seed: used to playback sampling
  int            _exclusiveSplitLimit;
  int            _verbose;
  final byte     _producerId;   // Id of node producing this tree
  final boolean  _regression; // If true, will build regression tree.
  boolean        _local_mode;
  boolean        _score_pojo;
  public TreeModel.CompressedTree compressedTree;

  /**
   * Constructor used to define the specs when building the tree from the top.
   */
  public Tree(final Key jobKey, final Key modelKey, final Data data, byte producerId, int maxDepth, StatType stat,
              int numSplitFeatures, long seed, int treeId, int exclusiveSplitLimit,
              final hex.singlenoderf.Sampling sampler, int verbose, boolean regression, boolean local_mode, boolean score_pojo) {
    _jobKey           = jobKey;
    _modelKey         = modelKey;
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
    _local_mode       = local_mode;
    _score_pojo       = score_pojo;
  }

  // Oops, uncaught exception
  @Override public boolean onExceptionalCompletion( Throwable ex, CountedCompleter cc) {
//    ex.printStackTrace();
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
    if (_type == StatType.TWOING) {
      result = new TwoingStatistic(data,_numSplitFeatures,_seed,exclusiveSplitLimit);
    } else {
      result = _type == StatType.GINI ?
              new GiniStatistic(data, _numSplitFeatures, _seed, exclusiveSplitLimit) :
              new EntropyStatistic(data, _numSplitFeatures, _seed, exclusiveSplitLimit);
    }
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
    if(Job.isRunning(_jobKey)) {
      Timer timer    = new Timer();
      _stats[0]      = new ThreadLocal<hex.singlenoderf.Statistic>();
      _stats[1]      = new ThreadLocal<hex.singlenoderf.Statistic>();
      Data d = _sampler.sample(_data, _seed, _modelKey, _local_mode);
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

      _stats = null; // GC
      if(_jobKey != null && !Job.isRunning(_jobKey)) throw new Job.JobCancelledException();

      // Atomically improve the Model as well
      Key tkey = toKey();
      Key dtreeKey = null;
      if (_score_pojo) dtreeKey = toCompressedKey();
      appendKey(_modelKey, tkey, dtreeKey, _verbose > 10 ? _tree.toString(new StringBuilder(""), Integer.MAX_VALUE).toString() : "", _data_id);
//      appendKey(_modelKey, tkey, _verbose > 10 ? _tree.toString(new StringBuilder(""), Integer.MAX_VALUE).toString() : "", _data_id);
      StringBuilder sb = new StringBuilder("[RF] Tree : ").append(_data_id+1);
      sb.append(" d=").append(_tree.depth()).append(" leaves=").append(_tree.leaves()).append(" done in ").append(timer).append('\n');
      Log.info(sb.toString());
      if (_verbose > 10) {
//        Log.info(Sys.RANDF, _tree.toString(sb, Integer.MAX_VALUE).toString());
//        Log.info(Sys.RANDF, _tree.toJava(sb, Integer.MAX_VALUE).toString());
      }
    } else throw new Job.JobCancelledException();
    // Wait for completion
    tryComplete();
  }

  // Stupid static method to make a static anonymous inner class
  // which serializes "for free".
  static void appendKey(Key model, final Key tKey, final Key dtKey, final String tString, final int tree_id) {
    final int selfIdx = H2O.SELF.index();
    new TAtomic<SpeeDRFModel>() {
      @Override public SpeeDRFModel atomic(SpeeDRFModel old) {
        if(old == null) return null;
        return SpeeDRFModel.make(old, tKey, dtKey, selfIdx, tString, tree_id);
      }
    }.invoke(model);
  }

  public static String deserialize(byte[] bytes) {
    AutoBuffer ab = new AutoBuffer(bytes);
    SB sb = new SB();
    // skip meta data of the tree
    ab.get4();    // Skip tree-id
    ab.get8();    // Skip seed
    ab.get1();    // Skip producer id
    int cap = 0;
    String abString = ab.toString();
    Pattern pattern = Pattern.compile("<= .* <= (.*?) <=");
    Matcher matcher = pattern.matcher(abString);
    if (matcher.find()) {
//      System.out.println(matcher.group(1));
      cap = Integer.valueOf(matcher.group(1));
    }
    // skip meta data of the tree
    ab.get4();    // Skip tree-id
    ab.get8();    // Skip seed
    ab.get1();    // Skip producer id
    while (ab.position() < cap) {
      byte currentNodeType = (byte) ab.get1();
      if (currentNodeType == 'S') {
        int _col = ab.get2();
        float splitValue = ab.get4f();
        sb.p("C").p(_col).p(" <= ").p(splitValue).p("(");
      } else if (currentNodeType == '[') {
        int cls = ab.get1();
        sb.p("["+cls+"]");
      }
    }
    return sb.toString();
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
      if(_jobKey != null && !Job.isRunning(_jobKey)) throw new Job.JobCancelledException();
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
      if(_jobKey != null && !Job.isRunning(_jobKey)) throw new Job.JobCancelledException();
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
    abstract StringBuilder toJava( StringBuilder sb, int len, int... depth );

    public abstract void print(TreePrinter treePrinter) throws IOException;
    abstract void write( AutoBuffer bs );
    int _size;                  // Byte-size in serialized form
    final int size( ) { return _size==0 ? (_size=size_impl()) : _size;  }
    abstract int size_impl();
    AutoBuffer compress(AutoBuffer ab) { return ab;}
    int dtreeSize() {return -1;}
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
    @Override public StringBuilder toJava(StringBuilder sb, int n, int... depth ) { return sb.append(_class).append(' '); }
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

    @Override AutoBuffer compress(AutoBuffer ab) {
      assert !Float.isNaN(classify( null ));
      // a little hacky here
      return ab.put4f(classify( null ));
    }
    @Override int dtreeSize() { return 4; }
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

    @Override public StringBuilder toJava( StringBuilder sb, int n, int... depth) {
      int d = 0;
      if (depth.length==0) {
        //root
        sb.append("  static final float predict(double[] data) {\n" +
                "    float pred = ");
      } else {d = depth[0];}
      // d is the distance from the node to its root.
      sb.append("Double.isNaN(data["+Integer.toString(_column)+"]) || (float) data["+Integer.toString(_column)+"] /* "+_name+"*/ ").append("<= ").append(Utils.p2d(split_value())).append("\n");
      if( sb.length() > n ) return sb;
      for (int i = -3 ; i < d ; i++) {sb.append("  ");}
      sb.append(" ? ");
      sb = _l.toJava(sb,n,d+1).append("\n");
      if( sb.length() > n ) return sb;
      for (int i = -3 ; i < d ; i++) {sb.append("  ");}
      sb.append(" : ");
      sb = _r.toJava(sb,n,d+1);
      return sb;
    }

    @Override void write( AutoBuffer bs ) {

      bs.put1('S');             // Node indicator
      assert Short.MIN_VALUE <= _column && _column < Short.MAX_VALUE;
      bs.put2((short) _column);
      bs.put4f(split_value());
      int skip = _l.size(); // Drop down the amount to skip over the left column
      if( skip <= 254 )  bs.put1(skip);
      else { bs.put1(0);
        if (! ((-1<<24) <= skip && skip < (1<<24))) throw H2O.fail("Trees have grown too deep. Use BigData RF or limit the tree depth for your model. For more information, contact support: support@h2o.ai");
        bs.put3(skip);
      }
      _l.write(bs);
      _r.write(bs);
    }
    @Override public int size_impl( ) {
      // Size is: 1 byte indicator, 2 bytes col, 4 bytes val, the skip, then left, right
      return _size=(1+2+4+(( _l.size() <= 254 ) ? 1 : 4)+_l.size()+_r.size());
    }
    public boolean isIn(final Row row) {  return row.getEncodedColumnValue(_column) <= _split; }
    public final boolean canDecideAbout(final Row row) { return row.hasValidValue(_column); }

    @Override public int dtreeSize() {
      int result = 1+2+4;
      int skip = _l.dtreeSize();
      result += skip;
      result += _r.dtreeSize();
      if ( _l instanceof LeafNode) { skip=0;}
      else {
        if (skip < 256) skip=1;
        else if (skip < 65535) skip=2;
        else if (skip < (1<<24)) skip=3;
        else skip=4;
      }
      result += skip;
      return result;
    }
    @Override AutoBuffer compress(AutoBuffer ab) {
      int pos = ab.position();
      int size = 7;
      byte _nodeType=0;
      // left child type
      if (_l instanceof LeafNode) _nodeType |= 0x30; // 00110000 = 0x30
      int leftSize = _l.dtreeSize(); // size of the left child
      size += leftSize;
      if (leftSize < 256)             _nodeType |= 0x00;
      else if (leftSize < 65535)      _nodeType |= 0x01;
      else if (leftSize < (1<<24))    _nodeType |= 0x02;
      else                            _nodeType |= 0x03;
      // right child type
      if (_r instanceof LeafNode) _nodeType |= 0xC0; // 11000000 = 0xC0
        ab.put1(_nodeType);
        assert _column != -1;
        ab.put2((short)_column);
        ab.put4f(_originalSplit); // assuming we only have _equal == 0 or 1 which is binary split
        if( _l instanceof LeafNode ) { /* don't have skip size if left child is leaf.*/}
        else {
          if(leftSize < 256)            {ab.put1(       leftSize); size += 1;}
          else if (leftSize < 65535)    {ab.put2((short)leftSize); size += 2;}
          else if (leftSize < (1<<24))  {ab.put3(       leftSize); size += 3;}
          else                          {ab.put4(       leftSize); size += 4;}// 1<<31-1
        }
      size += _r.dtreeSize();
      _l.compress(ab);
      _r.compress(ab);
      assert size == ab.position()-pos:"reported size = " + size + " , real size = " + (ab.position()-pos);
      return ab;
    }
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

    @Override AutoBuffer compress(AutoBuffer ab) {
      int pos = ab.position();
      int size = 7;
      byte _nodeType= 0x04; // 00000100
      // left child type
      if (_l instanceof LeafNode) _nodeType |= 0x30; // 00110000 = 0x30
      int leftSize = _l.dtreeSize(); // size of the left child
      size += leftSize;
      if (leftSize < 256)             _nodeType |= 0x00;
      else if (leftSize < 65535)      _nodeType |= 0x01;
      else if (leftSize < (1<<24))    _nodeType |= 0x02;
      else                            _nodeType |= 0x03;
      // right child type
      if (_r instanceof LeafNode) _nodeType |= 0xC0; // 11000000 = 0xC0
      ab.put1(_nodeType);
      assert _column != -1;
      ab.put2((short)_column);
      ab.put4f(_originalSplit); // assuming we only have _equal == 0 or 1 which is binary split

      if( _l instanceof LeafNode ) { /* don't have skip size if left child is leaf.*/}
      else {
        if(leftSize < 256)            {ab.put1(       leftSize); size += 1;}
        else if (leftSize < 65535)    {ab.put2((short)leftSize); size += 2;}
        else if (leftSize < (1<<24))  {ab.put3(       leftSize); size += 3;}
        else                          {ab.put4(       leftSize); size += 4;}// 1<<31-1
      }
      size += _r.dtreeSize();
      _l.compress(ab);
      _r.compress(ab);
      assert size == ab.position()-pos:"reported size = " + size + " , real size = " + (ab.position()-pos);
      return ab;
    }

    @Override public int dtreeSize() {
      int result = 1+2+4;
      int skip = _l.dtreeSize();
      result += skip;
      result += _r.dtreeSize();
      if ( _l instanceof LeafNode) { skip=0;}
      else {
        if (skip < 256) skip=1;
        else if (skip < 65535) skip=2;
        else if (skip < (1<<24)) skip=3;
        else skip=4;
      }
      result += skip;
      return result;
    }
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

  public Key toCompressedKey() {
    AutoBuffer bs = new AutoBuffer();
    TreeModel.CompressedTree compressedTree = compress();
    Key key = Key.make((byte)1,Key.DFJ_INTERNAL_USER, H2O.SELF);
    UKV.put(key, new Value(key, compressedTree));
    return key;
  }

  /** Classify this serialized tree - withOUT inflating it to a full tree.
   Use row 'row' in the dataset 'ary' (with pre-fetched bits 'databits')
   Returns classes from 0 to N-1*/
  public static float classify( AutoBuffer ts, Chunk[] chks, int row, int modelDataMap[], short badData, boolean regression ) {
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
      float fdat = Double.isNaN(ds[col]) ? fcmp - 1 : (float)ds[col];
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
      @Override protected TreeVisitor leafFloat(float fl) { _leaves++; if(_depth > _maxdepth) _maxdepth = _depth; return this; }
      @Override protected TreeVisitor leaf(int tclass ) { _leaves++; if( _depth > _maxdepth ) _maxdepth = _depth; return this; }
      @Override protected TreeVisitor pre (int col, float fcmp, int off0, int offl, int offr ) { _depth++; return this; }
      @Override protected TreeVisitor post(int col, float fcmp ) { _depth--; return this; }
      @Override long result( ) {return ((long)_maxdepth<<32) | _leaves; }
    }.visit().result();
  }

  // Build a compressed-tree struct
  public TreeModel.CompressedTree compress() {
//    Log.info(Sys.RANDF, _tree.toString(new StringBuilder(), Integer.MAX_VALUE).toString());
    int size = _tree.dtreeSize();
    if (_tree instanceof LeafNode) {
      size += 3;
    }
    AutoBuffer ab = new AutoBuffer(size);
    if( _tree instanceof LeafNode)
      ab.put1(0).put2((char)65535);
    _tree.compress(ab);
    assert ab.position() == size: "Actual size doesn't agree calculated size.";
    char _nclass = (char)_data.classes();
    return new TreeModel.CompressedTree(ab.buf(),_nclass,_seed);
  }

  public TreeModel.CompressedTree getCompressedTree() {
    if (compressedTree!=null) { return compressedTree; }
    else { compressedTree = compress(); }
    return compressedTree;
  }

  /**
   * @param tree binary form of a singlenoderf.Tree
   * @return AutoBuffer that contain all bytes in the singlenoderf.Tree
   */
  public static byte[] toDTreeCompressedTreeAB(byte[] tree, boolean regression) {
    AutoBuffer ab = new AutoBuffer(tree);
    AutoBuffer result = new AutoBuffer();
    return toDTreeCompressedTree(ab, regression).buf();
  }

  /**
   * @param ab AutoBuffer that contains the remaining tree nodes that we want to serialize.
   * @return binary form of a DTree.CompressedTree as a AutoBuffer
   */
  public static AutoBuffer toDTreeCompressedTree(AutoBuffer ab, boolean regression) {
    AutoBuffer result = new AutoBuffer();
    // get the length of the buffer
    int cap=0;
    String abString = ab.toString();
    Pattern pattern = Pattern.compile("<= .* <= (.*?) <=");
    Matcher matcher = pattern.matcher(abString);
    if (matcher.find()) {
//      System.out.println(matcher.group(1));
      cap = Integer.valueOf(matcher.group(1));
    }

    // skip meta data of the tree
    ab.get4();    // Skip tree-id
    ab.get8();    // Skip seed
    ab.get1();    // Skip producer id

    while (ab.position() < cap) {
      byte _nodeType = 0;
      byte currentNodeType = (byte) ab.get1();

      if (currentNodeType == 'S' || currentNodeType == 'E') {
        if (currentNodeType == 'E') {
          _nodeType |= 0x04; // 00000100
        }
        int _col =  ab.get2();
        float splitValue = ab.get4f();
        int skipSize = ab.get1();
        int skip;
        if (skipSize == 0) {
          // 4 bytes total
          _nodeType |= 0x02; // 3 bytes to store skip
          skip = ab.get3();
        } else {/* single byte for left size */ skip=skipSize; /* 1 byte to store skip*/}

        int currentPosition = ab.position();
        byte leftType = (byte) ab.get1();
        ab.position(currentPosition+skip); // jump to the right child.
        byte rightType = (byte) ab.get1();
        ab.position(currentPosition);
        if (leftType == '[') { _nodeType |= 0x30; }
        if (rightType == '[') { _nodeType |= 0xC0; }
//        int leftLeaves = getNumLeaves(ab, skip, regression); // number of left leaves.
        int skipModify = getSkip(ab, skip, regression);
        skip += skipModify;
        if (skip > 255) { _nodeType |= 0x02; }
        result.put1(_nodeType);
        result.put2((short) _col);
        result.put4f(splitValue);
        if (skip <= 255) {
          if (leftType == 'S' || leftType == 'E') result.put1(skip); // leaf will have no skip size because its size is fixed.
        }
        else {
          result.put3(skip);
        }
      }
      else if (currentNodeType == '[') {
//        result.put1(0).put2((short)65535); // if leaf then over look top level
        if (regression) { result.put4f(ab.get4f());}
        else { result.put4f((float)ab.get1());}
      } else { /* running out of the buffer*/ return result;}
    }
    return result;
  }

  public static int getNumLeaves(AutoBuffer ab, int leftSize, boolean regression) {
    int result = 0;
    int startPos = ab.position();
    while (ab.position() < startPos + leftSize) {
      byte currentNodeType = (byte) ab.get1();
      if (currentNodeType == 'S' || currentNodeType == 'E') {
        ab.get2(); ab.get4f(); // skip col and split value.
        int skipSize = ab.get1();
        if (skipSize == 0) { ab.get3();}
      } else if (currentNodeType == '[') {
        result ++;
        if (regression) ab.get4f();
        else ab.get1();
      } else {
        throw new NotImplementedException();
      }
    }
    ab.position(startPos); // return to the original position so the buffer seems untouched.
    return result;
  }

  public static int getSkip(AutoBuffer ab, int leftSize, boolean regression) {
    int numLeaves = 0;
    int numLeftLeaves = 0;
    int startPos = ab.position();
    boolean prevIsS = false;
    while (ab.position() < startPos + leftSize) {
      byte currentNodeType = (byte) ab.get1();
      if (currentNodeType == 'S' || currentNodeType == 'E') {
        ab.get2(); ab.get4f(); // skip col and split value.
        int skipSize = ab.get1();
        if (skipSize == 0) { ab.get3();}
        prevIsS = true;
      } else if (currentNodeType == '[') {
        numLeaves ++;
        if (regression) ab.get4f();
        else ab.get1();
        if (prevIsS) numLeftLeaves++;
        prevIsS = false;
      } else {
        throw new NotImplementedException();
      }
    }
    ab.position(startPos);
    return 2*numLeaves - numLeftLeaves; // only for regression tree.
  }

}
