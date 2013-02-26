package hex.rf;

import java.util.Arrays;
import java.util.Random;

import water.*;
import water.Job.Progress;
import water.util.Counter;

/**
 * A model is an ensemble of trees that can be serialized and that can be used
 * to classify data.
 */
public class RFModel extends Model implements Cloneable, Progress {
  /** Number of features these trees are built for */
  public int       _features;
  /** Sampling rate used when building trees. */
  public float     _sample;
  /** Number of split features */
  public int       _splitFeatures;

  /** Number of keys the model expects to be built for it */
  public int       _totalTrees;
  /** All the trees in the model */
  public Key[]     _tkeys;

  public static final String KEY_PREFIX = "__RFModel_";

  /** A RandomForest Model
   * @param treeskey    a key of keys of trees
   * @param classes     the number of response classes
   * @param data        the dataset
   */
  public RFModel(Key selfKey, int[] cols, Key dataKey, Key[] tkeys, int features, float sample, int splitFeatures, int totalTrees) {
    super(selfKey,cols,dataKey);
    _features       = features;
    _sample         = sample;
    _splitFeatures  = splitFeatures;
    _totalTrees     = totalTrees;
    _tkeys          = tkeys;
    for( Key tkey : _tkeys ) assert DKV.get(tkey)!=null;
  }

  public RFModel(Key selfKey, String [] colNames, String[] classNames, Key[] tkeys, int features, float sample) {
    super(selfKey,colNames,classNames);
    _features       = features;
    _sample         = sample;
    _splitFeatures  = features;
    _totalTrees     = tkeys.length;
    _tkeys          = tkeys;
    for( Key tkey : _tkeys ) assert DKV.get(tkey)!=null;
    assert classes() > 0;
  }

  /** Empty constructor for deserialization */
  public RFModel() { }

  @Override protected RFModel clone() {
    try {
      return (RFModel)super.clone();
    } catch( CloneNotSupportedException cne ) {
      throw H2O.unimpl();
    }
  }

  static public RFModel make(RFModel old, Key tkey) {
    RFModel m = old.clone();
    m._tkeys = Arrays.copyOf(old._tkeys,old._tkeys.length+1);
    m._tkeys[m._tkeys.length-1] = tkey;
    return m;
  }

  // Make a random RF key
  public static final Key makeKey() {
    return Key.make(KEY_PREFIX + Key.make());
  }

  /** The number of trees in this model. */
  public int treeCount() { return _tkeys.length; }
  public int size()      { return _tkeys.length; }
  public int classes()   { ValueArray.Column C = response();  return (int)(C._max - C._min + 1); }

  @Override public float progress() {
    return size() / (float) _totalTrees;
  }

  public String name(int atree) {
    if( atree == -1 ) atree = size();
    assert atree <= size();
    return _selfKey.toString()+"["+atree+"]";
  }

  /** Return the bits for a particular tree */
  public byte[] tree( int tree_id ) {
    return DKV.get(_tkeys[tree_id]).memOrLoad();
  }

  /** Bad name, I know.  But free all internal tree keys. */
  public void deleteKeys() {
    for( Key k : _tkeys )
      UKV.remove(k);
  }

  /**
   * Classify a row according to one particular tree.
   * @param tree_id  the number of the tree to use
   * @param chunk    the chunk we are using
   * @param row      the row number in the chunk
   * @param modelDataMap  mapping from model/tree columns to data columns
   * @return the predicted response class, or class+1 for broken rows
   */
  public short classify0(int tree_id, ValueArray data, AutoBuffer chunk, int row, int modelDataMap[], short badrow ) {
    return Tree.classify(new AutoBuffer(tree(tree_id)), data, chunk, row, modelDataMap, badrow);
  }

  private void vote(ValueArray data, AutoBuffer chunk, int row, int modelDataMap[], int[] votes ) {
    int numClasses = classes();
    assert votes.length == numClasses+1/*+1 to catch broken rows*/;
    for( int i = 0; i < treeCount(); i++ )
      votes[classify0(i, data, chunk, row, modelDataMap, (short)numClasses)]++;
  }

  public short classify(ValueArray data, AutoBuffer chunk, int row, int modelDataMap[], int[] votes, double[] classWt, Random rand ) {
    // Vote all the trees for the row
    vote(data, chunk, row, modelDataMap, votes);
    return classify(votes,classWt,rand);
  }
  public short classify(int[] votes, double[] classWt, Random rand ) {
    // Scale the votes by class weights: it as-if rows of the weighted classes
    // were replicated many times so get many votes.
    if( classWt != null )
      for( int i=0; i<votes.length-1; i++ )
        votes[i] = (int)(votes[i]*classWt[i]);
    // Tally results
    int result = 0;
    int tied = 1;
    for( int i = 1; i<votes.length-1; i++)
      if( votes[i] > votes[result] ) { result=i; tied=1; }
      else if( votes[i] == votes[result] ) { tied++; }
    if( tied==1 ) return (short)result;
    // Tie-breaker logic
    int j = rand == null ? 0 : rand.nextInt(tied); // From zero to number of tied classes-1
    int k = 0;
    for( int i=0; i<votes.length-1; i++ )
      if( votes[i]==votes[result] && (k++ >= j) )
        return (short)i;
    throw H2O.unimpl();
  }

  // The seed for a given tree
  long seed( int ntree ) {
    return UDP.get8(tree(ntree),4);
  }

  // Lazy initialization of tree leaves, depth
  private transient Counter _tl, _td;

  /** Internal computation of depth and number of leaves. */
  public void find_leaves_depth() {
    if( _tl != null ) return;
    _td = new Counter();
    _tl = new Counter();
    for( Key tkey : _tkeys ) {
      long dl = Tree.depth_leaves(new AutoBuffer(DKV.get(tkey).memOrLoad()));
      _td.add((int) (dl >> 32));
      _tl.add((int) dl);
    }
  }
  public Counter leaves() { find_leaves_depth(); return _tl; }
  public Counter depth()  { find_leaves_depth(); return _td; }

  /** Return the random seed used to sample this tree. */
  public long getTreeSeed(int i) {  return Tree.seed(tree(i)); }

  /** Single row scoring, on properly ordered data */
  protected double score0( double[] data ) {
    int numClasses = classes();
    int votes[] = new int[numClasses+1/*+1 to catch broken rows*/];
    for( int i = 0; i < treeCount(); i++ )
      votes[(int)Tree.classify(new AutoBuffer(tree(i)), data, numClasses)]++;
    return classify(votes,null,null)+response()._min;
  }

  /** Single row scoring, on a compatible ValueArray (when pushed throw the mapping) */
  protected double score0( ValueArray data, int row, int[] mapping ) { throw H2O.unimpl(); }

  /** Bulk scoring API, on a compatible ValueArray (when pushed throw the mapping) */
  protected double score0( ValueArray data, AutoBuffer ab, int row_in_chunk, int[] mapping ) { throw H2O.unimpl(); }
}
