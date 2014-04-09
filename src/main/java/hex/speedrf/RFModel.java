package hex.speedrf;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import hex.FrameTask;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.StringUtils;
import water.*;
import water.api.Constants;
import water.fvec.Chunk;
import water.fvec.Frame;
import water.fvec.Vec;
import water.util.Counter;

import java.util.Arrays;
import java.util.Random;

/**
 * A model is an ensemble of trees that can be serialized and that can be used
 * to classify data.
 */
public class RFModel extends Model {
  /** Number of features these trees are built for */
  public int _features;
  /** Sampling strategy used for model */
  public Sampling.Strategy _samplingStrategy;
  /** Sampling rate used when building trees. */
  public float _sample;
  /** Strata sampling rate used for local-node strata-sampling */
  public float[] _strataSamples;
  /** Number of split features defined by user. */
  public int _splitFeatures;
  /** Number of computed split features per node - number of split features can differ for each node.
   * However, such difference would point to a problem with data distribution. */
  public int[] _nodesSplitFeatures;
  /** Number of keys the model expects to be built for it */
  public int _totalTrees;
  /** All the trees in the model */
  public Key[]     _tkeys;
  /** Local forests produced by nodes */
  public Key[][]   _localForests;
  /** Remote chunks' keys used by individual nodes */
//  public Key[][]   _remoteChunksKeys;
  /** Total time in seconds to produce model */
  public long      _time;

  public Frame _fr;

  public Vec _response;

  public double[] _weights;



  public transient byte[][] _trees; // The raw tree data, for faster classification passes

  public static final String KEY_PREFIX = "__RFModel_";


  public int[] colMap(String[] names) {
    int res[] = new int[names.length];
    for(int i = 0; i < res.length; i++) {
      res[i] = _fr.find(names[i]);
    }
    return res;
  }

  public Vec get_response() {
    return _response;
  }

  public RFModel(Key selfKey, Frame fr, Vec response, int[] ignored_cols, Key dataKey, Key[] tkeys, int features, Sampling.Strategy samplingStrategy, float sample, float[] strataSamples, int splitFeatures, int totalTrees, double[] weights) {
    super(selfKey, dataKey, fr);
    _fr = FrameTask.DataInfo.prepareFrame(fr, response, ignored_cols, false, true);
    _features = features;
    _response = response;
    _sample = sample;
    _splitFeatures = splitFeatures;
    _totalTrees = totalTrees;
    _tkeys = tkeys;
    _strataSamples = strataSamples;
    _samplingStrategy = samplingStrategy;
    int csize = H2O.CLOUD.size();
    _nodesSplitFeatures = new int[csize];
    _localForests       = new Key[csize][];
    for(int i=0;i<csize;i++) _localForests    [i] = new Key[0];
    for( Key tkey : _tkeys ) assert DKV.get(tkey)!=null;
    _weights = weights;
  }

  public RFModel(Key selfKey, Frame fr, Key dataKey, Key[] tkeys, int features, float sample) {
    super(selfKey, dataKey, fr._names, fr.domains());
    _features       = features;
    _sample         = sample;
    _splitFeatures  = features;
    _totalTrees     = tkeys.length;
    _tkeys          = tkeys;
    _samplingStrategy   = Sampling.Strategy.RANDOM;
    int csize = H2O.CLOUD.size();
    _nodesSplitFeatures = new int[csize];
    _localForests       = new Key[csize][];
//    _remoteChunksKeys   = new Key[csize][];
    for(int i=0;i<csize;i++) _localForests    [i] = new Key[0];
//    for(int i=0;i<csize;i++) _remoteChunksKeys[i] = new Key[0];
    for( Key tkey : _tkeys ) assert DKV.get(tkey)!=null;
    assert classes() > 0;
  }

  /** Empty constructor for deserialization */
//  public RFModel() {
//    super();
//  }

  static public RFModel make(RFModel old, Key tkey, int nodeIdx) {
    RFModel m = (RFModel)old.clone();
    m._tkeys = Arrays.copyOf(old._tkeys,old._tkeys.length+1);
    m._tkeys[m._tkeys.length-1] = tkey;

    m._localForests[nodeIdx] = Arrays.copyOf(old._localForests[nodeIdx],old._localForests[nodeIdx].length+1);
    m._localForests[nodeIdx][m._localForests[nodeIdx].length-1] = tkey;
    return m;
  }

  // Make a random RF key
  public static final Key makeKey() {
    return Key.make(KEY_PREFIX + Key.make());
  }

  /** The number of trees in this model. */
  public int treeCount() { return _tkeys.length; }
  public int size()      { return _tkeys.length; }
  public int classes()   { return (int)(_response.max() - _response.min() + 1); }

//  @Override public float progress() {
//    return size() / (float) _totalTrees;
//  }

  public String name(int atree) {
    if( atree == -1 ) atree = size();
    assert atree <= size();
    return _key.toString() + "[" + atree + "]";
  }

  /** Return the bits for a particular tree */
  public byte[] tree(int tree_id) {
    byte[][] ts = _trees;
    if( ts == null ) _trees = ts = new byte[tree_id+1][];
    if( tree_id >= ts.length ) _trees = ts = Arrays.copyOf(ts,tree_id+1);
    if( ts[tree_id] == null ) ts[tree_id] = DKV.get(_tkeys[tree_id]).memOrLoad();
    return ts[tree_id];
  }

  /** Free all internal tree keys. */
  @Override public Futures delete_impl(Futures fs) {
    for( Key k : _tkeys )
      UKV.remove(k,fs);
    return fs;
  }

  /**
   * Classify a row according to one particular tree.
   * @param tree_id  the number of the tree to use
   * @param chunks    the chunk we are using
   * @param row      the row number in the chunk
   * @param modelDataMap  mapping from model/tree columns to data columns
   * @return the predicted response class, or class+1 for broken rows
   */
  public short classify0(int tree_id, Frame fr, Chunk[] chunks, int row, int modelDataMap[], short badrow) {
    return Tree.classify(new AutoBuffer(tree(tree_id)), fr, chunks, row, modelDataMap, badrow);
  }

  private void vote(Frame fr, Chunk[] chks, int row, int modelDataMap[], int[] votes) {
    int numClasses = classes();
    assert votes.length == numClasses + 1 /* +1 to catch broken rows */;
    for( int i = 0; i < treeCount(); i++ )
      votes[classify0(i, fr, chks, row, modelDataMap, (short) numClasses)]++;
  }

  public short classify(Frame fr, Chunk[] chks, int row, int modelDataMap[], int[] votes, double[] classWt, Random rand ) {
    // Vote all the trees for the row
    vote(fr, chks, row, modelDataMap, votes);
    return classify(votes, classWt, rand);
  }

  public short classify(int[] votes, double[] classWt, Random rand) {
    // Scale the votes by class weights: it as-if rows of the weighted classes
    // were replicated many times so get many votes.
    if( classWt != null )
      for( int i=0; i<votes.length-1; i++ )
        votes[i] = (int) (votes[i] * classWt[i]);
    // Tally results
    int result = 0;
    int tied = 1;
    for( int i = 1; i < votes.length - 1; i++ )
      if( votes[i] > votes[result] ) { result=i; tied=1; }
      else if( votes[i] == votes[result] ) { tied++; }
    if( tied == 1 ) return (short) result;
    // Tie-breaker logic
    int j = rand == null ? 0 : rand.nextInt(tied); // From zero to number of tied classes-1
    int k = 0;
    for( int i = 0; i < votes.length - 1; i++ )
      if( votes[i]==votes[result] && (k++ >= j) )
        return (short)i;
    throw H2O.unimpl();
  }

  // The seed for a given tree
  long seed(int ntree) { return UDP.get8(tree(ntree), 4); }
  // The producer for a given tree
  byte producerId(int ntree) { return tree(ntree)[12]; }

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


  @Override protected float[] score0(double[] data, float[] preds) {
    return new float[0];  //To change body of implemented methods use File | Settings | File Templates.
  }

//  /** Single row scoring, on properly ordered data */
//  @Override protected double score0(double[] data) {
//    int numClasses = classes();
//    int votes[] = new int[numClasses + 1/* +1 to catch broken rows */];
//    for( int i = 0; i < treeCount(); i++ )
//      votes[(int) Tree.classify(new AutoBuffer(tree(i)), data, numClasses)]++;
//    return classify(votes, null, null); //response()._min;
//  }
//
//  /** Single row scoring, on a compatible ValueArray (when pushed throw the mapping) */
//  @Override protected double score0( Frame fr, int row) { throw H2O.unimpl(); }
//
//  /** Bulk scoring API, on a compatible ValueArray (when pushed throw the mapping) */
//  @Override protected double score0(ValueArray data, AutoBuffer ab, int row_in_chunk) { throw H2O.unimpl(); }

  public JsonObject toJson() {
    JsonObject res = new JsonObject();
    res.addProperty(Constants.VERSION, H2O.VERSION);
    res.addProperty(Constants.TYPE, RFModel.class.getName());
    res.addProperty("features", _features);
    res.addProperty("sampling_strategy", _samplingStrategy.name());
    res.addProperty("sample", _sample);
    JsonArray vals = new JsonArray();
    for(float f:_strataSamples)
      vals.add(new JsonPrimitive(f));
    res.add("strataSamples", vals);
    res.addProperty("split_features", _splitFeatures);
    vals = new JsonArray();
    for(int i:_nodesSplitFeatures)
      vals.add(new JsonPrimitive(i));
    res.add("nodesSplitFeatures", vals);
    res.addProperty("totalTrees", _totalTrees);
    res.addProperty("time", _time);
    vals = new JsonArray();
    for( Key tkey : _tkeys ) {
      byte[] b = Base64.encodeBase64(DKV.get(tkey).memOrLoad(), false);
      vals.add(new JsonPrimitive(StringUtils.newStringUtf8(b)));
    }
    res.add("trees", vals);
    return res;
  }
}
