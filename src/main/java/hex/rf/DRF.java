package hex.rf;
import hex.rf.MinorityClasses.UnbalancedClass;
import hex.rf.Tree.StatType;

import java.util.*;

import jsr166y.CountedCompleter;
import water.*;
import water.ValueArray.Column;
import water.Timer;
import water.util.Utils;

/** Distributed RandomForest */
public final class DRF extends water.DRemoteTask {
  /** The RF Model.  Contains the dataset being worked on, the classification
   *  column, and the training columns.  */
  public RFModel _rfmodel;
  public Job _job;

  // ---------------
  // OPTIONS FOR RF
  // ---------------
  /** Total number of trees */
  int _ntrees;
  /** If true, build trees in parallel (default: true) */
  boolean _parallel;
  /** Maximum depth for trees (default MaxInt) */
  int _depth;
  /** Split statistic (1=Gini, 0=Entropy; default 0) */
  int _stat;
  /** Feature holding the classifier  (default: #features-1) */
  int _classcol;
  /** Proportion of observations to use for building each individual tree (default: .67)*/
  float _sample;
  /** Used to replay sampling */
  int _numrows;
  /** Limit of the cardinality of a feature before we bin. */
  short _binLimit;
  /** Pseudo random seed */
  long _seed;
  /** Weights of the different features (default: 1/features) */
  double[] _classWt;
  /** Stratify sampling flag */
  boolean _useStratifySampling;
  /** Arity under which we may use exclusive splits */
  public int _exclusiveSplitLimit;
  /** Output warnings and info*/
  public int _verbose;
  /** Number of features which are tried at each split
   *  If it is equal to -1 then it is computed as sqrt(num of usable columns) */
  int _numSplitFeatures;


  // --------------
  // INTERNAL DATA
  //--------------
  /** A list of unbalanced classes */
  UnbalancedClass[] _uClasses;
  /** Defined stratas for each class */
  int[] _strata;

  //-----------------
  // Node-local data
  //-----------------
  /** Data subset to validate with locally, or NULL */
  transient Data _validation;
  /** The local RandomForest */
  transient RandomForest _rf;
  /** Main computation timer */
  transient public Timer _t_main;
  /** Global histogram over all nodes */
  transient int []   _gHist;

  //-----------------
  // Published data.
  //-----------------
  /** Number of split features.
   * It is published since its value depends on loaded data.
   */
  public final int numSplitFeatures() { return _numSplitFeatures; }

  /** Key for the training data. */
  public final Key aryKey()           { return _rfmodel._dataKey; }

  /** Create DRF task, execute it and returns DFuture.
   *  Caller can block on the future to wait till execution finish.
   */
  public static final DRFFuture execute(Key modelKey, int[] cols, ValueArray ary, int ntrees, int depth, float sample, short binLimit,
      StatType stat, long seed, boolean parallelTrees, double[] classWt, int numSplitFeatures,
      boolean stratify, Map<Integer,Integer> strata, int verbose, int exclusiveSplitLimit) {
    final DRF drf = create(modelKey, cols, ary, ntrees, depth, sample, binLimit, stat, seed, parallelTrees, classWt, numSplitFeatures, stratify, strata, verbose, exclusiveSplitLimit);
    drf._job = new Job(jobName(drf), modelKey);
    drf._job.start();
    return drf.new DRFFuture(drf.fork(drf.aryKey()));
  }

  private static String jobName(final DRF drf) {
    return "RandomForest_" + drf._ntrees + "trees";
  }

  /** Create and configure a new DRF remote task.
   *  It does not execute DRF !!! */
  private static DRF create(
    Key modelKey, int[] cols, ValueArray ary, int ntrees, int depth, float sample, short binLimit,
    StatType stat, long seed, boolean parallelTrees, double[] classWt, int numSplitFeatures,
    boolean stratify, Map<Integer,Integer> strata, int verbose, int exclusiveSplitLimit) {

    // Construct the RFModel to be trained
    DRF drf      = new DRF();
    drf._rfmodel = new RFModel(modelKey, cols, ary._key,
                               new Key[0], ary._cols.length, sample, numSplitFeatures, ntrees);
    // Fill in args into DRF
    drf._ntrees = ntrees;
    drf._depth = depth;
    drf._sample = sample;
    drf._binLimit = binLimit;
    drf._stat = stat.ordinal();
    drf._classcol = cols[cols.length-1];
    drf._seed = seed;
    drf._parallel = parallelTrees;
    drf._classWt = classWt;
    drf._numSplitFeatures = numSplitFeatures;
    drf._useStratifySampling = stratify;
    drf._verbose = verbose;
    drf._exclusiveSplitLimit = exclusiveSplitLimit;

    RandomForest.OptArgs _ = new RandomForest.OptArgs();
    _.features = numSplitFeatures;
    _.ntrees   = ntrees;
    _.depth    = depth;
    _.classcol = drf._classcol;
    _.seed     = seed;
    _.binLimit = binLimit;
    _.verbose  = verbose;
    _.exclusive= exclusiveSplitLimit;
    String w = "";
    if (classWt != null) for(int i=0;i<classWt.length;i++) w += i+":"+classWt[i]+",";
    _.weights=w;
    _.parallel = parallelTrees ? 1 : 0;
    _.statType = stat.ordinal() == 1 ? "gini" : "entropy";
    _.sample = (int)(sample * 100);
    _.file = "";

    if (verbose>0) Utils.pln("Web arguments: " + _ + " key "+ary._key);

    // Validate parameters
    drf.validateInputData();
    // Start the timer.
    drf._t_main = new Timer();
    // Pre-process data in case of stratified sampling: extract minorities
    if(drf._useStratifySampling)  drf.extractMinorities(ary, strata);

    // Push the RFModel globally first
    UKV.put(modelKey, drf._rfmodel);
    DKV.write_barrier();

    return drf;
  }

  /** Hacky class to remove {@link Job} correctly.
   *
   * NOTE: need to be refined after new cyprien's jobs will be merged. */
  public final class DRFFuture {
    private final DFuture _future;
    private DRFFuture(final DFuture deleg) { super(); _future = deleg; }
    public DRF get() {
      // Block to the end of DRF.
      final DRF drf = (DRF) _future.get();
      // Remove DRF job.
      if (drf._job != null) drf._job.remove();
      return drf;
    }
  }

  /**Class columns that are not enums are not supported as we ony do classification and not (yet) regression.
   * We require there to be at least two classes and no more than 65534. Though we will die out of memory
   * much earlier.  */
  private void validateInputData(){
    Column cs[] = _rfmodel._va._cols;
    Column c = cs[cs.length-1];
    String err = "Response column must be an integer in the interval [2,254]";
    if(c.isFloat())
      throw new IllegalArgumentException("Regression is not supported: "+err);
    final int classes = (int)(c._max - c._min)+1;
    if( !(2 <= classes && classes <= 254 ) )
      throw new IllegalArgumentException("Found " + classes+" classes: "+err);
    if (0.0f > _sample || _sample > 1.0f)
      throw new IllegalArgumentException("Sampling rate must be in [0,1] but found "+_sample);
    if (_numSplitFeatures!=-1 && (_numSplitFeatures< 1 || _numSplitFeatures>cs.length-1))
      throw new IllegalArgumentException("Number of split features exceeds available data. Should be in [1,"+(cs.length-1)+"]");
  }

  /**Inhale the data, build a DataAdapter and kick-off the computation.
   * */
  @Override
  public final void compute2() {
    Timer t_extract = new Timer();
    // Build data adapter for this node.
    DataAdapter dapt = DABuilder.create(this).build(_keys);
    Utils.pln("[RF] Data adapter built in " + t_extract );
    // Prepare data and compute missing parameters.
    Data t            = Data.make(dapt);
    _numSplitFeatures = howManySplitFeatures(t);
    int ntrees        = howManyTrees();

    Utils.pln("[RF] Building "+ntrees+" trees");
    RandomForest.build(this, t, ntrees, _depth, 0.0, StatType.values()[_stat],_parallel,_numSplitFeatures);
    // Wait for the running jobs
    tryComplete();
  }

  @Override
  public final void reduce( DRemoteTask drt ) { }

  @Override
  public void onCompletion(CountedCompleter caller) {
    System.out.println("DRF.onCompletion(): keys  :" + _keys.length);
    System.out.println("DRF.onCompletion(): ntrees:" + _ntrees);
    System.out.println("DRF.onCompletion(): caller: " + caller);
  }

  /** Unless otherwise specified each split looks at sqrt(#features). */
  private int howManySplitFeatures(Data t) {
    if (_numSplitFeatures!=-1) return _numSplitFeatures;
    return (int)Math.sqrt(_rfmodel._va._cols.length-1/*we don't used the class column*/);
  }

  /** Figure the number of trees to make locally, so the total hits ntrees.
   *  Divide equally amongst all the nodes that actually have data.  First:
   *  compute how many nodes have data.  Give each Node ntrees/#nodes worth of
   *  trees.  Round down for later nodes, and round up for earlier nodes.
   */
  private int howManyTrees() {
    ValueArray ary = DKV.get(_rfmodel._dataKey).get();
    final long num_chunks = ary.chunks();
    final int  num_nodes  = H2O.CLOUD.size();
    HashSet<H2ONode> nodes = new HashSet();
    for( long i=0; i<num_chunks; i++ ) {
      nodes.add(ary.getChunkKey(i).home_node());
      if( nodes.size() == num_nodes ) // All of nodes covered?
        break;                        // That means we are done.
    }

    H2ONode[] array = nodes.toArray(new H2ONode[nodes.size()]);
    Arrays.sort(array);
    // Give each H2ONode ntrees/#nodes worth of trees.  Round down for later nodes,
    // and round up for earlier nodes
    int ntrees = _ntrees/nodes.size();
    if( Arrays.binarySearch(array, H2O.SELF) < _ntrees - ntrees*nodes.size() )
      ++ntrees;

    return ntrees;
  }

  /**
   * This method has two functions:
   *   1) computes default strata sizes (can be overriden by user)
   *   2) identifies unbalanced classes (based on stratas) and extracts them out of the dataset.
   */
  private void extractMinorities(ValueArray ary, Map<Integer,Integer> strata) {
    // Compute class histogram per node.
    int[][] _nHist = MinorityClasses.histogram(ary, _classcol);
    // Compute global histogram.
    _gHist = MinorityClasses.globalHistogram(_nHist);
    final int num_nodes = H2O.CLOUD.size();
    final long num_chunks = ary.chunks();
    HashSet<H2ONode> nodes = new HashSet();
    for( long i=0; i<num_chunks; i++ ) {
      H2ONode nod = ary.getChunk(i)._h2o;
      // FIXME this is adhoc solution
      nodes.add(nod != null ? nod : H2O.SELF);
      if( nodes.size() == num_nodes ) // All of them?
        break;                        // Done
    }
    int cloudSize = nodes.size();
    int [] nodesIdxs = new int[nodes.size()];
    int k = 0;
    for(H2ONode n:nodes) nodesIdxs[k++] = n.index();
    Arrays.sort(nodesIdxs);
    // Get count of rows with majority class over all nodes.
    int majority = 0;
    for(int i : _gHist) if(i > majority) majority = i;
    // Recompute respecting sampling rate and spread of data over nodes.
    majority = Math.round((_sample*majority)/cloudSize);
    // FIXME: Compute minimal strata.
    int minStrata = majority >> 9;
    _strata = new int[_gHist.length];
    for(int i = 0; i < _strata.length; ++i){
      // TODO should class weight be adjusted?
      // Compute required number of rows with class <i> on each node.
      int expClassNumPerNode = Math.round((_sample * _gHist[i])/cloudSize);
      // This is a required number of rows of given class <i> on each node.
      _strata[i] = Math.min(_gHist[i],Math.max(minStrata, expClassNumPerNode));
    }

    // Now consider strata specified by the user and recompute expected numbers
    // of rows per node and per class.
    if( strata != null) for(Map.Entry<Integer, Integer> e: strata.entrySet()) {
      int clsIdx = e.getKey();
      int clsVal = e.getValue();
      if(clsIdx < 0 || clsIdx >= _strata.length)
        Utils.pln("Ignoring illegal class index when parsing strata argument: " + e.getKey());
      else
        _strata[clsIdx] = Math.min(_gHist[clsIdx], clsVal);
    }

    for(int i : nodesIdxs) {
      if( _gHist[i] < (int)(_strata[i]/_sample) )
        Utils.pln("There is not enough samples of class " + i + ".");
    }
    // Decide which classes need to be extracted
    SortedSet<Integer> uClasses = new TreeSet<Integer>();
    for(int i:nodesIdxs) {
      for(int c = 0; c < _nHist[i].length; ++c) {
        // Node does not have enough samples
        if(_nHist[i][c] < _strata[c])uClasses.add(c);
      }
    }

    if(!uClasses.isEmpty()) {
      int [] u  = new int [uClasses.size()];
      int i = 0;
      for(int c:uClasses)u[i++] = c;
      _uClasses = MinorityClasses.extractUnbalancedClasses(ary, _classcol, u);
    }
  }
}
