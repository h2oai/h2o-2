package hex.rf;

import hex.rf.Tree.StatType;

import java.util.Arrays;
import java.util.HashSet;

import jsr166y.CountedCompleter;

import water.*;
import water.H2O.H2OCountedCompleter;
import water.ValueArray.Column;
import water.util.Log;
import water.util.Log.Tag.Sys;

/** Distributed RandomForest */
public abstract class DRF {

  /** Create DRF task, execute it and returns DFuture.
   *
   *  Caller can block on the returned job by calling <code>job.get()</code>
   *  to wait till execution finish.
   */
  public static final DRFJob execute(Key modelKey, int[] cols, ValueArray ary, int ntrees, int depth, int binLimit,
      StatType stat, long seed, boolean parallelTrees, double[] classWt, int numSplitFeatures,
      Sampling.Strategy samplingStrategy, float sample, float[] strataSamples, int verbose, int exclusiveSplitLimit) {

    // Create DRF remote task
    final DRFTask drfTask = create(modelKey, cols, ary, ntrees, depth, binLimit, stat, seed, parallelTrees, classWt, numSplitFeatures, samplingStrategy, sample, strataSamples, verbose, exclusiveSplitLimit);
    // Create DRF user job & start it
    final DRFJob  drfJob  = new DRFJob(jobName(drfTask), modelKey);
    drfJob.start(drfTask);
    drfTask._job = drfJob;
    // Execute the DRF task
    drfTask.dfork(drfTask._rfmodel._dataKey);

    return drfJob;
  }

  /** Create and configure a new DRF remote task.
   *  It does not execute DRF !!! */
  private static DRFTask create(
    Key modelKey, int[] cols, ValueArray ary, int ntrees, int depth, int binLimit,
    StatType stat, long seed, boolean parallelTrees, double[] classWt, int numSplitFeatures,
    Sampling.Strategy samplingStrategy, float sample, float[] strataSamples, int verbose, int exclusiveSplitLimit) {

    // Construct the RFModel to be trained
    DRFTask drf  = new DRFTask();
    drf._rfmodel = new RFModel(modelKey, cols, ary._key,
                               new Key[0], ary._cols.length, samplingStrategy, sample, strataSamples, numSplitFeatures, ntrees);
    // Save the number of rows per chunk - it is needed for proper sampling.
    // But it will need to be changed with new fluid vectors
    assert ary._rpc == null : "DRF does not support different sizes of chunks for now!";
    int numrows = (int) (ValueArray.CHUNK_SZ/ary._rowsize);
    drf._params = DRFParams.create(cols[cols.length-1], ntrees, depth, numrows, binLimit, stat, seed, parallelTrees, classWt, numSplitFeatures, samplingStrategy, sample, strataSamples, verbose, exclusiveSplitLimit);
    // Verbose debug print
    if (verbose>0) dumpRFParams(modelKey, cols, ary, ntrees, depth, binLimit, stat, seed, parallelTrees, classWt, numSplitFeatures, samplingStrategy, sample, strataSamples, verbose, exclusiveSplitLimit);
    // Validate parameters
    drf.validateInputData();
    // Start the timer.
    drf._t_main = new Timer();
    // Push the RFModel globally first
    UKV.put(modelKey, drf._rfmodel);
    DKV.write_barrier();

    return drf;
  }

  /** Returns a job name for DRF. */
  private static String jobName(final DRFTask drf) {
    return "RandomForest_" + drf._params._ntrees + "trees";
  }

  /** Remote task implementation execution RF logic */
  public final static class DRFTask extends water.DRemoteTask {
    /** The RF Model.  Contains the dataset being worked on, the classification
     *  column, and the training columns.  */
    public RFModel _rfmodel;
    /** Job representing this DRF execution. */
    public Job _job;
    /** RF parameters. */
    public DRFParams _params;

    //-----------------
    // Node-local data
    //-----------------
    /** Main computation timer */
    transient public Timer _t_main;

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
      if (0.0f > _params._sample || _params._sample > 1.0f)
        throw new IllegalArgumentException("Sampling rate must be in [0,1] but found "+ _params._sample);
      if (_params._numSplitFeatures!=-1 && (_params._numSplitFeatures< 1 || _params._numSplitFeatures>cs.length-1))
        throw new IllegalArgumentException("Number of split features exceeds available data. Should be in [1,"+(cs.length-1)+"]");
    }

    /**Inhale the data, build a DataAdapter and kick-off the computation.
     * */
    @Override public final void lcompute() {
      // Build data adapter for this node.
      Timer t_extract = new Timer();
      DataAdapter dapt = DABuilder.create(this).build(_keys);
      Log.debug(Sys.RANDF,"Data adapter built in " + t_extract );
      // Prepare data and compute missing parameters.
      Data localData        = Data.make(dapt);
      int numSplitFeatures  = howManySplitFeatures(localData);
      int ntrees            = howManyTrees();
      // write number of split features
      updateRFModel(_job.dest(), numSplitFeatures);

      // Build local random forest
      RandomForest.build(_job, _params, localData, ntrees, numSplitFeatures);
      // Wait for the running jobs
      tryComplete();
    }

    @Override public final void reduce( DRemoteTask drt ) { }

    /** Write number of split features computed on this node to a model */
    static void updateRFModel(Key modelKey, final int numSplitFeatures) {
      final int idx = H2O.SELF.index();
      new TAtomic<RFModel>() {
        @Override public RFModel atomic(RFModel old) {
          if(old == null) return null;
          RFModel newModel = old.clone();
          newModel._nodesSplitFeatures[idx] = numSplitFeatures;
          return newModel;
        }
      }.invoke(modelKey);
    }

    /** Unless otherwise specified each split looks at sqrt(#features). */
    private int howManySplitFeatures(Data t) {
      // FIXME should be run over the right data!
      if (_params._numSplitFeatures!=-1) return _params._numSplitFeatures;
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
      int ntrees = _params._ntrees/nodes.size();
      if( Arrays.binarySearch(array, H2O.SELF) < _params._ntrees - ntrees*nodes.size() )
        ++ntrees;

      return ntrees;
    }
  }

  /** RF execution parameters. */
  public final static class DRFParams extends Iced {
    /** Total number of trees */
    int _ntrees;
    /** If true, build trees in parallel (default: true) */
    boolean _parallel;
    /** Maximum depth for trees (default MaxInt) */
    int _depth;
    /** Split statistic (1=Gini, 0=Entropy; default 0) */
    StatType _stat;
    /** Feature holding the classifier  (default: #features-1) */
    int _classcol;
    /** Utilized sampling method */
    Sampling.Strategy _samplingStrategy;
    /** Proportion of observations to use for building each individual tree (default: .67)*/
    float _sample;
    /** Used to replay sampling */
    int _numrows;
    /** Limit of the cardinality of a feature before we bin. */
    int _binLimit;
    /** Pseudo random seed */
    long _seed;
    /** Weights of the different features (default: 1/features) */
    double[] _classWt;
    /** Arity under which we may use exclusive splits */
    public int _exclusiveSplitLimit;
    /** Output warnings and info*/
    public int _verbose;
    /** Number of features which are tried at each split
     *  If it is equal to -1 then it is computed as sqrt(num of usable columns) */
    int _numSplitFeatures;
    /** Defined stratas samples for each class */
    float[] _strataSamples;

    public static final DRFParams create(int col, int ntrees, int depth, int numrows, int binLimit,
        StatType statType, long seed, boolean parallelTrees, double[] classWt, int numSplitFeatures,
        Sampling.Strategy samplingStrategy, float sample, float[] strataSamples, int verbose, int exclusiveSplitLimit) {

      DRFParams drfp = new DRFParams();
      drfp._ntrees           = ntrees;
      drfp._depth            = depth;
      drfp._sample           = sample;
      drfp._binLimit         = binLimit;
      drfp._stat             = statType;
      drfp._classcol         = col;
      drfp._seed             = seed;
      drfp._parallel         = parallelTrees;
      drfp._classWt          = classWt;
      drfp._numSplitFeatures = numSplitFeatures;
      drfp._samplingStrategy = samplingStrategy;
      drfp._verbose          = verbose;
      drfp._exclusiveSplitLimit = exclusiveSplitLimit;
      drfp._strataSamples    = strataSamples;
      drfp._numrows          = numrows;
      return drfp;
    }
  }

  /** DRF job showing progress with reflect to a number of generated trees. */
  public static class DRFJob extends Job {

    public DRFJob(String desc, Key dest) {
      super(desc, dest);
    }

    @Override public H2OCountedCompleter start(H2OCountedCompleter fjtask) {
      H2OCountedCompleter jobRemoval = new H2O.H2OCountedCompleter() {
        @Override public void compute2() { }
        @Override public void onCompletion(CountedCompleter caller) {
          DRFJob.this.remove();
        }
      };
      fjtask.setCompleter(jobRemoval);

      return super.start(jobRemoval);
    }
    @Override public float progress() {
      Progress p = (Progress) UKV.get(_dest);
      return p.progress();
    }
  }

  static void dumpRFParams(
      Key modelKey, int[] cols, ValueArray ary, int ntrees, int depth, int binLimit,
      StatType stat, long seed, boolean parallelTrees, double[] classWt, int numSplitFeatures,
      Sampling.Strategy samplingStrategy, float sample, float[] strataSamples, int verbose, int exclusiveSplitLimit) {
    RandomForest.OptArgs _ = new RandomForest.OptArgs();
    _.features = numSplitFeatures;
    _.ntrees   = ntrees;
    _.depth    = depth;
    _.classcol = cols[cols.length-1];
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

    Log.info(Sys.RANDF,"Web arguments: " + _ + " key "+ary._key);
  }
}
