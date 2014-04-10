package hex.rf;

import hex.rf.Tree.StatType;

import java.util.Arrays;
import java.util.HashSet;

import jsr166y.CountedCompleter;
import water.*;
import water.H2O.H2OCountedCompleter;
import water.ValueArray.Column;
import water.api.Constants;
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
      Sampling.Strategy samplingStrategy, float sample, float[] strataSamples, int verbose, int exclusiveSplitLimit, boolean useNonLocalData) {

    // Create DRF remote task
    DRFJob  drfJob  = new DRFJob(ntrees, modelKey);
    DRFTask drfTask = create(drfJob,modelKey, cols, ary, ntrees, depth, binLimit, stat, seed, parallelTrees, classWt, numSplitFeatures, samplingStrategy, sample, strataSamples, verbose, exclusiveSplitLimit, useNonLocalData);
    // Create DRF user job & start it
    drfJob.start(drfTask);
    drfTask._job = drfJob;
    // Execute the DRF task
    drfTask.dfork(drfTask._rfmodel._dataKey);

    return drfJob;
  }

  /** Create and configure a new DRF remote task.
   *  It does not execute DRF !!! */
  private static DRFTask create(
    Job job, Key modelKey, int[] cols, ValueArray ary, int ntrees, int depth, int binLimit,
    StatType stat, long seed, boolean parallelTrees, double[] classWt, int numSplitFeatures,
    Sampling.Strategy samplingStrategy, float sample, float[] strataSamples,
    int verbose, int exclusiveSplitLimit, boolean useNonLocalData) {

    // Construct the RFModel to be trained
    DRFTask drf  = new DRFTask();
    drf._rfmodel = new RFModel(modelKey, cols, ary._key,
                               new Key[0], ary._cols.length, samplingStrategy, sample, strataSamples, numSplitFeatures, ntrees);
    // Save the number of rows per chunk - it is needed for proper sampling.
    // But it will need to be changed with new fluid vectors
    //assert ary._rpc == null : "DRF does not support different sizes of chunks for now!";
    int numrows = (int) (ValueArray.CHUNK_SZ/ary._rowsize);
    drf._params = DRFParams.create(cols[cols.length-1], ntrees, depth, numrows, binLimit, stat, seed, parallelTrees, classWt, numSplitFeatures, samplingStrategy, sample, strataSamples, verbose, exclusiveSplitLimit, useNonLocalData);
    // Verbose debug print
    if (verbose>0) dumpRFParams(modelKey, cols, ary, ntrees, depth, binLimit, stat, seed, parallelTrees, classWt, numSplitFeatures, samplingStrategy, sample, strataSamples, verbose, exclusiveSplitLimit, useNonLocalData);
    // Validate parameters
    drf.validateInputData();
    // Start the timer.
    drf._t_main = new Timer();
    // Push the RFModel globally first
    drf._rfmodel.delete_and_lock(job.self());
    DKV.write_barrier();

    return drf;
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
      ChunkAllocInfo cai = new ChunkAllocInfo();
      if (_params._useNonLocalData && !canLoadAll( (ValueArray) UKV.get(_rfmodel._dataKey), cai ))
        throw new IllegalArgumentException(
            "Cannot load all data from remote nodes - " +
            "the node " + cai.node + " requires " + PrettyPrint.bytes(cai.requiredMemory) + " to load all data and perform computation but there is only " + PrettyPrint.bytes(cai.availableMemory) + " of available memory. " +
            "Please provide more memory for JVMs or disable the option '"+Constants.USE_NON_LOCAL_DATA+"' (however, it may affect resulting accuracy).");
    }

    private boolean canLoadAll(final ValueArray ary, ChunkAllocInfo cai) {
      long[] localChunks = new long[H2O.CLOUD.size()];
      // Collect number of local chunks
      for(int i=0; i<ary.chunks(); i++) {
        Key k = ary.getChunkKey(i);
        localChunks[k.home(H2O.CLOUD)]++;
      }
      for(int i=0; i<localChunks.length; i++) {
        long needToLoad = ary.chunks() - localChunks[i]; // number of chunks to load
        long memoryForChunks = needToLoad * ValueArray.CHUNK_SZ;
        HeartBeat hb = H2O.CLOUD._memary[i]._heartbeat; // use last heartbeat to estimate free memory
        long nodeFreeMemory = (long)( (hb.get_max_mem()-(hb.get_tot_mem()-hb.get_free_mem())) * OVERHEAD_MAGIC);
        Log.debug(Sys.RANDF, i + ": computed available mem: " + PrettyPrint.bytes(nodeFreeMemory));
        Log.debug(Sys.RANDF, i + ": remote chunks require: " + PrettyPrint.bytes(memoryForChunks));
        if (nodeFreeMemory  - memoryForChunks <= 0) {
          cai.node = H2O.CLOUD._memary[i];
          cai.availableMemory = nodeFreeMemory;
          cai.requiredMemory = memoryForChunks;
          return false;
        }
      }
      return true;
    }

    /** Helper POJO to store required chunk allocation. */
    private static class ChunkAllocInfo {
      H2ONode node;
      long availableMemory;
      long requiredMemory;
    }

    /**Inhale the data, build a DataAdapter and kick-off the computation.
     * */
    @Override public final void lcompute() {
      // Build data adapter for this node.
      Timer t_extract = new Timer();
      // Collect remote keys to load if necessary
      final Key[] lkeys = _keys;
      final Key[] rkeys = getNonLocalChunks(lkeys);
      final DataAdapter dapt = DABuilder.create(this).build(lkeys, rkeys);
      Log.debug(Sys.RANDF,"Data adapter built in " + t_extract );
      // Prepare data and compute missing parameters.
      Data localData        = Data.make(dapt);
      int numSplitFeatures  = howManySplitFeatures(localData);
      int ntrees            = howManyTrees();
      int[] rowsPerChunks   = howManyRPC(lkeys, rkeys);
      // write number of split features
      updateRFModel(_job.dest(), numSplitFeatures, rkeys);

      // Build local random forest
      RandomForest.build(_job, _params, localData, ntrees, numSplitFeatures, rowsPerChunks);
      // Wait for the running jobs
      tryComplete();
    }

    @Override public final void reduce( DRemoteTask drt ) { }

    @Override public boolean onExceptionalCompletion(Throwable ex, CountedCompleter caller) {
      if (_job!=null) _job.cancel(ex);
      return super.onExceptionalCompletion(ex, caller);
    }
    @Override protected void postGlobal(){
      RFModel rf = UKV.get(_rfmodel._key);
      rf.unlock(_job.self());
    }

    /** Write number of split features computed on this node to a model */
    static void updateRFModel(Key modelKey, final int numSplitFeatures, final Key[] rkeys) {
      final int idx = H2O.SELF.index();
      new TAtomic<RFModel>() {
        @Override public RFModel atomic(RFModel old) {
          if(old == null) return null;
          RFModel newModel = (RFModel)old.clone();
          newModel._nodesSplitFeatures[idx] = numSplitFeatures;
          newModel._remoteChunksKeys  [idx] = rkeys;
          return newModel;
        }
      }.invoke(modelKey);
    }

    private static final Key[] NO_KEYS = new Key[] {};

    static final float OVERHEAD_MAGIC = 3/8.f; // memory overhead magic
    /** Return a list of chunk keys which can be loaded from other nodes. */
    private Key[] getNonLocalChunks(Key[] localCKeys) {
      Log.info(Sys.RANDF, "Use non-local data: " + _params._useNonLocalData);
      if (_params._useNonLocalData) {

        long totalmem = Runtime.getRuntime().maxMemory()-(Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory());
        long availMem = (long) (OVERHEAD_MAGIC * totalmem); // theoretically available memory with overhead

        final ValueArray ary = UKV.get(_rfmodel._dataKey);
        // Try to fill the memory up to ratio 3/8 - load all chunks now since the memory condition are checked at beginning
        int numkeys = (int) (ary.chunks()-localCKeys.length); //Math.min( (int) (availMem / ValueArray.CHUNK_SZ), (int) (ary.chunks()-localCKeys.length));
        Log.info(Sys.RANDF, "Computed available mem: " + PrettyPrint.bytes(availMem));
        Log.info(Sys.RANDF, "Trying to load non-local keys: " + numkeys + " from " + (ary.chunks()-localCKeys.length));
        assert availMem - numkeys * ValueArray.CHUNK_SZ > 0 : "There is not enough memory to load all remote keys!";
        if (numkeys>0) {
          Key[] rkeys = new Key[numkeys];
          int c = 0;
          for(int i=0; i<ary.chunks(); i++) {
            Key k = ary.getChunkKey(i);
            if (!k.home()) rkeys[c++] = k;
            if (c == numkeys) break;
          }
          return rkeys;
        }
      }
      return NO_KEYS;
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

    private int[] howManyRPC(Key[] lkeys, Key[] rkeys) {
      int[] result = new int[lkeys.length+rkeys.length];
      final ValueArray ary = UKV.get(_rfmodel._dataKey);
      int idx = 0;
      for (Key k : lkeys) result[idx++] = ary.rpc(ValueArray.getChunkIndex(k));
      for (Key k : rkeys) result[idx++] = ary.rpc(ValueArray.getChunkIndex(k));
      return result;
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
    /** Split statistic */
    StatType _stat;
    /** Feature holding the classifier  (default: #features-1) */
    int _classcol;
    /** Utilized sampling method */
    Sampling.Strategy _samplingStrategy;
    /** Proportion of observations to use for building each individual tree (default: .67)*/
    float _sample;
    /** Limit of the cardinality of a feature before we bin. */
    int _binLimit;
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
    /** Utilize not only local data but try to use data from other nodes. */
    boolean _useNonLocalData;
    /** Number of rows per chunk - used to replay sampling */
    int _numrows;
    /** Pseudo random seed initializing RF algorithm */
    long _seed;

    public static final DRFParams create(int col, int ntrees, int depth, int numrows, int binLimit,
        StatType statType, long seed, boolean parallelTrees, double[] classWt,
        int numSplitFeatures, Sampling.Strategy samplingStrategy, float sample,
        float[] strataSamples, int verbose, int exclusiveSplitLimit,
        boolean useNonLocalData) {

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
      drfp._useNonLocalData  = useNonLocalData;
      return drfp;
    }
  }

  /** DRF job showing progress with reflect to a number of generated trees. */
  public static class DRFJob extends Job {

    public DRFJob(int ntrees, Key destKey) {
      description = "RandomForest_" + ntrees + "trees";
      destination_key = destKey;
    }

    @Override public Job start(H2OCountedCompleter fjtask) {
      H2OCountedCompleter jobRemoval = new H2O.H2OCountedCompleter() {
        @Override public void compute2() {
          new TAtomic<RFModel>() {
            @Override public RFModel atomic(RFModel old) {
              if(old == null) return null;
              old._time = DRFJob.this.runTimeMs();
              return old;
            }
          }.invoke(dest());
        }
        @Override public void onCompletion(CountedCompleter caller) {
          DRFJob.this.remove();
        }
      };
      fjtask.setCompleter(jobRemoval);
      return super.start(jobRemoval);
    }
    @Override public float progress() {
      Progress p = (Progress) UKV.get(destination_key);
      return p.progress();
    }
  }

  static void dumpRFParams(
      Key modelKey, int[] cols, ValueArray ary, int ntrees, int depth, int binLimit,
      StatType stat, long seed, boolean parallelTrees, double[] classWt, int numSplitFeatures,
      Sampling.Strategy samplingStrategy, float sample, float[] strataSamples,
      int verbose, int exclusiveSplitLimit, boolean useNonLocalData) {
    RandomForest.OptArgs o = new RandomForest.OptArgs();
    o.features = numSplitFeatures;
    o.ntrees   = ntrees;
    o.depth    = depth;
    o.classcol = cols[cols.length-1];
    o.seed     = seed;
    o.binLimit = binLimit;
    o.verbose  = verbose;
    o.exclusive= exclusiveSplitLimit;
    String w = "";
    if (classWt != null) for(int i=0;i<classWt.length;i++) w += i+":"+classWt[i]+",";
    o.weights=w;
    o.parallel = parallelTrees ? 1 : 0;
    o.statType = stat.ordinal() == 1 ? "gini" : "entropy";
    o.sample = (int)(sample * 100);
    o.file = "";

    Log.info(Sys.RANDF,"Web arguments: " + o + " key "+ary._key);
  }
}
