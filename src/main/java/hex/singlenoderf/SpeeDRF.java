package hex.singlenoderf;

import hex.FrameTask;
import jsr166y.ForkJoinTask;
import water.*;
import water.api.Constants;
import water.fvec.Frame;
import water.fvec.Vec;
import water.util.Log;
import water.util.Utils;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;


public class SpeeDRF extends Job.ModelJob {

  //  protected final Key _dataKey = source._key;
//  protected final H2OHexKey         _dataKey    = new H2OHexKey(DATA_KEY);
//  protected final HexKeyClassCol    _classCol   = new HexKeyClassCol(CLASS, _dataKey);
//  public final Key _classCol = response._key;

  @API(help = "Number of trees", filter = Default.class, json = true, lmin = 1, lmax = Integer.MAX_VALUE)
  public int num_trees   = 50;
  @API(help = "Number of features to randomly select at each split.", filter = Default.class, json = true, lmin = -1, lmax = Integer.MAX_VALUE)
  public int mtry = -1;
  @API(help = "Max Depth", filter = Default.class, json = true, lmin = 0, lmax = Integer.MAX_VALUE)
  public int max_depth = 20;

//  protected final EnumArgument<StatType> _statType = new EnumArgument<Tree.StatType>(STAT_TYPE, StatType.ENTROPY);
//  protected final HexColumnSelect   _ignore     = new RFColumnSelect(IGNORE, _dataKey, _classCol);
//  protected final H2OCategoryWeights _weights   = new H2OCategoryWeights(WEIGHTS, source._key, _classCol, 1);
//  protected final EnumArgument<Sampling.Strategy> _samplingStrategy = new EnumArgument<Sampling.Strategy>(SAMPLING_STRATEGY, Sampling.Strategy.RANDOM, true);
//  protected final H2OCategoryStrata               _strataSamples    = new H2OCategoryStrata(STRATA_SAMPLES, _dataKey, _classCol, 67);

  @API(help = "Sampling Rate at each split.", filter = Default.class, json  = true, dmin = 0, dmax = 1)
  public double sample = 0.67;

  @API(help = "OOBEE", filter = Default.class, json = true)
  public boolean oobee = true;

  public Key _modelKey = dest();

  /* Advanced settings */
  @API(help = "bin limit", filter = Default.class, json = true, lmin = 0, lmax = 65534)
  public int bin_limit = 1024;

  @API(help = "seed", filter = Default.class, json = true)
  public long seed = (long) 1728318273;

  @API(help = "Build trees in parallel", filter = Default.class, json = true)
  public boolean  parallel  = true;

  @API(help = "split limit")
  public int _exclusiveSplitLimit = 0;

//  @API(help = "iterative cm")
//  public  boolean  _iterativeCM = true;

  @API(help = "use non local data")
  public boolean _useNonLocalData = true;

  /** Return the query link to this page */
//  public static String link(Key k, String content) {
//    RString rs = new RString("<a href='RF.query?%key_param=%$key'>%content</a>");
//    rs.replace("key_param", DATA_KEY);
//    rs.replace("key", k.toString());
//    rs.replace("content", content);
//    return rs.toString();
//  }

  public DRFParams drfParams;

  @Override protected void execImpl() {
    SpeeDRFModel rf_model = initModel();
    buildForest(rf_model);
    cleanup();
    remove();
  }

  @Override protected Response redirect() {
    return SpeeDRFProgressPage.redirect(this, self(), dest());
  }

  public final void buildForest(SpeeDRFModel model) {
    try {
      source.read_lock(self());
      logStart();
      if (model == null) model = UKV.get(dest());
      model.write_lock(self());
      drfParams = DRFParams.create(model.fr.find(model.response), model.total_trees, model.depth, (int)model.fr.numRows(), model.bin_limit,
              Tree.StatType.ENTROPY, seed, parallel, model.weights, mtry, model.sampling_strategy, (float) sample, model.strata_samples, 1, _exclusiveSplitLimit, _useNonLocalData);
      DRFTask tsk = new DRFTask();
      tsk._job = Job.findJob(self());
      tsk._params = drfParams;
      tsk._rfmodel = model;
      tsk._drf = this;
      tsk.validateInputData();
      tsk.invokeOnAllNodes();
    }
    catch(JobCancelledException ex) {
      Log.info("Random Forest building was cancelled.");
    }
    catch(Exception ex) {
      ex.printStackTrace();
      throw new RuntimeException(ex);
    }
    finally {
      if (model != null) {
        model = UKV.get(dest());
        model.unlock(self());
      }
      source.unlock(self());
      emptyLTrash();
    }
  }

  public SpeeDRFModel initModel() {
    try {
      source.read_lock(self());
      double[] weights = new double[(int)(response.max() - response.min() + 1)];
      float[] samples = new float[(int) (response.max() - response.min() + 1)];
      for(int i = 0; i < samples.length; ++i) samples[i] = (float)67.0;
      for(int i = 0; i < weights.length; ++i) weights[i] = 1.0;
      Frame train = FrameTask.DataInfo.prepareFrame(source, response, ignored_cols, false, false, false);
      SpeeDRFModel model = new SpeeDRFModel(dest(), self(), source._key, train, response, new Key[0]);
      model.bin_limit = bin_limit;
      if (mtry == -1) {
        model.mtry = (int) Math.floor(Math.sqrt(source.numCols()));
      }
      model.features = source.numCols();
      model.sampling_strategy = Sampling.Strategy.RANDOM;
      model.sample = (float) sample;
      model.weights = weights;
      model.time = 0;
      model.total_trees = num_trees;
      model.strata_samples = samples;
      model.depth = max_depth;
      return model;
    }
    finally {
      source.unlock(self());
    }
  }

  public final static class DRFTask extends water.DRemoteTask {
    /** The RF Model.  Contains the dataset being worked on, the classification
     *  column, and the training columns.  */
    public SpeeDRFModel _rfmodel;
    /** Job representing this DRF execution. */
    public Job _job;
    /** RF parameters. */
    public DRFParams _params;
    public SpeeDRF _drf;

    /**Inhale the data, build a DataAdapter and kick-off the computation.
     * */
    @Override public final void lcompute() {
      final DataAdapter dapt = DABuilder.create(_drf, _rfmodel).build(_rfmodel.fr);
      Data localData        = Data.make(dapt);
      int numSplitFeatures  = howManySplitFeatures();
      int ntrees            = howManyTrees();
      int[] rowsPerChunks   = howManyRPC(_rfmodel.fr);
      updateRFModel(_rfmodel._key, numSplitFeatures);
      SpeeDRF.build(_job, _params, localData, ntrees, numSplitFeatures, rowsPerChunks);
      tryComplete();
    }

    static void updateRFModel(Key modelKey, final int numSplitFeatures) {
      final int idx = H2O.SELF.index();
      new TAtomic<SpeeDRFModel>() {
        @Override public SpeeDRFModel atomic(SpeeDRFModel old) {
          if(old == null) return null;
          SpeeDRFModel newModel = (SpeeDRFModel)old.clone();
          newModel.node_split_features[idx] = numSplitFeatures;
          return newModel;
        }
      }.invoke(modelKey);
    }

    /** Unless otherwise specified each split looks at sqrt(#features). */
    private int howManySplitFeatures() {
      // FIXME should be run over the right data!
      if (_params._numSplitFeatures!=-1) return _params._numSplitFeatures;
      return (int)Math.sqrt(_rfmodel.fr.numCols()-1/*we don't used the class column*/);
    }

    /** Figure the number of trees to make locally, so the total hits ntrees.
     *  Divide equally amongst all the nodes that actually have data.  First:
     *  compute how many nodes have data.  Give each Node ntrees/#nodes worth of
     *  trees.  Round down for later nodes, and round up for earlier nodes.
     */
    private int howManyTrees() {
      Frame fr = _rfmodel.fr;
      final long num_chunks = fr.anyVec().nChunks();
      final int  num_nodes  = H2O.CLOUD.size();
      HashSet<H2ONode> nodes = new HashSet<H2ONode>();
      for( int i=0; i<num_chunks; i++ ) {
        nodes.add(fr.anyVec().chunkKey(i).home_node());
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

    private int[] howManyRPC(Frame fr) {
      int[] result = new int[fr.anyVec().nChunks()];
      for(int i = 0; i < result.length; ++i) {
        result[i] = fr.anyVec().chunkLen(i);
      }
      return result;
    }

    private void validateInputData(){
      Vec[] vecs = _rfmodel.fr.vecs();
      Vec c = _rfmodel.response;
      String err = "Response column must be an integer in the interval [2,254]";
      if(!(c.isEnum() || c.isInt()))
        throw new IllegalArgumentException("Regression is not supported: "+err);
      final int classes = (int)(c.max() - c.min())+1;
      if( !(2 <= classes && classes <= 254 ) )
        throw new IllegalArgumentException("Found " + classes+" classes: "+err);
      if (0.0f > _params._sample || _params._sample > 1.0f)
        throw new IllegalArgumentException("Sampling rate must be in [0,1] but found "+ _params._sample);
      if (_params._numSplitFeatures!=-1 && (_params._numSplitFeatures< 1 || _params._numSplitFeatures>vecs.length-1))
        throw new IllegalArgumentException("Number of split features exceeds available data. Should be in [1,"+(vecs.length-1)+"]");
      ChunkAllocInfo cai = new ChunkAllocInfo();
      if (_params._useNonLocalData && !canLoadAll( _rfmodel.fr, cai ))
        throw new IllegalArgumentException(
                "Cannot load all data from remote nodes - " +
                        "the node " + cai.node + " requires " + PrettyPrint.bytes(cai.requiredMemory) + " to load all data and perform computation but there is only " + PrettyPrint.bytes(cai.availableMemory) + " of available memory. " +
                        "Please provide more memory for JVMs or disable the option '"+ Constants.USE_NON_LOCAL_DATA+"' (however, it may affect resulting accuracy).");
    }

    private boolean canLoadAll(final Frame fr, ChunkAllocInfo cai) {
      int nchks = fr.anyVec().nChunks();
      long localBytes = 0l;
      for (int i = 0; i < nchks; ++i) {
        Key k = fr.anyVec().chunkKey(i);
        if (k.home()) {
          localBytes += fr.anyVec().chunkForChunkIdx(i).byteSize();
        }
      }
      long memForNonLocal = fr.byteSize() - localBytes;
      for(int i = 0; i < H2O.CLOUD._memary.length; i++) {
        HeartBeat hb = H2O.CLOUD._memary[i]._heartbeat;
        long nodeFreeMemory = (long)( (hb.get_max_mem()-(hb.get_tot_mem()-hb.get_free_mem())) * OVERHEAD_MAGIC);
        Log.debug(Log.Tag.Sys.RANDF, i + ": computed available mem: " + PrettyPrint.bytes(nodeFreeMemory));
        Log.debug(Log.Tag.Sys.RANDF, i + ": remote chunks require: " + PrettyPrint.bytes(memForNonLocal));
        if (nodeFreeMemory - memForNonLocal <= 0) {
          cai.node = H2O.CLOUD._memary[i];
          cai.availableMemory = nodeFreeMemory;
          cai.requiredMemory = memForNonLocal;
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

    static final float OVERHEAD_MAGIC = 3/8.f; // memory overhead magic

    @Override
    public void reduce(DRemoteTask drt) { }
  }



    private static final long ROOT_SEED_ADD  = 0x026244fd935c5111L;
    private static final long TREE_SEED_INIT = 0x1321e74a0192470cL;

    /** Build random forest for data stored on this node. */
    public static void build(
            final Job job,
            final DRFParams drfParams,
            final Data localData,
            int ntrees,
            int numSplitFeatures,
            int[] rowsPerChunks) {
      Timer  t_alltrees = new Timer();
      Tree[] trees      = new Tree[ntrees];
      Log.debug(Log.Tag.Sys.RANDF,"Building "+ntrees+" trees");
      Log.debug(Log.Tag.Sys.RANDF,"Number of split features: "+ numSplitFeatures);
      Log.debug(Log.Tag.Sys.RANDF,"Starting RF computation with "+ localData.rows()+" rows ");

      Random rnd = Utils.getRNG(localData.seed() + ROOT_SEED_ADD);
      Sampling sampler = createSampler(drfParams, rowsPerChunks); ///TODO
      byte producerId = (byte) H2O.SELF.index();
      for (int i = 0; i < ntrees; ++i) {
        long treeSeed = rnd.nextLong() + TREE_SEED_INIT; // make sure that enough bits is initialized
        trees[i] = new Tree(job, localData, producerId, drfParams._depth, drfParams._stat, numSplitFeatures, treeSeed,
                i, drfParams._exclusiveSplitLimit, sampler, drfParams._verbose);
        if (!drfParams._parallel)   ForkJoinTask.invokeAll(new Tree[]{trees[i]});
      }

      if(drfParams._parallel) DRemoteTask.invokeAll(trees);
      Log.debug(Log.Tag.Sys.RANDF,"All trees ("+ntrees+") done in "+ t_alltrees);
    }

    static Sampling createSampler(final DRFParams params, int[] rowsPerChunks) {
      switch(params._samplingStrategy) {
        case RANDOM          : return new Sampling.Random(params._sample, rowsPerChunks);
        case STRATIFIED_LOCAL:
          float[] ss = new float[params._strataSamples.length];
          for (int i=0;i<ss.length;i++) ss[i] = params._strataSamples[i] / 100.f;
          return new Sampling.StratifiedLocal(ss, params._numrows);
        default:
          assert false : "Unsupported sampling strategy";
          return null;
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
    Tree.StatType _stat;
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

    public static DRFParams create(int col, int ntrees, int depth, int numrows, int binLimit,
                                         Tree.StatType statType, long seed, boolean parallelTrees, double[] classWt,
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
}






