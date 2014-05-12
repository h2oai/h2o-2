package hex.singlenoderf;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import hex.FrameTask;
import jsr166y.ForkJoinTask;
import water.*;
import water.api.Constants;
import water.api.DocGen;
import water.api.ParamImportance;
import water.fvec.Frame;
import water.fvec.Vec;
import water.util.Log;
import water.util.Utils;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;


public class SpeeDRF extends Job.ValidatedJob {
  static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
  public static DocGen.FieldDoc[] DOC_FIELDS;
  public static final String DOC_GET = "SpeeDRF";

  @API(help = "Number of trees", filter = Default.class, json = true, lmin = 1, lmax = Integer.MAX_VALUE, importance = ParamImportance.CRITICAL)
  public int num_trees   = 50;

  @API(help = "Number of features to randomly select at each split.", filter = Default.class, json = true, lmin = -1, lmax = Integer.MAX_VALUE, importance = ParamImportance.SECONDARY)
  public int mtry = -1;

  @API(help = "Max Depth", filter = Default.class, json = true, lmin = 0, lmax = Integer.MAX_VALUE, importance = ParamImportance.CRITICAL)
  public int max_depth = 20;

  @API(help = "Split Criterion Type", filter = Default.class, json=true, importance = ParamImportance.SECONDARY)
  public Tree.StatType stat_type = Tree.StatType.ENTROPY;

  @API(help = "Class Weights (0.0,0.2,0.4,0.6,0.8,1.0)", filter = Default.class, displayName = "class weights", json = true, importance = ParamImportance.SECONDARY)
  public double[] class_weights = null;

  @API(help = "Sampling Strategy", filter = Default.class, json = true, importance = ParamImportance.SECONDARY)
  public Sampling.Strategy sampling_strategy = Sampling.Strategy.RANDOM;

  @API(help = "Strata Samples", filter = Default.class, json = true, importance = ParamImportance.SECONDARY)
  int[] strata_samples = null;

  @API(help = "Sampling Rate at each split.", filter = Default.class, json  = true, dmin = 0, dmax = 1, importance = ParamImportance.EXPERT)
  public double sample = 0.67;

  @API(help = "OOBEE", filter = Default.class, json = true, importance = ParamImportance.SECONDARY)
  public boolean oobee = true;

  @API(help = "Variable Importance", filter = Default.class, json = true)
  public boolean importance = false;

  public Key _modelKey = dest();

  /* Advanced settings */
  @API(help = "bin limit", filter = Default.class, json = true, lmin = 0, lmax = 65534, importance = ParamImportance.EXPERT)
  public int bin_limit = 1024;

  @API(help = "seed", filter = Default.class, json = true, importance = ParamImportance.EXPERT)
  public long seed = -1;

  @API(help = "Build trees in parallel", importance = ParamImportance.SECONDARY)
  public boolean  parallel  = true;

  @API(help = "split limit", importance = ParamImportance.EXPERT)
  public int _exclusiveSplitLimit = 0;

//  @API(help = "iterative cm")
//  public  boolean  _iterativeCM = true;

  @API(help = "use non local data", importance = ParamImportance.EXPERT)
  public boolean _useNonLocalData = true;

  private static final Random _seedGenerator = Utils.getDeterRNG(0xd280524ad7fe0602L);

  private boolean regression;

  /** Return the query link to this page */
//  public static String link(Key k, String content) {
//    RString rs = new RString("<a href='RF.query?%key_param=%$key'>%content</a>");
//    rs.replace("key_param", DATA_KEY);
//    rs.replace("key", k.toString());
//    rs.replace("content", content);
//    return rs.toString();
//  }

  public DRFParams drfParams;

  @Override protected void queryArgumentValueSet(Argument arg, java.util.Properties inputArgs) {
    super.queryArgumentValueSet(arg, inputArgs);

//    if (arg._name.equals("classification")) {
//      arg._hideInQuery = true;
//    }
    if (arg._name.equals("classification")) {
      regression = !this.classification;
    }
    if (arg._name.equals("class_weights")) {
      if (source == null || response == null) {
        arg.disable("Requires source and response to be specified.");
      }
    }
    if (arg._name.equals("sampling_strategy")) {
      arg.setRefreshOnChange();
      if (regression) {
        arg.disable("Random Sampling for regression trees.");
      }
    }
    if (arg._name.equals("stat_type")) {
      if(regression) {
        arg.disable("Minimize MSE for regression.");
      }
    }
    if (arg._name.equals("strata_samples")) {
      if (sampling_strategy != Sampling.Strategy.STRATIFIED_LOCAL) {
        arg.disable("No Strata for Random sampling.");
      }
      if (regression) {
        arg.disable("No strata for regression.");
      }
    }
    if (arg._name.equals("class_weights")) {
      if (source == null || response == null) {
        arg.disable("Requires a dataset and response.");
      }
      if (regression) {
        arg.disable("No class weights for regression.");
      }
    }
  }

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
      if (model == null) model = UKV.get(dest());
      model.write_lock(self());
      drfParams = DRFParams.create(model.fr.find(model.response), model.N, model.max_depth, (int)model.fr.numRows(), model.nbins,
              model.statType, seed, parallel, model.weights, mtry, model.sampling_strategy, (float) sample, model.strata_samples, 1, _exclusiveSplitLimit, _useNonLocalData, regression);
      logStart();
      DRFTask tsk = new DRFTask();
      tsk._job = Job.findJob(self());
      tsk._params = drfParams;
      tsk._rfmodel = model;
      tsk._drf = this;
      tsk.validateInputData();
      tsk.invokeOnAllNodes(); //this is bad when chunks aren't on each node!
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
      float[] samps = null;
      if(!regression) {
        samps = new float[(int) (response.max() - response.min() + 1)];
        for (int i = 0; i < samps.length; ++i ) samps[i] = 67;
        if (strata_samples == null) samps = null;
        if (strata_samples != null) {
          int[] _samples = new int[(int) (response.max() - response.min() + 1)];
          for (int i = 0; i < _samples.length; ++i ) _samples[i] = 67;
          if(strata_samples.length > _samples.length) {
            System.arraycopy(_samples, 0, strata_samples, 0, _samples.length);
            strata_samples = _samples;
          }
          if(strata_samples.length < _samples.length) {
            System.arraycopy(strata_samples, 0, _samples, 0, strata_samples.length);
            strata_samples = _samples;
          }
          for (int i = 0; i < _samples.length; ++i) {
            samps[i] = (float)strata_samples[i];
          }
        }
        if (class_weights != null) {
          double[] weights = new double[(int) (response.max() - response.min() + 1)];
          for (int i = 0; i < weights.length; ++i ) weights[i] = 1.0;
          if (class_weights == null) {
            class_weights = weights;
          }
          if(class_weights.length > weights.length) {
            System.arraycopy(class_weights, 0, weights, 0, weights.length);
            class_weights = weights;
          }
          if(class_weights.length < weights.length) {
            System.arraycopy(class_weights, 0, weights, 0, class_weights.length);
            class_weights = weights;
          }
        }
      } else { class_weights = null; strata_samples = null; samps = null; }
      if (seed == -1) {
        seed = _seedGenerator.nextLong();
      }
      Frame train = FrameTask.DataInfo.prepareFrame(source, response, ignored_cols, false, false, false);
      Frame test = null;
      if (validation != null) {
        test = FrameTask.DataInfo.prepareFrame(validation, validation.vecs()[source.find(response)], ignored_cols, false, false, false);
      }
      SpeeDRFModel model = new SpeeDRFModel(dest(), self(), source._key, train, response, new Key[0], seed, getCMDomain(), this);
      model.nbins = bin_limit;
      if (mtry == -1) {
        if(!regression) {
           model.mtry = (int) Math.floor(Math.sqrt(source.numCols()));
        } else {model.mtry = (int) Math.floor((float) source.numCols() / 3.0f); }
      } else {
        model.mtry = mtry;
      }
      model.regression = regression;
      model.features = source.numCols();
      model.sampling_strategy = regression ? Sampling.Strategy.RANDOM : sampling_strategy;
      model.sample = (float) sample;
      model.weights = regression ? null : class_weights;
      model.time = 0;
      model.N = num_trees;
      model.strata_samples = samps;
      model.max_depth = max_depth;
      model.oobee = validation == null && oobee;
      model.statType = regression ? Tree.StatType.MSE : stat_type;
      model.test_frame = test;
      model.testKey = validation == null ? null : validation._key;
      model.importance = importance;
      return model;
    }
    finally {
      source.unlock(self());
    }
  }

  public Frame score( Frame fr ) { return ((SpeeDRFModel)UKV.get(dest())).score(fr);  }


  public final static class DRFTask extends DRemoteTask {
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
      if (dapt == null) {
        tryComplete();
        return;
      }
      Data localData        = Data.make(dapt);
      int numSplitFeatures  = howManySplitFeatures();
      int ntrees            = howManyTrees();
      int[] rowsPerChunks   = howManyRPC(_rfmodel.fr);
      updateRFModel(_rfmodel._key, numSplitFeatures);
      updateRFModelStatus(_rfmodel._key, "Building Forest");
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

    static void updateRFModelStatus(Key modelKey, final String status) {
      new TAtomic<SpeeDRFModel>() {
        @Override public SpeeDRFModel atomic(SpeeDRFModel old) {
          if(old == null) return null;
          SpeeDRFModel newModel = (SpeeDRFModel)old.clone();
          newModel.current_status = status;
          return newModel;
        }
      }.invoke(modelKey);
    }

    /** Unless otherwise specified each split looks at sqrt(#features). */
    private int howManySplitFeatures() {
      if (_params.num_split_features!=-1) return _params.num_split_features;
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
      int ntrees = _params.num_trees/nodes.size();
      if( Arrays.binarySearch(array, H2O.SELF) < _params.num_trees - ntrees*nodes.size() )
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
      if( !_rfmodel.regression & (!(c.isEnum() || c.isInt())))
        throw new IllegalArgumentException("Classification cannot be performed on a float column: "+err);

      if(!_rfmodel.regression) {
        final int classes = (int)(c.max() - c.min())+1;
        if( !(2 <= classes && classes <= 254 ) )
          throw new IllegalArgumentException("Found " + classes+" classes: "+err);
      if (0.0f > _params.sample || _params.sample > 1.0f)
        throw new IllegalArgumentException("Sampling rate must be in [0,1] but found "+ _params.sample);
      }
      if (_params.num_split_features!=-1 && (_params.num_split_features< 1 || _params.num_split_features>vecs.length-1))
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
      Sampling sampler = createSampler(drfParams, rowsPerChunks);
      byte producerId = (byte) H2O.SELF.index();
      for (int i = 0; i < ntrees; ++i) {
        long treeSeed = rnd.nextLong() + TREE_SEED_INIT; // make sure that enough bits is initialized
        trees[i] = new Tree(job, localData, producerId, drfParams.max_depth, drfParams.stat_type, numSplitFeatures, treeSeed,
                i, drfParams._exclusiveSplitLimit, sampler, drfParams._verbose, drfParams.regression);
        if (!drfParams.parallel)   ForkJoinTask.invokeAll(new Tree[]{trees[i]});
      }

      if(drfParams.parallel) DRemoteTask.invokeAll(trees);
      Log.debug(Log.Tag.Sys.RANDF,"All trees ("+ntrees+") done in "+ t_alltrees);
    }

    static Sampling createSampler(final DRFParams params, int[] rowsPerChunks) {
      switch(params.sampling_strategy) {
        case RANDOM          : return new Sampling.Random(params.sample, rowsPerChunks);
        case STRATIFIED_LOCAL:
          float[] ss = new float[params.strata_samples.length];
          for (int i=0;i<ss.length;i++) ss[i] = params.strata_samples[i] / 100.f;
          return new Sampling.StratifiedLocal(ss, params._numrows);
        default:
          assert false : "Unsupported sampling strategy";
          return null;
      }
    }

  /** RF execution parameters. */
  public final static class DRFParams extends Iced {
    /** Total number of trees */
    int num_trees;
    /** If true, build trees in parallel (default: true) */
    boolean parallel;
    /** Maximum depth for trees (default MaxInt) */
    int max_depth;
    /** Split statistic */
    Tree.StatType stat_type;
    /** Feature holding the classifier  (default: #features-1) */
    int classcol;
    /** Utilized sampling method */
    Sampling.Strategy sampling_strategy;
    /** Proportion of observations to use for building each individual tree (default: .67)*/
    float sample;
    /** Limit of the cardinality of a feature before we bin. */
    int bin_limit;
    /** Weights of the different features (default: 1/features) */
    double[] class_weights;
    /** Arity under which we may use exclusive splits */
    public int _exclusiveSplitLimit;
    /** Output warnings and info*/
    public int _verbose;
    /** Number of features which are tried at each split
     *  If it is equal to -1 then it is computed as sqrt(num of usable columns) */
    int num_split_features;
    /** Defined stratas samples for each class */
    float[] strata_samples;
    /** Utilize not only local data but try to use data from other nodes. */
    boolean _useNonLocalData;
    /** Number of rows per chunk - used to replay sampling */
    int _numrows;
    /** Pseudo random seed initializing RF algorithm */
    long seed;
    /** Build regression trees if true */
    boolean regression;

    public static DRFParams create(int col, int ntrees, int depth, int numrows, int binLimit,
                                         Tree.StatType statType, long seed, boolean parallelTrees, double[] classWt,
                                         int numSplitFeatures, Sampling.Strategy samplingStrategy, float sample,
                                         float[] strataSamples, int verbose, int exclusiveSplitLimit,
                                         boolean useNonLocalData, boolean regression) {

      DRFParams drfp = new DRFParams();
      drfp.num_trees           = ntrees;
      drfp.max_depth            = depth;
      drfp.sample           = sample;
      drfp.bin_limit         = binLimit;
      drfp.stat_type             = statType;
      drfp.seed             = seed;
      drfp.parallel         = parallelTrees;
      drfp.class_weights          = classWt;
      drfp.num_split_features = numSplitFeatures;
      drfp.sampling_strategy = samplingStrategy;
      drfp.strata_samples    = strataSamples;
      drfp._numrows = numrows;
      drfp._useNonLocalData = useNonLocalData;
      drfp._exclusiveSplitLimit = exclusiveSplitLimit;
      drfp._verbose = verbose;
      drfp.classcol = col;
      drfp.regression = regression;
      return drfp;
    }
  }
}
