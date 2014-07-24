package hex.singlenoderf;


import static water.util.MRUtils.sampleFrameStratified;
import hex.*;
import hex.ConfusionMatrix;

import java.util.*;

import water.*;
import water.Timer;
import water.api.*;
import water.fvec.Frame;
import water.fvec.Vec;
import water.util.*;

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
  public Tree.SelectStatType select_stat_type = Tree.SelectStatType.ENTROPY;

  @API(help = "Use local data. Auto-enabled if data does not fit in a single node.", filter = Default.class, json = true, importance = ParamImportance.EXPERT)
  public boolean local_mode = false;

  /* Legacy parameter: */
  public double[] class_weights = null;

  @API(help = "Sampling Strategy", filter = Default.class, json = true, importance = ParamImportance.SECONDARY)
  public Sampling.Strategy sampling_strategy = Sampling.Strategy.RANDOM;


  @API(help = "Sampling Rate at each split.", filter = Default.class, json  = true, dmin = 0, dmax = 1, importance = ParamImportance.EXPERT)
  public double sample = 0.67;

  @API(help ="Score each iteration", filter = Default.class, json = true, importance = ParamImportance.SECONDARY)
  public boolean score_each_iteration = false;

  /*Imbalanced Classes*/
  /**
  * For imbalanced data, balance training data class counts via
  * over/under-sampling. This can result in improved predictive accuracy.
  */
  @API(help = "Balance training data class counts via over/under-sampling (for imbalanced data)", filter = Default.class, json = true, importance = ParamImportance.EXPERT)
  public boolean balance_classes = false;

  /**
  * When classes are balanced, limit the resulting dataset size to the
  * specified multiple of the original dataset size.
  */
  @API(help = "Maximum relative size of the training data after balancing class counts (can be less than 1.0)", filter = Default.class, json = true, dmin=1e-3, importance = ParamImportance.EXPERT)
  public float max_after_balance_size = Float.POSITIVE_INFINITY;

  @API(help = "Out of bag error estimate", filter = Default.class, json = true, importance = ParamImportance.SECONDARY)
  public boolean oobee = true;

  @API(help = "Variable Importance", filter = Default.class, json = true)
  public boolean importance = false;

  public Key _modelKey = dest();

  /* Advanced settings */
  @API(help = "bin limit", filter = Default.class, json = true, lmin = 0, lmax = 65534, importance = ParamImportance.EXPERT)
  public int bin_limit = 1024;

  @API(help = "seed", filter = Default.class, json = true, importance = ParamImportance.EXPERT)
  public long seed = -1;

  @API(help = "Tree splits and extra statistics printed to stdout.", filter = Default.class, json = true, importance = ParamImportance.EXPERT)
  public boolean verbose = false;

  @API(help = "split limit", importance = ParamImportance.EXPERT)
  public int _exclusiveSplitLimit = 0;

  private static final Random _seedGenerator = Utils.getDeterRNG(0xd280524ad7fe0602L);

  private boolean regression;

  public DRFParams drfParams;

  protected SpeeDRFModel makeModel( SpeeDRFModel model, double err, ConfusionMatrix cm, VarImp varimp, AUCData validAUC) {
    return new SpeeDRFModel(model, err, cm, varimp, validAUC);
  }

  @Override protected void queryArgumentValueSet(Argument arg, java.util.Properties inputArgs) {
    super.queryArgumentValueSet(arg, inputArgs);

    if (arg._name.equals("classification")) {
      arg._hideInQuery = true;
    }

    if (arg._name.equals("balance_classes")) {
      arg.setRefreshOnChange();
      if(regression) {
        arg.disable("Class balancing is only for classification.");
      }
    }

    // Regression is selected if classification is false and vice-versa.
    if (arg._name.equals("classification")) {
      regression = !this.classification;
    }

    // Regression only accepts the MSE stat type.
    if (arg._name.equals("select_stat_type")) {
      if(regression) {
        arg.disable("Minimize MSE for regression.");
      }
    }

    // Class weights depend on the source data set an response value to be specified and are invalid for regression
    if (arg._name.equals("class_weights")) {
      if (source == null || response == null) {
        arg.disable("Requires source and response to be specified.");
      }
      if (regression) {
        arg.disable("No class weights for regression.");
      }
    }

    // Prevent Stratified Local when building regression tress.
    if (arg._name.equals("sampling_strategy")) {
      arg.setRefreshOnChange();
      if (regression) {
        arg.disable("Random Sampling for regression trees.");
      }
    }

    // Variable Importance disabled in SpeeDRF regression currently
    if (arg._name.equals("importance")) {
      if (regression) {
        arg.disable("Variable Importance not supported in SpeeDRF regression.");
      }
    }

    // max balance size depends on balance_classes to be enabled
    if(classification) {
      if(arg._name.equals("max_after_balance_size") && !balance_classes) {
        arg.disable("Requires balance classes flag to be set.", inputArgs);
      }
    }
  }

  @Override protected void execImpl() {
    try {
      logStart();
      source.read_lock(self());
      buildForest();
      if (n_folds > 0) CrossValUtils.crossValidate(this);
    }
    catch(JobCancelledException ex) {
      Log.info("Random Forest building was cancelled.");
      throw ex;
    }
    catch(Exception ex) {
      ex.printStackTrace();
      throw new RuntimeException(ex);
    }
    finally{
      source.unlock(self());
      remove();
      state = UKV.<Job>get(self()).state;
      new TAtomic<SpeeDRFModel>() {
        @Override
        public SpeeDRFModel atomic(SpeeDRFModel m) {
          if (m != null) m.get_params().state = state;
          return m;
        }
      }.invoke(dest());
      emptyLTrash();
      cleanup();
    }
  }

  @Override protected Response redirect() { return SpeeDRFProgressPage.redirect(this, self(), dest()); }

  private void buildForest() {
    SpeeDRFModel model = null;
    try {
      model = initModel();
      model.start_training(null);
      model.write_lock(self());
      drfParams = DRFParams.create(model.fr.find(model.response), model.N, model.max_depth, (int) model.fr.numRows(), model.nbins,
              model.statType, seed, model.weights, mtry, model.sampling_strategy, (float) sample, model.strata_samples, model.verbose ? 100 : 1, _exclusiveSplitLimit, !local_mode, regression);
      DRFTask tsk = new DRFTask();
      tsk._job = Job.findJob(self());
      tsk._params = drfParams;
      tsk._rfmodel = model;
      tsk._drf = this;
      tsk.validateInputData();
      tsk.invokeOnAllNodes();
      model = UKV.get(dest());
    }
    finally {
      if (model != null) {
        model.unlock(self());
        model.stop_training();
      }
    }
  }


  /**
   * Check the user inputted class weights for errors.
   * @param weights: The user input class weights.
   * @return : Return an array of doubles.
   */

  private double[] checkClassWeights(double[] weights) {

    // Fill the defaults with 1.0 weight
    Vec resp = response.toEnum();
    double [] defaults = new double[resp.cardinality()]; //can leak if not yet enum
    gtrash(resp);
    for (int i = 0; i < defaults.length; ++i) defaults[i] = 1.0;

    //Create a results vector to be filled in below
    double [] result = new double[defaults.length];

    // User gave no weights, use defaults
    if (weights == null) return defaults;

    // User gave more weights than classes, only use the first N
    if (weights.length > defaults.length) {
      System.arraycopy(weights, 0, result, 0, defaults.length);
      return result;
    }

    //User specified fewer samples than classes, pad with defaults
    if (weights.length < defaults.length) {
      System.arraycopy(weights, 0, result, 0, weights.length);
      System.arraycopy(defaults, weights.length, result, weights.length, defaults.length - weights.length);
      return result;
    }

    return defaults;
  }

  // Initialize defaults and user specified model params
  public SpeeDRFModel initModel() {
    // Map the enum SelectStatType to the enum StatType
    Tree.StatType stat_type;
    if (regression) {
      stat_type = Tree.StatType.MSE;
    } else {
      if (select_stat_type == Tree.SelectStatType.ENTROPY) {
        stat_type = Tree.StatType.ENTROPY;
      } else {
        stat_type = Tree.StatType.GINI;
      }
    }

    // Initialize classification specific model parameters
    if(!regression) {

      // Handle bad user input for class weights
//      class_weights = checkClassWeights(class_weights);

      // Initialize regression specific model parameters
    } else {

      // Class Weights and Strata Samples do not apply to Regression
      class_weights = null;

      //TODO: Variable importance in regression not currently supported
      if (importance && regression) throw new IllegalArgumentException("Variable Importance for SpeeDRF regression not currently supported.");
    }

    // Generate a new seed by default.
    if (seed == -1) {
      seed = _seedGenerator.nextLong();
    }

    // Prepare the train/test data sets based on the user input for the model.
    Frame train = FrameTask.DataInfo.prepareFrame(source, response, ignored_cols, !regression /*toEnum is TRUE if regression is FALSE*/, false, false);
    if (train.lastVec().masterVec() != null && train.lastVec() != response) gtrash(train.lastVec());
    Frame test = null;
    if (validation != null) {
      test = FrameTask.DataInfo.prepareFrame(validation, validation.vecs()[source.find(response)], ignored_cols, !regression, false, false);
    }

    float[] priorDist = classification ? new MRUtils.ClassDist(train.lastVec()).doAll(train.lastVec()).rel_dist() : null;

    // Handle imbalanced classes by stratified over/under-sampling
    // initWorkFrame sets the modeled class distribution, and model.score() corrects the probabilities back using the distribution ratios
    float[] trainSamplingFactors;
    Vec resp =  regression ? null : train.lastVec().toEnum();
    if (resp != null) gtrash(resp);
    Frame fr = train;
    if (classification && balance_classes) {
      int response_idx = fr.find(_responseName);
      fr.replace(response_idx, resp);
      trainSamplingFactors = new float[resp.domain().length]; //leave initialized to 0 -> will be filled up below
      Frame stratified = sampleFrameStratified(fr, resp, trainSamplingFactors, (long)(max_after_balance_size*fr.numRows()), seed, true, false);
      if (stratified != fr) {
        fr = stratified;
        gtrash(stratified);
      }
    }

    // Check that that test/train data are consistent, throw warning if not
    if(classification && validation != null) {
      Vec testresp = test.lastVec().toEnum();
      gtrash(testresp);
      if (!isSubset(testresp.domain(), resp.domain())) {
        Log.warn("Test set domain: " + Arrays.toString(testresp.domain()) + " \nTrain set domain: " + Arrays.toString(resp.domain()));
        Log.warn("Train and Validation data have inconsistent response columns! Test data has a response not found in the Train data!");
      }
    }

    Key src_key = source._key;
    int src_ncols = source.numCols();
    // Set the model parameters
    SpeeDRFModel model = new SpeeDRFModel(dest(), src_key, fr, regression ? null : resp.domain(), this, priorDist);
    model.verbose = verbose;
    int csize = H2O.CLOUD.size();
    model.fr = fr;
    model.response = regression ? fr.lastVec() : fr.lastVec().toEnum();
    if (!regression) gtrash(model.response);
    model.t_keys = new Key[0];
    model.time = 0;
    model.local_forests = new Key[csize][];
    model.verbose_output = new String[]{""};
    for(int i=0;i<csize;i++) model.local_forests[i] = new Key[0];
    model.node_split_features = new int[csize];
    for( Key tkey : model.t_keys ) assert DKV.get(tkey)!=null;
    model.jobKey = self();
    model.current_status = "Initializing Model";
    model.confusion = null;
    model.zeed = seed;
    model.cmDomain = getCMDomain();
    model.varimp = null;
    model.validAUC = null;
    model.cms = new ConfusionMatrix[1];
    model.errs = new float[]{-1.f};
    model.nbins = bin_limit;
    model.max_depth = max_depth;
    model.oobee = validation == null && oobee;
    model.statType = regression ? Tree.StatType.MSE : stat_type;
    model.test_frame = test;
    model.testKey = validation == null ? null : validation._key;
    model.importance = importance;
    model.regression = regression;
    model.features = src_ncols;
    model.sampling_strategy = regression ? Sampling.Strategy.RANDOM : sampling_strategy;
    model.sample = (float) sample;
    model.weights = regression ? null : class_weights;
    model.time = 0;
    model.tree_pojos = new TreeP[1];
    model.N = num_trees;
    model.useNonLocal = !local_mode;
    if (!regression) model.setModelClassDistribution(new MRUtils.ClassDist(fr.lastVec()).doAll(fr.lastVec()).rel_dist());
    model.resp_min = (int) resp.min();

    if (mtry == -1) {
      if(!regression) {

        // Classification uses the square root of the number of features by default
        model.mtry = (int) Math.floor(Math.sqrt(fr.numCols() - 1)); // do not count class column
      } else {

        // Regression uses about a third of the features by default
        model.mtry = (int) Math.floor((float) (fr.numCols() - 1) / 3.0f);  // do not count class column
      }

    } else {

      // The user specified mtry
      model.mtry = mtry;
    }

    return model;
  }

  public Frame score( Frame fr ) { return ((SpeeDRFModel)UKV.get(dest())).score(fr);  }

  private boolean isSubset(String[] sub, String[] container) {
    HashSet<String> hs = new HashSet<String>();
    Collections.addAll(hs, container);
    for (String s: sub) {
      if (!hs.contains(s)) return false;
    }
    return true;
  }

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
      final DataAdapter dapt = DABuilder.create(_drf, _rfmodel).build(_rfmodel.fr, _params._useNonLocalData);
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

//    static void updateRFModelStopTraining(Key modelKey) {
//      new TAtomic<SpeeDRFModel>() {
//        @Override public SpeeDRFModel atomic(SpeeDRFModel m) {
//          if(m == null) return null;
//          m.stop_training();
//          return m;
//        }
//      }.invoke(modelKey);
//    }

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

    private void validateInputData() {
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
      boolean can_load_all = canLoadAll(_rfmodel.fr, cai);
      if (_params._useNonLocalData && !can_load_all) {
        Log.warn("Cannot load all data from remote nodes - " +
                "the node " + cai.node + " requires " + PrettyPrint.bytes(cai.requiredMemory) + " to load all data and perform computation but there is only " + PrettyPrint.bytes(cai.availableMemory) + " of available memory. " +
                "Please provide more memory for JVMs or disable the option '"+ Constants.USE_NON_LOCAL_DATA+"' (however, it may affect resulting accuracy).");
        Log.warn("Automatically disabling fast mode.");
        _params._useNonLocalData = false; /* In other words, use local data only... */
        _drf.local_mode = true;
      }

      if (can_load_all) {
        _params._useNonLocalData = true;
        _drf.local_mode = false;
        Log.info("Enough room to compute fast speedrf... Pulling all data locally and then launching RF.");
      }
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
      Log.info(Log.Tag.Sys.RANDF,"Building "+ntrees+" trees");
      Log.info(Log.Tag.Sys.RANDF,"Number of split features: "+ numSplitFeatures);
      Log.info(Log.Tag.Sys.RANDF,"Starting RF computation with "+ localData.rows()+" rows ");

      Random rnd = Utils.getRNG(localData.seed() + ROOT_SEED_ADD);
      Sampling sampler = createSampler(drfParams, rowsPerChunks);
      byte producerId = (byte) H2O.SELF.index();
      for (int i = 0; i < ntrees; ++i) {
        long treeSeed = rnd.nextLong() + TREE_SEED_INIT; // make sure that enough bits is initialized
        trees[i] = new Tree(job, localData, producerId, drfParams.max_depth, drfParams.stat_type, numSplitFeatures, treeSeed,
                i, drfParams._exclusiveSplitLimit, sampler, drfParams._verbose, drfParams.regression, !drfParams._useNonLocalData);
      }

      Log.info("Invoking the tree build tasks on all nodes.");
      DRemoteTask.invokeAll(trees);
      Log.info(Log.Tag.Sys.RANDF,"All trees ("+ntrees+") done in "+ t_alltrees);
    }

    static Sampling createSampler(final DRFParams params, int[] rowsPerChunks) {
      switch(params.sampling_strategy) {
        case RANDOM          : return new Sampling.Random(params.sample, rowsPerChunks);
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
                                         Tree.StatType statType, long seed, double[] classWt,
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
      drfp.parallel   = true;
      return drfp;
    }
  }

  /**
   * Cross-Validate a SpeeDRF model by building new models on N train/test holdout splits
   * @param splits Frames containing train/test splits
   * @param cv_preds Array of Frames to store the predictions for each cross-validation run
   * @param offsets Array to store the offsets of starting row indices for each cross-validation run
   * @param i Which fold of cross-validation to perform
   */
  @Override public void crossValidate(Frame[] splits, Frame[] cv_preds, long[] offsets, int i) {
    // Train a clone with slightly modified parameters (to account for cross-validation)
    SpeeDRF cv = (SpeeDRF) this.clone();
    cv.genericCrossValidation(splits, offsets, i);
    cv_preds[i] = ((SpeeDRFModel) UKV.get(cv.dest())).score(cv.validation);
  }
}
