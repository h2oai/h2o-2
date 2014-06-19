package hex.singlenoderf;


import hex.ConfusionMatrix;
import hex.FrameTask;
import hex.VarImp;
import jsr166y.ForkJoinTask;
import water.*;
import water.api.Constants;
import water.api.DocGen;
import water.api.ParamImportance;
import water.fvec.Frame;
import water.fvec.Vec;
import water.util.*;
import water.api.ParamImportance;
import static water.util.MRUtils.sampleFrameStratified;

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
  public Tree.SelectStatType select_stat_type = Tree.SelectStatType.ENTROPY;

//  @API(help = "Class Weights (0.0,0.2,0.4,0.6,0.8,1.0)", filter = Default.class, displayName = "class weights", json = true, importance = ParamImportance.SECONDARY)
  public double[] class_weights = null;

  @API(help = "Sampling Strategy", filter = Default.class, json = true, importance = ParamImportance.SECONDARY)
  public Sampling.Strategy sampling_strategy = Sampling.Strategy.RANDOM;

//  @API(help = "Strata Samples", filter = Default.class, json = true, lmin = 0, lmax = 100, importance = ParamImportance.SECONDARY)
//  int[] strata_samples = null;

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

  @API(help = "split limit", importance = ParamImportance.EXPERT)
  public int _exclusiveSplitLimit = 0;

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

  protected SpeeDRFModel makeModel( SpeeDRFModel model, double err, ConfusionMatrix cm, VarImp varimp, water.api.AUC validAUC) {
    return new SpeeDRFModel(model, err, cm, varimp, validAUC);
  }

  @Override protected void queryArgumentValueSet(Argument arg, java.util.Properties inputArgs) {
    super.queryArgumentValueSet(arg, inputArgs);

    if (arg._name.equals("classification")) {
      arg.setRefreshOnChange();
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

    // Strata samples are invalid for Random sampling and regression
//    if (arg._name.equals("strata_samples")) {
//      if (sampling_strategy != Sampling.Strategy.STRATIFIED_LOCAL) {
//        arg.disable("No Strata for Random sampling.");
//      }
//      if (regression) {
//        arg.disable("No strata for regression.");
//      }
//    }

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
    SpeeDRFModel rf_model = initModel();
    rf_model.start_training(null);
    buildForest(rf_model);
    // buildForest() caused a different SpeeDRFModel instance to get put into the DKV.  We
    // need to update that one, not rf_model
//    DRFTask.updateRFModelStopTraining(rf_model._key);
    rf_model.stop_training();
    if (n_folds > 0) CrossValUtils.crossValidate(this);
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
              model.statType, seed, model.weights, mtry, model.sampling_strategy, (float) sample, model.strata_samples, 1, _exclusiveSplitLimit, _useNonLocalData, regression);
      logStart();
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


  /**
   * Check the user inputted samples if Stratified Local is selected.
   * @param samples: The user input samples.
   * @return : Return a an array ints.
   */

  private int[] checkSamples(int[] samples) {

    //Fill the defaults with 67 as the sampling rate.
    int [] defaults = new int[response.toEnum().cardinality()];
    for (int i = 0; i < defaults.length; ++i) defaults[i] = 67;

    //User gave no samples, use defaults.
    if (samples == null) return defaults;

    //Create a results vector to be filled in below
    int [] result = new int[defaults.length];

    //User input more samples than classes, only use the first N
    if (samples.length > defaults.length)  {
      System.arraycopy(samples, 0, result, 0, result.length);
      return result;
    }

    //User specified fewer samples than classes, pad with defaults
    if (samples.length < defaults.length) {
      System.arraycopy(samples, 0, result, 0, samples.length);
      System.arraycopy(defaults, samples.length, result, samples.length, result.length - samples.length);
      return result;
    }
    return defaults;
  }

  /**
   * Check the user inputted class weights for errors.
   * @param weights: The user input class weights.
   * @return : Return an array of doubles.
   */

  private double[] checkClassWeights(double[] weights) {

    // Fill the defaults with 1.0 weight
    double [] defaults = new double[response.toEnum().cardinality()];
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
    try {
      source.read_lock(self());

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

      if (validation != null) {
        sample = 1.0;
      }

      // Initialize classification specific model parameters
      if(!regression) {

        // Handle bad user input for Stratified Samples (if Stratified Local is chosen)
//        if (sampling_strategy  == Sampling.Strategy.STRATIFIED_LOCAL) {
//          strata_samples = checkSamples(strata_samples);

          // If stratified local, turn of out of bag sampling
          oobee = false;
//        } else {
//          strata_samples = new int[response.toEnum().cardinality()];
//          for (int i = 0; i < strata_samples.length; i++) strata_samples[i] = 67;
//        }

        // Handle bad user input for class weights
        class_weights = checkClassWeights(class_weights);

        // Initialize regression specific model parameters
      } else {

        // Class Weights and Strata Samples do not apply to Regression
        class_weights = null;
//        strata_samples = null;

        //TODO: Variable importance in regression not currently supported
        if (importance && regression) throw new IllegalArgumentException("Variable Importance for SpeeDRF regression not currently supported.");
      }

      // Generate a new seed by default.
      if (seed == -1) {
        seed = _seedGenerator.nextLong();
      }

      // Prepare the train/test data sets based on the user input for the model.
      Frame train = FrameTask.DataInfo.prepareFrame(source, response, ignored_cols, !regression /*toEnum is TRUE if regression is FALSE*/, true, true);
      Frame test = null;
      if (validation != null) {
        test = FrameTask.DataInfo.prepareFrame(validation, validation.vecs()[source.find(response)], ignored_cols, !regression, true, true);
      }

      float[] priorDist = classification ? new MRUtils.ClassDist(train.lastVec()).doAll(train.lastVec()).rel_dist() : null;

      // Handle imbalanced classes by stratified over/under-sampling
      // initWorkFrame sets the modeled class distribution, and model.score() corrects the probabilities back using the distribution ratios
      float[] trainSamplingFactors;
      Vec v = train.lastVec().toEnum();
      Frame fr = train;
      if (classification && balance_classes) {
        int response_idx = fr.find(_responseName);
        fr.replace(response_idx, v);
        trainSamplingFactors = new float[v.domain().length]; //leave initialized to 0 -> will be filled up below
        Frame stratified = sampleFrameStratified(fr, v, trainSamplingFactors, (long)(max_after_balance_size*fr.numRows()), seed, true, false);
        if (stratified != fr) {
          fr = stratified;
          response = fr.vecs()[response_idx];
          }
      }

      if(classification && validation != null)
        if (!( Arrays.equals( train.lastVec().toEnum().domain(), test.lastVec().toEnum().domain())))
          throw new IllegalArgumentException("Train and Validation data have inconsistent response columns! They do not share the same factor levels.");

      // Set the model parameters
      SpeeDRFModel model = new SpeeDRFModel(dest(), source._key, fr, this, priorDist);
      int csize = H2O.CLOUD.size();
      model.fr = train;
      model.response = regression ? fr.lastVec() : fr.lastVec().toEnum();
      model.t_keys = new Key[0];
      model.time = 0;
      model.local_forests = new Key[csize][];
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
      model.features = source.numCols();
      model.sampling_strategy = regression ? Sampling.Strategy.RANDOM : sampling_strategy;
      model.sample = (float) sample;
      model.weights = regression ? null : class_weights;
      model.time = 0;
      model.N = num_trees;
//      model.strata_samples = regression ? null : new float[strata_samples.length];
      model.setModelClassDistribution(new MRUtils.ClassDist(fr.lastVec()).doAll(fr.lastVec()).rel_dist());

//      if (!regression) {
//        for (int i = 0; i < strata_samples.length; i++) {
//          assert model.strata_samples != null;
//          model.strata_samples[i] = (float) strata_samples[i];
//        }
//      }

      if (mtry == -1) {
        if(!regression) {

          // Classification uses the square root of the number of features by default
          model.mtry = (int) Math.floor(Math.sqrt(fr.numCols()));
        } else {

          // Regression uses about a third of the features by default
          model.mtry = (int) Math.floor((float) fr.numCols() / 3.0f);
        }

      } else {

        // The user specified mtry
        model.mtry = mtry;
      }

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

    static void updateRFModelStopTraining(Key modelKey) {
      new TAtomic<SpeeDRFModel>() {
        @Override public SpeeDRFModel atomic(SpeeDRFModel m) {
          if(m == null) return null;
          m.stop_training();
          return m;
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
//        case STRATIFIED_LOCAL:
//          float[] ss = new float[params.strata_samples.length];
//          for (int i=0;i<ss.length;i++) ss[i] = params.strata_samples[i] / 100.f;
//          return new Sampling.StratifiedLocal(ss, params._numrows);
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
