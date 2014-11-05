package hex.singlenoderf;


import dontweave.gson.JsonObject;
import hex.ConfusionMatrix;
import hex.FrameTask;
import hex.VarImp;
import hex.drf.DRF;
import water.*;
import water.Timer;
import water.api.AUCData;
import water.api.Constants;
import water.api.DocGen;
import water.api.ParamImportance;
import water.fvec.Frame;
import water.fvec.Vec;
import water.util.*;

import java.util.*;

import static water.util.MRUtils.sampleFrameStratified;

public class SpeeDRF extends Job.ValidatedJob {
  static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
  public static DocGen.FieldDoc[] DOC_FIELDS;
  public static final String DOC_GET = "SpeeDRF";

  @API(help = "Number of trees", filter = Default.class, json = true, lmin = 1, lmax = Integer.MAX_VALUE, importance = ParamImportance.CRITICAL)
  public int ntrees   = 50;

  @API(help = "Number of features to randomly select at each split.", filter = Default.class, json = true, lmin = -1, lmax = Integer.MAX_VALUE, importance = ParamImportance.SECONDARY)
  public int mtries = -1;

  @API(help = "Max Depth", filter = Default.class, json = true, lmin = 0, lmax = Integer.MAX_VALUE, importance = ParamImportance.CRITICAL)
  public int max_depth = 20;

  @API(help = "Split Criterion Type", filter = Default.class, json=true, importance = ParamImportance.SECONDARY)
  public Tree.SelectStatType select_stat_type = Tree.SelectStatType.ENTROPY;

//  @API(help = "Use local data. Auto-enabled if data does not fit in a single node.") /*, filter = Default.class, json = true, importance = ParamImportance.EXPERT) */
//  public boolean local_mode = false;

  /* Legacy parameter: */
  public double[] class_weights = null;

  @API(help = "Sampling Strategy", filter = Default.class, json = true, importance = ParamImportance.SECONDARY)
  public Sampling.Strategy sampling_strategy = Sampling.Strategy.RANDOM;


  @API(help = "Sampling Rate at each split.", filter = Default.class, json  = true, dmin = 0, dmax = 1, importance = ParamImportance.EXPERT)
  public double sample_rate = 0.67;

//  @API(help ="Score each iteration", filter = Default.class, json = true, importance = ParamImportance.SECONDARY)
  public boolean score_each_iteration = false;

  @API(help = "Create the Score POJO", filter = Default.class, json = true, importance = ParamImportance.EXPERT)
  public boolean score_pojo = true;

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
  public int nbins = 1024;

  @API(help = "seed", filter = Default.class, json = true, importance = ParamImportance.EXPERT)
  public long seed = -1;

  @API(help = "Tree splits and extra statistics printed to stdout.", filter = Default.class, json = true, importance = ParamImportance.EXPERT)
  public boolean verbose = false;

  @API(help = "split limit", importance = ParamImportance.EXPERT)
  public int _exclusiveSplitLimit = 0;

  private static Random _seedGenerator = Utils.getDeterRNG( new Random().nextLong() );//0xd280524ad7fe0602L);

  private boolean regression;

  public DRFParams drfParams;

  private long use_seed;

  Tree.StatType stat_type;

  /** Return the query link to this page */
  public static String link(Key k, String content) {
    RString rs = new RString("<a href='/2/SpeeDRF.query?source=%$key'>%content</a>");
    rs.replace("key", k.toString());
    rs.replace("content", content);
    return rs.toString();
  }

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

  // Put here all precondition verification
  @Override protected void init() {
    super.init();

    assert 0 <= ntrees && ntrees < 1000000; // Sanity check
    // Not enough rows to run
    if (source.numRows() - response.naCnt() <=0)
      throw new IllegalArgumentException("Dataset contains too many NAs!");

    if( !classification && (!(response.isEnum() || response.isInt())))
      throw new IllegalArgumentException("Classification cannot be performed on a float column!");

    if(classification) {
      if (0.0f > sample_rate || sample_rate > 1.0f)
        throw new IllegalArgumentException("Sampling rate must be in [0,1] but found " + sample_rate);
    }

    if(regression) throw new IllegalArgumentException("SpeeDRF does not currently support regression.");
  }

  @Override protected void execImpl() {
    SpeeDRFModel rf_model;
    try {
      source.read_lock(self());
      if (validation != null && validation != source) validation.read_lock(self());
      buildForest();
      if (n_folds > 0) CrossValUtils.crossValidate(this);
    } catch (JobCancelledException ex){
      rf_model = UKV.get(dest());
      state = JobState.CANCELLED; //for JSON REST response
      rf_model.get_params().state = state; //for parameter JSON on the HTML page
      Log.info("Random Forest was cancelled.");
    } catch(Exception ex) {
      ex.printStackTrace();
      throw new RuntimeException(ex);
    } finally {
      source.unlock(self());
      if (validation != null && validation != source) validation.unlock(self());
      remove();
      state = UKV.<Job>get(self()).state;
      // Argh, this is horrible
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
    logStart();
    SpeeDRFModel model = null;
    try {
      Frame train = setTrain();
      Frame test  = setTest();
      Vec resp = regression ? null : train.lastVec().toEnum();
      if (resp != null) gtrash(resp);
      float[] priorDist = setPriorDist(train);
      train = setStrat(train, test, resp);
      model = initModel(train, test, priorDist);
      model.start_training(null);
      model.write_lock(self());
      drfParams = DRFParams.create(train.find(resp), model.N, model.max_depth, (int) train.numRows(), model.nbins,
              model.statType, use_seed, model.weights, mtries, model.sampling_strategy, (float) sample_rate, model.strata_samples, model.verbose ? 100 : 1, _exclusiveSplitLimit, true, regression);

      DRFTask tsk = new DRFTask(self(), train, drfParams, model._key, model.src_key);
      tsk.validateInputData(train);
      tsk.invokeOnAllNodes();
      Log.info("Tree building complete. Scoring...");
      model = UKV.get(dest());
      model.scoreAllTrees(test == null ? train : test, resp);
      // Launch a Variable Importance Task
      if (importance && !regression) {
        Log.info("Scoring complete. Performing Variable Importance Calculations.");
        model.current_status = "Performing Variable Importance Calculation.";
        Timer VITimer = new Timer();
        model.variableImportanceCalc(train, resp);
        Log.info("Variable Importance on "+(train.numCols()-1)+" variables and "+ ntrees +" trees done in " + VITimer);
      }
      Log.info("Generating Tree Stats");
      JsonObject trees = new JsonObject();
      trees.addProperty(Constants.TREE_COUNT, model.size());
      if( model.size() > 0 ) {
        trees.add(Constants.TREE_DEPTH, model.depth().toJson());
        trees.add(Constants.TREE_LEAVES, model.leaves().toJson());
      }
      model.generateHTMLTreeStats(new StringBuilder(), trees);
      model.current_status = "Model Complete";
    } finally {
      if (model != null) {
        model.unlock(self());
        model.stop_training();
      }
    }
  }

  public SpeeDRFModel initModel(Frame train, Frame test, float[] priorDist) {
    setStatType();
    setSeed(seed);
    if (mtries == -1) setMtry(regression, train.numCols() - 1);
    Key src_key = source._key;
    int src_ncols = source.numCols();

    SpeeDRFModel model = new SpeeDRFModel(dest(), src_key, train, regression ? null : train.lastVec().domain(), this, priorDist);

    // Model INPUTS
    model.src_key = src_key.toString();
    model.verbose = verbose; model.verbose_output = new String[]{""};
    model.validation = test != null;
    model.confusion = null;
    model.zeed = use_seed;
    model.cmDomain = getCMDomain();
    model.nbins = nbins;
    model.max_depth = max_depth;
    model.oobee = validation == null && oobee;
    model.statType = regression ? Tree.StatType.MSE : stat_type;
    model.testKey = validation == null ? null : validation._key;
    model.importance = importance;
    model.regression = regression;
    model.features = src_ncols;
    model.sampling_strategy = regression ? Sampling.Strategy.RANDOM : sampling_strategy;
    model.sample = (float) sample_rate;
    model.weights = regression ? null : class_weights;
    model.time = 0;
    model.N = ntrees;
    model.useNonLocal = true;
    if (!regression) model.setModelClassDistribution(new MRUtils.ClassDist(train.lastVec()).doAll(train.lastVec()).rel_dist());
    model.resp_min = (int) train.lastVec().min();
    model.mtry = mtries;
    int csize = H2O.CLOUD.size();
    model.local_forests = new Key[csize][]; for(int i=0;i<csize;i++) model.local_forests[i] = new Key[0];
    model.node_split_features = new int[csize];
    model.t_keys = new Key[0];
    model.dtreeKeys = new Key[ntrees][regression ? 1 : model.classes()];
    model.time = 0;
    for( Key tkey : model.t_keys ) assert DKV.get(tkey)!=null;
    model.jobKey = self();
    model.score_pojo = score_pojo;
    model.current_status = "Initializing Model";

    // Model OUTPUTS
    model.varimp = null;
    model.validAUC = null;
    model.cms = new ConfusionMatrix[1];
    model.errs = new double[]{-1.0};
    return model;
  }

  private void setStatType() {
    if (regression) stat_type = Tree.StatType.MSE;
    stat_type = select_stat_type == Tree.SelectStatType.ENTROPY ? Tree.StatType.ENTROPY : Tree.StatType.GINI;
    if (select_stat_type == Tree.SelectStatType.TWOING) stat_type = Tree.StatType.TWOING;
  }
  private void setSeed(long s) {
    if (s == -1) { seed = _seedGenerator.nextLong(); use_seed = seed; }
    else {
      _seedGenerator = Utils.getDeterRNG(s);
      use_seed = _seedGenerator.nextLong();
    }
  }
  private void setMtry(boolean reg, int numCols) { mtries = reg ? (int) Math.floor((float) (numCols) / 3.0f) : (int) Math.floor(Math.sqrt(numCols)); }
  private Frame setTrain() { Frame train = FrameTask.DataInfo.prepareFrame(source, response, ignored_cols, !regression /*toEnum is TRUE if regression is FALSE*/, false, false); if (train.lastVec().masterVec() != null && train.lastVec() != response) gtrash(train.lastVec()); return train; }
  private Frame setTest() {
    if (validation == null) return null;
    Frame test = null;
    ArrayList<Integer> v_ignored_cols = new ArrayList<Integer>();
    for (int ignored_col : ignored_cols) if (validation.find(source.names()[ignored_col]) != -1) v_ignored_cols.add(ignored_col);
    int[] v_ignored = new int[v_ignored_cols.size()];
    for (int i = 0; i < v_ignored.length; ++i) v_ignored[i] = v_ignored_cols.get(i);
    if (validation != null) test = FrameTask.DataInfo.prepareFrame(validation, validation.vecs()[validation.find(source.names()[source.find(response)])], v_ignored, !regression, false, false);
    if (test != null && test.lastVec().masterVec() != null) gtrash(test.lastVec());
    return test;
  }
  private Frame setStrat(Frame train, Frame test, Vec resp) {
    Frame fr = train;
    float[] trainSamplingFactors;
    if (classification && balance_classes) {
      assert resp != null : "Regression called and stratified sampling was invoked to balance classes!";
      // Handle imbalanced classes by stratified over/under-sampling
      // initWorkFrame sets the modeled class distribution, and model.score() corrects the probabilities back using the distribution ratios
      int response_idx = fr.find(_responseName);
      fr.replace(response_idx, resp);
      trainSamplingFactors = new float[resp.domain().length]; //leave initialized to 0 -> will be filled up below
      Frame stratified = sampleFrameStratified(fr, resp, trainSamplingFactors, (long) (max_after_balance_size * fr.numRows()), use_seed, true, false);
      if (stratified != fr) {
        fr = stratified;
        gtrash(stratified);
      }
    }

    // Check that that test/train data are consistent, throw warning if not
    if(classification && validation != null) {
      assert resp != null : "Regression called and stratified sampling was invoked to balance classes!";
      Vec testresp = test.lastVec().toEnum();
      gtrash(testresp);
      if (!isSubset(testresp.domain(), resp.domain())) {
        Log.warn("Test set domain: " + Arrays.toString(testresp.domain()) + " \nTrain set domain: " + Arrays.toString(resp.domain()));
        Log.warn("Train and Validation data have inconsistent response columns! Test data has a response not found in the Train data!");
      }
    }
    return fr;
  }

  private float[] setPriorDist(Frame train) { return classification ? new MRUtils.ClassDist(train.lastVec()).doAll(train.lastVec()).rel_dist() : null; }
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
//    private final SpeeDRFModel _rfmodel;
    private final Key _rfmodel;
    /** Job representing this DRF execution. */
    private final Key _jobKey;
    /** RF parameters. */
    private final DRFParams _params;
    private final Frame _fr;
    private final String _key;

    DRFTask(Key jobKey, Frame frameKey, DRFParams params, Key rfmodel, String src_key) {
      _jobKey = jobKey; _fr = frameKey; _params = params; _rfmodel = rfmodel; _key = src_key;
    }

    /**Inhale the data, build a DataAdapter and kick-off the computation.
     * */
    @Override public final void lcompute() {
      final DataAdapter dapt = DABuilder.create(_params, _rfmodel).build(_fr, _params._useNonLocalData);
      if (dapt == null) {
        tryComplete();
        return;
      }
      Data localData        = Data.make(dapt);
      int numSplitFeatures  = howManySplitFeatures();
      int ntrees            = howManyTrees();
      int[] rowsPerChunks   = howManyRPC(_fr);
      updateRFModel(_rfmodel, numSplitFeatures);
      updateRFModelStatus(_rfmodel, "Building Forest");
      updateRFModelLocalForests(_rfmodel, ntrees);
      Log.info("Dispalying local forest stats:");
      SpeeDRF.build(_jobKey, _rfmodel, _params, localData, ntrees, numSplitFeatures, rowsPerChunks);
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

    static void updateRFModelLocalForests(Key modelKey, final int num_trees) {
      final int selfIdx = H2O.SELF.index();
      new TAtomic<SpeeDRFModel>() {
        @Override public SpeeDRFModel atomic(SpeeDRFModel old) {
          if (old == null) return null;
          SpeeDRFModel newModel = (SpeeDRFModel)old.clone();
          newModel.local_forests[selfIdx] = new Key[num_trees];
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
      return _params.num_split_features;
    }

    /** Figure the number of trees to make locally, so the total hits ntrees.
     *  Divide equally amongst all the nodes that actually have data.  First:
     *  compute how many nodes have data.  Give each Node ntrees/#nodes worth of
     *  trees.  Round down for later nodes, and round up for earlier nodes.
     */
    private int howManyTrees() {
      Frame fr = _fr;
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
      int ntrees = _params.num_trees / nodes.size();
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

    private void validateInputData(Frame fr) {
      Vec[] vecs = fr.vecs();
      Vec c = vecs[vecs.length-1];
      if (!_params.regression) {
        final int classes = c.cardinality();
        if (!(2 <= classes && classes <= 254))
          throw new IllegalArgumentException("Response contains " + classes + " classes, but algorithm supports only 254 levels");
      }
      if (_params.num_split_features!=-1 && (_params.num_split_features< 1 || _params.num_split_features>vecs.length-1))
        throw new IllegalArgumentException("Number of split features exceeds available data. Should be in [1,"+(vecs.length-1)+"]");
      ChunkAllocInfo cai = new ChunkAllocInfo();
      boolean can_load_all = canLoadAll(fr, cai);
      if (_params._useNonLocalData && !can_load_all) {
        String heap_warning = "This algorithm requires loading of all data from remote nodes." +
                "\nThe node " + cai.node + " requires " + PrettyPrint.bytes(cai.requiredMemory) + " more memory to load all data and perform computation but there is only " + PrettyPrint.bytes(cai.availableMemory) + " of available memory." +
                "\n\nPlease provide more memory for JVMs \n\n-OR-\n\n Try Big Data Random Forest: ";
        Log.warn(heap_warning);
        throw new IllegalArgumentException(heap_warning + DRF.link(Key.make(_key), "Big Data Random Forest") );
      }

      if (can_load_all) {
        _params._useNonLocalData = true;
        Log.info("Enough available free memory to compute on all data. Pulling all data locally and then launching RF.");
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
      // Also must add in the RF internal data structure overhead
      memForNonLocal += fr.numRows() * fr.numCols();
      for(int i = 0; i < H2O.CLOUD._memary.length; i++) {
        HeartBeat hb = H2O.CLOUD._memary[i]._heartbeat;
        long nodeFreeMemory = (long)(hb.get_max_mem() * 0.8); // * OVERHEAD_MAGIC;
        Log.debug(Log.Tag.Sys.RANDF, i + ": computed available mem: " + PrettyPrint.bytes(nodeFreeMemory));
        Log.debug(Log.Tag.Sys.RANDF, i + ": remote chunks require: " + PrettyPrint.bytes(memForNonLocal));
        if (nodeFreeMemory - memForNonLocal <= 0 || (nodeFreeMemory <= TWO_HUNDRED_MB && memForNonLocal >= ONE_FIFTY_MB)) {
          Log.info("Node free memory raw: "+nodeFreeMemory);
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
    static final long TWO_HUNDRED_MB = 200 * 1024 * 1024;
    static final long ONE_FIFTY_MB = 150 * 1024 * 1024;

    @Override
    public void reduce(DRemoteTask drt) { }
  }

    private static final long ROOT_SEED_ADD  = 0x026244fd935c5111L;
    private static final long TREE_SEED_INIT = 0x1321e74a0192470cL;

    /** Build random forest for data stored on this node. */
    public static void build(
            final Key jobKey,
            final Key modelKey,
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
        trees[i] = new Tree(jobKey, modelKey, localData, producerId, drfParams.max_depth, drfParams.stat_type, numSplitFeatures, treeSeed,
                i, drfParams._exclusiveSplitLimit, sampler, drfParams._verbose, drfParams.regression, !drfParams._useNonLocalData, ((SpeeDRFModel)UKV.get(modelKey)).score_pojo);
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
      drfp.num_trees = ntrees;
      drfp.max_depth = depth;
      drfp.sample = sample;
      drfp.bin_limit = binLimit;
      drfp.stat_type = statType;
      drfp.seed = seed;
      drfp.class_weights = classWt;
      drfp.num_split_features = numSplitFeatures;
      drfp.sampling_strategy = samplingStrategy;
      drfp.strata_samples    = strataSamples;
      drfp._numrows = numrows;
      drfp._useNonLocalData = useNonLocalData;
      drfp._exclusiveSplitLimit = exclusiveSplitLimit;
      drfp._verbose = verbose;
      drfp.classcol = col;
      drfp.regression = regression;
      drfp.parallel = true;
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
    final SpeeDRF cv = (SpeeDRF) this.clone();
    cv.genericCrossValidation(splits, offsets, i);
    cv_preds[i] = ((SpeeDRFModel) UKV.get(cv.dest())).score(cv.validation);
    new TAtomic<SpeeDRFModel>() {
      @Override public SpeeDRFModel atomic(SpeeDRFModel m) {
        if (!keep_cross_validation_splits && /*paranoid*/ cv.dest().toString().contains("xval")) {
          m.get_params().source = null;
          m.get_params().validation=null;
          m.get_params().response=null;
        }
        return m;
      }
    }.invoke(cv.dest());
  }
}
