package hex.deeplearning;

import static water.util.MRUtils.sampleFrame;
import static water.util.MRUtils.sampleFrameStratified;
import hex.FrameTask;
import hex.FrameTask.DataInfo;
import water.H2O;
import water.Job;
import water.Key;
import water.UKV;
import water.api.DeepLearningProgressPage;
import water.api.DocGen;
import water.api.RequestServer;
import water.fvec.Frame;
import water.fvec.RebalanceDataSet;
import water.fvec.Vec;
import water.util.Log;
import water.util.MRUtils;
import water.util.RString;
import water.util.Utils;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Random;

/**
 * Deep Learning Neural Net implementation based on MRTask2
 */
public class DeepLearning extends Job.ValidatedJob {
  static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
  public static DocGen.FieldDoc[] DOC_FIELDS;
  public static final String DOC_GET = "Deep Learning";

  @API(help = "Model checkpoint to resume training with.", filter= Default.class, json = true, gridable = false)
  public Key checkpoint;

  @API(help = "Enable expert mode (to access all options from GUI)", filter = Default.class, json = true, gridable = false)
  public boolean expert_mode = false;

  /*Neural Net Topology*/
  @API(help = "Activation function", filter = Default.class, json = true)
  public Activation activation = Activation.Tanh;

  @API(help = "Hidden layer sizes (e.g. 100,100). Grid search: (10,10), (20,20,20)", filter = Default.class, json = true)
  public int[] hidden = new int[] { 200, 200 };

  @API(help = "How many times the dataset should be iterated (streamed), can be fractional", filter = Default.class, dmin = 1e-3, json = true)
  public double epochs = 10;

  @API(help = "Number of training samples (globally) per MapReduce iteration. Special values are 0: one epoch, -1: all available data (e.g., replicated training data)", filter = Default.class, lmin = -1, json = true)
  public long train_samples_per_iteration = 10000l;

  @API(help = "Seed for random numbers (affects sampling) - Note: only reproducible when running single threaded", filter = Default.class, json = true)
  public long seed = new Random().nextLong();

  /*Adaptive Learning Rate*/
  @API(help = "Adaptive learning rate (ADADELTA)", filter = Default.class, json = true)
  public boolean adaptive_rate = true;

  @API(help = "Adaptive learning rate time decay factor (similarity to prior updates)", filter = Default.class, dmin = 0.01, dmax = 1, json = true)
  public double rho = 0.95;

  @API(help = "Adaptive learning rate smoothing factor (to avoid divisions by zero and allow progress)", filter = Default.class, dmin = 1e-15, dmax = 1, json = true)
  public double epsilon = 1e-6;

  /*Learning Rate*/
  @API(help = "Learning rate (higher => less stable, lower => slower convergence)", filter = Default.class, dmin = 1e-10, dmax = 1, json = true)
  public double rate = .005;

  @API(help = "Learning rate annealing: rate / (1 + rate_annealing * samples)", filter = Default.class, dmin = 0, dmax = 1, json = true)
  public double rate_annealing = 1 / 1e6;

  @API(help = "Learning rate decay factor between layers (N-th layer: rate*alpha^(N-1))", filter = Default.class, dmin = 0, json = true)
  public double rate_decay = 1.0;

  /*Momentum*/
  @API(help = "Initial momentum at the beginning of training (try 0.5)", filter = Default.class, dmin = 0, dmax = 0.9999999999, json = true)
  public double momentum_start = 0;

  @API(help = "Number of training samples for which momentum increases", filter = Default.class, lmin = 1, json = true)
  public long momentum_ramp = 1000000;

  @API(help = "Final momentum after the ramp is over (try 0.99)", filter = Default.class, dmin = 0, dmax = 0.9999999999, json = true)
  public double momentum_stable = 0;

  @API(help = "Use Nesterov accelerated gradient (recommended)", filter = Default.class, json = true)
  public boolean nesterov_accelerated_gradient = true;

  /*Regularization*/
  @API(help = "Input layer dropout ratio (can improve generalization, try 0.1 or 0.2)", filter = Default.class, dmin = 0, dmax = 1, json = true)
  public double input_dropout_ratio = 0.0;

  @API(help = "Hidden layer dropout ratios (can improve generalization), specify one value per hidden layer, defaults to 0.5", filter = Default.class, dmin = 0, dmax = 1, json = true)
  public double[] hidden_dropout_ratios;

  @API(help = "L1 regularization (can add stability and improve generalization, causes many weights to become 0)", filter = Default.class, dmin = 0, dmax = 1, json = true)
  public double l1 = 0.0;

  @API(help = "L2 regularization (can add stability and improve generalization, causes many weights to be small", filter = Default.class, dmin = 0, dmax = 1, json = true)
  public double l2 = 0.0;

  @API(help = "Constraint for squared sum of incoming weights per unit (e.g. for Rectifier)", filter = Default.class, json = true)
  public double max_w2 = Double.POSITIVE_INFINITY;

  /*Initialization*/
  @API(help = "Initial Weight Distribution", filter = Default.class, json = true)
  public InitialWeightDistribution initial_weight_distribution = InitialWeightDistribution.UniformAdaptive;

  @API(help = "Uniform: -value...value, Normal: stddev)", filter = Default.class, dmin = 0, json = true)
  public double initial_weight_scale = 1.0;

  @API(help = "Loss function", filter = Default.class, json = true)
  public Loss loss = Loss.CrossEntropy;

  /*Scoring*/
  @API(help = "Shortest time interval (in secs) between model scoring", filter = Default.class, dmin = 0, json = true)
  public double score_interval = 5;

  @API(help = "Number of training set samples for scoring (0 for all)", filter = Default.class, lmin = 0, json = true)
  public long score_training_samples = 10000l;

  @API(help = "Number of validation set samples for scoring (0 for all)", filter = Default.class, lmin = 0, json = true)
  public long score_validation_samples = 0l;

  @API(help = "Maximum duty cycle fraction for scoring (lower: more training, higher: more scoring).", filter = Default.class, dmin = 0, dmax = 1, json = true)
  public double score_duty_cycle = 0.1;

  @API(help = "Stopping criterion for classification error fraction on training data (-1 to disable)", filter = Default.class, dmin=-1, dmax=1, json = true, gridable = false)
  public double classification_stop = 0;

  @API(help = "Stopping criterion for regression error (MSE) on training data (-1 to disable)", filter = Default.class, dmin=-1, json = true, gridable = false)
  public double regression_stop = 1e-6;

  @API(help = "Enable quiet mode for less output to standard output", filter = Default.class, json = true, gridable = false)
  public boolean quiet_mode = false;

  @API(help = "Max. size (number of classes) for confusion matrices to be shown", filter = Default.class, json = true, gridable = false)
  public int max_confusion_matrix_size = 20;

  @API(help = "Max. number (top K) of predictions to use for hit ratio computation (for multi-class only, 0 to disable)", filter = Default.class, lmin=0, json = true, gridable = false)
  public int max_hit_ratio_k = 10;

  /*Imbalanced Classes*/
  @API(help = "Balance training data class counts via over/under-sampling (for imbalanced data)", filter = Default.class, json = true, gridable = false)
  public boolean balance_classes = false;

  @API(help = "Maximum relative size of the training data after balancing class counts (can be less than 1.0)", filter = Default.class, json = true, dmin=1e-3, gridable = false)
  public float max_after_balance_size = 5.0f;

  @API(help = "Method used to sample validation dataset for scoring", filter = Default.class, json = true, gridable = false)
  public ClassSamplingMethod score_validation_sampling = ClassSamplingMethod.Uniform;

  /*Misc*/
  @API(help = "Enable diagnostics for hidden layers", filter = Default.class, json = true, gridable = false)
  public boolean diagnostics = true;

  @API(help = "Compute variable importances for input features (Gedeon method)", filter = Default.class, json = true)
  public boolean variable_importances = true;

  @API(help = "Enable fast mode (minor approximation in back-propagation)", filter = Default.class, json = true)
  public boolean fast_mode = true;

  @API(help = "Ignore constant training columns (no information can be gained anyway)", filter = Default.class, json = true)
  public boolean ignore_const_cols = true;

  @API(help = "Force extra load balancing to increase training speed for small datasets (to keep all cores busy)", filter = Default.class, json = true)
  public boolean force_load_balance = true;

  @API(help = "Replicate the entire training dataset onto every node for faster training on small datasets", filter = Default.class, json = true)
  public boolean replicate_training_data = true;

  @API(help = "Run on a single node for fine-tuning of model parameters", filter = Default.class, json = true)
  public boolean single_node_mode = false;

  @API(help = "Enable shuffling of training data (recommended if training data is replicated and train_samples_per_iteration is close to #nodes x #rows)", filter = Default.class, json = true)
  public boolean shuffle_training_data = false;

  public enum ClassSamplingMethod {
    Uniform, Stratified
  }

  public enum InitialWeightDistribution {
    UniformAdaptive, Uniform, Normal
  }

  /**
   * Activation functions
   */
  public enum Activation {
    Tanh, TanhWithDropout, Rectifier, RectifierWithDropout, Maxout, MaxoutWithDropout
  }

  /**
   * Loss functions
   * CrossEntropy is recommended
   */
  public enum Loss {
    MeanSquare, CrossEntropy
  }

  // the following parameters can only be specified in expert mode
  transient final String [] expert_options = new String[] {
          "loss",
          "max_w2",
          "warmup_samples",
          "score_training_samples",
          "score_validation_samples",
          "initial_weight_distribution",
          "initial_weight_scale",
          "diagnostics",
          "rate_decay",
          "score_duty_cycle",
          "variable_importances",
          "fast_mode",
          "score_validation_sampling",
          "balance_classes",
          "max_after_balance_size",
          "max_after_balance_size",
          "ignore_const_cols",
          "force_load_balance",
          "replicate_training_data",
          "shuffle_training_data",
          "nesterov_accelerated_gradient",
          "classification_stop",
          "regression_stop",
          "quiet_mode",
          "max_confusion_matrix_size",
          "max_hit_ratio_k",
          "hidden_dropout_ratios",
          "single_node_mode",
  };

  // the following parameters can be modified when restarting from a checkpoint
  transient final String [] cp_modifiable = new String[] {
          "expert_mode",
          "seed",
          "epochs",
          "score_interval",
          "train_samples_per_iteration",
          "score_duty_cycle",
          "classification_stop",
          "regression_stop",
          "quiet_mode",
          "max_confusion_matrix_size",
          "max_hit_ratio_k",
          "diagnostics",
          "variable_importances",
          "force_load_balance",
          "replicate_training_data",
          "single_node_mode",
  };

  /**
   * Helper to specify which arguments trigger a refresh on change
   * @param ver
   */
  @Override
  protected void registered(RequestServer.API_VERSION ver) {
    super.registered(ver);
    for (Argument arg : _arguments) {
      if ( arg._name.equals("activation") || arg._name.equals("initial_weight_distribution")
              || arg._name.equals("expert_mode") || arg._name.equals("adaptive_rate")
              || arg._name.equals("replicate_training_data")
              || arg._name.equals("balance_classes") || arg._name.equals("checkpoint")) {
        arg.setRefreshOnChange();
      }
    }
  }

  /**
   * Helper to handle arguments based on existing input values
   * @param arg
   * @param inputArgs
   */
  @Override protected void queryArgumentValueSet(Argument arg, java.util.Properties inputArgs) {
    super.queryArgumentValueSet(arg, inputArgs);

    if (!arg._name.equals("checkpoint") && !Utils.contains(cp_modifiable, arg._name)) {
      if (checkpoint != null) {
        arg.disable("Taken from model checkpoint.");
        final DeepLearningModel cp_model = UKV.get(checkpoint);
        if (cp_model == null) {
          throw new IllegalArgumentException("Checkpointed model was not found.");
        }
        if (cp_model.model_info().unstable()) {
          throw new IllegalArgumentException("Checkpointed model was unstable. Not restarting.");
        }
        return;
      }
    }
    if(arg._name.equals("initial_weight_scale") &&
            (initial_weight_distribution == InitialWeightDistribution.UniformAdaptive)
            ) {
      arg.disable("Using sqrt(6 / (# units + # units of previous layer)) for Uniform distribution.", inputArgs);
    }
    if(arg._name.equals("loss") && !classification) {
      arg.disable("Using MeanSquare loss for regression.", inputArgs);
      loss = Loss.MeanSquare;
    }
    if (classification) {
      if(arg._name.equals("regression_stop")) {
        arg.disable("Only for regression.", inputArgs);
      }
      if(arg._name.equals("max_after_balance_size") && !balance_classes) {
        arg.disable("Requires balance_classes.", inputArgs);
      }
    }
    else {
      if(arg._name.equals("classification_stop")
              || arg._name.equals("max_confusion_matrix_size")
              || arg._name.equals("max_hit_ratio_k")
              || arg._name.equals("max_after_balance_size")
              || arg._name.equals("balance_classes")) {
        arg.disable("Only for classification.", inputArgs);
      }
      if (validation != null && arg._name.equals("score_validation_sampling")) {
        score_validation_sampling = ClassSamplingMethod.Uniform;
        arg.disable("Using uniform sampling for validation scoring dataset.", inputArgs);
      }
    }
    if ((arg._name.equals("score_validation_samples") || arg._name.equals("score_validation_sampling")) && validation == null) {
      arg.disable("Requires a validation data set.", inputArgs);
    }
    if (Utils.contains(expert_options, arg._name) && !expert_mode) {
      arg.disable("Only in expert mode.", inputArgs);
    }
    if (!adaptive_rate) {
      if (arg._name.equals("rho") || arg._name.equals("epsilon")) {
        arg.disable("Only for adaptive learning rate.", inputArgs);
        rho = 0;
        epsilon = 0;
      }
    } else {
      if (arg._name.equals("rate") || arg._name.equals("rate_annealing") || arg._name.equals("rate_decay") || arg._name.equals("nesterov_accelerated_gradient")
              || arg._name.equals("momentum_start") || arg._name.equals("momentum_ramp") || arg._name.equals("momentum_stable") ) {
        arg.disable("Only for non-adaptive learning rate.", inputArgs);
        momentum_start = 0;
        momentum_stable = 0;
      }
    }
    if (arg._name.equals("hidden_dropout_ratios")) {
      if (activation != Activation.TanhWithDropout && activation != Activation.MaxoutWithDropout && activation != Activation.RectifierWithDropout) {
        arg.disable("Only for activation functions with dropout.", inputArgs);
      }
    }
    if (arg._name.equals("replicate_training_data") && (H2O.CLOUD.size() == 1)) {
      arg.disable("Only for multi-node operation.");
      replicate_training_data = false;
    }
    if (arg._name.equals("single_node_mode") && (H2O.CLOUD.size() == 1 || !replicate_training_data)) {
      arg.disable("Only for multi-node operation with replication.");
      single_node_mode = false;
    }
  }

  /** Print model parameters as JSON */
  @Override public boolean toHTML(StringBuilder sb) {
    return makeJsonBox(sb);
  }

  /**
   * Return a query link to this page
   * @param k Model Key
   * @param content Link text
   * @return HTML Link
   */
  public static String link(Key k, String content) {
    return link(k, content, null, null, null);
  }

  /**
   * Return a query link to this page
   * @param k Model Key
   * @param content Link text
   * @param cp Key to checkpoint to continue training with (optional)
   * @param response Response
   * @param val Validation data set key
   * @return HTML Link
   */
  public static String link(Key k, String content, Key cp, String response, Key val) {
    DeepLearning req = new DeepLearning();
    RString rs = new RString("<a href='" + req.href() + ".query?source=%$key" +
            (cp == null ? "" : "&checkpoint=%$cp") +
            (response == null ? "" : "&response=%$resp") +
            (val == null ? "" : "&validation=%$valkey") +
            "'>%content</a>");
    rs.replace("key", k.toString());
    rs.replace("content", content);
    if (cp != null) rs.replace("cp", cp.toString());
    if (response != null) rs.replace("resp", response);
    if (val != null) rs.replace("valkey", val);
    return rs.toString();
  }

  /**
   * Report the relative progress of building a Deep Learning model (measured by how many epochs are done)
   * @return floating point number between 0 and 1
   */
  @Override public float progress(){
    if(UKV.get(dest()) == null)return 0;
    DeepLearningModel m = UKV.get(dest());
    if (m != null && m.model_info()!=null )
      return (float)Math.min(1, (m.epoch_counter / m.model_info().get_params().epochs));
    return 0;
  }

  /**
   * Train a Deep Learning model, assumes that all members are populated
   * @return JobState
   */
  @Override
  public final void execImpl() {
    DeepLearningModel cp;
    if (checkpoint == null) cp = initModel();
    else {
      final DeepLearningModel previous = UKV.get(checkpoint);
      if (previous == null) throw new IllegalArgumentException("Checkpoint not found.");
      epochs += previous.epoch_counter; //add new epochs to existing model
      Log.info("Adding " + String.format("%.3f", previous.epoch_counter) + " epochs from the checkpointed model.");
      cp = new DeepLearningModel(previous, destination_key, job_key);
      cp.model_info().get_params().state = JobState.RUNNING;
      try {
        Log.info("Resuming from checkpoint.");
        cp.write_lock(self());
        assert(state==JobState.RUNNING);
        if (source == null || !Arrays.equals(source._key._kb, previous.model_info().get_params().source._key._kb)) {
          throw new IllegalArgumentException("source must be the same as for the checkpointed model.");
        }
        if (response == null || !Arrays.equals(response._key._kb, previous.model_info().get_params().response._key._kb)) {
          throw new IllegalArgumentException("response must be the same as for the checkpointed model.");
        }
        if (Utils.difference(ignored_cols, previous.model_info().get_params().ignored_cols).length != 0) {
          throw new IllegalArgumentException("ignored_cols must be the same as for the checkpointed model.");
        }
        if ((validation!=null) != (previous.model_info().get_params().validation != null)
                || (validation != null && !Arrays.equals(validation._key._kb, previous.model_info().get_params().validation._key._kb))) {
          throw new IllegalArgumentException("validation must be the same as for the checkpointed model.");
        }
        if (classification != previous.model_info().get_params().classification) {
          Log.warn("Automatically switching to " + ((classification=!classification) ? "classification" : "regression") + " (same as the checkpointed model).");
        }
        final DeepLearning mp = cp.model_info().get_params();
        Object A = mp, B = this;
        for (Field fA : A.getClass().getDeclaredFields()) {
          if (Utils.contains(cp_modifiable, fA.getName())) {
            if (!expert_mode && Utils.contains(expert_options, fA.getName())) continue;
            for (Field fB : B.getClass().getDeclaredFields()) {
              if (fA.equals(fB)) {
                try {
                  if (!fA.get(A).toString().equals(fB.get(B).toString())) {
                    Log.info("Applying user-requested modification of '" + fA.getName() + "': " + fA.get(A) + " -> " + fB.get(B));
                    fA.set(A, fB.get(B));
                  }
                } catch (IllegalAccessException e) {
                  e.printStackTrace();
                }
              }
            }
          }
        }
        cp.update(self());
      } finally {
        cp.unlock(self());
      }
    }
    trainModel(cp);
    delete();
  }

  /**
   * Redirect to the model page for that model that is trained by this job
   * @return Response
   */
  @Override protected Response redirect() {
    return DeepLearningProgressPage.redirect(this, self(), dest());
  }

  private boolean _fakejob;
  //Sanity check for Deep Learning job parameters
  private void checkParams() {
    if (source.numCols() <= 1)
      throw new IllegalArgumentException("Training data must have at least 2 features (incl. response).");

    if (hidden == null) throw new IllegalArgumentException("There must be at least one hidden layer.");

    for (int i=0;i<hidden.length;++i) {
      if (hidden[i]==0)
        throw new IllegalArgumentException("Hidden layer size must be >0.");
    }

    //Auto-fill defaults
    if (hidden_dropout_ratios == null) {
      hidden_dropout_ratios = new double[hidden.length];
      if (activation == Activation.TanhWithDropout || activation == Activation.MaxoutWithDropout || activation == Activation.RectifierWithDropout) {
        Arrays.fill(hidden_dropout_ratios, 0.5);
      }
    }
    else if (hidden_dropout_ratios.length != hidden.length) throw new IllegalArgumentException("Must have " + hidden.length + " hidden layer dropout ratios.");

    if(!classification && loss != Loss.MeanSquare) {
      Log.warn("Setting loss to MeanSquare for regression.");
      loss = Loss.MeanSquare;
    }
    // make default job_key and destination_key in case they are missing
    if (dest() == null) {
      destination_key = Key.make();
    }
    if (self() == null) {
      job_key = Key.make();
    }
    if (UKV.get(self()) == null) {
      start_time = System.currentTimeMillis();
      state      = JobState.RUNNING;
      UKV.put(self(), this);
      _fakejob = true;
    }
  }

  /**
   * Create an initial Deep Learning model, typically to be trained by trainModel(model)
   * @return Randomly initialized model
   */
  public final DeepLearningModel initModel() {
    try {
      lock_data();
      checkParams();
      final boolean del_enum_resp = (classification && !response.isEnum());
      final Frame train = FrameTask.DataInfo.prepareFrame(source, response, ignored_cols, classification, ignore_const_cols, true /*drop >20% NA cols*/);
      final DataInfo dinfo = new FrameTask.DataInfo(train, 1, true, !classification);
      final Vec resp = dinfo._adaptedFrame.lastVec();
      assert(!classification ^ resp.isEnum()); //either regression or enum response
      float[] priorDist = classification ? new MRUtils.ClassDist(resp).doAll(resp).rel_dist() : null;
      final DeepLearningModel model = new DeepLearningModel(dest(), self(), source._key, dinfo, this, priorDist);
      model.model_info().initializeMembers();
      if (del_enum_resp) model.toDelete(resp._key);
      return model;
    }
    finally {
      unlock_data();
    }
  }

  /**
   * Incrementally train an existing model
   * @param model Initial model
   * @param epochs How many epochs to train for
   * @return Updated model
   */
  public final DeepLearningModel trainModel(DeepLearningModel model, double epochs) {
    model.model_info().get_params().epochs += epochs;
    return trainModel(model);
  }

  /**
   * Helper to update a Frame and adding it to the local trash at the same time
   * @param target Frame referece, to be overwritten
   * @param src Newly made frame, to be deleted via local trash
   * @return src
   */
  Frame updateFrame(Frame target, Frame src) {
    if (src != target) ltrash(src);
    return src;
  }

  /**
   * Train a Deep Learning neural net model
   * @param model Input model (e.g., from initModel(), or from a previous training run)
   * @return Trained model
   */
  public final DeepLearningModel trainModel(DeepLearningModel model) {
    Frame validScoreFrame = null;
    Frame train, trainScoreFrame;
    try {
      lock_data();
      if (checkpoint == null) logStart(); //if checkpoint is given, some Job's params might be uninitialized (but the restarted model's parameters are correct)
      if (model == null) {
        model = UKV.get(dest());
      }
      model.write_lock(self());
      final DeepLearning mp = model.model_info().get_params(); //use the model's parameters for everything below - NOT the job's parameters (can be different after checkpoint restart)

      prepareValidationWithModel(model);
      final long model_size = model.model_info().size();
      Log.info("Number of model parameters (weights/biases): " + String.format("%,d", model_size));
//      Log.info("Memory usage of the model: " + String.format("%.2f", (double)model_size*Float.SIZE / (1<<23)) + " MB.");
      train = model.model_info().data_info()._adaptedFrame;
      if (mp.force_load_balance) train = updateFrame(train, reBalance(train, mp.replicate_training_data /*rebalance into only 4*cores per node*/));
//      train = updateFrame(train, reBalance(train, mp.seed, mp.replicate_training_data, mp.force_load_balance, mp.shuffle_training_data));
      float[] trainSamplingFactors;
      if (mp.classification && mp.balance_classes) {
        trainSamplingFactors = new float[train.lastVec().domain().length]; //leave initialized to 0 -> will be filled up below
        train = updateFrame(train, sampleFrameStratified(
                train, train.lastVec(), trainSamplingFactors, (long)(mp.max_after_balance_size*train.numRows()), mp.seed, true, false));
        model.setModelClassDistribution(new MRUtils.ClassDist(train.lastVec()).doAll(train.lastVec()).rel_dist());
      }
      model.training_rows = train.numRows();
      trainScoreFrame = sampleFrame(train, mp.score_training_samples, mp.seed); //training scoring dataset is always sampled uniformly from the training dataset
      if (train != trainScoreFrame) ltrash(trainScoreFrame);

      Log.info("Number of chunks of the training data: " + train.anyVec().nChunks());
      if (validation != null) {
        Frame adaptedValid = getValidation();
        if (getValidAdaptor().needsAdaptation2CM()) {
          adaptedValid.add(getValidAdaptor().adaptedValidationResponse(_responseName), getValidAdaptor().getAdaptedValidationResponse2CM());
        }
        // validation scoring dataset can be sampled in multiple ways from the given validation dataset
        if (mp.classification && mp.balance_classes && mp.score_validation_sampling == ClassSamplingMethod.Stratified) {
          validScoreFrame = updateFrame(adaptedValid, sampleFrameStratified(adaptedValid, adaptedValid.lastVec(), null,
                  mp.score_validation_samples > 0 ? mp.score_validation_samples : adaptedValid.numRows(), mp.seed+1, false /* no oversampling */, false));
        } else {
          validScoreFrame = updateFrame(adaptedValid, sampleFrame(adaptedValid, mp.score_validation_samples, mp.seed+1));
        }
        if (mp.force_load_balance) validScoreFrame = updateFrame(validScoreFrame, reBalance(validScoreFrame, false /*always split up globally since scoring should be distributed*/));
        Log.info("Number of chunks of the validation data: " + validScoreFrame.anyVec().nChunks());
      }

      // Set train_samples_per_iteration size (cannot be done earlier since this depends on whether stratified sampling is done)
      mp.train_samples_per_iteration = computeTrainSamplesPerIteration(mp.train_samples_per_iteration, train.numRows(), mp.replicate_training_data, mp.single_node_mode);
      // Determine whether shuffling is enforced
      if(mp.replicate_training_data && (mp.train_samples_per_iteration == train.numRows()*H2O.CLOUD.size()) && !mp.shuffle_training_data && H2O.CLOUD.size() > 1) {
        Log.warn("Enabling training data shuffling, because all nodes train on the full dataset (replicated training data)");
        mp.shuffle_training_data = true;
      }
      final float rowUsageFraction = computeRowUsageFraction(train.numRows(), mp.train_samples_per_iteration, mp.replicate_training_data);

      if (!mp.quiet_mode) Log.info("Initial model:\n" + model.model_info());
      Log.info("Starting to train the Deep Learning model.");

      //main loop
      do model.set_model_info(H2O.CLOUD.size() > 1 && mp.replicate_training_data ? ( mp.single_node_mode ?
              new DeepLearningTask2(train, model.model_info(), rowUsageFraction).invoke(Key.make()).model_info() : //replicated data + single node mode
              new DeepLearningTask2(train, model.model_info(), rowUsageFraction).invokeOnAllNodes().model_info() ) : //replicated data + multi-node mode
              new DeepLearningTask(model.model_info(), rowUsageFraction).doAll(train).model_info()); //distributed data (always in multi-node mode)
      while (model.doScoring(train, trainScoreFrame, validScoreFrame, self(), getValidAdaptor()));

      Log.info("Finished training the Deep Learning model.");
      return model;
    }
    catch(JobCancelledException ex) {
      Log.info("Deep Learning model building was cancelled.");
      model = UKV.get(dest());
      return model;
    }
    catch(Exception ex) {
      ex.printStackTrace();
      throw new RuntimeException(ex);
    }
    finally {
      if (model != null) model.unlock(self());
      unlock_data();
      emptyLTrash();
    }
  }

  /**
   * Lock the input datasets against deletes
   */
  private void lock_data() {
    source.read_lock(self());
    if( validation != null && source._key != null && validation._key !=null && !source._key.equals(validation._key) )
      validation.read_lock(self());
  }

  /**
   * Release the lock for the input datasets
   */
  private void unlock_data() {
    source.unlock(self());
    if( validation != null && !source._key.equals(validation._key) )
      validation.unlock(self());
  }

  /**
   * Delete job related keys
   */
  public void delete() {
    cleanup();
    if (_fakejob) UKV.remove(job_key);
    remove();
  }

  /**
   * Rebalance a frame for load balancing
   * @param fr Input frame
   * @param local whether to only create enough chunks to max out all cores on one node only
   * @return Frame that has potentially more chunks
   */
  private Frame reBalance(final Frame fr, boolean local) {
    final int chunks = (int)Math.min( 4 * H2O.NUMCPUS * (local ? 1 : H2O.CLOUD.size()), fr.numRows());
    if (fr.anyVec().nChunks() > chunks) {
      Log.info("Dataset already contains " + fr.anyVec().nChunks() + " chunks. No need to rebalance.");
      return fr;
    }
    Log.info("Starting load balancing into (at least) " + chunks + " chunks.");
//      return MRUtils.shuffleAndBalance(fr, chunks, seed, local, shuffle_training_data);
    Key newKey = fr._key != null ? Key.make(fr._key.toString() + ".balanced") : Key.make();
    RebalanceDataSet rb = new RebalanceDataSet(fr, newKey, chunks);
    H2O.submitTask(rb);
    rb.join();
    Frame rebalanced = UKV.get(newKey);
    Log.info("Load balancing done.");
    return rebalanced;
  }

  /**
   * Compute the actual train_samples_per_iteration size from the user-given parameter
   * @param train_samples_per_iteration user-given train_samples_per_iteration size
   * @param numRows number of training rows
   * @param replicate_training_data whether or not the training data is replicated on each node
   * @param single_node_mode whether or not the single node mode is enabled
   * @return The total number of training rows to be processed per iteration (summed over on all nodes)
   */
  private static long computeTrainSamplesPerIteration(long train_samples_per_iteration, final long numRows, final boolean replicate_training_data, final boolean single_node_mode) {
    assert(train_samples_per_iteration == 0 || train_samples_per_iteration == -1 || train_samples_per_iteration >= 1);
    if (train_samples_per_iteration == 0 || (!replicate_training_data && (train_samples_per_iteration == -1 || train_samples_per_iteration > numRows)) || (replicate_training_data && single_node_mode))
      Log.info("Setting train_samples_per_iteration (" + train_samples_per_iteration + ") to one epoch: #rows (" + (train_samples_per_iteration=numRows) + ").");
    else if (train_samples_per_iteration == -1 || train_samples_per_iteration > H2O.CLOUD.size()*numRows)
      Log.info("Setting train_samples_per_iteration (" + train_samples_per_iteration + ") to the largest possible number: #nodes x #rows (" + (train_samples_per_iteration=H2O.CLOUD.size()*numRows) + ").");
    assert(train_samples_per_iteration != 0 && train_samples_per_iteration != -1 && train_samples_per_iteration >= 1);
    return train_samples_per_iteration;
  }

  /**
   * Compute the fraction of rows that need to be used for training during one iteration
   * @param numRows number of training rows
   * @param train_samples_per_iteration number of training rows to be processed per iteration
   * @param replicate_training_data whether of not the training data is replicated on each node
   * @return fraction of rows to be used for training during one iteration
   */
  private static float computeRowUsageFraction(final long numRows, long train_samples_per_iteration, boolean replicate_training_data) {
    float rowUsageFraction = (float)train_samples_per_iteration / numRows;
    if (replicate_training_data) rowUsageFraction /= H2O.CLOUD.size();
    assert(rowUsageFraction > 0 && rowUsageFraction <= 1.);
    return rowUsageFraction;
  }

}
