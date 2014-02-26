package hex.nn;

import hex.FrameTask;
import hex.FrameTask.DataInfo;
import water.H2O;
import water.Job;
import water.Key;
import water.UKV;
import water.api.DocGen;
import water.api.NNProgressPage;
import water.api.RequestServer;
import water.fvec.Frame;
import water.util.Log;
import water.util.MRUtils;
import water.util.RString;

import java.util.Random;

import static water.util.MRUtils.sampleFrame;

public class NN extends Job.ValidatedJob {
  static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
  public static DocGen.FieldDoc[] DOC_FIELDS;
  public static final String DOC_GET = "NN";

  @API(help = "Activation function", filter = Default.class, json = true)
  public Activation activation = Activation.Tanh;

  @API(help = "Input layer dropout ratio", filter = Default.class, dmin = 0, dmax = 1, json = true)
  public double input_dropout_ratio = 0.0;

  @API(help = "Hidden layer sizes (e.g. 100,100). Grid search: (10,10), (20,20,20)", filter = Default.class, json = true)
  public int[] hidden = new int[] { 200, 200 };

  @API(help = "Learning rate (higher => less stable, lower => slower convergence)", filter = Default.class, dmin = 1e-10, dmax = 1, json = true)
  public double rate = .005;

  @API(help = "Learning rate annealing: rate / (1 + rate_annealing * samples)", filter = Default.class, dmin = 0, dmax = 1, json = true)
  public double rate_annealing = 1 / 1e6;

  @API(help = "L1 regularization, can add stability and improve generalization", filter = Default.class, dmin = 0, dmax = 1, json = true)
  public double l1 = 0.0;

  @API(help = "L2 regularization, can add stability and improve generalization", filter = Default.class, dmin = 0, dmax = 1, json = true)
  public double l2 = 0.0;

  @API(help = "Initial momentum at the beginning of training", filter = Default.class, dmin = 0, dmax = 0.9999999999, json = true)
  public double momentum_start = .5;

  @API(help = "Number of training samples for which momentum increases", filter = Default.class, lmin = 1, json = true)
  public long momentum_ramp = 1000000;

  @API(help = "Final momentum after the ramp is over", filter = Default.class, dmin = 0, dmax = 0.9999999999, json = true)
  public double momentum_stable = 0.99;

  @API(help = "How many times the dataset should be iterated (streamed), can be fractional", filter = Default.class, dmin = 1e-3, json = true)
  public double epochs = 10;

  @API(help = "Seed for random numbers (reproducible results for small datasets (single chunk) only, cf. Hogwild!)", filter = Default.class, json = true)
  public long seed = new Random().nextLong();

  @API(help = "Enable expert mode (to access all options from GUI)", filter = Default.class, json = true, gridable = false)
  public boolean expert_mode = false;

  @API(help = "Initial Weight Distribution", filter = Default.class, json = true)
  public InitialWeightDistribution initial_weight_distribution = InitialWeightDistribution.UniformAdaptive;

  @API(help = "Uniform: -value...value, Normal: stddev)", filter = Default.class, dmin = 0, json = true)
  public double initial_weight_scale = 1.0;

  @API(help = "Loss function", filter = Default.class, json = true)
  public Loss loss = Loss.CrossEntropy;

  @API(help = "Learning rate decay factor between layers (N-th layer: rate*alpha^(N-1))", filter = Default.class, dmin = 0, json = true)
  public double rate_decay = 1.0;

  @API(help = "Constraint for squared sum of incoming weights per unit (e.g. for Rectifier)", filter = Default.class, json = true)
  public double max_w2 = Double.POSITIVE_INFINITY;

  @API(help = "Number of training set samples for scoring (0 for all)", filter = Default.class, lmin = 0, json = true)
  public long score_training_samples = 10000l;

  @API(help = "Number of validation set samples for scoring (0 for all)", filter = Default.class, lmin = 0, json = true)
  public long score_validation_samples = 0l;

  @API(help = "Shortest interval (in seconds) between scoring", filter = Default.class, dmin = 0, json = true)
  public double score_interval = 5;

  @API(help = "Number of training samples per mini-batch (0 for entire epoch).", filter = Default.class, lmin = 0, json = true)
  public long mini_batch = 10000l;

  @API(help = "Enable diagnostics for hidden layers", filter = Default.class, json = true, gridable = false)
  public boolean diagnostics = true;

  @API(help = "Enable fast mode (minor approximation in back-propagation)", filter = Default.class, json = true)
  public boolean fast_mode = true;

  @API(help = "Ignore constant training columns", filter = Default.class, json = true)
  public boolean ignore_const_cols = true;

  @API(help = "Force extra load balancing to increase training speed for small datasets", filter = Default.class, json = true)
  public boolean force_load_balance = true;

  @API(help = "Enable periodic shuffling of training data (can increase stochastic gradient descent performance)", filter = Default.class, json = true)
  public boolean shuffle_training_data = false;

  @API(help = "Use Nesterov accelerated gradient (recommended)", filter = Default.class, json = true)
  public boolean nesterov_accelerated_gradient = true;

  @API(help = "Stopping criterion for classification error fraction (-1 to disable)", filter = Default.class, dmin=-1, dmax=1, json = true, gridable = false)
  public double classification_stop = 0;

  @API(help = "Stopping criterion for regression error (MSE) (-1 to disable)", filter = Default.class, dmin=-1, json = true, gridable = false)
  public double regression_stop = 1e-6;

  @API(help = "Enable quiet mode for less output to standard output", filter = Default.class, json = true, gridable = false)
  public boolean quiet_mode = false;

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

  @Override
  protected void registered(RequestServer.API_VERSION ver) {
    super.registered(ver);
    for (Argument arg : _arguments) {
      if ( arg._name.equals("activation") || arg._name.equals("initial_weight_distribution") || arg._name.equals("expert_mode")) {
        arg.setRefreshOnChange();
      }
    }
  }

  @Override protected void queryArgumentValueSet(Argument arg, java.util.Properties inputArgs) {
    super.queryArgumentValueSet(arg, inputArgs);
    if(arg._name.equals("initial_weight_scale") &&
            (initial_weight_distribution == InitialWeightDistribution.UniformAdaptive)
            ) {
      arg.disable("Using sqrt(6 / (# units + # units of previous layer)) for Uniform distribution.", inputArgs);
    }
    if(arg._name.equals("loss") && !classification) {
      arg.disable("Using MeanSquare loss for regression.", inputArgs);
      loss = Loss.MeanSquare;
    }
    if(arg._name.equals("classification_stop") && !classification) {
      arg.disable("Only for classification.", inputArgs);
    }
    if (expert_mode && arg._name.equals("force_load_balance") && H2O.CLOUD.size()>1) {
      force_load_balance = false;
      arg.disable("Only for single-node operation.");
    }

    if(arg._name.equals("regression_stop") && classification) {
      arg.disable("Only for regression.", inputArgs);
    }
    if (arg._name.equals("score_validation_samples") && validation == null) {
      arg.disable("Only if a validation set is specified.", inputArgs);
    }
    if (arg._name.equals("loss") || arg._name.equals("max_w2") || arg._name.equals("warmup_samples")
            || arg._name.equals("score_training_samples")
            || arg._name.equals("score_validation_samples")
            || arg._name.equals("initial_weight_distribution")
            || arg._name.equals("initial_weight_scale")
            || arg._name.equals("score_interval")
            || arg._name.equals("diagnostics")
            || arg._name.equals("rate_decay")
            || arg._name.equals("mini_batch")
            || arg._name.equals("fast_mode")
            || arg._name.equals("ignore_const_cols")
            || arg._name.equals("force_load_balance")
            || arg._name.equals("shuffle_training_data")
            || arg._name.equals("nesterov_accelerated_gradient")
            || arg._name.equals("classification_stop")
            || arg._name.equals("regression_stop")
            || arg._name.equals("quiet_mode")
            ) {
      if (!expert_mode) arg.disable("Only in expert mode.", inputArgs);
    }
  }

  public Frame score( Frame fr ) { return ((NNModel)UKV.get(dest())).score(fr);  }

  /** Print model parameters as JSON */
  @Override public boolean toHTML(StringBuilder sb) {
    return makeJsonBox(sb);
  }

  /** Return the query link to this page */
  public static String link(Key k, String content) {
    NN req = new NN();
    RString rs = new RString("<a href='" + req.href() + ".query?%key_param=%$key'>%content</a>");
    rs.replace("key_param", "source");
    rs.replace("key", k.toString());
    rs.replace("content", content);
    return rs.toString();
  }

  @Override public float progress(){
    if(UKV.get(dest()) == null)return 0;
    NNModel m = UKV.get(dest());
    if (m != null && m.model_info()!=null )
      return (float)Math.min(1, (m.epoch_counter / m.model_info().get_params().epochs));
    return 0;
  }

  @Override public JobState exec() {
    buildModel(initModel());
    delete();
    return JobState.DONE;
  }

  @Override protected Response redirect() {
    return NNProgressPage.redirect(this, self(), dest());
  }

  private boolean _fakejob;
  void checkParams() {
    if (source.numCols() <= 1)
      throw new IllegalArgumentException("Training data must have at least 2 features (incl. response).");

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

  public final NNModel initModel() {
    try {
      lock_data();
      checkParams();
      final Frame train = FrameTask.DataInfo.prepareFrame(source, response, ignored_cols, classification, ignore_const_cols);
      final DataInfo dinfo = new FrameTask.DataInfo(train, 1, true, !classification);
      final NNModel model = new NNModel(dest(), self(), source._key, dinfo, this);
      model.model_info().initializeMembers();
      return model;
    }
    finally {
      unlock_data();
    }
  }

  public final NNModel buildModel(NNModel model) {
    Frame[] valid_adapted = null;
    Frame valid = null, validScoreFrame = null;
    Frame train = null, trainScoreFrame = null;
    try {
      lock_data();
      logStart();
      if (model == null) {
        model = UKV.get(dest());
      }
      model.write_lock(self());
      final long model_size = model.model_info().size();
      Log.info("Number of model parameters (weights/biases): " + String.format("%,d", model_size));
//      Log.info("Memory usage of the model: " + String.format("%.2f", (double)model_size*Float.SIZE / (1<<23)) + " MB.");
      train = reBalance(model.model_info().data_info()._adaptedFrame, seed);
      trainScoreFrame = sampleFrame(train, score_training_samples, seed);
      Log.info("Number of chunks of the training data: " + train.anyVec().nChunks());
      if (validation != null) {
        valid_adapted = model.adapt(validation, false);
        valid = reBalance(valid_adapted[0], seed+1);
        validScoreFrame = sampleFrame(valid, score_validation_samples, seed+1);
        Log.info("Number of chunks of the validation data: " + valid.anyVec().nChunks());
      }
      if (mini_batch > train.numRows()) {
        Log.warn("Setting mini_batch (" + mini_batch
                + ") to the number of rows of the training data (" + (mini_batch=train.numRows()) + ").");
      }
      // determines the number of rows processed during NNTask, affects synchronization (happens at the end of each NNTask)
      final float sync_fraction = mini_batch == 0l ? 1.0f : (float)mini_batch / train.numRows();

      if (!quiet_mode) Log.info("Initial model:\n" + model.model_info());

      Log.info("Starting to train the Neural Net model.");
      long timeStart = System.currentTimeMillis();

      //main loop
      long iter = 0;
      Frame newtrain = new Frame(train);
      do {
        model.set_model_info(new NNTask(model.model_info(), sync_fraction).doAll(newtrain).model_info());
        if (++iter % 10 != 0 && shuffle_training_data) {
          Frame newtrain2 = reBalance(newtrain, seed+iter);
          if (newtrain != newtrain2) {
            newtrain.delete();
            newtrain = newtrain2;
            trainScoreFrame = sampleFrame(newtrain, score_training_samples, seed+iter+0xDADDAAAA);
          }
        }
      }
      while (model.doScoring(trainScoreFrame, validScoreFrame, timeStart, self()));

      Log.info("Finished training the Neural Net model.");
      return model;
    }
    finally {
      model.unlock(self());
      //clean up
      if (validScoreFrame != null && validScoreFrame != valid) validScoreFrame.delete();
      if (trainScoreFrame != null && trainScoreFrame != train) trainScoreFrame.delete();
      if (validation != null) valid_adapted[1].delete(); //just deleted the adapted frames for validation
//    if (_newsource != null && _newsource != source) _newsource.delete();
      unlock_data();
    }
  }

  private void lock_data() {
    // Lock the input datasets against deletes
    source.read_lock(self());
    if( validation != null && source._key != null && validation._key !=null && !source._key.equals(validation._key) )
      validation.read_lock(self());
  }

  private void unlock_data() {
    source.unlock(self());
    if( validation != null && !source._key.equals(validation._key) )
      validation.unlock(self());
  }

  public void delete() {
    if (_fakejob) UKV.remove(job_key);
    remove();
  }

  private Frame reBalance(final Frame fr, long seed) {
    return force_load_balance || shuffle_training_data ? MRUtils.shuffleAndBalance(fr, seed, shuffle_training_data) : fr;
  }

}
