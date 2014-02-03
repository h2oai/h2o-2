package hex.nn;

import hex.FrameTask.DataInfo;
import water.*;
import water.api.DocGen;
import water.api.NNModelView;
import water.api.NNProgressPage;
import water.api.RequestServer;
import water.fvec.Frame;
import water.util.Log;
import water.util.RString;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

public class NN extends Job.ValidatedJob {
  static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
  public static DocGen.FieldDoc[] DOC_FIELDS;
  public static final String DOC_GET = "Neural Network";

  public DataInfo _dinfo;
  private boolean _gen_enum;

  @API(help = "Activation function", filter = Default.class, json = true)
  public Activation activation = Activation.Tanh;

  @API(help = "Input layer dropout ratio", filter = Default.class, dmin = 0, dmax = 1, json = true)
  public double input_dropout_ratio = 0.2;

  @API(help = "Hidden layer sizes, e.g. 1000, 1000. Grid search: (100, 100), (200, 200)", filter = Default.class, json = true)
  public int[] hidden = new int[] { 200, 200 };

  @API(help = "Learning rate (higher => less stable, lower => slower convergence)", filter = Default.class, dmin = 0, dmax = 1, json = true)
  public double rate = .005;

  @API(help = "Learning rate annealing: rate / (1 + rate_annealing * samples)", filter = Default.class, dmin = 0, dmax = 1, json = true)
  public double rate_annealing = 1 / 1e6;

  @API(help = "L1 regularization, can add stability", filter = Default.class, dmin = 0, dmax = 1, json = true)
  public double l1 = 0.0;

  @API(help = "L2 regularization, can add stability", filter = Default.class, dmin = 0, dmax = 1, json = true)
  public double l2 = 0.0;

  @API(help = "Initial momentum at the beginning of training", filter = Default.class, dmin = 0, json = true)
  public double momentum_start = .5;

  @API(help = "Number of training samples for which momentum increases", filter = Default.class, lmin = 0, json = true)
  public long momentum_ramp = 1000000;

  @API(help = "Final momentum after the ramp is over", filter = Default.class, dmin = 0, json = true)
  public double momentum_stable = 1.0;

  @API(help = "How many times the dataset should be iterated (streamed), can be less than 1.0", filter = Default.class, dmin = 0, json = true)
  public double epochs = 10;

  @API(help = "Seed for random numbers (reproducible results for single-threaded only, cf. Hogwild)", filter = Default.class, json = true)
  public long seed = new Random().nextLong();

  @API(help = "Enable expert mode", filter = Default.class, json = false)
  public boolean expert_mode = false;

  @API(help = "Initial Weight Distribution", filter = Default.class, json = true)
  public InitialWeightDistribution initial_weight_distribution = InitialWeightDistribution.UniformAdaptive;

  @API(help = "Uniform: -value...value, Normal: stddev)", filter = Default.class, dmin = 0, json = true)
  public double initial_weight_scale = 1.0;

  @API(help = "Loss function", filter = Default.class, json = true)
  public Loss loss = Loss.CrossEntropy;

  @API(help = "Learning rate decay factor between layers (N-th layer: rate*alpha^(N-1))", filter = Default.class, dmin = 0, json = true)
  public double rate_decay = 1.0;

  @API(help = "Constraint for squared sum of incoming weights per unit", filter = Default.class, json = true)
  public double max_w2 = Double.POSITIVE_INFINITY;

//  @API(help = "Number of samples to train with for improved stability", filter = Default.class, lmin = 0, json = true)
//  public long warmup_samples = 0l;

  @API(help = "Number of training set samples for scoring (0 for all)", filter = Default.class, lmin = 0, json = false)
  public long score_training = 1000l;

  @API(help = "Number of validation set samples for scoring (0 for all)", filter = Default.class, lmin = 0, json = false)
  public long score_validation = 0l;

  @API(help = "Minimum interval (in seconds) between scoring", filter = Default.class, dmin = 0, json = false)
  public double score_interval = 2;

  @API(help = "Enable diagnostics for hidden layers", filter = Default.class, json = false)
  public boolean diagnostics = true;

  public enum InitialWeightDistribution {
    UniformAdaptive, Uniform, Normal
  }

  /**
   * Activation functions
   * Tanh, Rectifier and RectifierWithDropout have been tested.  TanhWithDropout and Maxout are experimental.
   */
  public enum Activation {
    Tanh, TanhWithDropout, Rectifier, RectifierWithDropout, Maxout
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
    if (arg._name.equals("classification")) {
      classification = true;
      arg.disable("Regression is not currently supported.");
    }
    if (arg._name.equals("ignored_cols")) arg.disable("Not currently supported.");
    if (arg._name.equals("input_dropout_ratio") &&
            (activation != Activation.RectifierWithDropout && activation != Activation.TanhWithDropout)
            ) {
      arg.disable("Only with Dropout.", inputArgs);
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
    if (arg._name.equals("score_validation") && validation == null) {
      arg.disable("Only if a validation set is specified.");
    }
    if (arg._name.equals("loss") || arg._name.equals("max_w2") || arg._name.equals("warmup_samples")
            || arg._name.equals("score_training") || arg._name.equals("score_validation")
            || arg._name.equals("initial_weight_distribution") || arg._name.equals("initial_weight_scale")
            || arg._name.equals("score_interval") || arg._name.equals("diagnostics")
            || arg._name.equals("rate_decay")
            ) {
      if (!expert_mode)  arg.disable("Only in expert mode.");
    }
  }

  public Frame score( Frame fr ) { return ((NNModel)UKV.get(dest())).score(fr);  }

  @Override public Key defaultDestKey(){return null;}
  @Override public Key defaultJobKey() {return null;}

  @Override public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("description: " + description);
    sb.append("\nActivation function: " + activation.toString());
    sb.append("\nInput layer dropout ratio: " + input_dropout_ratio);
    String h = "" + hidden[0];
    for (int i=1; i<hidden.length; ++i) h += ", " + hidden[i];
    sb.append("\nHidden layer sizes: " + h);
    sb.append("\nLearning rate: " + rate);
    sb.append("\nLearning rate annealing: " + rate_annealing);
    sb.append("\nL1 regularization: " + l1);
    sb.append("\nL2 regularization: " + l2);
    sb.append("\nInitial momentum at the beginning of training: " + momentum_start);
    sb.append("\nNumber of training samples for which momentum increases: " + momentum_ramp);
    sb.append("\nFinal momentum after the ramp is over: " + momentum_stable);
    sb.append("\nNumber of epochs: " + epochs);
    sb.append("\nSeed for random numbers: " + seed);
//    sb.append("\nEnable expert mode: ", expert_mode);
    sb.append("\nInitial weight distribution: " + initial_weight_distribution);
    sb.append("\nInitial weight scale: " + initial_weight_scale);
    sb.append("\nLoss function: " + loss.toString());
    sb.append("\nLearning rate decay factor: " + rate_decay);
    sb.append("\nConstraint for squared sum of incoming weights per unit: " + max_w2);
    sb.append("\nNumber of training set samples for scoring: " + score_training);
    sb.append("\nNumber of validation set samples for scoring: " + score_validation);
//    sb.append("\nMinimum interval (in seconds) between scoring: " + score_interval);
//    sb.append("\nEnable diagnostics for hidden layers: " + diagnostics);
    return sb.toString();
  }

  @Override protected void logStart() {
    Log.info("Starting Neural Net model build...");
    super.logStart();
    for (String s : this.toString().split("\n")) Log.info(s);
  }

  /** Return the query link to this page */
  public static String link(Key k, String content) {
    RString rs = new RString("<a href='NN.query?source=%$key'>%content</a>");
    rs.replace("key", k.toString());
    rs.replace("content", content);
    return rs.toString();
  }


  @Override protected Response serve() {
    init();
    Frame fr = DataInfo.prepareFrame(source, response, ignored_cols, true);
    _dinfo = new DataInfo(fr, 1, true);
    if(destination_key == null)destination_key = Key.make("NNModel_"+Key.make());
    if(job_key == null)job_key = Key.make("NNJob_"+Key.make());
    fork();
    return NNModelView.redirect(this, dest());
  }

  @Override public float progress(){
    if(DKV.get(dest()) == null)return 0;
    NNModel m = DKV.get(dest()).get();
    return (float)(m.epoch_counter / m.model_info.get_params().epochs);
  }

  @Override protected Status exec() {
    initModel();
    trainModel(true);
    if( _gen_enum ) UKV.remove(response._key);
    remove();
    return Status.Done;
  }

  @Override protected Response redirect() {
    return NNProgressPage.redirect(this, self(), dest());
  }

  public void initModel() {
    logStart();
    NN.RNG.seed.set(seed);
    NNModel model = new NNModel(dest(), self(), _dinfo, this);
    model.delete_and_lock(self());
  }

  public void trainModel(boolean scorewhiletraining){
    NNModel model = UKV.get(dest());
    NNModel.NNModelInfo input = model.model_info;
    final Frame[] adapted = validation == null ? null : model.adapt(validation, false);
    for (int epoch = 1; epoch <= epochs; ++epoch) {
      boolean training = true;
      // train for one epoch, starting with weights/biases from modelinfo
      NNTask nntask = new NNTask(this, _dinfo, this, input, training).doAll(_dinfo._adaptedFrame);
      // take the result, update the model and use as input for next epoch
      model.model_info = nntask._output;
      input = model.model_info;
      if (diagnostics) input.computeDiagnostics(); //compute diagnostics on modelinfo here after global reduction (all have the same data)
      final String label =  (validation == null ? "Training" : "Validation")
              + " error after training for " + epoch
              + " epochs (" + model.model_info.processed() + " samples):";
      if (scorewhiletraining)
        doScoring(model, validation == null ? _dinfo._adaptedFrame : adapted[0], label, epoch==epochs);
      model.epoch_counter = epoch;
      model.run_time = (System.currentTimeMillis()-model.start_time);
//      System.out.println(model);
      model.update(self());
    }
    if (adapted != null) adapted[1].delete();
    model.unlock(self());
    System.out.println("Job finished.\n\n");
  }

  transient long _timeLastScoreStart, _timeLastScoreEnd, _firstScore;
  void doScoring(NNModel model, Frame ftest, String label, boolean force) {
    long now = System.currentTimeMillis();
    if( _firstScore == 0 ) _firstScore=now;
    long sinceLastScore = now-_timeLastScoreStart;
//    Score sc = null;
    if( force ||
            (now-_firstScore < 4000) || // Score every time for 4 secs
            // Throttle scoring to keep the cost sane; limit to a 10% duty cycle & every 4 secs
            (sinceLastScore > 4000 && // Limit scoring updates to every 4sec
            (double)(_timeLastScoreEnd-_timeLastScoreStart)/sinceLastScore < 0.1) ) { // 10% duty cycle
      _timeLastScoreStart = now;
      double error = model.classificationError(ftest, label, true);
      _timeLastScoreEnd = System.currentTimeMillis();
    }
  }

  // Expand grid search related argument sets
  @Override protected NanoHTTPD.Response serveGrid(NanoHTTPD server, Properties parms, RequestType type) {
    return superServeGrid(server, parms, type);
  }

  // Make a differently seeded random generator every time someone asks for one
  public static class RNG {
    // Atomicity is not really needed here (since in multi-threaded operation, the weights are simultaneously updated),
    // but it is still done for posterity since it's cheap (and to be able to count the number of actual getRNG() calls)
    public static AtomicLong seed = new AtomicLong(new Random().nextLong());

    public static Random getRNG() {
      return water.util.Utils.getDeterRNG(seed.getAndIncrement());
    }
  }

}
