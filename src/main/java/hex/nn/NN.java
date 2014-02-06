package hex.nn;

import hex.FrameTask.DataInfo;
import water.*;
import water.api.*;
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

  @API(help = "Initial momentum at the beginning of training", filter = Default.class, dmin = 0, dmax = 0.9999999999, json = true)
  public double momentum_start = .5;

  @API(help = "Number of training samples for which momentum increases", filter = Default.class, lmin = 0, json = true)
  public long momentum_ramp = 1000000;

  @API(help = "Final momentum after the ramp is over", filter = Default.class, dmin = 0, dmax = 0.9999999999, json = true)
  public double momentum_stable = 0.99;

  @API(help = "How many times the dataset should be iterated (streamed), can be less than 1.0", filter = Default.class, dmin = 0, json = true)
  public double epochs = 10;

  @API(help = "Seed for random numbers (reproducible results for single-threaded only, cf. Hogwild)", filter = Default.class, json = true)
  public long seed = new Random().nextLong();

  @API(help = "Enable expert mode", filter = Default.class, json = true)
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

  @API(help = "Number of training set samples for scoring (0 for all)", filter = Default.class, lmin = 0, json = true)
  public long score_training = 1000l;

  @API(help = "Number of validation set samples for scoring (0 for all)", filter = Default.class, lmin = 0, json = true)
  public long score_validation = 0l;

  @API(help = "Minimum interval (in seconds) between scoring", filter = Default.class, dmin = 0, json = true)
  public double score_interval = 2;

  @API(help = "Enable diagnostics for hidden layers", filter = Default.class, json = true)
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

  @Override public boolean toHTML(StringBuilder sb) {
    return makeJsonBox(sb);
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
    return (float)(m.epoch_counter / m.model_info().get_params().epochs);
  }

  @Override
  public Status exec() {
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
    NNModel model = new NNModel(dest(), self(), source._key, _dinfo, this);
    model.delete_and_lock(self());
  }

  public void trainModel(boolean scorewhiletraining){
    final NNModel model = UKV.get(dest());
    final Frame[] adapted = validation == null ? null : model.adapt(validation, false);

    // Optionally downsample data for scoring
    final FrameSplit split = new FrameSplit();
    Frame trainScoreFrame = _dinfo._adaptedFrame;
    double fraction = score_training > 0 ? (double)score_training / trainScoreFrame.numRows() : 1.0;
    fraction = Math.max(Math.min(fraction, 1), 0);
    if (fraction < 1 && fraction > 0) {
      trainScoreFrame = split.splitFrame(trainScoreFrame, new double[]{fraction, 1-fraction}, seed)[0];
      if (trainScoreFrame.numRows() == 0) trainScoreFrame = _dinfo._adaptedFrame;
    }
    Log.info("Scoring on " + trainScoreFrame.numRows() + " rows of training data.");
    Frame validScoreFrame = validation == null ? null : adapted[0];
    if (validScoreFrame != null) {
      fraction = score_validation > 0 ? (double)score_validation / validScoreFrame.numRows() : 1.0;
      fraction = Math.max(Math.min(fraction, 1), 0);
      if (fraction < 1 && fraction > 0) {
        validScoreFrame = split.splitFrame(validScoreFrame, new double[]{fraction, 1-fraction}, seed)[0];
      }
      if (validScoreFrame.numRows() == 0) validScoreFrame = adapted[0];
      Log.info("Scoring on " + validScoreFrame.numRows() + " rows of validation data.");
    }

    for (int epoch = 1; epoch <= epochs; ++epoch) {
      final NNTask nntask = new NNTask(this, _dinfo, model.model_info(), true).doAll(_dinfo._adaptedFrame);
      model.set_model_info(nntask.model_info());
      if (diagnostics) model.computeDiagnostics();
      model.epoch_counter = epoch;
      if (scorewhiletraining) {
        final String label = "Classification error after training for " + epoch
                + " epochs (" + model.model_info().get_processed() + " samples)";
        doScoring(model, trainScoreFrame, validScoreFrame, label, epoch==epochs, score_interval);
      }
//      System.out.println(model);
      model.update(self());
      if (model.model_info().unstable()) {
        Log.info("Model is unstable (Exponential growth). Aborting." +
                " Try using L1/L2/max_w2 regularization or a different activation function.");
        break;
      }
    }
    if (adapted != null) adapted[1].delete();
    model.unlock(self());
    if (validScoreFrame != null) validScoreFrame.delete();
    Log.info("NN training finished.");
  }

  transient long _timeLastScoreStart, _timeLastScoreEnd, _firstScore;
  void doScoring(NNModel model, Frame ftrain, Frame ftest, String label, boolean force, double score_interval) {
    long now = System.currentTimeMillis();
    if( _firstScore == 0 ) _firstScore=now;
    long sinceLastScore = now-_timeLastScoreStart;
//    Score sc = null;
    if( force ||
            (now-_firstScore < 4000) || // Score every time for 4 secs
            // Throttle scoring to keep the cost sane; limit to a 10% duty cycle & every 4 secs
            (sinceLastScore > score_interval*1000 && // Limit scoring updates
            (double)(_timeLastScoreEnd-_timeLastScoreStart)/sinceLastScore < 0.1) ) { // 10% duty cycle
      _timeLastScoreStart = now;
      model.classificationError(ftrain, label + "\nOn training data:", true);
      if (ftest != null)
        model.classificationError(ftest, label + "\nOn validation data:", true);
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
