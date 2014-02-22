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
import water.util.RString;

import java.util.Random;

import static water.util.MRUtils.sampleFrame;

public class NN extends Job.ValidatedJob {
  static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
  public static DocGen.FieldDoc[] DOC_FIELDS;
  public static final String DOC_GET = "Neural Network 2";

  public DataInfo _dinfo;

  @API(help = "Activation function", filter = Default.class, json = true)
  public Activation activation = Activation.Tanh;

  @API(help = "Input layer dropout ratio", filter = Default.class, dmin = 0, dmax = 1, json = true)
  public double input_dropout_ratio = 0.0;

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
  public double epochs = 1000;

  @API(help = "Seed for random numbers (reproducible results for single-threaded only, cf. Hogwild)", filter = Default.class, json = true)
  public long seed = new Random().nextLong();

  @API(help = "Enable expert mode", filter = Default.class, json = true, gridable = false)
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
  public long score_training_samples = 10000l;

  @API(help = "Number of validation set samples for scoring (0 for all)", filter = Default.class, lmin = 0, json = true)
  public long score_validation_samples = 0l;

  @API(help = "Shortest interval (in seconds) between scoring", filter = Default.class, dmin = 0, json = true)
  public double score_interval = 2;

  @API(help = "Number of training set samples between multi-node synchronization (0 for all).", filter = Default.class, lmin = 0, json = true)
  public long sync_samples = 10000l;

  @API(help = "Enable diagnostics for hidden layers", filter = Default.class, json = true, gridable = false)
  public boolean diagnostics = true;

  @API(help = "Enable fast mode (minor approximation in back-propagation)", filter = Default.class, json = true)
  public boolean fast_mode = true;

  @API(help = "Ignore constant training columns", filter = Default.class, json = true)
  public boolean ignore_const_cols = true;

  @API(help = "Enable periodic shuffling of training data (can increase stochastic gradient descent performance)", filter = Default.class, json = true)
  public boolean shuffle_training_data = false;

  @API(help = "Use Nesterov accelerated gradient (recommended)", filter = Default.class, json = true)
  public boolean nesterov_accelerated_gradient = true;

  @API(help = "Stopping criterion for classification error fraction (negative number to disable)", filter = Default.class, json = true, gridable = false)
  public double classification_stop = 0;

  @API(help = "Stopping criterion for regression error (negative number to disable)", filter = Default.class, json = true, gridable = false)
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
    if(arg._name.equals("regression_stop") && classification) {
      arg.disable("Only for regression.", inputArgs);
    }
    if (arg._name.equals("score_validation_samples") && validation == null) {
      arg.disable("Only if a validation set is specified.", inputArgs);
    }
    if (arg._name.equals("sync_samples") && H2O.CLOUD.size() == 1) {
      sync_samples = 0; //sync once per epoch on a single node
      arg.disable("Only for multi-node operation.", inputArgs);
    }
    if (arg._name.equals("loss") || arg._name.equals("max_w2") || arg._name.equals("warmup_samples")
            || arg._name.equals("score_training_samples")
            || arg._name.equals("score_validation_samples")
            || arg._name.equals("initial_weight_distribution")
            || arg._name.equals("initial_weight_scale")
            || arg._name.equals("score_interval")
            || arg._name.equals("diagnostics")
            || arg._name.equals("rate_decay")
            || arg._name.equals("sync_samples")
            || arg._name.equals("fast_mode")
            || arg._name.equals("ignore_const_cols")
            || arg._name.equals("shuffle_training_data")
            || arg._name.equals("nesterov_accelerated_gradient") || arg._name.equals("classification_stop")
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
      return (float)(m.epoch_counter / m.model_info().get_params().epochs);
    return 0;
  }

  @Override public JobState exec() {
    buildModel(initModel());
    return JobState.DONE;
  }

  @Override protected Response redirect() {
    return NNProgressPage.redirect(this, self(), dest());
  }

  private boolean _fakejob;
  void checkParams() {
    if(!classification && loss != Loss.MeanSquare) {
      Log.warn("Setting loss to MeanSquare for regression.");
      loss = Loss.MeanSquare;
    }
    if (H2O.CLOUD.size() == 1 && sync_samples != 0) {
      Log.warn("Setting sync_samples to 0 for single-node operation.");
      sync_samples = 0;
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

  public NNModel initModel() {
    checkParams();
    lock();
    if (_dinfo == null)
      _dinfo = new FrameTask.DataInfo(FrameTask.DataInfo.prepareFrame(source, response, ignored_cols, classification, ignore_const_cols), 1, true, !classification);
    NNModel model = new NNModel(dest(), self(), source._key, _dinfo, this);
    model.model_info().initializeMembers();
    //Log.info("Initial model:\n" + model.model_info());
    unlock();
    return model;
  }

  public NNModel buildModel(NNModel model) {
    logStart();
    lock();
    Log.info("Number of chunks of the training data: " + source.anyVec().nChunks());
    if (validation != null)
      Log.info("Number of chunks of the validation data: " + validation.anyVec().nChunks());
    if (model == null) model = UKV.get(dest());

    final long model_size = model.model_info().size();
    Log.info("Number of model parameters (weights/biases): " + String.format("%,d", model_size));
    Log.info("Memory usage of the model: " + String.format("%.2f", (double)model_size*Float.SIZE / (1<<23)) + " MB.");

    model.delete_and_lock(self()); //claim ownership of the model

    final Frame[] valid_adapted = validation == null ? null : model.adapt(validation, false);
    Frame train = _dinfo._adaptedFrame;
    Frame valid = validation == null ? null : valid_adapted[0];

    // Optionally downsample data for scoring
    Frame trainScoreFrame = sampleFrame(train, score_training_samples, seed);
    Frame validScoreFrame = sampleFrame(valid, score_validation_samples, seed+1);

    if (sync_samples > train.numRows()) {
      Log.warn("Setting sync_samples (" + sync_samples
              + ") to the number of rows of the training data (" + (sync_samples=train.numRows()) + ").");
    }
    // determines the number of rows processed during NNTask, affects synchronization (happens at the end of each NNTask)
    final float sync_fraction = sync_samples == 0l ? 1.0f : (float)sync_samples / train.numRows();

    Log.info("Starting to train the Neural Net model.");
    long timeStart = System.currentTimeMillis();
    //main loop
    do {
//      shuffle();
      NNTask nntask = new NNTask(_dinfo, model.model_info(), true, sync_fraction, shuffle_training_data).doAll(train);
      model.set_model_info(nntask.model_info());
    } while (model.doDiagnostics(trainScoreFrame, validScoreFrame, timeStart, self()));

    model.unlock(self()); //release model ownership

    //delete temporary frames
    if (validScoreFrame != null && validScoreFrame != valid) validScoreFrame.delete();
    if (trainScoreFrame != null && trainScoreFrame != train) trainScoreFrame.delete();
    if (validation != null) valid_adapted[1].delete(); //just deleted the adapted frames for validation
//    if (_newsource != null && _newsource != source) _newsource.delete();

    unlock();
    delete();

    Log.info("Finished training the Neural Net model.");
    return model;
  }

  private void lock() {
    // Lock the input datasets against deletes
    source.read_lock(self());
    if( validation != null && source._key != null && validation._key !=null && !source._key.equals(validation._key) )
      validation.read_lock(self());
  }

  private void unlock() {
    source.unlock(self());
    if( validation != null && !source._key.equals(validation._key) )
      validation.unlock(self());
  }

  private void delete() {
    if (_fakejob) UKV.remove(job_key);
    remove();
  }

  /*
  long _iter = 0;
  Frame _newsource = null;
  private void shuffle() {
    if (!shuffle_training_data) return;
    Log.info("Shuffling.");
    _newsource = shuffleAndBalance(source, seed+_iter++, shuffle_training_data);
    Vec resp = _newsource.vecs()[resp_pos];
    _dinfo = new FrameTask.DataInfo(FrameTask.DataInfo.prepareFrame(_newsource, resp, ignored_cols, true, ignore_const_cols), 1, true);
    Log.info("Shuffling done.");
  }

  // master node collects all rows, and distributes them across the cluster - slow
  private static Frame shuffleAndBalance(Frame fr, long seed, final boolean shuffle) {
    int cores = 0;
    for( H2ONode node : H2O.CLOUD._memary )
      cores += node._heartbeat._num_cpus;
    final int splits = 4*cores;

    long[] idx = null;
    if (shuffle) {
      idx = new long[(int)fr.numRows()]; //HACK: int instead of of long
      for (int r=0; r<idx.length; ++r) idx[r] = r;
      Utils.shuffleArray(idx, seed);
    }

    Vec[] vecs = fr.vecs();
    if( vecs[0].nChunks() < splits || shuffle ) {
      Key keys[] = new Vec.VectorGroup().addVecs(vecs.length);
      for( int v = 0; v < vecs.length; v++ ) {
        AppendableVec vec = new AppendableVec(keys[v]);
        final long rows = fr.numRows();
        for( int split = 0; split < splits; split++ ) {
          long off = rows * split / splits;
          long lim = rows * (split + 1) / splits;
          NewChunk chunk = new NewChunk(vec, split);
          for( long r = off; r < lim; r++ ) {
            if (shuffle) chunk.addNum(fr.vecs()[v].at(idx[(int)r]));
            else chunk.addNum(fr.vecs()[v].at(r));
          }
          chunk.close(split, null);
        }
        Vec t = vec.close(null);
        t._domain = vecs[v]._domain;
        vecs[v] = t;
      }
    }
    return new Frame(fr.names(), vecs);
  }
  */

}
