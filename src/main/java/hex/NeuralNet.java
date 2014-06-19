package hex;

import static hex.NeuralNet.ExecutionMode.*;
import hex.Layer.*;
import water.*;
import water.H2O.H2OCountedCompleter;
import water.Job.ValidatedJob;
import water.api.DocGen;
import water.api.NeuralNetProgressPage;
import water.api.RequestServer;
import water.fvec.*;
import water.util.*;

import java.util.Arrays;
import java.util.Random;


/**
* Neural network.
*
* @author cypof
*/
public class NeuralNet extends ValidatedJob {
  static final int API_WEAVER = 1;
  public static DocGen.FieldDoc[] DOC_FIELDS;
  public static final String DOC_GET = "Neural Network";

  @API(help = "Execution Mode", filter = Default.class, json = true)
  public ExecutionMode mode = ExecutionMode.SingleNode;

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

  @API(help = "Initial momentum at the beginning of training", filter = Default.class, dmin = 0, json = true)
  public double momentum_start = .5;

  @API(help = "Number of training samples for which momentum increases", filter = Default.class, lmin = 0, json = true)
  public long momentum_ramp = 1000000;

  @API(help = "Final momentum after the ramp is over", filter = Default.class, dmin = 0, json = true)
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

  @API(help = "Number of samples to train with non-distributed mode for improved stability", filter = Default.class, lmin = 0, json = true)
  public long warmup_samples = 0l;

  @API(help = "Number of training set samples for scoring (0 for all)", filter = Default.class, lmin = 0, json = true)
  public long score_training = 1000l;

  @API(help = "Number of validation set samples for scoring (0 for all)", filter = Default.class, lmin = 0, json = true)
  public long score_validation = 0l;

  @API(help = "Minimum interval (in seconds) between scoring", filter = Default.class, dmin = 0, json = true)
  public double score_interval = 2;

  @API(help = "Enable diagnostics for hidden layers", filter = Default.class, json = true)
  public boolean diagnostics = true;

  @API(help = "Enable fast mode (minor approximation in back-propagation)", filter = Default.class, json = true)
  public boolean fast_mode = true;

  @Override public boolean toHTML(StringBuilder sb) {
    return makeJsonBox(sb);
  }

  @Override
  protected void registered(RequestServer.API_VERSION ver) {
    super.registered(ver);
    for (Argument arg : _arguments) {
      if ( arg._name.equals("activation") || arg._name.equals("initial_weight_distribution")
              || arg._name.equals("mode") || arg._name.equals("expert_mode")) {
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
    if(arg._name.equals("initial_weight_scale") &&
            (initial_weight_distribution == InitialWeightDistribution.UniformAdaptive)
            ) {
      arg.disable("Using sqrt(6 / (# units + # units of previous layer)) for Uniform distribution.", inputArgs);
    }
    if( arg._name.equals("mode") ) {
      if (H2O.CLOUD._memary.length > 1) {
        //TODO: re-enable this
// arg.disable("Using MapReduce since cluster size > 1.", inputArgs);
// mode = ExecutionMode.MapReduce;
        //Temporary solution
        if (mode == ExecutionMode.MapReduce) {
          arg.disable("Distributed MapReduce mode is not yet fully supported. Will run in single-node mode, wasting "
                  + (H2O.CLOUD._memary.length - 1) + " cluster node(s).", inputArgs);
          mode = ExecutionMode.SingleNode;
        }
      }
    }
    if( arg._name.equals("warmup_samples") && mode == MapReduce && H2O.CLOUD._memary.length > 1) {
      arg.disable("Not yet implemented for distributed MapReduce execution modes, using a value of 0.");
      warmup_samples = 0;
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


  public enum ExecutionMode {
    SingleThread, SingleNode, MapReduce
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

  // Hack: used to stop the monitor thread
  public static volatile boolean running = true;

  public NeuralNet() {
    description = DOC_GET;
  }

  @Override public final void execImpl() {
    startTrain();
  }

  void startTrain() {
    logStart();
    running = true;
// Vec[] vecs = Utils.append(_train, response);
// reChunk(vecs);
// final Vec[] train = new Vec[vecs.length - 1];
// System.arraycopy(vecs, 0, train, 0, train.length);
// final Vec trainResp = classification ? vecs[vecs.length - 1].toEnum() : vecs[vecs.length - 1];

    final Vec[] train = _train;
    final Vec trainResp = classification ? response.toEnum() : response;

    final Layer[] ls = new Layer[hidden.length + 2];
    ls[0] = new VecsInput(train, null);
    for( int i = 0; i < hidden.length; i++ ) {
      switch( activation ) {
        case Tanh:
          ls[i + 1] = new Tanh(hidden[i]);
          break;
        case TanhWithDropout:
          ls[i + 1] = new TanhDropout(hidden[i]);
          break;
        case Rectifier:
          ls[i + 1] = new Rectifier(hidden[i]);
          break;
        case RectifierWithDropout:
          ls[i + 1] = new RectifierDropout(hidden[i]);
          break;
        case Maxout:
          ls[i + 1] = new Maxout(hidden[i]);
          break;
        case MaxoutWithDropout:
          ls[i + 1] = new MaxoutDropout(hidden[i]);
          break;
      }
    }

    if( classification )
      ls[ls.length - 1] = new VecSoftmax(trainResp, null);
    else
      ls[ls.length - 1] = new VecLinear(trainResp, null);

    //copy parameters from NeuralNet, and set previous/input layer links
    for( int i = 0; i < ls.length; i++ )
      ls[i].init(ls, i, this);

    final Key sourceKey = Key.make(input("source"));
    final Frame frame = new Frame(_names, train);
    frame.add(_responseName, trainResp);
    final Errors[] trainErrors0 = new Errors[] { new Errors() };
    final Errors[] validErrors0 = validation == null ? null : new Errors[] { new Errors() };

    NeuralNetModel model = new NeuralNetModel(destination_key, sourceKey, frame, ls, this);
    model.training_errors = trainErrors0;
    model.validation_errors = validErrors0;

    model.delete_and_lock(self());

    final Frame[] adapted = validation == null ? null : model.adapt(validation, false);
    final Trainer trainer;

    final long num_rows = source.numRows();

    if (mode == SingleThread) {
      Log.info("Entering single-threaded execution mode");
      trainer = new Trainer.Direct(ls, epochs, self());
    } else {
      // one node works on the first batch of points serially for improved stability
      if (warmup_samples > 0) {
        Log.info("Training the first " + warmup_samples + " samples in serial for improved stability.");
        Trainer warmup = new Trainer.Direct(ls, (double)warmup_samples/num_rows, self());
        warmup.start();
        warmup.join();
        //TODO: for MapReduce send weights from master VM to all other VMs
      }
      if (mode == SingleNode) {
        Log.info("Entering single-node (multi-threaded Hogwild) execution mode.");
        trainer = new Trainer.Threaded(ls, epochs, self(), -1);
      } else if (mode == MapReduce) {
        if (warmup_samples > 0 && mode == MapReduce) {
          Log.info("Multi-threaded warmup with " + warmup_samples + " samples.");
          Trainer warmup = new Trainer.Threaded(ls, (double)warmup_samples/num_rows, self(), -1);
          warmup.start();
          warmup.join();
          //TODO: for MapReduce send weights from master VM to all other VMs
        }
        Log.info("Entering multi-node (MapReduce + multi-threaded Hogwild) execution mode.");
        trainer = new Trainer.MapReduce(ls, epochs, self());
      } else throw new RuntimeException("invalid execution mode.");
    }

    Log.info("Running for " + epochs + " epochs.");

    final NeuralNet nn = this;

    // Use a separate thread for monitoring (blocked most of the time)
    Thread monitor = new Thread() {
      Errors[] trainErrors = trainErrors0, validErrors = validErrors0;

      @Override public void run() {
        try {
          Vec[] valid = null;
          Vec validResp = null;
          if( validation != null ) {
            assert adapted != null;
            final Vec[] vs = adapted[0].vecs();
            valid = Arrays.copyOf(vs, vs.length - 1);
            System.arraycopy(adapted[0].vecs(), 0, valid, 0, valid.length);
            validResp = vs[vs.length - 1];
          }
          //score the model every 2 seconds (or less often, if it takes longer to score)
          final long num_samples_total = (long)(Math.ceil(num_rows * epochs));
          long num = -1, last_eval = runTimeMs();
          do {
            final long interval = (long)(score_interval * 1000); //time between evaluations
            long time_taken = runTimeMs() - last_eval;
            if (num >= 0 && time_taken < interval) {
              Thread.sleep(interval - time_taken);
            }
            last_eval = runTimeMs();
            num = eval(valid, validResp);

            if (num >= num_samples_total) break;
            if (mode != MapReduce) {
              if (!isRunning(self()) || !running) break;
            } else {
              if (!running) break; //MapReduce calls cancel() early, we are waiting for running = false
            }
          } while (true);

          // remove validation data
          if( adapted != null && adapted[1] != null )
            adapted[1].delete();
          Log.info("Training finished.");
        } catch( Exception ex ) {
          cancel(ex);
        }
      }

      private long eval(Vec[] valid, Vec validResp) {
        long[][] cm = null;
        if( classification ) {
          int classes = ls[ls.length - 1].units;
          cm = new long[classes][classes];
        }
        NeuralNetModel model = new NeuralNetModel(destination_key, sourceKey, frame, ls, nn);

        // score model on training set
        Errors e = eval(train, trainResp, score_training, valid == null ? cm : null);
        e.score_training = score_training == 0 ? train[0].length() : score_training;
        trainErrors = Utils.append(trainErrors, e);
        model.unstable |= Double.isNaN(e.mean_square) || Double.isNaN(e.cross_entropy);
        model.training_errors = trainErrors;

        // score model on validation set
        if( valid != null ) {
          e = eval(valid, validResp, score_validation, cm);
          e.score_validation = score_validation == 0 ? valid[0].length() : score_validation;
          validErrors = Utils.append(validErrors, e);
          model.unstable |= Double.isNaN(e.mean_square) || Double.isNaN(e.cross_entropy);
        }
        model.validation_errors = validErrors;

        model.confusion_matrix = cm;
        model.update(self());

        // terminate model building if we detect that a model is unstable
        if (model.unstable) NeuralNet.running = false;

        return e.training_samples;
      }

      private Errors eval(Vec[] vecs, Vec resp, long n, long[][] cm) {
        Errors e = NeuralNet.eval(ls, vecs, resp, n, cm);
        e.training_samples = trainer.processed();
        e.training_time_ms = runTimeMs();
        return e;
      }
    };
    trainer.start();
    monitor.start();
    trainer.join();

    // Gracefully terminate the job submitted via H2O web API
    if (mode != MapReduce) {
      running = false; //tell the monitor thread to finish too
      try {
        monitor.join();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    } else {
      while (running) { //MapReduce will inform us that running = false
        try {
          Thread.sleep(1);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }

    // remove this job -> stop H2O interface from refreshing
    H2OCountedCompleter task = _fjtask;
    if( task != null )
      task.tryComplete();
    this.remove();

  }

  @Override public float progress() {
    NeuralNetModel model = UKV.get(destination_key);
    if( model != null && source != null) {
      Errors e = model.training_errors[model.training_errors.length - 1];
      return Math.min(1f, 0.1f + Math.min(1, e.training_samples / (float) (epochs * source.numRows())));
    }
    return 0;
  }

  public static Errors eval(Layer[] ls, Vec[] vecs, Vec resp, long n, long[][] cm) {
    Output output = (Output) ls[ls.length - 1];
    if( output instanceof VecSoftmax )
      output = new VecSoftmax(resp, (VecSoftmax) output);
    else
      output = new VecLinear(resp, (VecLinear) output);
    return eval(ls, new VecsInput(vecs, (VecsInput) ls[0]), output, n, cm);
  }

  private static Errors eval(Layer[] ls, Input input, Output output, long n, long[][] cm) {
    Layer[] clones = new Layer[ls.length];
    clones[0] = input;
    for( int y = 1; y < clones.length - 1; y++ )
      clones[y] = ls[y].clone();
    clones[clones.length - 1] = output;
    for( int y = 0; y < clones.length; y++ )
      clones[y].init(clones, y, false);
    Layer.shareWeights(ls, clones);
    return eval(clones, n, cm);
  }

  public static Errors eval(Layer[] ls, long n, long[][] cm) {
    Errors e = new Errors();
    Input input = (Input) ls[0];
    long len = input._len;

    // TODO: choose random subset instead of first n points (do this once per run)
    if( n != 0 )
      len = Math.min(len, n);

    // classification
    if( ls[ls.length - 1] instanceof Softmax ) {
      int correct = 0;
      e.mean_square = 0;
      e.cross_entropy = 0;
      for( input._pos = 0; input._pos < len; input._pos++ ) {
        if( ((Softmax) ls[ls.length - 1]).target() == Layer.missing_int_value ) //NA
          continue;
        if( correct(ls, e, cm) )
          correct++;
      }
      e.classification = (len - (double) correct) / len;
      e.mean_square /= len;
      e.cross_entropy /= len; //want to report the averaged cross-entropy
    }
    // regression
    else {
      e.mean_square = 0;
      for( input._pos = 0; input._pos < len; input._pos++ )
        if( ls[ls.length - 1]._a[0] != Layer.missing_float_value )
          error(ls, e);
      e.classification = Double.POSITIVE_INFINITY;
      e.mean_square /= len;
    }
    input._pos = 0;
    return e;
  }

  // classification scoring
  static boolean correct(Layer[] ls, Errors e, long[][] confusion) {
    Softmax output = (Softmax) ls[ls.length - 1];
    if( output.target() == -1 )
      return false;
    for (Layer l : ls) l.fprop(-1, false);
    float[] out = ls[ls.length - 1]._a;
    int target = output.target();
    for( int o = 0; o < out.length; o++ ) {
      final boolean hitpos = (o == target);
      final double t = hitpos ? 1 : 0;
      final double d = t - out[o];
      e.mean_square += d * d;
      e.cross_entropy += hitpos ? -Math.log(out[o]) : 0;
    }
    float[] preds = new float[out.length+1];
    for (int i=0;i<out.length;++i) preds[i+1] = out[i];
    double[] data = new double[ls[0]._a.length];
    preds[0] = ModelUtils.getPrediction(preds, data);
    if( confusion != null ) {
      if (output.target() != Layer.missing_int_value) confusion[output.target()][(int)preds[0]]++;
    }
    return preds[0] == output.target();
  }

  // regression scoring
  static void error(Layer[] ls, Errors e) {
    Linear linear = (Linear) ls[ls.length - 1];
    for (Layer l : ls) l.fprop(-1, false);
    float[] output = ls[ls.length - 1]._a;
    float[] target = linear.target();
    e.mean_square = 0;
    for( int o = 0; o < output.length; o++ ) {
      final double d = target[o] - output[o];
      e.mean_square += d * d;
    }
  }

  @Override protected Response redirect() {
    return NeuralNetProgressPage.redirect(this, self(), dest());
  }

  public static String link(Key k, String content) {
    NeuralNet req = new NeuralNet();
    RString rs = new RString("<a href='" + req.href() + ".query?%key_param=%$key'>%content</a>");
    rs.replace("key_param", "source");
    rs.replace("key", k.toString());
    rs.replace("content", content);
    return rs.toString();
  }

  @Override public String speedDescription() {
    return "time/epoch";
  }

  @Override public long speedValue() {
    Value value = DKV.get(dest());
    NeuralNetModel m = value != null ? (NeuralNetModel) value.get() : null;
    long sv = 0;
    if( m != null ) {
      Errors[] e = m.training_errors;
      double epochsSoFar = e[e.length - 1].training_samples / (double) source.numRows();
      sv = (epochsSoFar <= 0) ? 0 : (long) (e[e.length - 1].training_time_ms / epochsSoFar);
    }
    return sv;
  }

  public static class Errors extends Iced {
    static final int API_WEAVER = 1;
    static public DocGen.FieldDoc[] DOC_FIELDS;

    @API(help = "How many rows the algorithm has processed")
    public long training_samples;

    @API(help = "How long the algorithm ran in ms")
    public long training_time_ms;

    @API(help = "Classification error")
    public double classification = 1;

    @API(help = "Mean square error")
    public double mean_square = Double.POSITIVE_INFINITY;

    @API(help = "Cross entropy")
    public double cross_entropy = Double.POSITIVE_INFINITY;

    @API(help = "Number of training set samples for scoring")
    public long score_training;

    @API(help = "Number of validation set samples for scoring")
    public long score_validation;


    @Override public String toString() {
        return String.format("%.2f", (100 * classification))
              + "% (MSE:" + String.format("%.2e", mean_square)
              + ", MCE:" + String.format("%.2e", cross_entropy)
              + ")";
    }
  }

  public static class NeuralNetModel extends Model {
    static final int API_WEAVER = 1;
    static public DocGen.FieldDoc[] DOC_FIELDS;

    @API(help = "Model parameters")
    public NeuralNet parameters;

    //@API(help = "Layers")
    public Layer[] layers;

    //@API(help = "Layer weights")
    public float[][] weights;

    //@API(help = "Layer biases")
    public float[][] biases;

    @API(help = "Errors on the training set")
    public Errors[] training_errors;

    @API(help = "Errors on the validation set")
    public Errors[] validation_errors;

    @API(help = "Confusion matrix")
    public long[][] confusion_matrix;

    @API(help = "Mean bias")
    public float[] mean_bias;

    @API(help = "RMS bias")
    public float[] rms_bias;

    @API(help = "Mean weight")
    public float[] mean_weight;

    @API(help = "RMS weight")
    public float[] rms_weight;

    @API(help = "Unstable")
    public boolean unstable = false;

    NeuralNetModel(Key selfKey, Key dataKey, Frame fr, Layer[] ls, NeuralNet p) {
      super(selfKey, dataKey, fr, /* priorClassDistribution */ null);
      parameters = p;
      layers = ls;
      weights = new float[ls.length][];
      biases = new float[ls.length][];
      for( int y = 1; y < layers.length; y++ ) {
        weights[y] = layers[y]._w;
        biases[y] = layers[y]._b;
      }

      if (parameters.diagnostics) {
        // compute stats on all nodes
        mean_bias = new float[ls.length];
        rms_bias = new float[ls.length];
        mean_weight = new float[ls.length];
        rms_weight = new float[ls.length];
        for( int y = 1; y < layers.length; y++ ) {
          final Layer l = layers[y];
          final int len = l._a.length;

          // compute mean values
          mean_bias[y] = rms_bias[y] = 0;
          mean_weight[y] = rms_weight[y] = 0;
          for(int u = 0; u < len; u++) {
            mean_bias[y] += biases[y][u];
            for( int i = 0; i < l._previous._a.length; i++ ) {
              int w = u * l._previous._a.length + i;
              mean_weight[y] += weights[y][w];
            }
          }

          mean_bias[y] /= len;
          mean_weight[y] /= len * l._previous._a.length;

          // compute rms values
          for(int u = 0; u < len; ++u) {
            final double db = biases[y][u] - mean_bias[y];
            rms_bias[y] += db * db;

            for( int i = 0; i < l._previous._a.length; i++ ) {
              int w = u * l._previous._a.length + i;
              final double dw = weights[y][w] - mean_weight[y];
              rms_weight[y] += dw * dw;
            }

          }
          rms_bias[y] = (float)Math.sqrt(rms_bias[y]/len);
          rms_weight[y] = (float)Math.sqrt(rms_weight[y]/len/l._previous._a.length);

          unstable |= Double.isNaN(mean_bias[y]) || Double.isNaN(rms_bias[y])
                  || Double.isNaN(mean_weight[y]) || Double.isNaN(rms_weight[y]);

          // Abort the run if weights or biases are unreasonably large (Note that all input values are normalized upfront)
          // This can happen with Rectifier units when L1/L2/max_w2 are all set to 0, especially when using more than 1 hidden layer.
          final double thresh = 1e10;
          unstable |= mean_bias[y] > thresh || rms_bias[y] > thresh
                  || mean_weight[y] > thresh || rms_weight[y] > thresh;
        }
      }
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      for (int i=0; i<weights.length; ++i)
        sb.append("\nweights["+i+"][]="+Arrays.toString(weights[i]));
      for (int i=0; i<biases.length; ++i)
        sb.append("\nbiases["+i+"][]="+Arrays.toString(biases[i]));

      sb.append("\n");
      return sb.toString();
    }

    public void toJavaHtml(StringBuilder sb) {
      //DocGen.HTML.title(sb, "The Java Neural Net model is not implemented yet.");
    }

    public boolean generateHTML(String title, StringBuilder sb) {
      final String mse_format = "%2.6f";
      final String cross_entropy_format = "%2.6f";

      DocGen.HTML.title(sb, title);
      DocGen.HTML.paragraph(sb, "Model Key: " + _key);
      sb.append("<div class='alert'>Actions: " + water.api.Predict.link(_key, "Score on dataset") + ", "
              + NeuralNet.link(_dataKey, "Compute new model") + "</div>");
      parameters.toHTML(sb);

      // Plot training error
      {
        float[] train_err = new float[training_errors.length];
        float[] train_samples = new float[training_errors.length];
        for (int i=0; i<train_err.length; ++i) {
          train_err[i] = (float)training_errors[i].classification;
          train_samples[i] = training_errors[i].training_samples;
        }
        new D3Plot(train_samples, train_err, "training samples", "classification error",
                "Classification Error on Training Set").generate(sb);
      }

      // Plot validation error
      if (validation_errors != null) {
        float[] valid_err = new float[validation_errors.length];
        float[] valid_samples = new float[validation_errors.length];
        for (int i=0; i<valid_err.length; ++i) {
          valid_err[i] = (float)validation_errors[i].classification;
          valid_samples[i] = validation_errors[i].training_samples;
        }
        new D3Plot(valid_samples, valid_err, "training samples", "classification error",
                "Classification Error on Validation Set").generate(sb);
      }


      final boolean classification = isClassifier();
      final String cmTitle = "Confusion Matrix" + (validation_errors == null ? " (Training Data)" : "");

      // stats for training and validation
      final Errors train = training_errors[training_errors.length - 1];
      final Errors valid = validation_errors != null ? validation_errors[validation_errors.length - 1] : null;

      if (classification) {
        DocGen.HTML.section(sb, "Training classification error: " + formatPct(train.classification));
      }
      DocGen.HTML.section(sb, "Training mean square error: " + String.format(mse_format, train.mean_square));
      if (classification) {
        DocGen.HTML.section(sb, "Training cross entropy: " + String.format(cross_entropy_format, train.cross_entropy));
        if( valid != null ) {
          DocGen.HTML.section(sb, "Validation classification error: " + formatPct(valid.classification));
        }
      }
      if( validation_errors != null ) {
        assert valid != null;
        DocGen.HTML.section(sb, "Validation mean square error: " + String.format(mse_format, valid.mean_square));
        if (classification) {
          DocGen.HTML.section(sb, "Validation mean cross entropy: " + String.format(cross_entropy_format, valid.cross_entropy));
        }
        if (valid.training_time_ms > 0)
          DocGen.HTML.section(sb, "Training speed: " + valid.training_samples * 1000 / valid.training_time_ms + " samples/s");
      }
      else {
        if (train.training_time_ms > 0)
          DocGen.HTML.section(sb, "Training speed: " + train.training_samples * 1000 / train.training_time_ms + " samples/s");
      }
      if (parameters != null && parameters.diagnostics) {
        DocGen.HTML.section(sb, "Status of Hidden and Output Layers");
        sb.append("<table class='table table-striped table-bordered table-condensed'>");
        sb.append("<tr>");
        sb.append("<th>").append("#").append("</th>");
        sb.append("<th>").append("Units").append("</th>");
        sb.append("<th>").append("Activation").append("</th>");
        sb.append("<th>").append("Rate").append("</th>");
        sb.append("<th>").append("L1").append("</th>");
        sb.append("<th>").append("L2").append("</th>");
        sb.append("<th>").append("Momentum").append("</th>");
        sb.append("<th>").append("Weight (Mean, RMS)").append("</th>");
        sb.append("<th>").append("Bias (Mean, RMS)").append("</th>");
        sb.append("</tr>");
        for (int i=1; i<layers.length; ++i) {
          sb.append("<tr>");
          sb.append("<td>").append("<b>").append(i).append("</b>").append("</td>");
          sb.append("<td>").append("<b>").append(layers[i].units).append("</b>").append("</td>");
          sb.append("<td>").append(layers[i].getClass().getSimpleName().replace("Vec","").replace("Chunk", "")).append("</td>");
          sb.append("<td>").append(String.format("%.5g", layers[i].rate(train.training_samples))).append("</td>");
          sb.append("<td>").append(layers[i].params.l1).append("</td>");
          sb.append("<td>").append(layers[i].params.l2).append("</td>");
          final String format = "%g";
          sb.append("<td>").append(layers[i].momentum(train.training_samples)).append("</td>");
          sb.append("<td>(").append(String.format(format, mean_weight[i])).
                  append(", ").append(String.format(format, rms_weight[i])).append(")</td>");
          sb.append("<td>(").append(String.format(format, mean_bias[i])).
                  append(", ").append(String.format(format, rms_bias[i])).append(")</td>");
          sb.append("</tr>");
        }
        sb.append("</table>");
      }
      if (unstable) {
        final String msg = "Job was aborted due to observed numerical instability (exponential growth)."
                + " Try a bounded activation function or regularization with L1, L2 or max_w2 and/or use a smaller learning rate or faster annealing.";
        DocGen.HTML.section(sb, "=======================================================================================");
        DocGen.HTML.section(sb, msg);
        DocGen.HTML.section(sb, "=======================================================================================");
      }
      if( confusion_matrix != null && confusion_matrix.length < 100 ) {
        assert(classification);
        String[] classes = classNames();
        NeuralNetScore.confusion(sb, cmTitle, classes, confusion_matrix);
      }
      sb.append("<h3>" + "Progress" + "</h3>");
      String training = "Number of training set samples for scoring: " + train.score_training;
      if (train.score_training > 0) {
        if (train.score_training < 1000) training += " (low, scoring might be inaccurate -> consider increasing this number in the expert mode)";
        if (train.score_training > 10000) training += " (large, scoring can be slow -> consider reducing this number in the expert mode or scoring manually)";
      }
      DocGen.HTML.section(sb, training);
      if (valid != null) {
        String validation = "Number of validation set samples for scoring: " + valid.score_validation;
        if (valid.score_validation > 0) {
          if (valid.score_validation < 1000) validation += " (low, scoring might be inaccurate -> consider increasing this number in the expert mode)";
          if (valid.score_validation > 10000) validation += " (large, scoring can be slow -> consider reducing this number in the expert mode or scoring manually)";
        }
        DocGen.HTML.section(sb, validation);
      }
      sb.append("<table class='table table-striped table-bordered table-condensed'>");
      sb.append("<tr>");
      sb.append("<th>Training Time</th>");
      sb.append("<th>Training Samples</th>");
      sb.append("<th>Training MSE</th>");
      if (classification) {
        sb.append("<th>Training MCE</th>");
        sb.append("<th>Training Classification Error</th>");
      }
      sb.append("<th>Validation MSE</th>");
      if (classification) {
        sb.append("<th>Validation MCE</th>");
        sb.append("<th>Validation Classification Error</th>");
      }
      sb.append("</tr>");
      for( int i = training_errors.length - 1; i >= 0; i-- ) {
        sb.append("<tr>");
        sb.append("<td>" + PrettyPrint.msecs(training_errors[i].training_time_ms, true) + "</td>");
        if( validation_errors != null ) {
          sb.append("<td>" + String.format("%,d", validation_errors[i].training_samples) + "</td>");
        } else {
          sb.append("<td>" + String.format("%,d", training_errors[i].training_samples) + "</td>");
        }
        sb.append("<td>" + String.format(mse_format, training_errors[i].mean_square) + "</td>");
        if (classification) {
          sb.append("<td>" + String.format(cross_entropy_format, training_errors[i].cross_entropy) + "</td>");
          sb.append("<td>" + formatPct(training_errors[i].classification) + "</td>");
        }
        if( validation_errors != null ) {
          sb.append("<td>" + String.format(mse_format, validation_errors[i].mean_square) + "</td>");
          if (classification) {
            sb.append("<td>" + String.format(cross_entropy_format, validation_errors[i].cross_entropy) + "</td>");
            sb.append("<td>" + formatPct(validation_errors[i].classification) + "</td>");
          }
        } else
          sb.append("<td></td><td></td><td></td>");
        sb.append("</tr>");
      }
      sb.append("</table>");
      return true;
    }

    private static String formatPct(double pct) {
      String s = "N/A";
      if( !Double.isNaN(pct) )
        s = String.format("%5.2f %%", 100 * pct);
      return s;
    }

    @Override protected float[] score0(Chunk[] chunks, int rowInChunk, double[] tmp, float[] preds) {
      Layer[] clones = new Layer[layers.length];
      clones[0] = new ChunksInput(Utils.remove(chunks, chunks.length - 1), (VecsInput) layers[0]);
      for( int y = 1; y < layers.length - 1; y++ )
        clones[y] = layers[y].clone();
      Layer output = layers[layers.length - 1];
      if( output instanceof VecSoftmax )
        clones[clones.length - 1] = new ChunkSoftmax(chunks[chunks.length - 1], (VecSoftmax) output);
      else
        clones[clones.length - 1] = new ChunkLinear(chunks[chunks.length - 1], (VecLinear) output);
      for( int y = 0; y < clones.length; y++ ) {
        clones[y]._w = weights[y];
        clones[y]._b = biases[y];
        clones[y].init(clones, y, false);
      }
      ((Input) clones[0])._pos = rowInChunk;
      for (Layer clone : clones) clone.fprop(-1, false);
      float[] out = clones[clones.length - 1]._a;
      assert out.length == preds.length;
      for (int i=0; i<out.length; ++i) preds[i+1] = out[i];
      double[] data = new double[out.length];
      for (int i=0; i<out.length; ++i) data[i] = out[i];
      preds[0] = ModelUtils.getPrediction(preds, data);
      return preds;
    }

    @Override protected float[] score0(double[] data, float[] preds) {
      throw new UnsupportedOperationException();
    }

    @Override public ConfusionMatrix cm() {
      long[][] cm = confusion_matrix;
      if( cm != null )
        return new ConfusionMatrix(cm);
      return null;
    }

  }

  public static class NeuralNetScore extends ModelJob {
    static final int API_WEAVER = 1;
    static public DocGen.FieldDoc[] DOC_FIELDS;
    static final String DOC_GET = "Neural network scoring";

    @API(help = "Model", required = true, filter = Default.class)
    public NeuralNetModel model;

    @API(help = "Rows to consider for scoring, 0 (default) means the whole frame", filter = Default.class)
    public long max_rows;

    @API(help = "Classification error")
    public double classification_error;

    @API(help = "Mean square error")
    public double mean_square_error;

    @API(help = "Cross entropy")
    public double cross_entropy;

    @API(help = "Confusion matrix")
    public long[][] confusion_matrix;

    public NeuralNetScore() {
      description = DOC_GET;
    }

    @Override protected Response serve() {
      init();
      Frame[] frs = model.adapt(source, false);
      int classes = model.layers[model.layers.length - 1].units;
      confusion_matrix = new long[classes][classes];
      Layer[] clones = new Layer[model.layers.length];
      for( int y = 0; y < model.layers.length; y++ ) {
        clones[y] = model.layers[y].clone();
        clones[y]._w = model.weights[y];
        clones[y]._b = model.biases[y];
      }
      Vec[] vecs = frs[0].vecs();
      Vec[] data = Utils.remove(vecs, vecs.length - 1);
      Vec resp = vecs[vecs.length - 1];
      Errors e = eval(clones, data, resp, max_rows, confusion_matrix);
      classification_error = e.classification;
      mean_square_error = e.mean_square;
      cross_entropy = e.cross_entropy;
      if( frs[1] != null )
        frs[1].delete();
      return Response.done(this);
    }

    @Override public boolean toHTML(StringBuilder sb) {
      final boolean classification = model.isClassifier();
      if (classification) {
        DocGen.HTML.section(sb, "Classification error: " + String.format("%5.2f %%", 100 * classification_error));
      }
      DocGen.HTML.section(sb, "Mean square error: " + mean_square_error);
      if (classification) {
        DocGen.HTML.section(sb, "Mean cross entropy: " + cross_entropy);

        String[] domain = null;
        if (response.domain() != null) {
          domain = response.domain();
        } else {
          // find the names for the categories from the model's domains, after finding the correct column
          int idx = source.find(response);
          if( idx == -1 ) {
            Vec vm = response.masterVec();
            if( vm != null ) idx = source.find(vm);
          }
          if (idx != -1) domain = model._domains[idx];
        }
        confusion(sb, "Confusion Matrix", domain, confusion_matrix);
      }
      return true;
    }

    static void confusion(StringBuilder sb, String title, String[] classes, long[][] confusionMatrix) {
      //sb.append("<h3>" + title + "</h3>");
      sb.append("<table class='table table-striped table-bordered table-condensed'>");
      sb.append("<tr><th>Actual \\ Predicted</th>");
      if( classes == null ) {
        classes = new String[confusionMatrix.length];
        for( int i = 0; i < classes.length; i++ )
          classes[i] = "" + i;
      }
      for( String c : classes )
        sb.append("<th>" + c + "</th>");
      sb.append("<th>Error</th></tr>");
      long[] totals = new long[classes.length];
      long sumTotal = 0;
      long sumError = 0;
      for( int crow = 0; crow < classes.length; ++crow ) {
        long total = 0;
        long error = 0;
        sb.append("<tr><th>" + classes[crow] + "</th>");
        for( int ccol = 0; ccol < classes.length; ++ccol ) {
          long num = confusionMatrix[crow][ccol];
          total += num;
          totals[ccol] += num;
          if( ccol == crow ) {
            sb.append("<td style='background-color:LightGreen'>");
          } else {
            sb.append("<td>");
            error += num;
          }
          sb.append(num);
          sb.append("</td>");
        }
        sb.append("<td>");
        sb.append(String.format("%5.3f = %d / %d", (double) error / total, error, total));
        sb.append("</td></tr>");
        sumTotal += total;
        sumError += error;
      }
      sb.append("<tr><th>Totals</th>");
      for (long total : totals) sb.append("<td>" + total + "</td>");
      sb.append("<td><b>");
      sb.append(String.format("%5.3f = %d / %d", (double) sumError / sumTotal, sumError, sumTotal));
      sb.append("</b></td></tr>");
      sb.append("</table>");
    }
  }

  static int cores() {
    int cores = 0;
    for( H2ONode node : H2O.CLOUD._memary )
      cores += node._heartbeat._num_cpus;
    return cores;
  }

  /**
* Makes sure small datasets are spread over enough chunks to parallelize training.
*/
  public static void reChunk(Vec[] vecs) {
    final int splits = cores() * 2; // More in case of unbalance
    if( vecs[0].nChunks() < splits ) {
      // A new random VectorGroup
      Key keys[] = new Vec.VectorGroup().addVecs(vecs.length);
      for( int v = 0; v < vecs.length; v++ ) {
        AppendableVec vec = new AppendableVec(keys[v]);
        long rows = vecs[0].length();
        Chunk cache = null;
        for( int split = 0; split < splits; split++ ) {
          long off = rows * split / splits;
          long lim = rows * (split + 1) / splits;
          NewChunk chunk = new NewChunk(vec, split);
          for( long r = off; r < lim; r++ ) {
            if( cache == null || r < cache._start || r >= cache._start + cache._len )
              cache = vecs[v].chunkForRow(r);
            if( !cache.isNA(r) ) {
              if( vecs[v]._domain != null )
                chunk.addEnum((int) cache.at8(r));
              else if( vecs[v].isInt() )
                chunk.addNum(cache.at8(r), 0);
              else
                chunk.addNum(cache.at(r));
            } else {
              if( vecs[v].isInt() )
                chunk.addNA();
              else {
                // Don't use addNA() for doubles, as NewChunk uses separate array
                chunk.addNum(Double.NaN);
              }
            }
          }
          chunk.close(split, null);
        }
        Vec t = vec.close(null);
        t._domain = vecs[v]._domain;
        vecs[v] = t;
      }
    }
  }
}

