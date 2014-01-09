package hex;

import hex.Layer.*;
import jsr166y.CountedCompleter;
import water.*;
import water.H2O.H2OCountedCompleter;
import water.Job.ValidatedJob;
import water.api.DocGen;
import water.api.Progress2;
import water.api.Request;
import water.api.RequestServer;
import water.fvec.*;
import water.util.RString;
import water.util.Utils;

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

  public enum ExecutionMode {
    Serial, Threaded_Hogwild, MapReduce_Hogwild
  }

  @API(help = "Execution Mode", filter = Default.class)
  public ExecutionMode mode = ExecutionMode.Threaded_Hogwild;

  @API(help = "Activation function", filter = Default.class)
  public Activation activation = Activation.Tanh;

  @API(help = "Input layer dropout ratio", filter = Default.class, dmin = 0, dmax = 1)
  public float input_dropout_ratio = 0;

  @API(help = "Hidden layer sizes, e.g. 1000, 1000. Grid search: (100, 100), (200, 200)", filter = Default.class)
  public int[] hidden = new int[] { 500 };

  @API(help = "Initial Weight Distribution", filter = Default.class, dmin = 0)
  public InitialWeightDistribution initial_weight_distribution = InitialWeightDistribution.UniformAdaptive;

  @API(help = "Uniform: -value...value, Normal: stddev)", filter = Default.class, dmin = 0)
  public double initial_weight_scale = 0.01;

  @API(help = "Learning rate", filter = Default.class, dmin = 0)
  public double rate = .005;

  @API(help = "Learning rate annealing: rate / (1 + rate_annealing * samples)", filter = Default.class)
  public double rate_annealing = 1 / 1e6;

  @API(help = "Constraint for squared sum of incoming weights per unit", filter = Default.class)
  public float max_w2 = 15;

  @API(help = "Momentum at the beginning of training", filter = Default.class)
  public double momentum_start = .5;

  @API(help = "Number of samples for which momentum increases", filter = Default.class)
  public long momentum_ramp = 300 * 60000;

  @API(help = "Momentum once the initial increase is over", filter = Default.class, dmin = 0)
  public double momentum_stable = .99;

  //TODO: add a ramp down to 0 for l1 and l2

  @API(help = "L1 regularization", filter = Default.class, dmin = 0)
  public double l1;

  @API(help = "L2 regularization", filter = Default.class, dmin = 0)
  public double l2 = .001;

  @API(help = "Loss function", filter =Default.class)
  private Layer.Loss loss = Layer.Loss.CrossEntropy;

  @API(help = "How many times the dataset should be iterated", filter = Default.class, dmin = 0)
  public double epochs = 100;

  @API(help = "Seed for the random number generator", filter = Default.class)
  public static long seed = new Random().nextLong();

  @Override
  protected void registered(RequestServer.API_VERSION ver) {
    super.registered(ver);
    for (Argument arg : _arguments) {
      if (arg._name.equals("activation") || arg._name.equals("initial_weight_distribution")) {
         arg.setRefreshOnChange();
      }
    }
  }

  @Override protected void queryArgumentValueSet(Argument arg, java.util.Properties inputArgs) {
    super.queryArgumentValueSet(arg, inputArgs);
    if (arg._name.equals("input_dropout_ratio") &&
            (activation != Layer.Activation.RectifierWithDropout && activation != Layer.Activation.TanhWithDropout)
            ) {
      arg.disable("Only with Dropout", inputArgs);
    }
    if(arg._name.equals("initial_weight_scale") &&
            (initial_weight_distribution == Layer.InitialWeightDistribution.UniformAdaptive)
            ) {
      arg.disable("Only with Uniform or Normal initial weight distributions", inputArgs);
    }
  }

  // used to stop the monitor thread
  private static volatile boolean running = true;

  public NeuralNet() {
    description = DOC_GET;
  }

  @Override public Job fork() {
    init();
    H2OCountedCompleter job = new H2OCountedCompleter() {
      @Override public void compute2() {
        startTrain();
      }

      @Override public boolean onExceptionalCompletion(Throwable ex, CountedCompleter caller) {
        Job job = Job.findJob(job_key);
        if( job != null )
          job.cancel(Utils.getStackAsString(ex));
        return super.onExceptionalCompletion(ex, caller);
      }
    };
    start(job);
    H2O.submitTask(job);
    return this;
  }

  void startTrain() {
    running = true;
    Vec[] vecs = Utils.append(_train, response);
    reChunk(vecs);
    final Vec[] train = new Vec[vecs.length - 1];
    System.arraycopy(vecs, 0, train, 0, train.length);
    final Vec trainResp = classification ? vecs[vecs.length - 1].toEnum() : vecs[vecs.length - 1];

    final Layer[] ls = new Layer[hidden.length + 2];
    ls[0] = new VecsInput(train, null, input_dropout_ratio);
    for( int i = 0; i < hidden.length; i++ ) {
      switch( activation ) {
        case Tanh:
          ls[i + 1] = new Layer.Tanh(hidden[i]);
          break;
        case TanhWithDropout:
          ls[i + 1] = new Layer.TanhDropout(hidden[i]);
          break;
        case Rectifier:
          ls[i + 1] = new Layer.Rectifier(hidden[i]);
          break;
        case RectifierWithDropout:
          ls[i + 1] = new Layer.RectifierDropout(hidden[i]);
          break;
        case Maxout:
          ls[i + 1] = new Layer.Maxout(hidden[i]);
          break;
      }
      ls[i + 1].initial_weight_distribution = initial_weight_distribution;
      ls[i + 1].initial_weight_scale = initial_weight_scale;
      ls[i + 1].rate = (float) rate;
      ls[i + 1].rate_annealing = (float) rate_annealing;
      ls[i + 1].momentum_start = (float) momentum_start;
      ls[i + 1].momentum_ramp = momentum_ramp;
      ls[i + 1].momentum_stable = (float) momentum_stable;
      ls[i + 1].l1 = (float) l1;
      ls[i + 1].l2 = (float) l2;
      ls[i + 1].max_w2 = max_w2;
      ls[i + 1].loss = loss;
    }
    if( classification )
      ls[ls.length - 1] = new VecSoftmax(trainResp, null);
    else
      ls[ls.length - 1] = new VecLinear(trainResp, null);
    ls[ls.length - 1].initial_weight_distribution = initial_weight_distribution;
    ls[ls.length - 1].initial_weight_scale = initial_weight_scale;
    ls[ls.length - 1].rate = (float) rate;
    ls[ls.length - 1].rate_annealing = (float) rate_annealing;
    ls[ls.length - 1].l1 = (float) l1;
    ls[ls.length - 1].l2 = (float) l2;
    ls[ls.length - 1].max_w2 = max_w2;
    ls[ls.length - 1].loss = loss;

    for( int i = 0; i < ls.length; i++ )
      ls[i].init(ls, i);

    final Key sourceKey = Key.make(input("source"));
    final Frame frame = new Frame(_names, train);
    frame.add(_responseName, trainResp);
    final Errors[] trainErrors0 = new Errors[] { new Errors() };
    final Errors[] validErrors0 = validation == null ? null : new Errors[] { new Errors() };

    NeuralNetModel model = new NeuralNetModel(destination_key, sourceKey, frame, ls);
    model.training_errors = trainErrors0;
    model.validation_errors = validErrors0;

    model.mode = mode;
    model.activation = activation;
    model.input_dropout_ratio = input_dropout_ratio;
    model.initial_weight_distribution = initial_weight_distribution;
    model.initial_weight_scale = initial_weight_scale;
    model.rate = rate;
    model.rate_annealing = rate_annealing;
    model.max_w2 = max_w2;
    model.momentum_start = momentum_start;
    model.momentum_ramp = momentum_ramp;
    model.momentum_stable = momentum_stable;
    model.l1 = l1;
    model.l2 = l2;
    model.loss = loss;
    model.seed = seed;

    UKV.put(destination_key, model);

    final Frame[] adapted = validation == null ? null : model.adapt(validation, false);
    final Trainer trainer;

    final long num_rows = source.numRows();
    // work on first batch of points serially for better reproducibility
    if (mode != ExecutionMode.Serial) {
      final long serial_rows = 1000l;
      System.out.println("Training the first " + serial_rows + " rows serially.");
      Trainer pretrainer = new Trainer.Direct(ls, (double)serial_rows/num_rows, self());
      pretrainer.start();
      pretrainer.join();
    }

    if (mode == ExecutionMode.Serial) {
      System.out.println("Serial execution mode");
      trainer = new Trainer.Direct(ls, epochs, self());
    } else if (mode == ExecutionMode.Threaded_Hogwild) {
      System.out.println("Threaded (Hogwild) execution mode");
      trainer = new Trainer.Threaded(ls, epochs, self());
    } else if (mode == ExecutionMode.MapReduce_Hogwild) {
      System.out.println("MapReduce + Threaded (Hogwild) execution mode");
      trainer = new Trainer.MapReduce(ls, epochs, self());
    } else throw new RuntimeException("invalid execution mode.");

    System.out.println("Running for " + epochs + " epochs.");

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

          //validate continuously
          final long total_samples = (long) (epochs * num_rows);
          long eval_samples = 0;
          while(!cancelled() && running) {
            eval_samples = eval(valid, validResp);
          }
          // make sure to do the final eval on a regular run
          if (mode != ExecutionMode.MapReduce_Hogwild && !cancelled() && eval_samples < total_samples) {
            eval_samples = eval(valid, validResp);
          }
          // hack for MapReduce, which calls cancel() from outside, but doesn't set running to false
          if (cancelled() && mode == ExecutionMode.MapReduce_Hogwild && running) {
            eval_samples = eval(valid, validResp);
            running = false;
          }
          // make sure we we finished properly
          assert(eval_samples == total_samples || (cancelled() && !running));

          // remove validation data
          if( adapted != null && adapted[1] != null )
            adapted[1].remove();
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
        Errors e = eval(train, trainResp, 10000, valid == null ? cm : null);
        trainErrors = Utils.append(trainErrors, e);
        if( valid != null ) {
          e = eval(valid, validResp, 0, cm);
          validErrors = Utils.append(validErrors, e);
        }
        NeuralNetModel model = new NeuralNetModel(destination_key, sourceKey, frame, ls);
        model.training_errors = trainErrors;
        model.validation_errors = validErrors;
        model.confusion_matrix = cm;
        // also copy model parameters
        model.mode = mode;
        model.activation = activation;
        model.input_dropout_ratio = input_dropout_ratio;
        model.initial_weight_distribution = initial_weight_distribution;
        model.initial_weight_scale = initial_weight_scale;
        model.rate = rate;
        model.rate_annealing = rate_annealing;
        model.max_w2 = max_w2;
        model.momentum_start = momentum_start;
        model.momentum_ramp = momentum_ramp;
        model.momentum_stable = momentum_stable;
        model.l1 = l1;
        model.l2 = l2;
        model.loss = loss;
        model.seed = seed;
        UKV.put(model._selfKey, model);
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

    // hack to gracefully terminate the job submitted via H2O web API
    if (mode != ExecutionMode.MapReduce_Hogwild) {
      running = false; //tell the monitor thread to finish too
      try {
        monitor.join();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }

      // remove this job
      H2OCountedCompleter task = _fjtask;
      if( task != null )
        task.tryComplete();
      this.remove();
      System.out.println("Training finished.");
    }

  }

  @Override public float progress() {
    NeuralNetModel model = UKV.get(destination_key);
    if( model != null && source != null) {
      Errors e = model.training_errors[model.training_errors.length - 1];
      return 0.1f + Math.min(1, e.training_samples / (float) (epochs * source.numRows()));
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
      clones[y].init(clones, y, false, 0, null);
    Layer.shareWeights(ls, clones);
    return eval(clones, n, cm);
  }

  public static Errors eval(Layer[] ls, long n, long[][] cm) {
    Errors e = new Errors();
    Input input = (Input) ls[0];
    long len = input._len;
    if( n != 0 )
      len = Math.min(len, n);
    // classification
    if( ls[ls.length - 1] instanceof Softmax ) {
      int correct = 0;
      e.mean_square = 0;
      e.cross_entropy = 0;
      for( input._pos = 0; input._pos < len; input._pos++ ) {
        if( ((Softmax) ls[ls.length - 1]).target() == -2 ) //NA
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
        if( !Float.isNaN(ls[ls.length - 1]._a[0]) )
          error(ls, e);
      e.classification = Double.NaN;
      e.mean_square /= len;
    }
    input._pos = 0;
    return e;
  }

  // classification scoring
  private static boolean correct(Layer[] ls, Errors e, long[][] confusion) {
    //Softmax for classification, one value per output class
    Softmax output = (Softmax) ls[ls.length - 1];
    if( output.target() == -1 )
      return false;
    //Testing for this row
    for (Layer l : ls) l.fprop(false);
    //Predicted output values
    float[] out = ls[ls.length - 1]._a;
    //True target value
    int target = output.target();
    //Score
    for( int o = 0; o < out.length; o++ ) {
      final boolean hitpos = (o == target);
      final float t = hitpos ? 1 : 0;
      final float d = t - out[o];
      e.mean_square += d * d;
      e.cross_entropy += hitpos ? -Math.log(out[o]) : 0;
    }
    float max = out[0];
    int idx = 0;
    for( int o = 1; o < out.length; o++ ) {
      if( out[o] > max ) {
        max = out[o];
        idx = o;
      }
    }
    if( confusion != null )
      confusion[output.target()][idx]++;
    return idx == output.target();
  }

  // TODO extract to layer
  // regression scoring
  private static void error(Layer[] ls, Errors e) {
    //Linear output layer for regression
    Linear linear = (Linear) ls[ls.length - 1];
    //Testing for this row
    for (Layer l : ls) l.fprop(false);
    //Predicted target values
    float[] output = ls[ls.length - 1]._a;
    //True target values
    float[] target = linear.target();
    e.mean_square = 0;
    for( int o = 0; o < output.length; o++ ) {
      final float d = target[o] - output[o];
      e.mean_square += d * d;
    }
  }

  @Override protected Response redirect() {
    String redirectName = NeuralNetProgress.class.getSimpleName();
    return Response.redirect(this, redirectName, //
        "job_key", job_key, //
        "destination_key", destination_key);
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

    @API(help = "MSE")
    public double mean_square = Double.POSITIVE_INFINITY;

    @API(help = "Cross entropy")
    public double cross_entropy = Double.POSITIVE_INFINITY;

    @API(help = "Layer learning rates")
    public double[] rates;

    @Override public String toString() {
      return String.format("%.2f", (100 * classification)) + "% (MSE:" + String.format("%.2e", mean_square) + ")";
    }
  }

  public static class NeuralNetModel extends Model {
    static final int API_WEAVER = 1;
    static public DocGen.FieldDoc[] DOC_FIELDS;

    @API(help = "Execution Mode")
    public ExecutionMode mode;

    @API(help = "Activation function")
    public Layer.Activation activation;

    @API(help = "Input layer dropout ratio")
    public double input_dropout_ratio;

    @API(help = "Initial Weight Distribution")
    public Layer.InitialWeightDistribution initial_weight_distribution;

    @API(help = "Uniform: -value...value, Normal: stddev)")
    public double initial_weight_scale;

    @API(help = "Learning rate")
    public double rate;

    @API(help = "Learning rate annealing")
    public double rate_annealing;

    @API(help = "Constraint for squared sum of incoming weights per unit")
    public float max_w2;

    @API(help = "Momentum at the beginning of training")
    public double momentum_start;

    @API(help = "Number of samples for which momentum increases")
    public long momentum_ramp;

    @API(help = "Momentum once the initial increase is over")
    public double momentum_stable;

    @API(help = "L1 regularization")
    public double l1;

    @API(help = "L2 regularization")
    public double l2;

    @API(help = "Loss function")
    public Layer.Loss loss;

    @API(help = "Seed for the random number generator")
    public long seed;

    @API(help = "Layers")
    public Layer[] layers;

    @API(help = "Layer weights")
    public float[][] weights, biases;

    @API(help = "Errors on the training set")
    public Errors[] training_errors;

    @API(help = "Errors on the validation set")
    public Errors[] validation_errors;

    @API(help = "Confusion matrix")
    public long[][] confusion_matrix;

    NeuralNetModel(Key selfKey, Key dataKey, Frame fr, Layer[] ls) {
      super(selfKey, dataKey, fr);

      layers = ls;
      weights = new float[ls.length][];
      biases = new float[ls.length][];
      for( int y = 1; y < layers.length; y++ ) {
        weights[y] = layers[y]._w;
        biases[y] = layers[y]._b;
      }
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
        clones[y].init(clones, y, false, 0, null);
      }
      ((Input) clones[0])._pos = rowInChunk;
      for (Layer clone : clones) clone.fprop(false);
      float[] out = clones[clones.length - 1]._a;
      assert out.length == preds.length;
      return out;
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

    public Response redirect(Request req) {
      String redirectName = new NeuralNetProgress().href();
      return Response.redirect(req, redirectName, "destination_key", _selfKey);
    }
  }

  public static class NeuralNetProgress extends Progress2 {
    static final int API_WEAVER = 1;
    static public DocGen.FieldDoc[] DOC_FIELDS;

    @API(help = "Execution Mode")
    public ExecutionMode mode;

    @API(help = "Activation function")
    public Layer.Activation activation;

    @API(help = "Dropout ratio for the input layer (for RectifierWithDropout)")
    public double input_dropout_ratio;

    @API(help = "Hidden layer sizes, e.g. 1000, 1000. Grid search: (100, 100), (200, 200)")
    public int[] hidden;

    @API(help = "Initial Weight Distribution")
    public Layer.InitialWeightDistribution initial_weight_distribution;

    @API(help = "Uniform: -value...value, Normal: stddev)")
    public double initial_weight_scale;

    @API(help = "Learning rate")
    public double rate;

    @API(help = "Learning rate annealing: rate / (1 + rate_annealing * samples)")
    public double rate_annealing;

    @API(help = "Constraint for squared sum of incoming weights per unit")
    public float max_w2;

    @API(help = "Momentum at the beginning of training")
    public double momentum_start;

    @API(help = "Number of samples for which momentum increases")
    public long momentum_ramp;

    @API(help = "Momentum once the initial increase is over")
    public double momentum_stable;

    @API(help = "L1 regularization")
    public double l1;

    @API(help = "L2 regularization")
    public double l2;

    @API(help = "Loss function")
    public Layer.Loss loss;

    @API(help = "How many times the dataset should be iterated")
    public double epochs;

    @API(help = "Seed for the random number generator")
    public long seed;

    @API(help = "Errors on the training set")
    public Errors[] training_errors;

    @API(help = "Errors on the validation set")
    public Errors[] validation_errors;

    @API(help = "Dataset headers")
    public String[] class_names;

    @API(help = "Confusion matrix")
    public long[][] confusion_matrix;

    @Override protected String name() {
      return DOC_GET;
    }

    @Override protected Response serve() {
      NeuralNet job = job_key == null ? null : (NeuralNet) Job.findJob(job_key);
      if( job != null ) {
        mode = job.mode;
        activation = job.activation;
        input_dropout_ratio = job.input_dropout_ratio;
        hidden = job.hidden;
        initial_weight_distribution = job.initial_weight_distribution;
        initial_weight_scale = job.initial_weight_scale;
        rate = job.rate;
        rate_annealing = job.rate_annealing;
        max_w2 = job.max_w2;
        momentum_start = job.momentum_start;
        momentum_ramp = job.momentum_ramp;
        momentum_stable = job.momentum_stable;
        l1 = job.l1;
        l2 = job.l2;
        loss = job.loss;
        epochs = job.epochs;
        seed = NeuralNet.seed;
      }
      NeuralNetModel model = UKV.get(destination_key);
      if( model != null ) {
        training_errors = model.training_errors;
        validation_errors = model.validation_errors;
        class_names = model.classNames();
        confusion_matrix = model.confusion_matrix;
      }
      return super.serve();
    }

    @Override public boolean toHTML(StringBuilder sb) {
      final String mse_format = "%2.6f";
      final String cross_entropy_format = "%2.6f";
      Job nn = job_key == null ? null : Job.findJob(job_key);
      NeuralNetModel model = UKV.get(destination_key);
      if( model != null ) {
        String cmTitle = "Confusion Matrix", validC = "", validS = "", validCE = "";
        Errors train = model.training_errors[model.training_errors.length - 1];
        if( model.validation_errors != null ) {
          Errors valid = model.validation_errors[model.validation_errors.length - 1];
          validC = format(valid.classification);
          validS = String.format(mse_format, valid.mean_square);
          validCE = String.format(cross_entropy_format, valid.cross_entropy);
        } else
          cmTitle += " (Training Data)";
        DocGen.HTML.section(sb, "Training classification error: " + format(train.classification));
        DocGen.HTML.section(sb, "Training mean square error: " + String.format(mse_format, train.mean_square));
        DocGen.HTML.section(sb, "Training cross entropy: " + String.format(cross_entropy_format, train.cross_entropy));
        DocGen.HTML.section(sb, "Validation classification error: " + validC);
        DocGen.HTML.section(sb, "Validation mean square error: " + validS);
        DocGen.HTML.section(sb, "Validation mean cross entropy: " + validCE);
        if( nn != null ) {
          long ps = train.training_samples * 1000 / nn.runTimeMs();
          DocGen.HTML.section(sb, "Training speed: " + ps + " samples/s");
        }
        if( model.confusion_matrix != null && model.confusion_matrix.length < 100 ) {
          String[] classes = model.classNames();
          NeuralNetScore.confusion(sb, cmTitle, classes, model.confusion_matrix);
        }
        DocGen.HTML.section(sb, "Progress");
        sb.append("<table class='table table-striped table-bordered table-condensed'>");
        sb.append("<tr>");
        sb.append("<th>Training Time</th>");
        sb.append("<th>Training Samples</th>");
        sb.append("<th>Training MSE</th>");
        sb.append("<th>Training MCE</th>");
        sb.append("<th>Training Classification Error</th>");
        sb.append("<th>Validation MSE</th>");
        sb.append("<th>Validation MCE</th>");
        sb.append("<th>Validation Classification Error</th>");
        sb.append("</tr>");
        Errors[] trains = model.training_errors;
        for( int i = trains.length - 1; i >= 0; i-- ) {
          sb.append("<tr>");
          sb.append("<td>" + PrettyPrint.msecs(trains[i].training_time_ms, true) + "</td>");
          sb.append("<td>" + String.format("%,d", trains[i].training_samples) + "</td>");
          sb.append("<td>" + String.format(mse_format, trains[i].mean_square) + "</td>");
          sb.append("<td>" + String.format(cross_entropy_format, trains[i].cross_entropy) + "</td>");
          sb.append("<td>" + format(trains[i].classification) + "</td>");
          if( model.validation_errors != null ) {
            sb.append("<td>" + String.format(mse_format, model.validation_errors[i].mean_square) + "</td>");
            sb.append("<td>" + String.format(cross_entropy_format, model.validation_errors[i].cross_entropy) + "</td>");
            sb.append("<td>" + format(model.validation_errors[i].classification) + "</td>");
          } else
            sb.append("<td></td><td></td><td></td>");
          sb.append("</tr>");
        }
        sb.append("</table>");
      }
      return true;
    }

    private static String format(double classification) {
      String s = "N/A";
      if( !Double.isNaN(classification) )
        s = String.format("%5.2f %%", 100 * classification);
      return s;
    }

    @Override protected Response jobDone(Job job, Key dst) {
      return Response.done(this);
    }

    public static String link(Key job, Key model, String content) {
      NeuralNetProgress req = new NeuralNetProgress();
      return "<a href='" + req.href() + ".html?job_key=" + job + "&destination_key=" + model + "'>" + content + "</a>";
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
        frs[1].remove();
      return Response.done(this);
    }

    @Override public boolean toHTML(StringBuilder sb) {
      DocGen.HTML.section(sb, "Classification error: " + String.format("%5.2f %%", 100 * classification_error));
      DocGen.HTML.section(sb, "Mean square error: " + mean_square_error);
      confusion(sb, "Confusion Matrix", response.domain(), confusion_matrix);
      return true;
    }

    static void confusion(StringBuilder sb, String title, String[] classes, long[][] confusionMatrix) {
      sb.append("<h3>" + title + "</h3>");
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
      for( int i = 0; i < totals.length; ++i )
        sb.append("<td>" + totals[i] + "</td>");
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
              cache = vecs[v].chunk(r);
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
