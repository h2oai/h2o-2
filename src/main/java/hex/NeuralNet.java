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

/**
 * Neural network.
 *
 * @author cypof
 */
public class NeuralNet extends ValidatedJob {
  static final int API_WEAVER = 1;
  public static DocGen.FieldDoc[] DOC_FIELDS;
  public static final String DOC_GET = "Neural Network";

  @API(help = "Execution Mode", filter = Default.class)
  public NeuralNetParams.ExecutionMode mode = NeuralNetParams.ExecutionMode.MapReduce_Hogwild;

  @API(help = "Activation function", filter = Default.class)
  public NeuralNetParams.Activation activation = NeuralNetParams.Activation.RectifierWithDropout;

  @API(help = "Input layer dropout ratio", filter = Default.class, dmin = 0, dmax = 1)
  public double input_dropout_ratio = 0.2;

  @API(help = "Hidden layer sizes, e.g. 1000, 1000. Grid search: (100, 100), (200, 200)", filter = Default.class)
  public int[] hidden = new int[] { 1024, 1024, 2048 };

  @API(help = "Initial Weight Distribution, UniformAdaptive is ~ sqrt(6 / (# units + # input_units))", filter = Default.class)
  public NeuralNetParams.InitialWeightDistribution initial_weight_distribution = NeuralNetParams.InitialWeightDistribution.UniformAdaptive;

  @API(help = "Uniform: -value...value, Normal: stddev)", filter = Default.class, dmin = 0)
  public double initial_weight_scale = 0.01;

  @API(help = "Learning rate", filter = Default.class, dmin = 0)
  public double rate = .01;

  @API(help = "Learning rate annealing: rate / (1 + rate_annealing * samples)", filter = Default.class)
  public double rate_annealing = 1 / 1e6;

  @API(help = "Constraint for squared sum of incoming weights per unit", filter = Default.class)
  public float max_w2 = 15;

  @API(help = "Momentum at the beginning of training", filter = Default.class)
  public double momentum_start = .5;

  @API(help = "Number of samples for which momentum increases", filter = Default.class)
  public long momentum_ramp = 30 * 60000;

  @API(help = "Momentum once the initial increase is over", filter = Default.class, dmin = 0)
  public double momentum_stable = .99;

  //TODO: add a ramp down to 0 for l1 and l2

  @API(help = "L1 regularization", filter = Default.class, dmin = 0)
  public double l1 = 0.00001;

  @API(help = "L2 regularization", filter = Default.class, dmin = 0)
  public double l2 = 0.0;

  @API(help = "Loss function", filter = Default.class)
  public NeuralNetParams.Loss loss = NeuralNetParams.Loss.CrossEntropy;

  @API(help = "How many times the dataset should be iterated", filter = Default.class, dmin = 0)
  public double epochs = 100;

  @API(help = "Enable diagnostics (incl. stability check)", filter = Default.class)
  public boolean diagnostics = true;

  @Override
  protected void registered(RequestServer.API_VERSION ver) {
    super.registered(ver);
    for (Argument arg : _arguments) {
      if ( arg._name.equals("activation") || arg._name.equals("initial_weight_distribution") ) {
         arg.setRefreshOnChange();
      }
    }
  }

  @Override protected void queryArgumentValueSet(Argument arg, java.util.Properties inputArgs) {
    super.queryArgumentValueSet(arg, inputArgs);
    if (arg._name.equals("input_dropout_ratio") &&
            (activation != NeuralNetParams.Activation.RectifierWithDropout && activation != NeuralNetParams.Activation.TanhWithDropout)
            ) {
      arg.disable("Only with Dropout", inputArgs);
    }
    if(arg._name.equals("initial_weight_scale") &&
            (initial_weight_distribution == NeuralNetParams.InitialWeightDistribution.UniformAdaptive)
            ) {
      arg.disable("Only with Uniform or Normal initial weight distributions", inputArgs);
    }
    if( arg._name.equals("mode") && (H2O.CLOUD._memary.length > 1) ) {
      mode = NeuralNetParams.ExecutionMode.MapReduce_Hogwild;
      arg.disable("Using MapReduce since cluster size > 1.", inputArgs);
    }
  }

  // Wrapper class to pass around
  // @API stuff is used for JSON serialization
  public static class NeuralNetParams extends Iced {
    static final int API_WEAVER = 1;
    static public DocGen.FieldDoc[] DOC_FIELDS;

    @API(help = "Execution Mode")
    public ExecutionMode mode;

    @API(help = "Activation function")
    public Activation activation;

    @API(help = "Input layer dropout ratio")
    public double input_dropout_ratio;

    @API(help = "Hidden layer sizes, e.g. 1000, 1000. Grid search: (100, 100), (200, 200)")
    public int[] hidden;

    @API(help = "Initial Weight Distribution")
    public InitialWeightDistribution initial_weight_distribution;

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

    //TODO: add a ramp down to 0 for l1 and l2

    @API(help = "L1 regularization")
    public double l1;

    @API(help = "L2 regularization")
    public double l2;

    @API(help = "Loss function")
    public Loss loss = Loss.CrossEntropy;

    @API(help = "How many times the dataset should be iterated")
    public double epochs;

    @API(help = "Enable diagnostics (incl. stability check)")
    public boolean diagnostics;

    public NeuralNetParams() {}

    public NeuralNetParams(ExecutionMode mode, Activation activation, double input_dropout_ratio, int[] hidden, InitialWeightDistribution initial_weight_distribution, double initial_weight_scale, double rate, double rate_annealing, float max_w2, double momentum_start, long momentum_ramp, double momentum_stable, double l1, double l2, Loss loss, double epochs, boolean diagnostics) {
      this.mode = mode;
      this.activation = activation;
      this.input_dropout_ratio = input_dropout_ratio;
      this.hidden = hidden;
      this.initial_weight_distribution = initial_weight_distribution;
      this.initial_weight_scale = initial_weight_scale;
      this.rate = rate;
      this.rate_annealing = rate_annealing;
      this.max_w2 = max_w2;
      this.momentum_start = momentum_start;
      this.momentum_ramp = momentum_ramp;
      this.momentum_stable = momentum_stable;
      this.l1 = l1;
      this.l2 = l2;
      this.loss = loss;
      this.epochs = epochs;
      this.diagnostics = diagnostics;
    }

    public enum ExecutionMode {
      Serial, Threaded_Hogwild, MapReduce_Hogwild
    }

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
  };

  NeuralNetParams _params;

  // Hack: used to stop the monitor thread
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
    _params = new NeuralNetParams(mode, activation, input_dropout_ratio, hidden, initial_weight_distribution, initial_weight_scale, rate, rate_annealing, max_w2, momentum_start, momentum_ramp, momentum_stable, l1, l2, loss, epochs, diagnostics);

    running = true;
//    Vec[] vecs = Utils.append(_train, response);
//    reChunk(vecs);
//    final Vec[] train = new Vec[vecs.length - 1];
//    System.arraycopy(vecs, 0, train, 0, train.length);
//    final Vec trainResp = classification ? vecs[vecs.length - 1].toEnum() : vecs[vecs.length - 1];

    final Vec[] train = _train;
    final Vec trainResp = classification ? response.toEnum() : response;

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
    }

    if( classification )
      ls[ls.length - 1] = new VecSoftmax(trainResp, null, loss);
    else
      ls[ls.length - 1] = new VecLinear(trainResp, null, loss);

    for( int i = 0; i < ls.length; i++ )
      ls[i].init(ls, i, _params);

    final Key sourceKey = Key.make(input("source"));
    final Frame frame = new Frame(_names, train);
    frame.add(_responseName, trainResp);
    final Errors[] trainErrors0 = new Errors[] { new Errors() };
    final Errors[] validErrors0 = validation == null ? null : new Errors[] { new Errors() };

    NeuralNetModel model = new NeuralNetModel_JSON(destination_key, sourceKey, frame, ls, _params);
    model.training_errors = trainErrors0;
    model.validation_errors = validErrors0;

    UKV.put(destination_key, model);

    final Frame[] adapted = validation == null ? null : model.adapt(validation, false);
    final Trainer trainer;

    final long num_rows = source.numRows();
    // work on first batch of points serially for better reproducibility
    if (mode != NeuralNetParams.ExecutionMode.Serial) {
      final long serial_rows = 1000l;
      System.out.println("Training the first " + serial_rows + " rows serially.");
      Trainer pretrainer = new Trainer.Direct(ls, (double)serial_rows/num_rows, self());
      pretrainer.start();
      pretrainer.join();
    }

    if (mode == NeuralNetParams.ExecutionMode.Serial) {
      System.out.println("Serial execution mode");
      trainer = new Trainer.Direct(ls, epochs, self());
    } else if (mode == NeuralNetParams.ExecutionMode.Threaded_Hogwild) {
      System.out.println("Single-node threaded (Hogwild) execution mode");
      trainer = new Trainer.Threaded(ls, epochs, self());
    } else if (mode == NeuralNetParams.ExecutionMode.MapReduce_Hogwild) {
      System.out.println("Multi-node MapReduce (Hogwild) execution mode");
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
          while(!cancelled() && running) {
            eval(valid, validResp);
            Thread.sleep(2000);
          }

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
        NeuralNetModel model = new NeuralNetModel(destination_key, sourceKey, frame, ls, _params);
        model.training_errors = trainErrors;
        model.validation_errors = validErrors;
        model.confusion_matrix = cm;
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
    if (mode != NeuralNetParams.ExecutionMode.MapReduce_Hogwild) {
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
      output = new VecSoftmax(resp, (VecSoftmax) output, output.loss);
    else
      output = new VecLinear(resp, (VecLinear) output, output.loss);
    return eval(ls, new VecsInput(vecs, (VecsInput) ls[0]), output, n, cm);
  }

  private static Errors eval(Layer[] ls, Input input, Output output, long n, long[][] cm) {
    Layer[] clones = new Layer[ls.length];
    clones[0] = input;
    for( int y = 1; y < clones.length - 1; y++ )
      clones[y] = ls[y].clone();
    clones[clones.length - 1] = output;
    for( int y = 0; y < clones.length; y++ )
      clones[y].init(clones, y, false, 0);
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
        if( Layer.correct(ls, e, cm) )
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
          Layer.error(ls, e);
      e.classification = Double.NaN;
      e.mean_square /= len;
    }
    input._pos = 0;
    return e;
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

    @Override public String toString() {
        return String.format("%.2f", (100 * classification))
              + "% (MSE:" + String.format("%.2e", mean_square)
              + ", MCE:" + String.format("%.2e", cross_entropy)
              + ")";
    }
  }

  public static class NeuralNetModel_JSON extends NeuralNetModel {
    NeuralNetModel_JSON(Key selfKey, Key dataKey, Frame fr, Layer[] ls, NeuralNetParams p) {
      super(selfKey, dataKey, fr, ls, p);
    }

    @Override
    public AutoBuffer writeJSONFields(AutoBuffer bb) {
      AutoBuffer b = super.writeJSONFields(bb);
      _params.writeJSONFields(b);
      return b;
    }

  }

  public static class NeuralNetModel extends Model {
    static final int API_WEAVER = 1;
    static public DocGen.FieldDoc[] DOC_FIELDS;
    public NeuralNetParams _params;

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

    @API(help = "Mean activation")
    public double[] mean_activation;

    @API(help = "RMS activation")
    public double[] rms_activation;

    @API(help = "Mean bias")
    public double[] mean_bias;

    @API(help = "RMS bias")
    public double[] rms_bias;

    @API(help = "Mean weight")
    public double[] mean_weight;

    @API(help = "RMS weight")
    public double[] rms_weight;

    @API(help = "Mean error")
    public double[] mean_error;

    @API(help = "RMS error")
    public double[] rms_error;

    public boolean unstable;

    NeuralNetModel(Key selfKey, Key dataKey, Frame fr, Layer[] ls, NeuralNetParams p) {
      super(selfKey, dataKey, fr);
      _params = p;
      layers = ls;
      weights = new float[ls.length][];
      biases = new float[ls.length][];
      for( int y = 1; y < layers.length-1; y++ ) {
        weights[y] = layers[y]._w;
        biases[y] = layers[y]._b;
      }

      if (_params.diagnostics) {
        // compute stats on all nodes
        mean_activation = new double[ls.length];
        rms_activation = new double[ls.length];
        mean_bias = new double[ls.length];
        rms_bias = new double[ls.length];
        mean_weight = new double[ls.length];
        rms_weight = new double[ls.length];
        mean_error = new double[ls.length];
        rms_error = new double[ls.length];
        for( int y = 1; y < layers.length-1; y++ ) {
          final Layer l = layers[y];
          final int len = l._a.length;

          // compute mean values
          mean_activation[y] = rms_activation[y] = 0;
          mean_bias[y] = rms_bias[y] = 0;
          mean_weight[y] = rms_weight[y] = 0;
          mean_error[y] = rms_error[y] = 0;
          for(int u = 0; u < len; u++) {
            mean_activation[y] += l._a[u];
            mean_bias[y] += l._b[u];
            mean_error[y] += l._e[u];
            for( int i = 0; i < l._previous._a.length; i++ ) {
              int w = u * l._previous._a.length + i;
              mean_weight[y] += l._w[w];
            }
          }

          mean_activation[y] /= len;
          mean_bias[y] /= len;
          mean_error[y] /= len;
          mean_weight[y] /= len * l._previous._a.length;

          // compute rms values
          for(int u = 0; u < len; ++u) {
            final double da = l._a[u] - mean_activation[y];
            rms_activation[y] += da * da;
            final double db = l._b[u] - mean_bias[y];
            rms_bias[y] += db * db;
            final double de = l._e[u] - mean_error[y];
            rms_error[y] += de * de;

            for( int i = 0; i < l._previous._a.length; i++ ) {
              int w = u * l._previous._a.length + i;
              final double dw = l._w[w] - mean_weight[y];
              rms_weight[y] += dw * dw;
            }

          }
          rms_activation[y] = Math.sqrt(rms_activation[y]/len);
          rms_bias[y] = Math.sqrt(rms_bias[y]/len);
          rms_error[y] = Math.sqrt(rms_error[y]/len);
          rms_weight[y] = Math.sqrt(rms_weight[y]/len/l._previous._a.length);
          // Double.NaN will trigger when Float.NaN is reached
          unstable = Double.isNaN(mean_activation[y]) || Double.isNaN(rms_activation[y])
                  || Double.isNaN(mean_bias[y])       || Double.isNaN(rms_bias[y])
                  || Double.isNaN(mean_weight[y])     || Double.isNaN(rms_weight[y])
                  || Double.isNaN(mean_error[y])      || Double.isNaN(rms_error[y]);
        }
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
        clones[y].init(clones, y, false, 0);
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

    public NeuralNetParams _params;

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
        _params = job._params;
      }
      NeuralNetModel model = UKV.get(destination_key);
      if( model != null ) {
        training_errors = model.training_errors;
        validation_errors = model.validation_errors;
        class_names = model.classNames();
        confusion_matrix = model.confusion_matrix;
        if (model.unstable && job != null) job.cancel();
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
        if (_params != null && _params.diagnostics) {
          DocGen.HTML.section(sb, "Hidden Layer Status");
          sb.append("<table class='table table-striped table-bordered table-condensed'>");
          sb.append("<tr>");
          sb.append("<th>").append("#").append("</th>");
          sb.append("<th>").append("Units").append("</th>");
          sb.append("<th>").append("Activation").append("</th>");
          sb.append("<th>").append("Rate").append("</th>");
          sb.append("<th>").append("L1").append("</th>");
          sb.append("<th>").append("L2").append("</th>");
          sb.append("<th>").append("Momentum").append("</th>");
          sb.append("<th>").append("Activation (Mean, RMS)").append("</th>");
          sb.append("<th>").append("Weight (Mean, RMS)").append("</th>");
          sb.append("<th>").append("Bias (Mean, RMS)").append("</th>");
          sb.append("<th>").append("Error (Mean, RMS)").append("</th>");
          sb.append("</tr>");
          for (int i=1; i<model.layers.length-1; ++i) {
            sb.append("<tr>");
            sb.append("<td>").append("<b>").append(i).append("</b>").append("</td>");
            sb.append("<td>").append("<b>").append(model.layers[i].units).append("</b>").append("</td>");
            sb.append("<td>").append(model.layers[i].getClass().getCanonicalName().replace("hex.Layer.", "")).append("</td>");
            sb.append("<td>").append(model.layers[i].rate(train.training_samples)).append("</td>");
            sb.append("<td>").append(model.layers[i].l1).append("</td>");
            sb.append("<td>").append(model.layers[i].l2).append("</td>");
            final String format = "%2.5f";
            sb.append("<td>").append(model.layers[i].momentum(train.training_samples)).append("</td>");
            sb.append("<td>(").append(String.format(format, model.mean_activation[i])).
                    append(", ").append(String.format(format, model.rms_activation[i])).append(")</td>");
            sb.append("<td>(").append(String.format(format, model.mean_weight[i])).
                    append(", ").append(String.format(format, model.rms_weight[i])).append(")</td>");
            sb.append("<td>(").append(String.format(format, model.mean_bias[i])).
                    append(", ").append(String.format(format, model.rms_bias[i])).append(")</td>");
            sb.append("<td>(").append(String.format("%2.7f", model.mean_error[i])).
                    append(", ").append(String.format("%2.7f", model.rms_error[i])).append(")</td>");
            sb.append("</tr>");
          }
          DocGen.HTML.arrayTail(sb);
          if (model.unstable) {
            DocGen.HTML.section(sb, "### Note:  Instability detected and job aborted.  Try a smaller learning rate and/or single-node mode. ###") ;
          }
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
