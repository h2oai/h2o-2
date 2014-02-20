package hex.nn;

import hex.FrameTask.DataInfo;
import water.*;
import water.api.ConfusionMatrix;
import water.api.DocGen;
import water.api.Inspect2;
import water.api.Request.API;
import water.fvec.Frame;
import water.util.D3Plot;
import water.util.Log;
import water.util.Utils;

import java.util.Arrays;
import java.util.Random;

public class NNModel extends Model {
  static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  @API(help="Model info", json = true)
  private volatile NNModelInfo model_info;
  void set_model_info(NNModelInfo mi) { model_info = mi; }
  final public NNModelInfo model_info() { return model_info; }

  @API(help="Job that built the model", json = true)
  private Key jobKey;

  @API(help="Time to build the model", json = true)
  private long run_time;
  private long start_time;

  @API(help="Number of training epochs", json = true)
  public double epoch_counter;

  @API(help = "Scoring during model building")
  public Errors[] errors;

  @Override public void delete() {
    super.delete();
    model_info.cleanUp();
  }

  public static class Errors extends Iced {
    static final int API_WEAVER = 1;
    static public DocGen.FieldDoc[] DOC_FIELDS;

    @API(help = "How many epochs the algorithm has processed")
    public double epoch_counter;
    @API(help = "How many rows the algorithm has processed")
    public long training_samples;
    @API(help = "How long the algorithm ran in ms")
    public long training_time_ms;
    @API(help = "Classification error on training data")
    public double train_err = 1;
    @API(help = "Whether a validation set was provided")
    boolean validation;
    @API(help = "Classification error on validation data")
    public double valid_err = 1;
    @API(help = "Training MSE")
    public double train_mse = Double.POSITIVE_INFINITY;
    @API(help = "Validation MSE")
    public double valid_mse = Double.POSITIVE_INFINITY;
//    @API(help = "Training MCE")
//    public double train_mce = Double.POSITIVE_INFINITY;
//    @API(help = "Validation MCE")
//    public double valid_mce = Double.POSITIVE_INFINITY;
    @API(help = "Confusion matrix on training data")
    public water.api.ConfusionMatrix train_confusion_matrix;
    @API(help = "Confusion matrix on validation data")
    public water.api.ConfusionMatrix valid_confusion_matrix;

    @Override public String toString() {
      String s = "Training misclassification: " + String.format("%.2f", (100 * train_err)) + "%";
      if (validation) s += ", validation misclassification: " + String.format("%.2f", (100 * valid_err)) + "%";
      return s;
    }
  }

  /** for grid search error reporting */
  @Override
  public hex.ConfusionMatrix cm() {
    if (errors == null) return null;
    water.api.ConfusionMatrix cm = errors[errors.length-1].validation ?
            errors[errors.length-1].valid_confusion_matrix :
            errors[errors.length-1].train_confusion_matrix;
    if (cm == null) return null;
    return new hex.ConfusionMatrix(cm.cm);
  }

  // This describes the model, together with the parameters
  // This will be shared: one per node
  public static class NNModelInfo extends Iced {
    static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
    static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

    @API(help="Input data info")
    final private DataInfo data_info;
    public DataInfo data_info() { return data_info; }

    // model is described by parameters and the following 2 arrays
    final private float[][] weights; //one 2D weight matrix per layer (stored as a 1D array each)
    final private double[][] biases; //one 1D bias array per layer

    // helpers for storing previous step deltas
    // Note: These two arrays *could* be made transient and then initialized freshly in makeNeurons() and in NNTask.initLocal()
    // But then, after each reduction, the weights would be lost and would have to restart afresh -> not *exactly* right, but close...
    private float[][] weights_momenta;
    private double[][] biases_momenta;

    // compute model size (number of model parameters required for making predictions)
    // momenta are not counted here, but they are needed for model building
    public long size() {
      long siz = 0;
      for (float[] w : weights) siz += w.length;
      for (double[] b : biases) siz += b.length;
      return siz;
    }

    // accessors to (shared) weights and biases - those will be updated racily (c.f. Hogwild!)
    final boolean _has_momenta;
    boolean has_momenta() { return _has_momenta; }
    public final float[] get_weights(int i) { return weights[i]; }
    public final double[] get_biases(int i) { return biases[i]; }
    public final float[] get_weights_momenta(int i) { return weights_momenta[i]; }
    public final double[] get_biases_momenta(int i) { return biases_momenta[i]; }

    @API(help = "Model parameters", json = true)
    final private NN parameters;
    public final NN get_params() { return parameters; }
    public final NN job() { return get_params(); }

    @API(help = "Mean bias", json = true)
    private double[] mean_bias;

    @API(help = "RMS bias", json = true)
    private double[] rms_bias;

    @API(help = "Mean weight", json = true)
    private double[] mean_weight;

    @API(help = "RMS weight", json = true)
    public double[] rms_weight;

    @API(help = "Unstable", json = true)
    private boolean unstable = false;
    public boolean unstable() { return unstable; }

    @API(help = "Processed samples", json = true)
    private long processed_global;
    public synchronized long get_processed_global() { return processed_global; }
    public synchronized void set_processed_global(long p) { processed_global = p; }
    public synchronized void add_processed_global(long p) { processed_global += p; }

    private long processed_local;
    public synchronized long get_processed_local() { return processed_local; }
    public synchronized void set_processed_local(long p) { processed_local = p; }
    public synchronized void add_processed_local(long p) { processed_local += p; }

    public synchronized long get_processed_total() { return processed_global + processed_local; }

    // package local helpers
    final int[] units; //number of neurons per layer, extracted from parameters and from datainfo

//    public NNModelInfo(NN params, int num_input, int num_output) {
    public NNModelInfo(NN params, DataInfo dinfo) {
      data_info = dinfo; //should be deep_clone()?
      final int num_input = dinfo.fullN();
      final int num_output = params.classification ? dinfo._adaptedFrame.lastVec().domain().length : 1;
      assert(num_input > 0);
      assert(num_output > 0);
      parameters = params;
      _has_momenta = ( parameters.momentum_start != 0 || parameters.momentum_stable != 0 );
      final int layers=parameters.hidden.length;
      // units (# neurons for each layer)
      units = new int[layers+2];
      units[0] = num_input;
      System.arraycopy(parameters.hidden, 0, units, 1, layers);
      units[layers+1] = num_output;
      // weights (to connect layers)
      weights = new float[layers+1][];
      for (int i=0; i<=layers; ++i) weights[i] = new float[units[i]*units[i+1]];
      // biases (only for hidden layers and output layer)
      biases = new double[layers+1][];
      for (int i=0; i<=layers; ++i) biases[i] = new double[units[i+1]];
      createMomenta();
      // for diagnostics
      mean_bias = new double[units.length];
      rms_bias = new double[units.length];
      mean_weight = new double[units.length];
      rms_weight = new double[units.length];
    }

    protected void createMomenta() {
      if (has_momenta() && weights_momenta == null) {
        weights_momenta = new float[weights.length][];
        for (int i=0; i<weights_momenta.length; ++i) weights_momenta[i] = new float[units[i]*units[i+1]];
        biases_momenta = new double[biases.length][];
        for (int i=0; i<biases_momenta.length; ++i) biases_momenta[i] = new double[units[i+1]];
      }
    }
    public void cleanUp() {
      // ugly: whoever made data_info should also clean this up... but sometimes it was made by Weaver from UKV!
      UKV.remove(data_info()._adaptedFrame.lastVec()._key);
    }

    @Override public String toString() {
      StringBuilder sb = new StringBuilder();
      if (parameters.diagnostics) {
        Neurons[] neurons = NNTask.makeNeuronsForTesting(this);
        computeStats();
        sb.append("Status of Neuron Layers:\n");
        sb.append("#  Units         Type      Dropout      Rate      L1       L2    Momentum     Weight (Mean, RMS)      Bias (Mean,RMS)\n");
        final String format = "%7g";
        for (int i=0; i<neurons.length; ++i) {
          sb.append((i+1) + " " + String.format("%6d", neurons[i].units)
                  + " " + String.format("%16s", neurons[i].getClass().getSimpleName()));
          if (i == 0) {
            sb.append("  " + String.format("%.5g", neurons[i].params.input_dropout_ratio*100) + "%\n");
            continue;
          }
          else if (i < neurons.length-1) {
            sb.append( neurons[i] instanceof Neurons.TanhDropout
                    || neurons[i] instanceof Neurons.RectifierDropout
                    || neurons[i] instanceof Neurons.MaxoutDropout ? "    50%   " : "     0%   ");
          } else {
            sb.append("          ");
          }
          sb.append(
                  " " + String.format("%10g", neurons[i].rate(get_processed_total()))
                  + " " + String.format("%5f", neurons[i].params.l1)
                  + " " + String.format("%5f", neurons[i].params.l2)
                  + " " + String.format("%5f", neurons[i].momentum(get_processed_total()))
                  + " (" + String.format(format, mean_weight[i])
                  + ", " + String.format(format, rms_weight[i]) + ")"
                  + " (" + String.format(format, mean_bias[i])
                  + ", " + String.format(format, rms_bias[i]) + ")\n");
        }
      }

      // DEBUGGING
//      for (int i=0; i<weights.length; ++i)
//        sb.append("\nweights["+i+"][]="+Arrays.toString(weights[i]));
//      for (int i=0; i<biases.length; ++i)
//        sb.append("\nbiases["+i+"][]="+Arrays.toString(biases[i]));
////      if (weights_momenta != null) {
////        for (int i=0; i<weights_momenta.length; ++i)
////          sb.append("\nweights_momenta["+i+"][]="+Arrays.toString(weights_momenta[i]));
////      }
////      if (biases_momenta != null) {
////        for (int i=0; i<biases_momenta.length; ++i)
////          sb.append("\nbiases_momenta["+i+"][]="+Arrays.toString(biases_momenta[i]));
////      }
////      sb.append("\nunits[]="+Arrays.toString(units));
////      sb.append("\nprocessed global: "+get_processed_global());
////      sb.append("\nprocessed local:  "+get_processed_local());
////      sb.append("\nprocessed total:  " + get_processed_total());
//      sb.append("\n");
      return sb.toString();
    }
    void initializeMembers() {
      randomizeWeights();
      //TODO: determine good/optimal/best initialization scheme for biases
      // hidden layers
      for (int i=0; i<parameters.hidden.length; ++i) {
        if (parameters.activation == NN.Activation.Rectifier
                || parameters.activation == NN.Activation.RectifierWithDropout
                || parameters.activation == NN.Activation.Maxout
                || parameters.activation == NN.Activation.MaxoutWithDropout
                ) {
//          Arrays.fill(biases[i], 1.); //old behavior
          Arrays.fill(biases[i], i == 0 ? 0.5 : 1.); //new behavior, might be slightly better
        }
        else if (parameters.activation == NN.Activation.Tanh || parameters.activation == NN.Activation.TanhWithDropout) {
          Arrays.fill(biases[i], 0.0);
        }
      }
      Arrays.fill(biases[biases.length-1], 0.0); //output layer
    }
    public void add(NNModelInfo other) {
      Utils.add(weights, other.weights);
      Utils.add(biases,  other.biases);
      if (has_momenta()) {
        assert(other.has_momenta());
        Utils.add(weights_momenta, other.weights_momenta);
        Utils.add(biases_momenta,  other.biases_momenta);
      }
      add_processed_local(other.get_processed_local());
    }
    protected void div(double N) {
      for (float[] weight : weights) Utils.div(weight, (float) N);
      for (double[] bias : biases) Utils.div(bias, N);
      if (has_momenta()) {
        for (float[] weight_momenta : weights_momenta) Utils.div(weight_momenta, (float) N);
        for (double[] bias_momenta : biases_momenta) Utils.div(bias_momenta, N);
      }
    }
    double uniformDist(Random rand, double min, double max) {
      return min + rand.nextFloat() * (max - min);
    }
    void randomizeWeights() {
      for (int i=0; i<weights.length; ++i) {
        final Random rng = water.util.Utils.getDeterRNG(get_params().seed + 0xBAD5EED + i+1); //to match NeuralNet behavior
        for( int j = 0; j < weights[i].length; j++ ) {
          if (parameters.initial_weight_distribution == NN.InitialWeightDistribution.UniformAdaptive) {
            // cf. http://machinelearning.wustl.edu/mlpapers/paper_files/AISTATS2010_GlorotB10.pdf
            final double range = Math.sqrt(6. / (units[i] + units[i+1]));
            weights[i][j] = (float)uniformDist(rng, -range, range);
            if (i==weights.length-1 && parameters.classification) weights[i][j] *= 4; //Softmax might need an extra factor 4, since it's like a sigmoid
          }
          else if (parameters.initial_weight_distribution == NN.InitialWeightDistribution.Uniform) {
            weights[i][j] = (float)uniformDist(rng, -parameters.initial_weight_scale, parameters.initial_weight_scale);
          }
          else if (parameters.initial_weight_distribution == NN.InitialWeightDistribution.Normal) {
            weights[i][j] = (float)(rng.nextGaussian() * parameters.initial_weight_scale);
          }
        }
      }
    }

    // TODO: Add "subset randomize" function
//        int count = Math.min(15, _previous.units);
//        double min = -.1f, max = +.1f;
//        //double min = -1f, max = +1f;
//        for( int o = 0; o < units; o++ ) {
//          for( int n = 0; n < count; n++ ) {
//            int i = rand.nextInt(_previous.units);
//            int w = o * _previous.units + i;
//            _w[w] = uniformDist(rand, min, max);
//          }
//        }

    // compute stats on all nodes
    public void computeStats() {
      for( int y = 1; y < units.length; y++ ) {
        mean_bias[y] = rms_bias[y] = 0;
        mean_weight[y] = rms_weight[y] = 0;
        for(int u = 0; u < biases[y-1].length; u++) {
          mean_bias[y] += biases[y-1][u];
        }
        for(int u = 0; u < weights[y-1].length; u++) {
          mean_weight[y] += weights[y-1][u];
        }
        mean_bias[y] /= biases[y-1].length;
        mean_weight[y] /= weights[y-1].length;

        for(int u = 0; u < biases[y-1].length; u++) {
          final double db = biases[y-1][u] - mean_bias[y];
          rms_bias[y] += db * db;
        }
        for(int u = 0; u < weights[y-1].length; u++) {
          final double dw = weights[y-1][u] - mean_weight[y];
          rms_weight[y] += dw * dw;
        }
        rms_bias[y] = Math.sqrt(rms_bias[y]/biases[y-1].length);
        rms_weight[y] = Math.sqrt(rms_weight[y]/weights[y-1].length);

        unstable |= Double.isNaN(mean_bias[y])  || Double.isNaN(rms_bias[y])
                || Double.isNaN(mean_weight[y]) || Double.isNaN(rms_weight[y]);

        // Abort the run if weights or biases are unreasonably large (Note that all input values are normalized upfront)
        // This can happen with Rectifier units when L1/L2/max_w2 are all set to 0, especially when using more than 1 hidden layer.
        final double thresh = 1e10;
        unstable |= mean_bias[y] > thresh  || Double.isNaN(mean_bias[y])
                || rms_bias[y] > thresh    || Double.isNaN(rms_bias[y])
                || mean_weight[y] > thresh || Double.isNaN(mean_weight[y])
                || rms_weight[y] > thresh  || Double.isNaN(rms_weight[y]);
      }
    }
  }

  public NNModel(Key selfKey, Key jobKey, Key dataKey, DataInfo dinfo, NN params) {
    super(selfKey, dataKey, dinfo._adaptedFrame);
    this.jobKey = jobKey;
    run_time = 0;
    start_time = System.currentTimeMillis();
    model_info = new NNModelInfo(params, dinfo);
    errors = new Errors[1];
    errors[0] = new Errors();
    errors[0].validation = (params.validation != null);
  }

  transient long _now, _timeLastScoreStart, _timeLastPrintStart;
  /**
   *
   * @param ftrain potentially downsampled training data for scoring
   * @param ftest  potentially downsampled validation data for scoring
   * @param timeStart start time in milliseconds, used to report training speed
   * @param dest_key where to store the model with the diagnostics in it
   * @return true if model building is ongoing
   */
  boolean doDiagnostics(Frame ftrain, Frame ftest, long timeStart, Key dest_key) {
    epoch_counter = (float)model_info().get_processed_total()/model_info().data_info._adaptedFrame.numRows();
    run_time = (System.currentTimeMillis()-start_time);
    boolean keep_running = (epoch_counter < model_info().parameters.epochs);
    _now = System.currentTimeMillis();
    final long sinceLastScore = _now-_timeLastScoreStart;
    final long sinceLastPrint = _now-_timeLastPrintStart;
    final long samples = model_info().get_processed_total();
    if (sinceLastPrint > model_info().parameters.score_interval*1000) {
      _timeLastPrintStart = _now;
      Log.info("Training time: " + PrettyPrint.msecs(_now - start_time, true)
              + " processed " + samples + " samples" + " (" + String.format("%.3f", epoch_counter) + " epochs)."
              + " Speed: " + String.format("%.3f", (double)samples/((_now - start_time)/1000.)) + " samples/sec.");
    }
    // this is potentially slow - only do every so often
    if( !keep_running || sinceLastScore > model_info().parameters.score_interval*1000) {
      Log.info("Scoring the model.");
      _timeLastScoreStart = _now;
      boolean printCM = false;
      // compute errors
      Errors err = new Errors();
      err.training_time_ms = _now - timeStart;
      err.epoch_counter = epoch_counter;
      err.validation = ftest != null;
      err.training_samples = model_info().get_processed_total();
      err.train_confusion_matrix = new ConfusionMatrix();
      err.train_err = calcError(ftrain, "Error on training data:", printCM, err.train_confusion_matrix);
      if (err.validation) {
        err.valid_confusion_matrix = new ConfusionMatrix();
        err.valid_err = calcError(ftest, "Error on validation data:", printCM, err.valid_confusion_matrix);
      }
      // enlarge the error array by one, push latest score back
      if (errors == null) {
         errors = new Errors[]{err};
      } else {
        Errors[] err2 = new Errors[errors.length+1];
        System.arraycopy(errors, 0, err2, 0, errors.length);
        err2[err2.length-1] = err;
        errors = err2;
      }
      // print the freshly scored model to ASCII
      for (String s : toString().split("\n")) Log.info(s);
      Log.info("Scoring time: " + PrettyPrint.msecs(System.currentTimeMillis() - _now, true));
    }
    if (model_info().unstable()) {
      Log.err("Canceling job since the model is unstable (exponential growth observed).");
      Log.err("Try a bounded activation function or regularization with L1, L2 or max_w2 and/or use a smaller learning rate or faster annealing.");
      keep_running = false;
    } else if (ftest == null && (model_info().parameters.classification && errors[errors.length-1].train_err == 0) ) {
      Log.info("Achieved 100% classification accuracy on the training data. We are done here.");
      keep_running = false;
    } else if (ftest != null && (model_info().parameters.classification && errors[errors.length-1].valid_err == 0) ) {
      Log.info("Achieved 100% classification accuracy on the validation data. We are done here.");
      keep_running = false;
    }
    update(dest_key); //update model in UKV
//    System.out.println(this);
    return keep_running;
  }

  @Override public String toString() {
    StringBuilder sb = new StringBuilder();
//    sb.append(super.toString());
//    sb.append("\n"+data_info.toString()); //not implemented yet
    sb.append(model_info.toString());
    sb.append(errors[errors.length-1].toString());
//    sb.append("\nrun time: " + PrettyPrint.msecs(run_time, true));
//    sb.append("\nepoch counter: " + epoch_counter);
    return sb.toString();
  }

  @Override public float[] score0(double[] data, float[] preds) {
    Neurons[] neurons = NNTask.makeNeuronsForTesting(model_info);
    ((Neurons.Input)neurons[0]).setInput(-1, data);
    NNTask.step(-1, neurons, model_info, false, null);
    double[] out = neurons[neurons.length - 1]._a;
    for (int i=0; i<out.length; ++i) preds[i+1] = (float)out[i];
    preds[0] = getPrediction(preds, data);
    return preds;
  }

  public double calcError(Frame ftest, String label, boolean printCM, ConfusionMatrix CM) {
    Frame fpreds;
    fpreds = score(ftest);
    if (CM == null) CM = new ConfusionMatrix();
    CM.actual = ftest;
    CM.vactual = ftest.lastVec();
    CM.predict = fpreds;
    CM.vpredict = fpreds.vecs()[0];
    CM.serve();
    StringBuilder sb = new StringBuilder();
    final double error = CM.toASCII(sb);
    if (printCM) {
      Log.info(label);
      for (String s : sb.toString().split("\n")) Log.info(s);
    }
    fpreds.delete();
    return error;
  }

  public boolean generateHTML(String title, StringBuilder sb) {
    if (_key == null) {
      DocGen.HTML.title(sb, "No model yet");
      return true;
    }

    final String mse_format = "%2.6f";
    final String cross_entropy_format = "%2.6f";

    DocGen.HTML.title(sb, title);
    DocGen.HTML.paragraph(sb, "Model type: " + (model_info().parameters.classification ? " Classification" : " Regression"));
    DocGen.HTML.paragraph(sb, "Model Key: " + _key);
    DocGen.HTML.paragraph(sb, "Job Key: " + jobKey);
    Inspect2 is2 = new Inspect2();
    DocGen.HTML.paragraph(sb, "Training Data Key: " + _dataKey);
    if (model_info.parameters.validation != null) {
      DocGen.HTML.paragraph(sb, "Validation Data Key: " + model_info.parameters.validation._key);
    }
    DocGen.HTML.paragraph(sb, "Number of model parameters (weights/biases): " + String.format("%,d", model_info().size()));

    model_info.job().toHTML(sb);
    sb.append("<div class='alert'>Actions: "
            + is2.link("Inspect training data", _dataKey) + ", "
            + (model_info().parameters.validation != null ? (is2.link("Inspect validation data", model_info().parameters.validation._key) + ", ") : "")
            + water.api.Predict.link(_key, "Score on dataset") + ", " +
            NN.link(_dataKey, "Compute new model") + "</div>");

    // stats for training and validation
    final Errors error = errors[errors.length - 1];
    assert(error != null);

    if (errors.length > 1) {
      if (isClassifier()) {
        // Plot training error
        float[] err = new float[errors.length];
        float[] samples = new float[errors.length];
        for (int i=0; i<err.length; ++i) {
          err[i] = (float)errors[i].train_err;
          samples[i] = errors[i].training_samples;
        }
        new D3Plot(samples, err, "training samples", "classification error",
                "Classification Error on Training Set").generate(sb);

        // Plot validation error
        if (model_info.parameters.validation != null) {
          for (int i=0; i<err.length; ++i) {
            err[i] = (float)errors[i].valid_err;
          }
          new D3Plot(samples, err, "training samples", "classification error",
                  "Classification Error on Validation Set").generate(sb);
        }
      } else {
        // Plot training MSE
        float[] err = new float[errors.length];
        float[] samples = new float[errors.length];
        for (int i=0; i<err.length; ++i) {
          err[i] = (float)errors[i].train_mse;
          samples[i] = errors[i].training_samples;
        }
        new D3Plot(samples, err, "training samples", "mean squared error",
                "Regression Error on Training Set").generate(sb);

        // Plot validation MSE
        if (model_info.parameters.validation != null) {
          for (int i=0; i<err.length; ++i) {
            err[i] = (float)errors[i].valid_mse;
          }
          new D3Plot(samples, err, "training samples", "mean squared error",
                  "Regression Error on Validation Set").generate(sb);
        }
      }
    }

    if (isClassifier()) {
      DocGen.HTML.section(sb, "Training classification error: " + formatPct(error.train_err));
//      DocGen.HTML.section(sb, "Training cross entropy: " + String.format(cross_entropy_format, error.train_mce));
      if(error.validation) {
        DocGen.HTML.section(sb, "Validation classification error: " + formatPct(error.valid_err));
//        DocGen.HTML.section(sb, "Validation mean cross entropy: " + String.format(cross_entropy_format, error.valid_mce));
      }
    } else {
      DocGen.HTML.section(sb, "Training mean square error: " + String.format(mse_format, error.train_mse));
      if(error.validation) {
        DocGen.HTML.section(sb, "Validation mean square error: " + String.format(mse_format, error.valid_mse));
      }
    }
    if (error.training_time_ms > 0)
      DocGen.HTML.section(sb, "Training speed: " + error.training_samples * 1000 / error.training_time_ms + " samples/s");
    if (model_info.parameters != null && model_info.parameters.diagnostics) {
      DocGen.HTML.section(sb, "Status of Neuron Layers");
      sb.append("<table class='table table-striped table-bordered table-condensed'>");
      sb.append("<tr>");
      sb.append("<th>").append("#").append("</th>");
      sb.append("<th>").append("Units").append("</th>");
      sb.append("<th>").append("Type").append("</th>");
      sb.append("<th>").append("Dropout").append("</th>");
      sb.append("<th>").append("Rate").append("</th>");
      sb.append("<th>").append("L1").append("</th>");
      sb.append("<th>").append("L2").append("</th>");
      sb.append("<th>").append("Momentum").append("</th>");
      sb.append("<th>").append("Weight (Mean, RMS)").append("</th>");
      sb.append("<th>").append("Bias (Mean, RMS)").append("</th>");
      sb.append("</tr>");
      Neurons[] neurons = NNTask.makeNeuronsForTesting(model_info()); //link the weights to the neurons, for easy access
      for (int i=0; i<neurons.length; ++i) {
        sb.append("<tr>");
        sb.append("<td>").append("<b>").append(i+1).append("</b>").append("</td>");
        sb.append("<td>").append("<b>").append(neurons[i].units).append("</b>").append("</td>");
        sb.append("<td>").append(neurons[i].getClass().getSimpleName()).append("</td>");

        if (i == 0) {
          sb.append("<td>");
          sb.append(formatPct(neurons[i].params.input_dropout_ratio));
          sb.append("</td>");
          sb.append("<td></td>");
          sb.append("<td></td>");
          sb.append("<td></td>");
          sb.append("<td></td>");
          sb.append("<td></td>");
          sb.append("<td></td>");
          continue;
        }
        else if (i < neurons.length-1) {
          sb.append("<td>");
          sb.append( neurons[i] instanceof Neurons.TanhDropout
                  || neurons[i] instanceof Neurons.RectifierDropout
                  || neurons[i] instanceof Neurons.MaxoutDropout ? "50%" : "0%");
          sb.append("</td>");
        } else {
          sb.append("<td></td>");
        }

        sb.append("<td>").append(String.format("%.5g", neurons[i].rate(error.training_samples))).append("</td>");
       sb.append("<td>").append(neurons[i].params.l1).append("</td>");
        sb.append("<td>").append(neurons[i].params.l2).append("</td>");
        final String format = "%g";
        sb.append("<td>").append(String.format("%.5f", neurons[i].momentum(error.training_samples))).append("</td>");
        sb.append("<td>(").append(String.format(format, model_info.mean_weight[i])).
                append(", ").append(String.format(format, model_info.rms_weight[i])).append(")</td>");
        sb.append("<td>(").append(String.format(format, model_info.mean_bias[i])).
                append(", ").append(String.format(format, model_info.rms_bias[i])).append(")</td>");
        sb.append("</tr>");
      }
      sb.append("</table>");
    }
    if (model_info.unstable()) {
      final String msg = "Job was aborted due to observed numerical instability (exponential growth)."
              + " Try a bounded activation function or regularization with L1, L2 or max_w2 and/or use a smaller learning rate or faster annealing.";
      DocGen.HTML.section(sb, "=======================================================================================");
      DocGen.HTML.section(sb, msg);
      DocGen.HTML.section(sb, "=======================================================================================");
    }
    long score_valid = model_info().get_params().score_validation_samples;
    long score_train = model_info().get_params().score_training_samples;
    final String cmTitle = "Confusion Matrix on " + (error.validation ?
            "Validation Data" + (score_valid==0 ? "" : " (~" + score_valid + " samples)")
            : "Training Data" + (score_train==0 ? "" : " (~" + score_train + " samples)"));
    DocGen.HTML.section(sb, cmTitle);
    if (error.train_confusion_matrix != null) {
      if (error.train_confusion_matrix.cm.length < 100) {
        if (error.validation && error.valid_confusion_matrix != null) error.valid_confusion_matrix.toHTML(sb);
        else if (error.train_confusion_matrix != null) error.train_confusion_matrix.toHTML(sb);
      } else sb.append("<h5>Not shown here (too large).</h5>");
    }
    else sb.append("<h5>Not yet computed.</h5>");

    sb.append("<h3>" + "Progress" + "</h3>");
    sb.append("<h4>" + "Epochs: " + String.format("%.3f", epoch_counter) + "</h4>");

    String training = "Number of training set samples for scoring: " + (score_train == 0 ? "all" : score_train);
    if (score_train > 0) {
      if (score_train < 1000) training += " (low, scoring might be inaccurate -> consider increasing this number in the expert mode)";
      if (score_train > 10000) training += " (large, scoring can be slow -> consider reducing this number in the expert mode or scoring manually)";
    }
    DocGen.HTML.section(sb, training);
    if (error.validation) {
      String validation = "Number of validation set samples for scoring: " + (score_valid == 0 ? "all" : score_valid);
      if (score_valid > 0) {
        if (score_valid < 1000) validation += " (low, scoring might be inaccurate -> consider increasing this number in the expert mode)";
        if (score_valid > 10000) validation += " (large, scoring can be slow -> consider reducing this number in the expert mode or scoring manually)";
      }
      DocGen.HTML.section(sb, validation);
    }

//    String training = "Number of training set samples for scoring: " + error.score_training;
    if (error.validation) {
//      String validation = "Number of validation set samples for scoring: " + error.score_validation;
    }
    sb.append("<table class='table table-striped table-bordered table-condensed'>");
    sb.append("<tr>");
    sb.append("<th>Training Time</th>");
    sb.append("<th>Training Epochs</th>");
    sb.append("<th>Training Samples</th>");
    if (isClassifier()) {
//      sb.append("<th>Training MCE</th>");
      sb.append("<th>Training Error</th>");
    } else {
      sb.append("<th>Training MSE</th>");
    }
    if (error.validation) {
      if (isClassifier()) {
//      sb.append("<th>Validation MCE</th>");
        sb.append("<th>Validation Error</th>");
      } else {
        sb.append("<th>Validation MSE</th>");
      }
    }
    sb.append("</tr>");
    for( int i = errors.length - 1; i >= 0; i-- ) {
      final Errors e = errors[i];
      sb.append("<tr>");
      sb.append("<td>" + PrettyPrint.msecs(e.training_time_ms, true) + "</td>");
      sb.append("<td>" + String.format("%g", e.epoch_counter) + "</td>");
      sb.append("<td>" + String.format("%,d", e.training_samples) + "</td>");
      if (isClassifier()) {
//        sb.append("<td>" + String.format(cross_entropy_format, e.train_mce) + "</td>");
        sb.append("<td>" + formatPct(e.train_err) + "</td>");
      } else {
        sb.append("<td>" + String.format(mse_format, e.train_mse) + "</td>");
      }
      if(e.validation) {
        if (isClassifier()) {
//          sb.append("<td>" + String.format(cross_entropy_format, e.valid_mce) + "</td>");
          sb.append("<td>" + formatPct(e.valid_err) + "</td>");
        } else {
          sb.append("<td>" + String.format(mse_format, e.valid_mse) + "</td>");
        }
      }
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

  public boolean toJavaHtml(StringBuilder sb) { return false; }
  @Override public String toJava() { return "Not yet implemented."; }
}

