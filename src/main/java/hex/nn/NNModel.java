package hex.nn;

import hex.FrameTask.DataInfo;
import water.Iced;
import water.Key;
import water.Model;
import water.PrettyPrint;
import water.api.DocGen;
import water.api.Request.API;
import water.fvec.Frame;
import water.util.D3Plot;
import water.util.Log;
import water.util.Utils;

import java.util.Arrays;
import java.util.Random;

import static hex.nn.NN.RNG.getRNG;

public class NNModel extends Model {
  static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  @API(help="Key assigned to the job building this model")
  private final Key job_key;

  @API(help="Input data info")
  final private DataInfo data_info;

  @API(help="Model info", json = true)
  private volatile NNModelInfo model_info;
  void set_model_info(NNModelInfo mi) { model_info = mi; }
  final public NNModelInfo model_info() { return model_info; }

  @API(help="Time to build the model", json = true)
  private long run_time;
  private long start_time;

  @API(help="Number of training epochs", json = true)
  public double epoch_counter;

  @API(help = "Scoring during model building")
  public Errors[] errors;

  public static class Errors extends Iced {
    static final int API_WEAVER = 1;
    static public DocGen.FieldDoc[] DOC_FIELDS;

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
    @API(help = "Training MCE")
    public double train_mce = Double.POSITIVE_INFINITY;
    @API(help = "Validation MCE")
    public double valid_mce = Double.POSITIVE_INFINITY;

//    @API(help = "Number of training set samples for scoring")
//    public long score_training;
//    @API(help = "Number of validation set samples for scoring")
//    public long score_validation;

    @API(help = "Confusion Matrix")
    public water.api.ConfusionMatrix confusion_matrix;

    @Override public String toString() {

      return   "Training error: " + String.format("%.2f", (100 * train_err)) + "%"
           + ", validation error: " + String.format("%.2f", (100 * valid_err)) + "%"
//              + " (MSE:" + String.format("%.2e", mean_square)
//              + ", MCE:" + String.format("%.2e", cross_entropy)
//              + ")"
      ;
    }
  }

  // This describes the model, together with the parameters
  // This will be shared: one per node
  public static class NNModelInfo extends Iced {
    static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
    static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

    // model is described by parameters and the following 4 arrays
    //TODO: check impact of making these volatile
    final private float[][] weights; //one 2D weight matrix per layer (stored as a 1D array each)
    final private double[][] biases; //one 1D bias array per layer
    private float[][] weights_momenta;
    private double[][] biases_momenta;

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
    private double[] rms_weight;

    @API(help = "Unstable", json = true)
    private boolean unstable = false;
    public boolean unstable() { return unstable; }

    @API(help = "Processed samples", json = true)
    private long processed_global;
    public synchronized void set_processed_global(long p) { processed_global = p; }
    public synchronized long get_processed_global() { return processed_global; }
    public synchronized void add_processed_global(long p) { processed_global += p; }

    private long processed_local;
    public synchronized long get_processed_local() { return processed_local; }
    public synchronized void set_processed_local(long p) { processed_local = p; }
    public synchronized void add_processed_local(long p) { processed_local += p; }

    public synchronized long get_processed_total() { return processed_global + processed_local; }

    // package local helpers
    final int[] units; //number of neurons per layer, extracted from parameters and from datainfo

    public NNModelInfo(NN params, int num_input, int num_output) {
      assert(num_input > 0);
      assert(num_output > 0);
      parameters = params;
      _has_momenta = ( parameters.momentum_start != 0 || parameters.momentum_stable != 0 );
      final int layers=parameters.hidden.length;
      if (!parameters.classification) assert(num_output == 1); else assert(num_output > 1);
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
      if (has_momenta()) {
        weights_momenta = new float[layers+1][];
        for (int i=0; i<=layers; ++i) weights_momenta[i] = new float[units[i]*units[i+1]];
        biases_momenta = new double[layers+1][];
        for (int i=0; i<=layers; ++i) biases_momenta[i] = new double[units[i+1]];
      }
      // for diagnostics
      mean_bias = new double[units.length];
      rms_bias = new double[units.length];
      mean_weight = new double[units.length];
      rms_weight = new double[units.length];
    }
    public NNModelInfo(NNModelInfo other) {
      this(other.parameters, other.units[0], other.units[other.units.length-1]);
      set_processed_local(other.get_processed_local());
      set_processed_global(other.get_processed_global());
      for (int i=0; i<other.weights.length; ++i)
        weights[i] = other.weights[i].clone();
      for (int i=0; i<other.biases.length; ++i)
        biases[i] = other.biases[i].clone();
      if (has_momenta()) {
        for (int i=0; i<other.weights_momenta.length; ++i)
          weights_momenta[i] = other.weights_momenta[i].clone();
        for (int i=0; i<other.biases_momenta.length; ++i)
          biases_momenta[i] = other.biases_momenta[i].clone();
      }
      mean_bias = other.mean_bias.clone();
      rms_bias = other.rms_bias.clone();
      mean_weight = other.mean_weight.clone();
      rms_weight = other.rms_weight.clone();
      unstable = other.unstable;
    }
    @Override public String toString() {
      StringBuilder sb = new StringBuilder();
      for (int i=0; i<weights.length; ++i)
        sb.append("\nweights["+i+"][]="+Arrays.toString(weights[i]));
      for (int i=0; i<biases.length; ++i)
        sb.append("\nbiases["+i+"][]="+Arrays.toString(biases[i]));
      if (weights_momenta != null) {
        for (int i=0; i<weights_momenta.length; ++i)
          sb.append("\nweights_momenta["+i+"][]="+Arrays.toString(weights_momenta[i]));
      }
      if (biases_momenta != null) {
        for (int i=0; i<biases_momenta.length; ++i)
          sb.append("\nbiases_momenta["+i+"][]="+Arrays.toString(biases_momenta[i]));
      }
      sb.append("\nunits[]="+Arrays.toString(units));
      sb.append("\nprocessed global: "+get_processed_global());
      sb.append("\nprocessed local:  "+get_processed_local());
      sb.append("\nprocessed total:  " + get_processed_total());
      return sb.toString();
    }
    void initializeMembers() {
      randomizeWeights();
      for (int i=0; i<=parameters.hidden.length; ++i) {
        if (parameters.activation == NN.Activation.Rectifier
                || parameters.activation == NN.Activation.Maxout) {
          Arrays.fill(biases[i], 1.);
        }
      }

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
        final Random rng = getRNG(); //Use a newly seeded generator for each layer for backward compatibility (for now)
        for( int j = 0; j < weights[i].length; j++ ) {
          if (parameters.initial_weight_distribution == NN.InitialWeightDistribution.UniformAdaptive) {
            // cf. http://machinelearning.wustl.edu/mlpapers/paper_files/AISTATS2010_GlorotB10.pdf
            final double range = Math.sqrt(6. / (units[i] + units[i+1]));
            weights[i][j] = (float)uniformDist(rng, -range, range);
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

    public void computeDiagnostics() {
      // compute stats on all nodes
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

        System.out.println("Layer " + y + " mean weight: " + mean_weight[y]);
        System.out.println("Layer " + y + " rms  weight: " + rms_weight[y]);
        System.out.println("Layer " + y + " mean   bias: " + mean_bias[y]);
        System.out.println("Layer " + y + " rms    bias: " + rms_bias[y]);
      }
    }

  }

  public void computeDiagnostics() {
    run_time = (System.currentTimeMillis()-start_time);
    model_info.computeDiagnostics();
  }

//  public NNModel(NNModel other) {
//    super(other._key,null,other.data_info._adaptedFrame);
//
//    job_key = other.job_key;
//    data_info = other.data_info;
//    run_time = 0;
//    start_time = System.currentTimeMillis();
//    model_info = new NNModelInfo(other.model_info);
//  }

  public NNModel(Key selfKey, Key jobKey, Key dataKey, DataInfo dinfo, NN params) {
    super(selfKey, dataKey, dinfo._adaptedFrame);
    job_key = jobKey;
    data_info = dinfo;
    run_time = 0;
    start_time = System.currentTimeMillis();
    errors = new Errors[(int)params.epochs];
    for (int i=0; i<errors.length; ++i) {
      errors[i] = new Errors();
    }

    model_info = new NNModelInfo(params, data_info.fullN(), data_info._adaptedFrame.lastVec().domain().length);
    model_info.initializeMembers();
  }


  /**
   *
   * @param ftrain potentially downsampled training data for scoring
   * @param ftest  potentially downsampled validation data for scoring
   * @param timeStart start time in milliseconds, used to report training speed
   * @return true if model building is ongoing
   */
  transient long _timeLastScoreStart, _timeLastPrint;
  boolean doDiagnostics(Frame ftrain, Frame ftest, long timeStart, Key dest_key) {
    epoch_counter = (float)model_info().get_processed_total()/data_info._adaptedFrame.numRows();
    boolean keep_running = (epoch_counter < model_info().parameters.epochs);
    final long now = System.currentTimeMillis();
    final long sinceLastScore = now-_timeLastScoreStart;
    final long sinceLastPrint = now-_timeLastPrint;
    if( (sinceLastPrint > 5000) ) {
      final long samples = model_info().get_processed_total();
      Log.info("Training time: " + PrettyPrint.msecs(now - timeStart, true)
              + " processed " + samples + " samples" + " (" + String.format("%.3f", epoch_counter) + " epochs)."
              + " Speed: " + String.format("%.3f", (double)samples/((now - timeStart)/1000.)) + " samples/sec.");
      _timeLastPrint = now;
    }
    // this is potentially slow - only do every so often
    if( !keep_running || (now-timeStart < 30000) // Score every time for first 30 seconds
            || (sinceLastScore > model_info().parameters.score_interval*1000) ) {
      if (model_info.parameters.diagnostics) computeDiagnostics();
      _timeLastScoreStart = now;
      classificationError(ftrain, "Classification error on training data:", true);
      if (ftest != null)
        classificationError(ftest, "Classification error on validation data:", true);
    }
    if (model_info().unstable()) {
      Log.err("Canceling job since the model is unstable (exponential growth observed).");
      Log.err("Try using L1/L2/max_w2 regularization, a different activation function, or more synchronization in multi-node operation.");
      keep_running = false;
    }
    update(dest_key); //update model in UKV
//    System.out.println(this);
    return keep_running;
  }


  @Override public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(super.toString());
    sb.append("\n"+data_info.toString());
    sb.append("\n"+model_info.toString());
    sb.append("\nrun time: " + run_time);
    sb.append("\nstart time: " + start_time);
    sb.append("\nepoch counter: " + epoch_counter);
    return sb.toString();
  }

  @Override public float[] score0(double[] data, float[] preds) {
    Neurons[] neurons = NNTask.makeNeurons(data_info, model_info);
    ((Neurons.Input)neurons[0]).setInput(data);
    NNTask.step(neurons, model_info, false, null);
    double[] out = neurons[neurons.length - 1]._a;
    assert out.length == preds.length;
    for (int i=0; i<out.length; ++i) preds[i] = (float)out[i];
    return preds;
  }

  public double classificationError(Frame ftest, String label, boolean printCM) {
    Frame fpreds;
    fpreds = score(ftest);
    water.api.ConfusionMatrix CM = new water.api.ConfusionMatrix();
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

    DocGen.HTML.title(sb,title);
    DocGen.HTML.paragraph(sb, "Model Key: " + _key);
    model_info.parameters.toHTML(sb);
    sb.append("<div class='alert'>Actions: " + water.api.Predict.link(_key, "Score on dataset") + ", " +
            NN.link(_dataKey, "Compute new model") + "</div>");
    DocGen.HTML.title(sb, "Epochs: " + epoch_counter);

    // stats for training and validation
    final Errors error = errors[errors.length - 1];

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
    }

    if (isClassifier()) {
      DocGen.HTML.section(sb, "Training classification error: " + formatPct(error.train_err));
    }
    DocGen.HTML.section(sb, "Training mean square error: " + String.format(mse_format, error.train_mse));
    if (isClassifier()) {
      DocGen.HTML.section(sb, "Training cross entropy: " + String.format(cross_entropy_format, error.train_mce));
      if(error.validation) {
        DocGen.HTML.section(sb, "Validation classification error: " + formatPct(error.valid_err));
      }
    }
    if(error.validation) {
      DocGen.HTML.section(sb, "Validation mean square error: " + String.format(mse_format, error.valid_mse));
      if (isClassifier()) {
        DocGen.HTML.section(sb, "Validation mean cross entropy: " + String.format(cross_entropy_format, error.valid_mce));
      }
      if (error.training_time_ms > 0)
        DocGen.HTML.section(sb, "Training speed: " + error.training_samples * 1000 / error.training_time_ms + " samples/s");
    }
    else {
      if (error.training_time_ms > 0)
        DocGen.HTML.section(sb, "Training speed: " + error.training_samples * 1000 / error.training_time_ms + " samples/s");
    }
    if (model_info.parameters != null && model_info.parameters.diagnostics) {
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
      Neurons[] neurons = NNTask.makeNeurons(model_info.parameters._dinfo, model_info()); //link the weights to the neurons, for easy access
      for (int i=1; i<neurons.length; ++i) {
        sb.append("<tr>");
        sb.append("<td>").append("<b>").append(i).append("</b>").append("</td>");
        sb.append("<td>").append("<b>").append(neurons[i].units).append("</b>").append("</td>");
        sb.append("<td>").append(neurons[i].getClass().getSimpleName().replace("Vec","").replace("Chunk", "")).append("</td>");
        sb.append("<td>").append(String.format("%.5g", neurons[i].rate(error.training_samples))).append("</td>");
       sb.append("<td>").append(neurons[i].l1).append("</td>");
        sb.append("<td>").append(neurons[i].l2).append("</td>");
        final String format = "%g";
        sb.append("<td>").append(neurons[i].momentum(error.training_samples)).append("</td>");
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
    final String cmTitle = "Confusion Matrix on " + (error.validation ? " Validation Data" : " Training Data");
    DocGen.HTML.section(sb, cmTitle);
    if (error.confusion_matrix != null)
      error.confusion_matrix.toHTML(sb);

    sb.append("<h3>" + "Progress" + "</h3>");
//    String training = "Number of training set samples for scoring: " + error.score_training;
    if (error.validation) {
//      String validation = "Number of validation set samples for scoring: " + error.score_validation;
    }
    sb.append("<table class='table table-striped table-bordered table-condensed'>");
    sb.append("<tr>");
    sb.append("<th>Training Time</th>");
    sb.append("<th>Training Samples</th>");
    sb.append("<th>Training MSE</th>");
    if (isClassifier()) {
      sb.append("<th>Training MCE</th>");
      sb.append("<th>Training Classification Error</th>");
    }
    sb.append("<th>Validation MSE</th>");
    if (isClassifier()) {
      sb.append("<th>Validation MCE</th>");
      sb.append("<th>Validation Classification Error</th>");
    }
    sb.append("</tr>");
    for( int i = errors.length - 1; i >= 0; i-- ) {
      final Errors e = errors[i];
      sb.append("<tr>");
      sb.append("<td>" + PrettyPrint.msecs(e.training_time_ms, true) + "</td>");
      sb.append("<td>" + String.format("%,d", e.training_samples) + "</td>");
      sb.append("<td>" + String.format(mse_format, e.train_mse) + "</td>");
      if (isClassifier()) {
        sb.append("<td>" + String.format(cross_entropy_format, e.train_mce) + "</td>");
        sb.append("<td>" + formatPct(e.train_err) + "</td>");
      }
      if(e.validation) {
        sb.append("<td>" + String.format(mse_format, e.valid_mse) + "</td>");
        if (isClassifier()) {
          sb.append("<td>" + String.format(cross_entropy_format, e.valid_mce) + "</td>");
          sb.append("<td>" + formatPct(e.valid_err) + "</td>");
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

  public boolean toJavaHtml(StringBuilder sb) { return false; }
  @Override public String toJava() { return "Not yet implemented."; }
}

