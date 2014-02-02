package hex.nn;

import hex.FrameTask.DataInfo;
import water.Iced;
import water.Key;
import water.Model;
import water.api.DocGen;
import water.api.Request.API;
import water.fvec.Frame;
import water.util.Utils;

import java.util.Arrays;
import java.util.Random;

import static hex.nn.NN.RNG.getRNG;

public class NNModel extends Model {
  static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  @API(help="job key assigned to the job building this model")
  private final Key job_key;

  @API(help="Input data info")
  private DataInfo data_info;

  @API(help="model info", json = true)
  public NNModelInfo model_info;

  @API(help="Overall run time", json = true)
  private long run_time;

  @API(help="computation started at", json = true)
  private long start_time;

  public double epoch_counter;

  // This describes the model, together with the parameters
  // This will be shared: one per node
  public static class NNModelInfo extends Iced {
    static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
    static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

    public float[][] weights; //one 2D weight matrix per layer (stored as a 1D array each)
    public double[][] biases; //one 1D bias array per layer
//  public float[][] _wm;
//  public double[][] _bm;
    private int[] units; //number of neurons per layer, extracted from parameters and from datainfo

    @API(help = "Model parameters", json = true)
    public NN parameters;

    @API(help = "Mean bias", json = true)
    public double[] mean_bias;

    @API(help = "RMS bias", json = true)
    public double[] rms_bias;

    @API(help = "Mean weight", json = true)
    public double[] mean_weight;

    @API(help = "RMS weight", json = true)
    public double[] rms_weight;

    @API(help = "Unstable", json = true)
    public boolean unstable = false;

    @API(help = "Processed samples", json = true)
    public long processed;
    public long processed() { return processed; }

    public int chunk_node_count;
    public long chunk_processed_rows;

    public NNModelInfo() {}

    public NNModelInfo(NN params, int num_input, int num_output) {
      parameters = params;
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
      biases = new double[layers+2][];
      for (int i=0; i<=layers+1; ++i) biases[i] = new double[units[i]];
      // for diagnostics
      mean_bias = new double[units.length];
      rms_bias = new double[units.length];
      mean_weight = new double[units.length];
      rms_weight = new double[units.length];
    }

    @Override public String toString() {
      StringBuilder sb = new StringBuilder();
      for (int i=0; i<weights.length; ++i)
        sb.append("weights["+i+"][]="+Arrays.toString(weights[i]));
      for (int i=0; i<biases.length; ++i)
      sb.append("\nbiases["+i+"][]="+Arrays.toString(biases[i]));
      sb.append("\nunits[]="+Arrays.toString(units));
      return sb.toString();
    }
    void initializeMembers() {
      randomizeWeights();
      for (int i=0; i<=parameters.hidden.length+1; ++i) {
        if (parameters.activation == NN.Activation.Rectifier
                || parameters.activation == NN.Activation.Maxout) {
          Arrays.fill(biases[i], 1.);
        }
      }

    }
    public NNModelInfo(NNModelInfo other) {
      this(other.parameters, other.units[0], other.units[other.units.length-1]);
      processed = other.processed;
      chunk_node_count = other.chunk_node_count;
      chunk_processed_rows = other.chunk_processed_rows;
      for (int i=0; i<other.weights.length; ++i)
        weights[i] = other.weights[i].clone();
      for (int i=0; i<other.biases.length; ++i)
        biases[i] = other.biases[i].clone();
      mean_bias = other.mean_bias.clone();
      rms_bias = other.rms_bias.clone();
      mean_weight = other.mean_weight.clone();
      rms_weight = other.rms_weight.clone();
      unstable = other.unstable;
    }
    public void add(NNModelInfo other) {
      if (other == this) {
        System.out.println("reduce: locally shared matrix, already have added weights and biases.");
        return;
      }
      if (other.chunk_node_count != 0) {
        System.out.println("reduce: adding remote weights and biases.");
        Utils.add(weights, other.weights);
        Utils.add(biases,  other.biases);
        chunk_processed_rows += other.chunk_processed_rows;
        chunk_node_count += other.chunk_node_count;
      }
    }
    protected void div(double N) {
      for (float[] weight : weights) Utils.div(weight, (float) N);
      for (double[] biase : biases) Utils.div(biase, N);
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


    void computeDiagnostics() {
      // compute stats on all nodes
      for( int y = 1; y < units.length; y++ ) {
        mean_bias[y] = rms_bias[y] = 0;
        mean_weight[y] = rms_weight[y] = 0;
        for(int u = 0; u < biases[y].length; u++) {
          mean_bias[y] += biases[y][u];
        }
        for(int u = 0; u < weights[y-1].length; u++) {
          mean_weight[y] += weights[y-1][u];
        }
        mean_bias[y] /= biases[y].length;
        mean_weight[y] /= weights[y-1].length;

        for(int u = 0; u < biases[y].length; u++) {
          final double db = biases[y][u] - mean_bias[y];
          rms_bias[y] += db * db;
        }
        for(int u = 0; u < weights[y-1].length; u++) {
          final double dw = weights[y-1][u] - mean_weight[y];
          rms_weight[y] += dw * dw;
        }
        rms_bias[y] = Math.sqrt(rms_bias[y]/biases[y].length);
        rms_weight[y] = Math.sqrt(rms_weight[y]/weights[y-1].length);

        unstable |= Double.isNaN(mean_bias[y])  || Double.isNaN(rms_bias[y])
                || Double.isNaN(mean_weight[y]) || Double.isNaN(rms_weight[y]);

        // Abort the run if weights or biases are unreasonably large (Note that all input values are normalized upfront)
        // This can happen with Rectifier units when L1/L2/max_w2 are all set to 0, especially when using more than 1 hidden layer.
        final double thresh = 1e10;
        unstable |= mean_bias[y] > thresh   || rms_bias[y] > thresh
                || mean_weight[y] > thresh  || rms_weight[y] > thresh;

        System.out.println("Layer " + y + " mean weight: " + mean_weight[y]);
        System.out.println("Layer " + y + " rms  weight: " + rms_weight[y]);
        System.out.println("Layer " + y + " mean   bias: " + mean_bias[y]);
        System.out.println("Layer " + y + " rms    bias: " + rms_bias[y]);
      }
    }

  }

//  @API(help = "Errors on the training set")
//  public Errors[] training_errors;
//
//  @API(help = "Errors on the validation set")
//  public Errors[] validation_errors;
//
//  @API(help = "Confusion matrix")
//  public long[][] confusion_matrix;
//

  public NNModel(NNModel other) {
    super(other._key,null,other.data_info._adaptedFrame);

    job_key = other.job_key;
    data_info = other.data_info;
    run_time = 0;
    start_time = System.currentTimeMillis();
    model_info = new NNModelInfo(other.model_info);
  }

  public NNModel(Key selfKey, Key jobKey, DataInfo dinfo, NN params) {
    super(selfKey,null,dinfo._adaptedFrame);
    job_key = jobKey;
    data_info = dinfo;
    run_time = 0;
    start_time = System.currentTimeMillis();
    model_info = new NNModelInfo(params, data_info.fullN(), data_info._adaptedFrame.lastVec().domain().length);
    model_info.initializeMembers();
  }

  @Override public String toString() {
    StringBuilder sb = new StringBuilder();
    super.toString();
    sb.append("\n"+data_info.toString());
    sb.append("\n"+model_info.toString());
    sb.append("\nrun time: " + run_time);
    sb.append("\nstart time: " + start_time);
    sb.append("\nepoch counter: " + epoch_counter);
    return sb.toString();
  }

  @Override protected float[] score0(double[] data, float[] preds) {
    Neurons[] neurons = NNTask.makeNeurons(data_info, model_info);

    ((Neurons.Input)neurons[0]).setInput(data);
    NNTask.step(neurons, model_info, false, null);

    double[] out = neurons[neurons.length - 1]._a;
    assert out.length == preds.length;
    // convert to float
    float[] out2 = new float[out.length];
    for (int i=0; i<out.length; ++i) out2[i] = (float)out[i];
    return out2;
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
    if (printCM) System.out.println(label + "\n" + sb);
    fpreds.delete();
    return error;
  }

  public boolean generateHTML(String title, StringBuilder sb) {
    DocGen.HTML.title(sb, title);
    DocGen.HTML.title(sb, "Epochs: " + epoch_counter);
    return true;
  }
  public boolean toJavaHtml(StringBuilder sb) { return false; }
  @Override public String toJava() { return "Not yet implemented."; }
}

