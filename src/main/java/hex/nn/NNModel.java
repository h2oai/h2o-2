package hex.nn;

import hex.FrameTask.DataInfo;
import water.Iced;
import water.Key;
import water.Model;
import water.api.DocGen;
import water.api.Request.API;
import water.fvec.Chunk;
import water.util.Utils;

import java.util.Random;

import static hex.nn.NN.RNG.getRNG;

public class NNModel extends Model {
  static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  @API(help="job key assigned to the job building this model")
  final Key job_key;

  @API(help="Input data info")
  DataInfo data_info;

  @API(help="model info", json = true)
  NNModelInfo model_info;

  @API(help="Overall run time", json = true)
  long run_time;

  @API(help="computation started at", json = true)
  long start_time;

  public double epoch_counter;

  // This describes the model, together with the parameters
  // This will be shared: one per node
  public static class NNModelInfo extends Iced {
    static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
    static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

    public float[][] weights; //one 2D weight matrix per layer (stored as a 1D array each)
    public double[][] biases; //one 1D bias array per layer
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

    // disregard for now: momenta
//  public float[] _wm;
//  public double[] _bm;

//    public float[] incoming_weights(int layer) { return weights[layer-1]; }

    public NNModelInfo(NN params, int num_input, int num_output) {
      parameters = params;
      final int layers=parameters.hidden.length;
      if (!parameters.classification) assert(num_output == 1); else assert(num_output > 1);
      // units (# neurons for each layer)
      units = new int[layers+2];
      units[0] = num_input;
      for (int i=1; i<=layers; ++i) units[i] = parameters.hidden[i-1];
      units[layers+1] = num_output;
      // weights (to connect layers)
      weights = new float[layers+1][];
      for (int i=0; i<=layers; ++i) weights[i] = new float[units[i]*units[i+1]];
      // biases (only for hidden layers and output layer)
      biases = new double[layers+2][];
      for (int i=0; i<=layers+1; ++i) biases[i] = new double[units[i]];
      randomize(getRNG());
    }
    NNModelInfo deep_copy() {
      NNModelInfo n = new NNModelInfo(parameters, units[0], units[units.length-1]);
      n.processed = processed;
      n.weights = weights.clone();
      n.biases = biases.clone();
      for (int i=0; i<weights.length; ++i) n.weights[i] = weights[i].clone();
      for (int i=0; i<biases.length; ++i) n.biases[i] = biases[i].clone();
      return n;
    }
    public void add(NNModelInfo other) {
      if (other == this) return;
      Utils.add(weights, other.weights);
      Utils.add(biases,  other.biases);
      processed += other.processed;
    }
    // use to average weights and biases
    public void div(double N) {
      for (int i=0; i<weights.length; ++i) Utils.div(weights[i], (float)N);
      for (int i=0; i<biases.length; ++i) Utils.div(biases[i], N);
    }

    private double uniformDist(Random rand, double min, double max) {
      return min + rand.nextFloat() * (max - min);
    }

    /**
     *
     // helper to initialize weights
     // adaptive initialization uses prefactor * sqrt(6 / (units_input_layer + units_this_layer))
     // cf. http://machinelearning.wustl.edu/mlpapers/paper_files/AISTATS2010_GlorotB10.pdf
     * @param rng random generator to use
     */
    void randomize(Random rng) {
      for (int i=0; i<weights.length; ++i) {
        final double range = Math.sqrt(6. / (units[i] + units[i+1]));
        for( int j = 0; j < weights[i].length; j++ ) {
          if (parameters.initial_weight_distribution == NN.InitialWeightDistribution.UniformAdaptive) {
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
      mean_bias = new double[units.length];
      rms_bias = new double[units.length];
      mean_weight = new double[units.length];
      rms_weight = new double[units.length];
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

  @Override public NNModel clone(){
    NNModel res = (NNModel)super.clone();
    return res;
  }

  public NNModel(Key jobKey, Key selfKey, DataInfo dinfo, NN params, NNModel.NNModelInfo modelinfo) {
    super(selfKey,null,dinfo._adaptedFrame);

    job_key = jobKey;
    data_info = dinfo;
    run_time = 0;
    start_time = System.currentTimeMillis();
    model_info = modelinfo != null ? modelinfo.deep_copy() : new NNModelInfo(params, data_info.fullN(), params.response.toEnum().cardinality());
  }

  @Override
  protected float[] score0(Chunk[] chks, int row_in_chunk, double[] tmp, float[] preds) {
    return super.score0(chks, row_in_chunk, tmp, preds);
  }

  @Override protected float[] score0(double[] data, float[] preds) {
    Neurons[] neurons = NNTask.makeNeurons(data_info,model_info);

//    public final int fullN(){return _nums + _catOffsets[_cats];}
//    public final int largestCat(){return _cats > 0?_catOffsets[1]:0;}
//    public final int numStart(){return _catOffsets[_cats];}

    // expanded categoricals
    int[] cats = new int[data_info.numStart()];
    for(int i = 0; i < cats.length; ++i) cats[i] = (int)data[data_info._catOffsets[i]];

    // numerical values
    double[] nums = new double[data_info.fullN() - data_info.numStart()];
    System.arraycopy(data, data_info.numStart(), nums, 0, nums.length);

    NNTask.step(neurons, data_info, model_info, false, nums, cats, null);

    double[] out = neurons[neurons.length - 1]._a;
    if (out.length != preds.length) {
      System.out.println("Need to call .toEnum()!");
    };
    assert out.length == preds.length;
    // convert to float
    float[] out2 = new float[out.length];
    for (int i=0; i<out.length; ++i) out2[i] = (float)out[i];
    return out2;
  }
  public boolean generateHTML(String title, StringBuilder sb) {
    DocGen.HTML.title(sb, title);
    DocGen.HTML.title(sb, "Epochs: " + epoch_counter);
    return true;
  }
  public boolean toJavaHtml(StringBuilder sb) { return false; }
  @Override public String toJava() { return "Not yet implemented."; }
}

