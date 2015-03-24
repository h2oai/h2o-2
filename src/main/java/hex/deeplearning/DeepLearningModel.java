package hex.deeplearning;

import static java.lang.Double.isNaN;
import hex.FrameTask.DataInfo;
import hex.VarImp;
import water.*;
import water.api.*;
import water.api.Request.API;
import water.fvec.Chunk;
import water.fvec.Frame;
import water.fvec.Vec;
import water.util.*;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Random;

/**
 * The Deep Learning model
 * It contains a DeepLearningModelInfo with the most up-to-date model,
 * a scoring history, as well as some helpers to indicate the progress
 */
public class DeepLearningModel extends Model implements Comparable<DeepLearningModel> {
  static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  @API(help="Model info", json = true)
  private volatile DeepLearningModelInfo model_info;
  void set_model_info(DeepLearningModelInfo mi) { model_info = mi; }
  final public DeepLearningModelInfo model_info() { return model_info; }

  @API(help="Job that built the model", json = true)
  final private Key jobKey;

  @API(help="Validation dataset used for model building", json = true)
  public final Key _validationKey;

  @API(help="Time to build the model", json = true)
  private long run_time;
  final private long start_time;

  public long actual_train_samples_per_iteration;
  public double time_for_communication_us; //helper for auto-tuning: time in microseconds for collective bcast/reduce of the model

  @API(help="Number of training epochs", json = true)
  public double epoch_counter;

  @API(help="Number of rows in training data", json = true)
  public long training_rows;

  @API(help="Number of rows in validation data", json = true)
  public long validation_rows;

  @API(help = "Scoring during model building")
  private Errors[] errors;
  public Errors[] scoring_history() { return errors; }

  // Keep the best model so far, based on a single criterion (overall class. error or MSE)
  private float _bestError = Float.MAX_VALUE;

  @API(help = "Key to the best model so far (based on overall error on scoring data set)")
  public Key actual_best_model_key;

  // return the most up-to-date model metrics
  Errors last_scored() { return errors == null ? null : errors[errors.length-1]; }

  @Override public final DeepLearning get_params() { return model_info.get_params(); }
  @Override public final Request2 job() { return model_info.get_job(); }

  @Override protected double missingColumnsType() { return get_params().sparse ? 0 : Double.NaN; }

  public float error() { return (float) (isClassifier() ? cm().err() : mse()); }

  @Override public boolean isClassifier() { return super.isClassifier() && !model_info.get_params().autoencoder; }
  @Override public boolean isSupervised() { return !model_info.get_params().autoencoder; }

  @Override public int nfeatures() { return model_info.get_params().autoencoder ? _names.length : _names.length - 1; }

  public int compareTo(DeepLearningModel o) {
    if (o.isClassifier() != isClassifier()) throw new UnsupportedOperationException("Cannot compare classifier against regressor.");
    if (o.nclasses() != nclasses()) throw new UnsupportedOperationException("Cannot compare models with different number of classes.");
    return (error() < o.error() ? -1 : error() > o.error() ? 1 : 0);
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

    //training/validation sets
    @API(help = "Whether a validation set was provided")
    boolean validation;
    @API(help = "Number of folds for cross-validation (for validation=false)")
    int num_folds;
    @API(help = "Number of training set samples for scoring")
    public long score_training_samples;
    @API(help = "Number of validation set samples for scoring")
    public long score_validation_samples;

    @API(help="Do classification or regression")
    public boolean classification;

    @API(help = "Variable importances")
    VarImp variable_importances;

    // classification
    @API(help = "Confusion matrix on training data")
    public water.api.ConfusionMatrix train_confusion_matrix;
    @API(help = "Confusion matrix on validation data")
    public water.api.ConfusionMatrix valid_confusion_matrix;
    @API(help = "Classification error on training data")
    public double train_err = 1;
    @API(help = "Classification error on validation data")
    public double valid_err = 1;
    @API(help = "AUC on training data")
    public AUCData trainAUC;
    @API(help = "AUC on validation data")
    public AUCData validAUC;
    @API(help = "Hit ratio on training data")
    public water.api.HitRatio train_hitratio;
    @API(help = "Hit ratio on validation data")
    public water.api.HitRatio valid_hitratio;

    // regression
    @API(help = "Training MSE")
    public double train_mse = Double.POSITIVE_INFINITY;
    @API(help = "Validation MSE")
    public double valid_mse = Double.POSITIVE_INFINITY;

    @API(help = "Time taken for scoring")
    public long scoring_time;

    Errors deep_clone() {
      AutoBuffer ab = new AutoBuffer();
      this.write(ab);
      ab.flipForReading();
      return new Errors().read(ab);
    }

    @Override public String toString() {
      StringBuilder sb = new StringBuilder();
      if (classification) {
        sb.append("Error on training data (misclassification)"
                + (trainAUC != null ? " [using threshold for " + trainAUC.threshold_criterion.toString().replace("_"," ") +"]: ": ": ")
                + String.format("%.2f", 100*train_err) + "%");

        if (trainAUC != null) sb.append(", AUC on training data: " + String.format("%.4f", 100*trainAUC.AUC) + "%");
        if (validation || num_folds>0)
          sb.append("\nError on " + (num_folds>0 ? num_folds + "-fold cross-":"")+ "validation data (misclassification)"
                + (validAUC != null ? " [using threshold for " + validAUC.threshold_criterion.toString().replace("_"," ") +"]: ": ": ")
                + String.format("%.2f", (100*valid_err)) + "%");
        if (validAUC != null) sb.append(", AUC on validation data: " + String.format("%.4f", 100*validAUC.AUC) + "%");
      } else if (!Double.isInfinite(train_mse)) {
        sb.append("Error on training data (MSE): " + train_mse);
        if (validation || num_folds>0)
          sb.append("\nError on "+ (num_folds>0 ? num_folds + "-fold cross-":"")+ "validation data (MSE): " + valid_mse);
      }
      return sb.toString();
    }
  }

  final private static class ConfMat extends hex.ConfusionMatrix {
    final private double _err;
    final private double _f1;
    public ConfMat(double err, double f1) {
      super(null);
      _err=err;
      _f1=f1;
    }
    @Override public double err() { return _err; }
    @Override public double F1() { return _f1; }
    @Override public double[] classErr() { return null; }
  }

  /** for grid search error reporting */
  @Override
  public hex.ConfusionMatrix cm() {
    final Errors lasterror = last_scored();
    if (lasterror == null) return null;
    water.api.ConfusionMatrix cm = lasterror.validation || lasterror.num_folds > 0 ?
            lasterror.valid_confusion_matrix :
            lasterror.train_confusion_matrix;
    if (cm == null || cm.cm == null) {
      if (lasterror.validation || lasterror.num_folds > 0) {
        return new ConfMat(lasterror.valid_err, lasterror.validAUC != null ? lasterror.validAUC.F1() : 0);
      } else {
        return new ConfMat(lasterror.train_err, lasterror.trainAUC != null ? lasterror.trainAUC.F1() : 0);
      }
    }
    // cm.cm has NaN padding, reduce it to N-1 size
    return new hex.ConfusionMatrix(cm.cm, cm.cm.length-1);
  }

  @Override
  public double mse() {
    if (errors == null) return super.mse();
    return last_scored().validation || last_scored().num_folds > 0 ? last_scored().valid_mse : last_scored().train_mse;
  }

  @Override
  public VarImp varimp() {
    if (errors == null) return null;
    return last_scored().variable_importances;
  }

  // This describes the model, together with the parameters
  // This will be shared: one per node
  public static class DeepLearningModelInfo extends Iced {
    static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
    static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

    @API(help="Input data info")
    private DataInfo data_info;
    public DataInfo data_info() { return data_info; }

    // model is described by parameters and the following arrays
    private Neurons.DenseRowMatrix[] dense_row_weights; //one 2D weight matrix per layer (stored as a 1D array each)
    private Neurons.DenseColMatrix[] dense_col_weights; //one 2D weight matrix per layer (stored as a 1D array each)
    private Neurons.DenseVector[] biases; //one 1D bias array per layer
    private Neurons.DenseVector[] avg_activations; //one 1D array per hidden layer

    // helpers for storing previous step deltas
    // Note: These two arrays *could* be made transient and then initialized freshly in makeNeurons() and in DeepLearningTask.initLocal()
    // But then, after each reduction, the weights would be lost and would have to restart afresh -> not *exactly* right, but close...
    private Neurons.DenseRowMatrix[] dense_row_weights_momenta;
    private Neurons.DenseColMatrix[] dense_col_weights_momenta;
    private Neurons.DenseVector[] biases_momenta;

    // helpers for AdaDelta
    private Neurons.DenseRowMatrix[] dense_row_ada_dx_g;
    private Neurons.DenseColMatrix[] dense_col_ada_dx_g;
    private Neurons.DenseVector[] biases_ada_dx_g;

    // compute model size (number of model parameters required for making predictions)
    // momenta are not counted here, but they are needed for model building
    public long size() {
      long siz = 0;
      for (Neurons.Matrix w : dense_row_weights) if (w != null) siz += w.size();
      for (Neurons.Matrix w : dense_col_weights) if (w != null) siz += w.size();
      for (Neurons.Vector b : biases) siz += b.size();
      return siz;
    }

    // accessors to (shared) weights and biases - those will be updated racily (c.f. Hogwild!)
    boolean has_momenta() { return get_params().momentum_start != 0 || get_params().momentum_stable != 0; }
    boolean adaDelta() { return get_params().adaptive_rate; }
    public final Neurons.Matrix get_weights(int i) { return dense_row_weights[i] == null ? dense_col_weights[i] : dense_row_weights[i]; }
    public final Neurons.DenseVector get_biases(int i) { return biases[i]; }
    public final Neurons.Matrix get_weights_momenta(int i) { return dense_row_weights_momenta[i] == null ? dense_col_weights_momenta[i] : dense_row_weights_momenta[i]; }
    public final Neurons.DenseVector get_biases_momenta(int i) { return biases_momenta[i]; }
    public final Neurons.Matrix get_ada_dx_g(int i) { return dense_row_ada_dx_g[i] == null ? dense_col_ada_dx_g[i] : dense_row_ada_dx_g[i]; }
    public final Neurons.DenseVector get_biases_ada_dx_g(int i) { return biases_ada_dx_g[i]; }

    //accessor to shared parameter defining avg activations
    public final Neurons.DenseVector get_avg_activations(int i) { return avg_activations[i]; }


    @API(help = "Model parameters", json = true)
    private Request2 job;
    public final DeepLearning get_params() { return (DeepLearning)job; }
    public final Request2 get_job() { return job; }

    @API(help = "Mean rate", json = true)
    private float[] mean_rate;

    @API(help = "RMS rate", json = true)
    private float[] rms_rate;

    @API(help = "Mean bias", json = true)
    private float[] mean_bias;

    @API(help = "RMS bias", json = true)
    private float[] rms_bias;

    @API(help = "Mean weight", json = true)
    private float[] mean_weight;

    @API(help = "RMS weight", json = true)
    public float[] rms_weight;

    @API(help = "Mean Activation", json = true)
    public float[] mean_a;

    @API(help = "Unstable", json = true)
    private volatile boolean unstable = false;
    public boolean unstable() { return unstable; }
    public void set_unstable() { if (!unstable) computeStats(); unstable = true; }

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
    int[] units; //number of neurons per layer, extracted from parameters and from datainfo

    public DeepLearningModelInfo() {}

    public DeepLearningModelInfo(final Job job, final DataInfo dinfo) {
      this.job = job;
      data_info = dinfo;
      final int num_input = dinfo.fullN();
      final int num_output = get_params().autoencoder ? num_input : get_params().classification ? dinfo._adaptedFrame.domains()[dinfo._adaptedFrame.domains().length-1].length : 1;
      assert(num_input > 0);
      assert(num_output > 0);
      if (has_momenta() && adaDelta()) throw new IllegalArgumentException("Cannot have non-zero momentum and adaptive rate at the same time.");
      final int layers=get_params().hidden.length;
      // units (# neurons for each layer)
      units = new int[layers+2];
      if (get_params().max_categorical_features <= Integer.MAX_VALUE - dinfo._nums)
        units[0] = Math.min(dinfo._nums + get_params().max_categorical_features, num_input);
      else
        units[0] = num_input;
      System.arraycopy(get_params().hidden, 0, units, 1, layers);
      units[layers+1] = num_output;

      if ((long)units[0] > 100000L) {
        final String[][] domains = dinfo._adaptedFrame.domains();
        int[] levels = new int[domains.length];
        for (int i=0; i<levels.length; ++i) {
          levels[i] = domains[i] != null ? domains[i].length : 0;
        }
        Arrays.sort(levels);
        Log.warn("===================================================================================================================================");
        Log.warn(num_input + " input features" + (dinfo._cats > 0 ? " (after categorical one-hot encoding)" : "") + ". Can be slow and require a lot of memory.");
        if (levels[levels.length-1] > 0) {
          int levelcutoff = levels[levels.length-1-Math.min(10, levels.length)];
          int count = 0;
          for (int i=0; i<dinfo._adaptedFrame.numCols() - (get_params().autoencoder ? 0 : 1) && count < 10; ++i) {
            if (dinfo._adaptedFrame.domains()[i] != null && dinfo._adaptedFrame.domains()[i].length >= levelcutoff) {
              Log.warn("Categorical feature '" + dinfo._adaptedFrame._names[i] + "' has cardinality " + dinfo._adaptedFrame.domains()[i].length + ".");
              count++;
            }
          }
        }
        Log.warn("Suggestions:");
        Log.warn(" *) Limit the size of the first hidden layer");
        if (dinfo._cats > 0) {
          Log.warn(" *) Limit the total number of one-hot encoded features with the parameter 'max_categorical_features'");
          Log.warn(" *) Run h2o.interaction(...,pairwise=F) on high-cardinality categorical columns to limit the factor count, see http://learn.h2o.ai");
        }
        Log.warn("===================================================================================================================================");
      }

      // weights (to connect layers)
      dense_row_weights = new Neurons.DenseRowMatrix[layers+1];
      dense_col_weights = new Neurons.DenseColMatrix[layers+1];

      // decide format of weight matrices row-major or col-major
      if (get_params().col_major) dense_col_weights[0] = new Neurons.DenseColMatrix(units[1], units[0]);
      else dense_row_weights[0] = new Neurons.DenseRowMatrix(units[1], units[0]);
      for (int i = 1; i <= layers; ++i)
        dense_row_weights[i] = new Neurons.DenseRowMatrix(units[i + 1] /*rows*/, units[i] /*cols*/);

      // biases (only for hidden layers and output layer)
      biases = new Neurons.DenseVector[layers+1];
      for (int i=0; i<=layers; ++i) biases[i] = new Neurons.DenseVector(units[i+1]);
      // average activation (only for hidden layers)
      if (get_params().autoencoder && get_params().sparsity_beta > 0) {
        avg_activations = new Neurons.DenseVector[layers];
        mean_a = new float[layers];
        for (int i = 0; i < layers; ++i) avg_activations[i] = new Neurons.DenseVector(units[i + 1]);
      }
      fillHelpers();
      // for diagnostics
      mean_rate = new float[units.length];
      rms_rate = new float[units.length];
      mean_bias = new float[units.length];
      rms_bias = new float[units.length];
      mean_weight = new float[units.length];
      rms_weight = new float[units.length];
    }

    // deep clone all weights/biases
    DeepLearningModelInfo deep_clone() {
      AutoBuffer ab = new AutoBuffer();
      this.write(ab);
      ab.flipForReading();
      return new DeepLearningModelInfo().read(ab);
    }

    void fillHelpers() {
      if (has_momenta()) {
        dense_row_weights_momenta = new Neurons.DenseRowMatrix[dense_row_weights.length];
        dense_col_weights_momenta = new Neurons.DenseColMatrix[dense_col_weights.length];
        if (dense_row_weights[0] != null)
          dense_row_weights_momenta[0] = new Neurons.DenseRowMatrix(units[1], units[0]);
        else
          dense_col_weights_momenta[0] = new Neurons.DenseColMatrix(units[1], units[0]);
        for (int i=1; i<dense_row_weights_momenta.length; ++i) dense_row_weights_momenta[i] = new Neurons.DenseRowMatrix(units[i+1], units[i]);

        biases_momenta = new Neurons.DenseVector[biases.length];
        for (int i=0; i<biases_momenta.length; ++i) biases_momenta[i] = new Neurons.DenseVector(units[i+1]);
      }
      else if (adaDelta()) {
        dense_row_ada_dx_g = new Neurons.DenseRowMatrix[dense_row_weights.length];
        dense_col_ada_dx_g = new Neurons.DenseColMatrix[dense_col_weights.length];
        //AdaGrad
        if (dense_row_weights[0] != null) {
          dense_row_ada_dx_g[0] = new Neurons.DenseRowMatrix(units[1], 2*units[0]);
        } else {
          dense_col_ada_dx_g[0] = new Neurons.DenseColMatrix(2*units[1], units[0]);
        }
        for (int i=1; i<dense_row_ada_dx_g.length; ++i) {
          dense_row_ada_dx_g[i] = new Neurons.DenseRowMatrix(units[i+1], 2*units[i]);
        }
        biases_ada_dx_g = new Neurons.DenseVector[biases.length];
        for (int i=0; i<biases_ada_dx_g.length; ++i) {
          biases_ada_dx_g[i] = new Neurons.DenseVector(2*units[i+1]);
        }
      }
    }

    @Override public String toString() {
      StringBuilder sb = new StringBuilder();
      if (get_params().diagnostics && !get_params().quiet_mode) {
        Neurons[] neurons = DeepLearningTask.makeNeuronsForTesting(this);

        sb.append("Number of hidden layers is " + get_params().hidden.length + " \n");

        if (get_params().sparsity_beta > 0) {
          for (int k = 0; k < get_params().hidden.length; k++)
            sb.append("Average activation in hidden layer " + k + " is  " + mean_a[k] + " \n");
        }

        sb.append("Status of Neuron Layers:\n");
        sb.append("#  Units         Type      Dropout    L1       L2    " + (get_params().adaptive_rate ? "  Rate (Mean,RMS)   " : "  Rate      Momentum") + "   Weight (Mean, RMS)      Bias (Mean,RMS)\n");
        final String format = "%7g";
        for (int i=0; i<neurons.length; ++i) {
          sb.append((i+1) + " " + String.format("%6d", neurons[i].units)
                  + " " + String.format("%16s", neurons[i].getClass().getSimpleName()));
          if (i == 0) {
            sb.append("  " + Utils.formatPct(neurons[i].params.input_dropout_ratio) + " \n");
            continue;
          }
          else if (i < neurons.length-1) {
            if (neurons[i].params.hidden_dropout_ratios == null)
              sb.append("  " + Utils.formatPct(0) + " ");
            else
              sb.append("  " + Utils.formatPct(neurons[i].params.hidden_dropout_ratios[i - 1]) + " ");
          } else {
            sb.append("          ");
          }
          sb.append(
                  " " + String.format("%5f", neurons[i].params.l1)
                          + " " + String.format("%5f", neurons[i].params.l2)
                          + " " + (get_params().adaptive_rate ? (" (" + String.format(format, mean_rate[i]) + ", " + String.format(format, rms_rate[i]) + ")" )
                          : (String.format("%10g", neurons[i].rate(get_processed_total())) + " " + String.format("%5f", neurons[i].momentum(get_processed_total()))))
                          + " (" + String.format(format, mean_weight[i])
                          + ", " + String.format(format, rms_weight[i]) + ")"
                          + " (" + String.format(format, mean_bias[i])
                          + ", " + String.format(format, rms_bias[i]) + ")\n");

          if (get_params().sparsity_beta > 0) {
            // sb.append("  " + String.format(format, mean_a[i]) + " \n");
          }
        }
      }
      return sb.toString();
    }

    // DEBUGGING
    public String toStringAll() {
      StringBuilder sb = new StringBuilder();
      sb.append(toString());

      for (int i=0; i<units.length-1; ++i)
        sb.append("\nweights["+i+"][]="+Arrays.toString(get_weights(i).raw()));
      for (int i=0; i<units.length-1; ++i)
        sb.append("\nbiases["+i+"][]="+Arrays.toString(get_biases(i).raw()));
      if (has_momenta()) {
        for (int i=0; i<units.length-1; ++i)
          sb.append("\nweights_momenta["+i+"][]="+Arrays.toString(get_weights_momenta(i).raw()));
      }
      if (biases_momenta != null) {
        for (int i=0; i<units.length-1; ++i)
          sb.append("\nbiases_momenta["+i+"][]="+Arrays.toString(biases_momenta[i].raw()));
      }
      sb.append("\nunits[]="+Arrays.toString(units));
      sb.append("\nprocessed global: "+get_processed_global());
      sb.append("\nprocessed local:  "+get_processed_local());
      sb.append("\nprocessed total:  " + get_processed_total());
      sb.append("\n");
      return sb.toString();
    }

    void initializeMembers() {
      randomizeWeights();
      //TODO: determine good/optimal/best initialization scheme for biases
      // hidden layers
      for (int i=0; i<get_params().hidden.length; ++i) {
        if (get_params().activation == DeepLearning.Activation.Rectifier
                || get_params().activation == DeepLearning.Activation.RectifierWithDropout
                || get_params().activation == DeepLearning.Activation.Maxout
                || get_params().activation == DeepLearning.Activation.MaxoutWithDropout
                ) {
//          Arrays.fill(biases[i], 1.); //old behavior
          Arrays.fill(biases[i].raw(), i == 0 ? 0.5f : 1f); //new behavior, might be slightly better
        }
        else if (get_params().activation == DeepLearning.Activation.Tanh || get_params().activation == DeepLearning.Activation.TanhWithDropout) {
          Arrays.fill(biases[i].raw(), 0f);
        }
      }
      Arrays.fill(biases[biases.length-1].raw(), 0f); //output layer
    }
    public void add(DeepLearningModelInfo other) {
      for (int i=0;i<dense_row_weights.length;++i)
        Utils.add(get_weights(i).raw(), other.get_weights(i).raw());
      for (int i=0;i<biases.length;++i) Utils.add(biases[i].raw(), other.biases[i].raw());
      if (avg_activations != null)
        for (int i=0;i<avg_activations.length;++i)
          Utils.add(avg_activations[i].raw(), other.biases[i].raw());
      if (has_momenta()) {
        assert(other.has_momenta());
        for (int i=0;i<dense_row_weights_momenta.length;++i)
          Utils.add(get_weights_momenta(i).raw(), other.get_weights_momenta(i).raw());
        for (int i=0;i<biases_momenta.length;++i)
          Utils.add(biases_momenta[i].raw(),  other.biases_momenta[i].raw());
      }
      if (adaDelta()) {
        assert(other.adaDelta());
        for (int i=0;i<dense_row_ada_dx_g.length;++i) {
          Utils.add(get_ada_dx_g(i).raw(), other.get_ada_dx_g(i).raw());
        }
      }
      add_processed_local(other.get_processed_local());
    }
    protected void div(float N) {
      for (int i=0; i<dense_row_weights.length; ++i)
        Utils.div(get_weights(i).raw(), N);
      for (Neurons.Vector bias : biases) Utils.div(bias.raw(), N);
      if (avg_activations != null)
        for (Neurons.Vector avgac : avg_activations)
          Utils.div(avgac.raw(), N);
      if (has_momenta()) {
        for (int i=0; i<dense_row_weights_momenta.length; ++i)
          Utils.div(get_weights_momenta(i).raw(), N);
        for (Neurons.Vector bias_momenta : biases_momenta) Utils.div(bias_momenta.raw(), N);
      }
      if (adaDelta()) {
        for (int i=0;i<dense_row_ada_dx_g.length;++i) {
          Utils.div(get_ada_dx_g(i).raw(), N);
        }
      }
    }
    double uniformDist(Random rand, double min, double max) {
      return min + rand.nextFloat() * (max - min);
    }
    void randomizeWeights() {
      for (int w=0; w<dense_row_weights.length; ++w) {
        final Random rng = water.util.Utils.getDeterRNG(get_params().seed + 0xBAD5EED + w+1); //to match NeuralNet behavior
        final double range = Math.sqrt(6. / (units[w] + units[w+1]));
        for( int i = 0; i < get_weights(w).rows(); i++ ) {
          for( int j = 0; j < get_weights(w).cols(); j++ ) {
            if (get_params().initial_weight_distribution == DeepLearning.InitialWeightDistribution.UniformAdaptive) {
              // cf. http://machinelearning.wustl.edu/mlpapers/paper_files/AISTATS2010_GlorotB10.pdf
              if (w==dense_row_weights.length-1 && get_params().classification)
                get_weights(w).set(i,j, (float)(4.*uniformDist(rng, -range, range))); //Softmax might need an extra factor 4, since it's like a sigmoid
              else
                get_weights(w).set(i,j, (float)uniformDist(rng, -range, range));
            }
            else if (get_params().initial_weight_distribution == DeepLearning.InitialWeightDistribution.Uniform) {
              get_weights(w).set(i,j, (float)uniformDist(rng, -get_params().initial_weight_scale, get_params().initial_weight_scale));
            }
            else if (get_params().initial_weight_distribution == DeepLearning.InitialWeightDistribution.Normal) {
              get_weights(w).set(i,j, (float)(rng.nextGaussian() * get_params().initial_weight_scale));
            }
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

    /**
     * Compute Variable Importance, based on
     * GEDEON: DATA MINING OF INPUTS: ANALYSING MAGNITUDE AND FUNCTIONAL MEASURES
     * @return variable importances for input features
     */
    public float[] computeVariableImportances() {
      float[] vi = new float[units[0]];
      Arrays.fill(vi, 0f);

      float[][] Qik = new float[units[0]][units[2]]; //importance of input i on output k
      float[] sum_wj = new float[units[1]]; //sum of incoming weights into first hidden layer
      float[] sum_wk = new float[units[2]]; //sum of incoming weights into output layer (or second hidden layer)
      for (float[] Qi : Qik) Arrays.fill(Qi, 0f);
      Arrays.fill(sum_wj, 0f);
      Arrays.fill(sum_wk, 0f);

      // compute sum of absolute incoming weights
      for( int j = 0; j < units[1]; j++ ) {
        for( int i = 0; i < units[0]; i++ ) {
          float wij = get_weights(0).get(j, i);
          sum_wj[j] += Math.abs(wij);
        }
      }
      for( int k = 0; k < units[2]; k++ ) {
        for( int j = 0; j < units[1]; j++ ) {
          float wjk = get_weights(1).get(k,j);
          sum_wk[k] += Math.abs(wjk);
        }
      }
      // compute importance of input i on output k as product of connecting weights going through j
      for( int i = 0; i < units[0]; i++ ) {
        for( int k = 0; k < units[2]; k++ ) {
          for( int j = 0; j < units[1]; j++ ) {
            float wij = get_weights(0).get(j,i);
            float wjk = get_weights(1).get(k,j);
            //Qik[i][k] += Math.abs(wij)/sum_wj[j] * wjk; //Wong,Gedeon,Taggart '95
            Qik[i][k] += Math.abs(wij)/sum_wj[j] * Math.abs(wjk)/sum_wk[k]; //Gedeon '97
          }
        }
      }
      // normalize Qik over all outputs k
      for( int k = 0; k < units[2]; k++ ) {
        float sumQk = 0;
        for( int i = 0; i < units[0]; i++ ) sumQk += Qik[i][k];
        for( int i = 0; i < units[0]; i++ ) Qik[i][k] /= sumQk;
      }
      // importance for feature i is the sum over k of i->k importances
      for( int i = 0; i < units[0]; i++ ) vi[i] = Utils.sum(Qik[i]);

      //normalize importances such that max(vi) = 1
      Utils.div(vi, Utils.maxValue(vi));
      return vi;
    }

    // compute stats on all nodes
    public void computeStats() {
      float[][] rate = get_params().adaptive_rate ? new float[units.length-1][] : null;

      if (get_params().autoencoder && get_params().sparsity_beta > 0) {
        for (int k = 0; k < get_params().hidden.length; k++) {
          mean_a[k] = 0;
          for (int j = 0; j < avg_activations[k].size(); j++)
            mean_a[k] += avg_activations[k].get(j);
          mean_a[k] /= avg_activations[k].size();
        }
      }

      for( int y = 1; y < units.length; y++ ) {
        mean_rate[y] = rms_rate[y] = 0;
        mean_bias[y] = rms_bias[y] = 0;
        mean_weight[y] = rms_weight[y] = 0;
        for(int u = 0; u < biases[y-1].size(); u++) {
          mean_bias[y] += biases[y-1].get(u);
        }
        if (rate != null) rate[y-1] = new float[get_weights(y-1).raw().length];
        for(int u = 0; u < get_weights(y-1).raw().length; u++) {
          mean_weight[y] += get_weights(y-1).raw()[u];
          if (rate != null) {
//            final float RMS_dx = (float)Math.sqrt(ada[y-1][2*u]+(float)get_params().epsilon);
//            final float invRMS_g = (float)(1/Math.sqrt(ada[y-1][2*u+1]+(float)get_params().epsilon));
            final float RMS_dx = Utils.approxSqrt(get_ada_dx_g(y-1).raw()[2*u]+(float)get_params().epsilon);
            final float invRMS_g = Utils.approxInvSqrt(get_ada_dx_g(y-1).raw()[2*u+1]+(float)get_params().epsilon);
            rate[y-1][u] = RMS_dx*invRMS_g; //not exactly right, RMS_dx should be from the previous time step -> but close enough for diagnostics.
            mean_rate[y] += rate[y-1][u];
          }
        }


        mean_bias[y] /= biases[y-1].size();

        mean_weight[y] /= get_weights(y-1).size();
        if (rate != null) mean_rate[y] /= rate[y-1].length;

        for(int u = 0; u < biases[y-1].size(); u++) {
          final double db = biases[y-1].get(u) - mean_bias[y];
          rms_bias[y] += db * db;
        }
        for(int u = 0; u < get_weights(y-1).size(); u++) {
          final double dw = get_weights(y-1).raw()[u] - mean_weight[y];
          rms_weight[y] += dw * dw;
          if (rate != null) {
            final double drate = rate[y-1][u] - mean_rate[y];
            rms_rate[y] += drate * drate;
          }
        }
        rms_bias[y] = Utils.approxSqrt(rms_bias[y]/biases[y-1].size());
        rms_weight[y] = Utils.approxSqrt(rms_weight[y]/get_weights(y-1).size());
        if (rate != null) rms_rate[y] = Utils.approxSqrt(rms_rate[y]/rate[y-1].length);
//        rms_bias[y] = (float)Math.sqrt(rms_bias[y]/biases[y-1].length);
//        rms_weight[y] = (float)Math.sqrt(rms_weight[y]/weights[y-1].length);
//        if (rate != null) rms_rate[y] = (float)Math.sqrt(rms_rate[y]/rate[y-1].length);

        // Abort the run if weights or biases are unreasonably large (Note that all input values are normalized upfront)
        // This can happen with Rectifier units when L1/L2/max_w2 are all set to 0, especially when using more than 1 hidden layer.
        final double thresh = 1e10;
        unstable |= mean_bias[y] > thresh  || isNaN(mean_bias[y])
                || rms_bias[y] > thresh    || isNaN(rms_bias[y])
                || mean_weight[y] > thresh || isNaN(mean_weight[y])
                || rms_weight[y] > thresh  || isNaN(rms_weight[y]);
      }
    }
  }

  /**
   * Constructor to restart from a checkpointed model
   * @param cp Checkpoint to restart from
   * @param destKey New destination key for the model
   * @param jobKey New job key (job which updates the model)
   */
  public DeepLearningModel(final DeepLearningModel cp, final Key destKey, final Key jobKey, final DataInfo dataInfo) {
    super(destKey, cp._dataKey, dataInfo._adaptedFrame.names(), dataInfo._adaptedFrame.domains(), cp._priorClassDist != null ? cp._priorClassDist.clone() : null, null);
    final boolean store_best_model = (jobKey == null);
    this.jobKey = jobKey;
    this._validationKey = cp._validationKey;
    if (store_best_model) {
      model_info = cp.model_info.deep_clone(); //don't want to interfere with model being built, just make a deep copy and store that
      model_info.data_info = dataInfo.deep_clone(); //replace previous data_info with updated version that's passed in (contains enum for classification)
      get_params().state = Job.JobState.DONE; //change the deep_clone'd state to DONE
      _modelClassDist = cp._modelClassDist != null ? cp._modelClassDist.clone() : null;
    } else {
      model_info = (DeepLearningModelInfo) cp.model_info.clone(); //shallow clone is ok (won't modify the Checkpoint in K-V store during checkpoint restart)
      model_info.data_info = dataInfo; //shallow clone is ok
      get_params().checkpoint = cp._key; //it's only a "real" checkpoint if job != null, otherwise a best model copy
      get_params().state = ((DeepLearning)UKV.get(jobKey)).state; //make the job state consistent
    }
    get_params().job_key = jobKey;
    get_params().destination_key = destKey;
    get_params().start_time = System.currentTimeMillis(); //for displaying the model progress
    actual_best_model_key = cp.actual_best_model_key;
    start_time = cp.start_time;
    run_time = cp.run_time;
    training_rows = cp.training_rows; //copy the value to display the right number on the model page before training has started
    validation_rows = cp.validation_rows; //copy the value to display the right number on the model page before training has started
    _bestError = cp._bestError;

    // deep clone scoring history
    errors = cp.errors.clone();
    for (int i=0; i<errors.length;++i)
      errors[i] = cp.errors[i].deep_clone();

    // set proper timing
    _timeLastScoreEnter = System.currentTimeMillis();
    _timeLastScoreStart = 0;
    _timeLastScoreEnd = 0;
    _timeLastPrintStart = 0;
    assert(Arrays.equals(_key._kb, destKey._kb));
  }

  public DeepLearningModel(final Key destKey, final Key jobKey, final Key dataKey, final DataInfo dinfo, final DeepLearning params, final float[] priorDist) {
    super(destKey, dataKey, dinfo._adaptedFrame, priorDist);
    this.jobKey = jobKey;
    this._validationKey = params.validation != null ? params.validation._key : null;
    run_time = 0;
    start_time = System.currentTimeMillis();
    _timeLastScoreEnter = start_time;
    model_info = new DeepLearningModelInfo(params, dinfo);
    actual_best_model_key = Key.makeSystem(Key.make().toString());

    if (params.n_folds != 0) actual_best_model_key = null;
    Object job = UKV.get(jobKey);
    if (job instanceof DeepLearning)
      get_params().state = ((DeepLearning)UKV.get(jobKey)).state; //make the job state consistent
    else
      get_params().state = ((Job.JobHandle)UKV.get(jobKey)).state; //make the job state consistent
    if (!get_params().autoencoder) {
      errors = new Errors[1];
      errors[0] = new Errors();
      errors[0].validation = (params.validation != null);
      errors[0].num_folds = params.n_folds;
    }
    assert(Arrays.equals(_key._kb, destKey._kb));
  }

  public long _timeLastScoreEnter; //not transient: needed for HTML display page
  transient private long _timeLastScoreStart;
  transient private long _timeLastScoreEnd;
  transient private long _timeLastPrintStart;
  /**
   *
   * @param train training data from which the model is built (for epoch counting only)
   * @param ftrain potentially downsampled training data for scoring
   * @param ftest  potentially downsampled validation data for scoring
   * @param job_key key of the owning job
   * @return true if model building is ongoing
   */
  boolean doScoring(Frame train, Frame ftrain, Frame ftest, Key job_key, Job.ValidatedJob.Response2CMAdaptor vadaptor) {
    try {
      final long now = System.currentTimeMillis();
      epoch_counter = (float)model_info().get_processed_total()/training_rows;
      final double time_last_iter_millis = now-_timeLastScoreEnter;

      // Auto-tuning
      // if multi-node and auto-tuning and at least 10 ms for communication (to avoid doing thins on multi-JVM on same node),
      // then adjust the auto-tuning parameter 'actual_train_samples_per_iteration' such that the targeted ratio of comm to comp is achieved
      // Note: actual communication time is estimated by the NetworkTest's collective test.
      if (H2O.CLOUD.size() > 1 && get_params().train_samples_per_iteration == -2 && time_for_communication_us > 1e4) {
//        Log.info("Time taken for communication: " + PrettyPrint.usecs((long)time_for_communication_us));
//        Log.info("Time taken for Map/Reduce iteration: " + PrettyPrint.msecs((long)time_last_iter_millis, true));
        final double comm_to_work_ratio = (time_for_communication_us *1e-3) / time_last_iter_millis;
//        Log.info("Ratio of network communication to computation: " + String.format("%.3f", comm_to_work_ratio));
//        Log.info("target_comm_to_work: " + get_params().target_ratio_comm_to_comp);
        final double correction = get_params().target_ratio_comm_to_comp / comm_to_work_ratio;
//        Log.warn("Suggested value for train_samples_per_iteration: " + get_params().actual_train_samples_per_iteration/correction);
        actual_train_samples_per_iteration /= correction;
        actual_train_samples_per_iteration = Math.max(1, actual_train_samples_per_iteration);
      }

      run_time += time_last_iter_millis;
      _timeLastScoreEnter = now;
      boolean keep_running = (epoch_counter < get_params().epochs);
      final long sinceLastScore = now -_timeLastScoreStart;
      final long sinceLastPrint = now -_timeLastPrintStart;
      final long samples = model_info().get_processed_total();
      if (!keep_running || sinceLastPrint > get_params().score_interval*1000) {
        _timeLastPrintStart = now;
        Log.info("Training time: " + PrettyPrint.msecs(run_time, true)
                + ". Processed " + String.format("%,d", samples) + " samples" + " (" + String.format("%.3f", epoch_counter) + " epochs)."
                + " Speed: " + String.format("%.3f", 1000.*samples/run_time) + " samples/sec.");
      }

      // this is potentially slow - only do every so often
      if( !keep_running ||
              (sinceLastScore > get_params().score_interval*1000 //don't score too often
                      &&(double)(_timeLastScoreEnd-_timeLastScoreStart)/sinceLastScore < get_params().score_duty_cycle) ) { //duty cycle
        final boolean printme = !get_params().quiet_mode;
        final boolean adaptCM = (isClassifier() && vadaptor.needsAdaptation2CM());
        _timeLastScoreStart = now;
        if (get_params().diagnostics) model_info().computeStats();
        Errors err = new Errors();
        err.training_time_ms = run_time;
        err.epoch_counter = epoch_counter;
        err.training_samples = model_info().get_processed_total();
        err.validation = ftest != null;
        err.score_training_samples = ftrain.numRows();

        if (get_params().autoencoder) {
          if (printme) Log.info("Scoring the auto-encoder.");
          // training
          {
            final Frame mse_frame = scoreAutoEncoder(ftrain);
            final Vec l2 = mse_frame.anyVec();
            Log.info("Mean reconstruction error on training data: " + l2.mean() + "\n");
            err.train_mse = l2.mean();
            mse_frame.delete();
          }
        } else {
          if (printme) Log.info("Scoring the model.");
          // compute errors
          err.classification = isClassifier();
          assert (err.classification == get_params().classification);
          err.num_folds = get_params().n_folds;
          err.train_confusion_matrix = new ConfusionMatrix();
          final int hit_k = Math.min(nclasses(), get_params().max_hit_ratio_k);
          if (err.classification && nclasses() > 2 && hit_k > 0) {
            err.train_hitratio = new HitRatio();
            err.train_hitratio.set_max_k(hit_k);
          }
          final String m = model_info().toString();
          if (m.length() > 0) Log.info(m);
          final Frame trainPredict = score(ftrain, false);
          AUC trainAUC = null;
          if (err.classification && nclasses() == 2) trainAUC = new AUC();
          final double trainErr = calcError(ftrain, ftrain.lastVec(), trainPredict, trainPredict, "training",
                  printme, get_params().max_confusion_matrix_size, err.train_confusion_matrix, trainAUC, err.train_hitratio);
          if (isClassifier()) err.train_err = trainErr;
          if (trainAUC != null) err.trainAUC = trainAUC.data();
          else err.train_mse = trainErr;

          trainPredict.delete();

          if (err.validation) {
            assert ftest != null;
            err.score_validation_samples = ftest.numRows();
            err.valid_confusion_matrix = new ConfusionMatrix();
            if (err.classification && nclasses() > 2 && hit_k > 0) {
              err.valid_hitratio = new HitRatio();
              err.valid_hitratio.set_max_k(hit_k);
            }
            final String adaptRespName = vadaptor.adaptedValidationResponse(responseName());
            Vec adaptCMresp = null;
            if (adaptCM) {
              Vec[] v = ftest.vecs();
              assert (ftest.find(adaptRespName) == v.length - 1); //make sure to have (adapted) response in the test set
              adaptCMresp = ftest.remove(v.length - 1); //model would remove any extra columns anyway (need to keep it here for later)
            }

            final Frame validPredict = score(ftest, adaptCM);
            final Frame hitratio_validPredict = new Frame(validPredict);
            Vec orig_label = validPredict.vecs()[0];
            // Adapt output response domain, in case validation domain is different from training domain
            // Note: doesn't change predictions, just the *possible* label domain
            if (adaptCM) {
              assert (adaptCMresp != null);
              assert (ftest.find(adaptRespName) == -1);
              ftest.add(adaptRespName, adaptCMresp);
              final Vec CMadapted = vadaptor.adaptModelResponse2CM(validPredict.vecs()[0]);
              validPredict.replace(0, CMadapted); //replace label
              validPredict.add("to_be_deleted", CMadapted); //keep the Vec around to be deleted later (no leak)
            }
            AUC validAUC = null;
            if (err.classification && nclasses() == 2) validAUC = new AUC();
            final double validErr = calcError(ftest, ftest.lastVec(), validPredict, hitratio_validPredict, "validation",
                    printme, get_params().max_confusion_matrix_size, err.valid_confusion_matrix, validAUC, err.valid_hitratio);
            if (isClassifier()) err.valid_err = validErr;
            if (trainAUC != null) err.validAUC = validAUC.data();
            else err.valid_mse = validErr;
            validPredict.delete();
            //also delete the replaced label
            if (adaptCM) orig_label.remove(new Futures()).blockForPending();
          }

          // only keep confusion matrices for the last step if there are fewer than specified number of output classes
          if (err.train_confusion_matrix.cm != null
                  && err.train_confusion_matrix.cm.length - 1 >= get_params().max_confusion_matrix_size) {
            err.train_confusion_matrix = null;
            err.valid_confusion_matrix = null;
          }
        }

        if (get_params().variable_importances) {
          if (!get_params().quiet_mode) Log.info("Computing variable importances.");
          final float[] vi = model_info().computeVariableImportances();
          err.variable_importances = new VarImp(vi, Arrays.copyOfRange(model_info().data_info().coefNames(), 0, vi.length));
        }

        _timeLastScoreEnd = System.currentTimeMillis();
        err.scoring_time = System.currentTimeMillis() - now;
        // enlarge the error array by one, push latest score back
        if (errors == null) {
          errors = new Errors[]{err};
        } else {
          Errors[] err2 = new Errors[errors.length + 1];
          System.arraycopy(errors, 0, err2, 0, errors.length);
          err2[err2.length - 1] = err;
          errors = err2;
        }

        if (!get_params().autoencoder) {
          // always keep a copy of the best model so far (based on the following criterion)
          if (actual_best_model_key != null && (
                  // if we have a best_model in DKV, then compare against its error() (unless it's a different model as judged by the network size)
                  (UKV.get(actual_best_model_key) != null && (error() < UKV.<DeepLearningModel>get(actual_best_model_key).error() || !Arrays.equals(model_info().units, UKV.<DeepLearningModel>get(actual_best_model_key).model_info().units)))
                          ||
                          // otherwise, compare against our own _bestError
                          (UKV.get(actual_best_model_key) == null && error() < _bestError)
          ) ) {
            if (!get_params().quiet_mode)
              Log.info("Error reduced from " + _bestError + " to " + error() + ". Storing best model so far under key " + actual_best_model_key.toString() + ".");
            _bestError = error();
            putMeAsBestModel(actual_best_model_key);

            // debugging check
            if (false) {
              DeepLearningModel bestModel = UKV.get(actual_best_model_key);
              final Frame fr = ftest != null ? ftest : ftrain;
              final Frame bestPredict = bestModel.score(fr, ftest != null ? adaptCM : false);
              final Frame hitRatio_bestPredict = new Frame(bestPredict);
              // Adapt output response domain, in case validation domain is different from training domain
              // Note: doesn't change predictions, just the *possible* label domain
              if (adaptCM) {
                final Vec CMadapted = vadaptor.adaptModelResponse2CM(bestPredict.vecs()[0]);
                bestPredict.replace(0, CMadapted); //replace label
                bestPredict.add("to_be_deleted", CMadapted); //keep the Vec around to be deleted later (no leak)
              }
              final double err3 = calcError(fr, fr.lastVec(), bestPredict, hitRatio_bestPredict, "cross-check",
                      printme, get_params().max_confusion_matrix_size, new water.api.ConfusionMatrix(), isClassifier() && nclasses() == 2 ? new AUC() : null, null);
              if (isClassifier())
                assert (ftest != null ? Math.abs(err.valid_err - err3) < 1e-5 : Math.abs(err.train_err - err3) < 1e-5);
              else
                assert (ftest != null ? Math.abs(err.valid_mse - err3) < 1e-5 : Math.abs(err.train_mse - err3) < 1e-5);
              bestPredict.delete();
            }
          }
//        else {
//          // keep output JSON small
//          if (errors.length > 1) {
//            if (last_scored().trainAUC != null) last_scored().trainAUC.clear();
//            if (last_scored().validAUC != null) last_scored().validAUC.clear();
//            last_scored().variable_importances = null;
//          }
//        }

          // print the freshly scored model to ASCII
          for (String s : toString().split("\n")) Log.info(s);
          if (printme) Log.info("Time taken for scoring and diagnostics: " + PrettyPrint.msecs(err.scoring_time, true));
        }
      }
      if (model_info().unstable()) {
        Log.warn(unstable_msg);
        keep_running = false;
      } else if ( (isClassifier() && last_scored().train_err <= get_params().classification_stop)
              || (!isClassifier() && last_scored().train_mse <= get_params().regression_stop) ) {
        Log.info("Achieved requested predictive accuracy on the training data. Model building completed.");
        keep_running = false;
      }
      update(job_key);

//    System.out.println(this);
      return keep_running;
    }
    catch (Exception ex) {
      return false;
    }
  }

  @Override protected void setCrossValidationError(Job.ValidatedJob job, double cv_error, ConfusionMatrix cm, AUCData auc, HitRatio hr) {
    _have_cv_results = true;
    if (!get_params().classification)
      last_scored().valid_mse = cv_error;
    else
      last_scored().valid_err = cv_error;
    last_scored().score_validation_samples = last_scored().score_training_samples / get_params().n_folds;
    last_scored().num_folds = get_params().n_folds;
    last_scored().valid_confusion_matrix = cm;
    last_scored().validAUC = auc;
    last_scored().valid_hitratio = hr;
    DKV.put(this._key, this); //overwrite this model
  }

  @Override public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(model_info.toString());
    sb.append(last_scored().toString());
    return sb.toString();
  }

  public String toStringAll() {
    StringBuilder sb = new StringBuilder();
    sb.append(model_info.toStringAll());
    sb.append(last_scored().toString());
    return sb.toString();
  }

  public String getHeader() {
    assert get_params().autoencoder;
    StringBuilder sb = new StringBuilder();
    final int len = model_info().data_info().fullN();
    String prefix = "reconstr_";
    assert (model_info().data_info()._responses == 0);
    String[] coefnames = model_info().data_info().coefNames();
    assert (len == coefnames.length);
    for (int c = 0; c < len; c++) {
      if (c>0) sb.append(",");
      sb.append(prefix + coefnames[c]);
    }
    return sb.toString();
  }

  /**
   * This is an overridden version of Model.score(). Make either a prediction or a reconstruction.
   * @param frame Test dataset
   * @return A frame containing the prediction or reconstruction
   */
  @Override
  public Frame score(Frame frame) {
    if (!get_params().autoencoder) {
      return super.score(frame);
    } else {
      // Reconstruction
      // Adapt the Frame layout - returns adapted frame and frame containing only
      // newly created vectors
      Frame[] adaptFrms = adapt(frame,false,false/*no response*/);
      // Adapted frame containing all columns - mix of original vectors from fr
      // and newly created vectors serving as adaptors
      Frame adaptFrm = adaptFrms[0];
      // Contains only newly created vectors. The frame eases deletion of these vectors.
      Frame onlyAdaptFrm = adaptFrms[1];

      final int len = model_info().data_info().fullN();
      String prefix = "reconstr_";
      assert(model_info().data_info()._responses == 0);
      String[] coefnames = model_info().data_info().coefNames();
      assert(len == coefnames.length);
      for( int c=0; c<len; c++ )
        adaptFrm.add(prefix+coefnames[c],adaptFrm.anyVec().makeZero());
      new MRTask2() {
        @Override public void map( Chunk chks[] ) {
          double tmp [] = new double[_names.length];
          float preds[] = new float [len];
          final Neurons[] neurons = DeepLearningTask.makeNeuronsForTesting(model_info);
          for( int row=0; row<chks[0]._len; row++ ) {
            float p[] = score_autoencoder(chks, row, tmp, preds, neurons);
            for( int c=0; c<preds.length; c++ )
              chks[_names.length+c].set0(row,p[c]);
          }
        }
      }.doAll(adaptFrm);

      // Return the predicted columns
      int x=_names.length, y=adaptFrm.numCols();
      Frame f = adaptFrm.extractFrame(x, y); //this will call vec_impl() and we cannot call the delete() below just yet
      onlyAdaptFrm.delete();
      return f;
    }
  }

  /**
   * Predict from raw double values representing the data
   * @param data raw array containing categorical values (horizontalized to 1,0,0,1,0,0 etc.) and numerical values (0.35,1.24,5.3234,etc), both can contain NaNs
   * @param preds predicted label and per-class probabilities (for classification), predicted target (regression), can contain NaNs
   * @return preds, can contain NaNs
   */
  @Override public float[] score0(double[] data, float[] preds) {
    if (model_info().unstable()) {
      Log.warn(unstable_msg);
      throw new UnsupportedOperationException("Trying to predict with an unstable model.");
    }
    Neurons[] neurons = DeepLearningTask.makeNeuronsForTesting(model_info);
    ((Neurons.Input)neurons[0]).setInput(-1, data);
    DeepLearningTask.step(-1, neurons, model_info, false, null);
    float[] out = neurons[neurons.length - 1]._a.raw();
    if (isClassifier()) {
      assert (preds.length == out.length + 1);
      for (int i = 0; i < preds.length - 1; ++i) {
        preds[i + 1] = out[i];
        if (Float.isNaN(preds[i + 1])) throw new RuntimeException("Predicted class probability NaN!");
      }
      preds[0] = ModelUtils.getPrediction(preds, data);
    } else {
      assert (preds.length == 1 && out.length == 1);
      if (model_info().data_info()._normRespMul != null)
        preds[0] = (float) (out[0] / model_info().data_info()._normRespMul[0] + model_info().data_info()._normRespSub[0]);
      else
        preds[0] = out[0];
      if (Float.isNaN(preds[0])) throw new RuntimeException("Predicted regression target NaN!");
    }
    return preds;
  }

  /**
   * Score auto-encoded reconstruction (on-the-fly, without allocating the reconstruction as done in Frame score(Frame fr))
   * @param frame Original data (can contain response, will be ignored)
   * @return Frame containing one Vec with reconstruction error (MSE) of each reconstructed row, caller is responsible for deletion
   */
  public Frame scoreAutoEncoder(Frame frame) {
    final int len = _names.length;
    // Adapt the Frame layout - returns adapted frame and frame containing only
    // newly created vectors
    Frame[] adaptFrms = adapt(frame,false,false/*no response*/);
    // Adapted frame containing all columns - mix of original vectors from fr
    // and newly created vectors serving as adaptors
    Frame adaptFrm = adaptFrms[0];
    // Contains only newly created vectors. The frame eases deletion of these vectors.
    Frame onlyAdaptFrm = adaptFrms[1];
    adaptFrm.add("Reconstruction.MSE", adaptFrm.anyVec().makeZero());
    new MRTask2() {
      @Override public void map( Chunk chks[] ) {
        double tmp [] = new double[len];
        final Neurons[] neurons = DeepLearningTask.makeNeuronsForTesting(model_info);
        for( int row=0; row<chks[0]._len; row++ ) {
          for( int i=0; i<_names.length; i++ )
            tmp[i] = chks[i].at0(row); //original data
          chks[len].set0(row, score_autoencoder(tmp, null, neurons)); //store the per-row reconstruction error (MSE) in the last column
        }
      }
    }.doAll(adaptFrm);

    // Return just the output columns
    int x=_names.length, y=adaptFrm.numCols();
    final Frame l2 = adaptFrm.extractFrame(x, y);
    onlyAdaptFrm.delete();
    return l2;
  }

  /**
   * Score auto-encoded reconstruction (on-the-fly, without allocating the reconstruction as done in Frame score(Frame fr))
   * @param frame Original data (can contain response, will be ignored)
   * @return Frame containing one Vec with reconstruction error (MSE) of each reconstructed row, caller is responsible for deletion
   */
  public Frame scoreDeepFeatures(Frame frame, final int layer) {
    assert(layer >= 0 && layer < model_info().get_params().hidden.length);
    final int len = nfeatures();
    Vec resp = null;
    if (isSupervised()) {
      int ridx = frame.find(responseName());
      if (ridx != -1) { // drop the response for scoring!
        frame = new Frame(frame);
        resp = frame.vecs()[ridx];
        frame.remove(ridx);
      }
    }
    // Adapt the Frame layout - returns adapted frame and frame containing only
    // newly created vectors
    Frame[] adaptFrms = adapt(frame,false,false/*no response*/);
    // Adapted frame containing all columns - mix of original vectors from fr
    // and newly created vectors serving as adaptors
    Frame adaptFrm = adaptFrms[0];
    // Contains only newly created vectors. The frame eases deletion of these vectors.
    Frame onlyAdaptFrm = adaptFrms[1];
    //create new features, will be dense
    final int features = model_info().get_params().hidden[layer];
    Vec[] vecs = adaptFrm.anyVec().makeZeros(features);
    for (int j=0; j<features; ++j) {
      adaptFrm.add("DF.C" + (j+1), vecs[j]);
    }
    new MRTask2() {
      @Override public void map( Chunk chks[] ) {
        double tmp [] = new double[len];
        float df[] = new float [features];
        final Neurons[] neurons = DeepLearningTask.makeNeuronsForTesting(model_info);
        for( int row=0; row<chks[0]._len; row++ ) {
          for( int i=0; i<len; i++ )
            tmp[i] = chks[i].at0(row);
          ((Neurons.Input)neurons[0]).setInput(-1, tmp);
          DeepLearningTask.step(-1, neurons, model_info, false, null);
          float[] out = neurons[layer+1]._a.raw(); //extract the layer-th hidden feature
          for( int c=0; c<df.length; c++ )
            chks[_names.length+c].set0(row,out[c]);
        }
      }
    }.doAll(adaptFrm);

    // Return just the output columns
    int x=_names.length, y=adaptFrm.numCols();
    Frame ret = adaptFrm.extractFrame(x, y);
    onlyAdaptFrm.delete();
    if (resp != null) ret.prepend(responseName(), resp);
    return ret;
  }

  // Make (potentially expanded) reconstruction
  private float[] score_autoencoder(Chunk[] chks, int row_in_chunk, double[] tmp, float[] preds, Neurons[] neurons) {
    assert(get_params().autoencoder);
    assert(tmp.length == _names.length);
    for( int i=0; i<tmp.length; i++ )
      tmp[i] = chks[i].at0(row_in_chunk);
    score_autoencoder(tmp, preds, neurons); // this fills preds, returns MSE error (ignored here)
    return preds;
  }

  /**
   * Helper to reconstruct original data into preds array and compute the reconstruction error (MSE)
   * @param data Original data (unexpanded)
   * @param preds Reconstruction (potentially expanded)
   * @return reconstruction error
   */
  private double score_autoencoder(double[] data, float[] preds, Neurons[] neurons) {
    assert(model_info().get_params().autoencoder);
    if (model_info().unstable()) {
      Log.warn(unstable_msg);
      throw new UnsupportedOperationException("Trying to predict with an unstable model.");
    }
    ((Neurons.Input)neurons[0]).setInput(-1, data); // expands categoricals inside
    DeepLearningTask.step(-1, neurons, model_info, false, null); // reconstructs data in expanded space
    float[] in  = neurons[0]._a.raw(); //input (expanded)
    float[] out = neurons[neurons.length - 1]._a.raw(); //output (expanded)
    // DEBUGGING
//    Log.info(Arrays.toString(data));
//    Log.info(Arrays.toString(in));
//    Log.info(Arrays.toString(out));
    assert(in.length == out.length);

    // First normalize categorical reconstructions to be probabilities
    // (such that they can be better compared to the input where one factor was 1 and the rest was 0)
//    model_info().data_info().softMaxCategoricals(out,out); //only modifies the categoricals

    // Compute MSE of reconstruction in expanded space (with categorical probabilities)
    double l2 = 0;
    for (int i = 0; i < in.length; ++i)
      l2 += Math.pow((out[i] - in[i]), 2);
    l2 /= in.length;

    if (preds!=null) {
      // Now scale back numerical columns to original data space (scale + shift)
      model_info().data_info().unScaleNumericals(out, out); //only modifies the numericals
      System.arraycopy(out, 0, preds, 0, out.length); //copy reconstruction into preds
    }
    // DEBUGGING
//    Log.info(Arrays.toString(preds));
//    Log.info("");
    return l2;
  }

  /**
   * Compute quantile-based threshold (in reconstruction error) to find outliers
   * @param mse Vector containing reconstruction errors
   * @param quantile Quantile for cut-off
   * @return Threshold in MSE value for a point to be above the quantile
   */
  public double calcOutlierThreshold(Vec mse, double quantile) {
    Frame mse_frame = new Frame(Key.make(), new String[]{"Reconstruction.MSE"}, new Vec[]{mse});
    QuantilesPage qp = new QuantilesPage();
    qp.column = mse_frame.vec(0);
    qp.source_key = mse_frame;
    qp.quantile = quantile;
    qp.invoke();
    DKV.remove(mse_frame._key);
    return qp.result;
  }

  @Override public ModelAutobufferSerializer getModelSerializer() {
    // Return a serializer which knows how to serialize keys
    return new ModelAutobufferSerializer() {
      @Override protected AutoBuffer postLoad(Model m, AutoBuffer ab) {
        Job.hygiene(((DeepLearningModel)m).get_params());
        return ab;
      }
    };
  }

  public boolean generateHTML(String title, StringBuilder sb) {
    if (_key == null) {
      DocGen.HTML.title(sb, "No model yet");
      return true;
    }

    // optional JFrame creation for visualization of weights
//    DeepLearningVisualization.visualize(this);

    final String mse_format = "%g";
//    final String cross_entropy_format = "%2.6f";

    // stats for training and validation
    final Errors error = last_scored();

    DocGen.HTML.title(sb, title);

    if (get_params().source == null || DKV.get(get_params().source._key) == null ||
            (get_params().validation != null && DKV.get(get_params().validation._key) == null)) (Job.hygiene(get_params())).toHTML(sb);
    else job().toHTML(sb);

    sb.append("<div class='alert'>Actions: "
            + (jobKey != null && UKV.get(jobKey) != null && Job.isRunning(jobKey) ? "<i class=\"icon-stop\"></i>" + Cancel.link(jobKey, "Stop training") + ", " : "")
            + Inspect2.link("Inspect training data (" + _dataKey + ")", _dataKey) + ", "
            + (_validationKey != null ? (Inspect2.link("Inspect validation data (" + _validationKey + ")", _validationKey) + ", ") : "")
            + water.api.Predict.link(_key, "Score on dataset") + ", "
            + DeepLearning.link(_dataKey, "Compute new model", null, responseName(), _validationKey)
            + (actual_best_model_key != null && UKV.get(actual_best_model_key) != null && actual_best_model_key != _key ? ", " + DeepLearningModelView.link("Go to best model", actual_best_model_key) : "")
            + (jobKey == null || ((jobKey != null && UKV.get(jobKey) == null)) || (jobKey != null && UKV.get(jobKey) != null && Job.isEnded(jobKey)) ? ", <i class=\"icon-play\"></i>" + DeepLearning.link(_dataKey, "Continue training this model", _key, responseName(), _validationKey) : "") + ", "
            + UIUtils.qlink(SaveModel.class, "model", _key, "Save model") + ", "
            + "</div>");

    DocGen.HTML.paragraph(sb, "Model Key: " + _key);
    if (jobKey != null) DocGen.HTML.paragraph(sb, "Job Key: " + jobKey);
    if (!get_params().autoencoder)
      DocGen.HTML.paragraph(sb, "Model type: " + (get_params().classification ? " Classification" : " Regression") + ", predicting: " + responseName());
    else
      DocGen.HTML.paragraph(sb, "Model type: Auto-Encoder");
    DocGen.HTML.paragraph(sb, "Number of model parameters (weights/biases): " + String.format("%,d", model_info().size()));

    if (model_info.unstable()) {
      DocGen.HTML.section(sb, "=======================================================================================");
      DocGen.HTML.section(sb, unstable_msg.replace("\n"," "));
      DocGen.HTML.section(sb, "=======================================================================================");
    }

    if (error == null) return true;

    DocGen.HTML.title(sb, "Progress");
    // update epoch counter every time the website is displayed
    epoch_counter = training_rows > 0 ? (float)model_info().get_processed_total()/training_rows : 0;
    final double progress = get_params().progress();

    if (get_params() != null && get_params().diagnostics) {
      DocGen.HTML.section(sb, "Status of Neuron Layers");
      sb.append("<table class='table table-striped table-bordered table-condensed'>");
      sb.append("<tr>");
      sb.append("<th>").append("#").append("</th>");
      sb.append("<th>").append("Units").append("</th>");
      sb.append("<th>").append("Type").append("</th>");
      sb.append("<th>").append("Dropout").append("</th>");
      sb.append("<th>").append("L1").append("</th>");
      sb.append("<th>").append("L2").append("</th>");
      if (get_params().adaptive_rate) {
        sb.append("<th>").append("Rate (Mean, RMS)").append("</th>");
      } else {
        sb.append("<th>").append("Rate").append("</th>");
        sb.append("<th>").append("Momentum").append("</th>");
      }
      sb.append("<th>").append("Weight (Mean, RMS)").append("</th>");
      sb.append("<th>").append("Bias (Mean, RMS)").append("</th>");
      sb.append("</tr>");
      Neurons[] neurons = DeepLearningTask.makeNeuronsForTesting(model_info()); //link the weights to the neurons, for easy access
      for (int i=0; i<neurons.length; ++i) {
        sb.append("<tr>");
        sb.append("<td>").append("<b>").append(i+1).append("</b>").append("</td>");
        sb.append("<td>").append("<b>").append(neurons[i].units).append("</b>").append("</td>");
        sb.append("<td>").append(neurons[i].getClass().getSimpleName()).append("</td>");

        if (i == 0) {
          sb.append("<td>");
          sb.append(Utils.formatPct(neurons[i].params.input_dropout_ratio));
          sb.append("</td>");
          sb.append("<td></td>");
          sb.append("<td></td>");
          sb.append("<td></td>");
          if (!get_params().adaptive_rate) sb.append("<td></td>");
          sb.append("<td></td>");
          sb.append("<td></td>");
          sb.append("</tr>");
          continue;
        }
        else if (i < neurons.length-1) {
          sb.append("<td>");
          if (neurons[i].params.hidden_dropout_ratios == null)
            sb.append(Utils.formatPct(0));
          else
            sb.append(Utils.formatPct(neurons[i].params.hidden_dropout_ratios[i - 1]));
          sb.append("</td>");
        } else {
          sb.append("<td></td>");
        }

        final String format = "%g";
        sb.append("<td>").append(neurons[i].params.l1).append("</td>");
        sb.append("<td>").append(neurons[i].params.l2).append("</td>");
        if (get_params().adaptive_rate) {
          sb.append("<td>(").append(String.format(format, model_info.mean_rate[i])).
                  append(", ").append(String.format(format, model_info.rms_rate[i])).append(")</td>");
        } else {
          sb.append("<td>").append(String.format("%.5g", neurons[i].rate(error.training_samples))).append("</td>");
          sb.append("<td>").append(String.format("%.5f", neurons[i].momentum(error.training_samples))).append("</td>");
        }
        sb.append("<td>(").append(String.format(format, model_info.mean_weight[i])).
                append(", ").append(String.format(format, model_info.rms_weight[i])).append(")</td>");
        sb.append("<td>(").append(String.format(format, model_info.mean_bias[i])).
                append(", ").append(String.format(format, model_info.rms_bias[i])).append(")</td>");
        sb.append("</tr>");
      }
      sb.append("</table>");
    }

    if (isClassifier() && !get_params().autoencoder) {
      DocGen.HTML.section(sb, "Classification error on training data: " + Utils.formatPct(error.train_err));
      if(error.validation) {
        DocGen.HTML.section(sb, "Classification error on validation data: " + Utils.formatPct(error.valid_err));
      } else if(error.num_folds > 0) {
        DocGen.HTML.section(sb, "Classification error on " + error.num_folds + "-fold cross-validated training data"
                + (_have_cv_results ? ": " + Utils.formatPct(error.valid_err) : " is being computed - please reload this page later."));
      }
    } else {
      DocGen.HTML.section(sb, "MSE on training data: " + String.format(mse_format, error.train_mse));
      if(error.validation) {
        DocGen.HTML.section(sb, "MSE on validation data: " + String.format(mse_format, error.valid_mse));
      } else if(error.num_folds > 0) {
        DocGen.HTML.section(sb, "MSE on " + error.num_folds + "-fold cross-validated training data"
                + (_have_cv_results ? ": " + String.format(mse_format, error.valid_mse) : " is being computed - please reload this page later."));
      }
    }
    DocGen.HTML.paragraph(sb, "Training samples: " + String.format("%,d", model_info().get_processed_total()));
    DocGen.HTML.paragraph(sb, "Epochs: " + String.format("%.3f", epoch_counter) + " / " + String.format("%.3f", get_params().epochs));
    int cores = 0; for (H2ONode n : H2O.CLOUD._memary) cores += n._heartbeat._num_cpus;
    DocGen.HTML.paragraph(sb, "Number of compute nodes: " + (model_info.get_params().single_node_mode ? ("1 (" + H2O.NUMCPUS + " threads)") : (H2O.CLOUD.size() + " (" + cores + " threads)")));
    DocGen.HTML.paragraph(sb, "Training samples per iteration" + (
            get_params().train_samples_per_iteration == -2 ? " (-2 -> auto-tuning): " :
            get_params().train_samples_per_iteration == -1 ? " (-1 -> max. available data): " :
            get_params().train_samples_per_iteration == 0 ? " (0 -> one epoch): " : " (user-given): ")
                    + String.format("%,d", actual_train_samples_per_iteration));

    final boolean isEnded = get_params().self() == null || (UKV.get(get_params().self()) != null && Job.isEnded(get_params().self()));
    final long time_so_far = isEnded ? run_time : run_time + System.currentTimeMillis() - _timeLastScoreEnter;
    if (time_so_far > 0) {
      long time_for_speed = isEnded || H2O.CLOUD.size() > 1 ? run_time : time_so_far;
      if (time_for_speed > 0)
        DocGen.HTML.paragraph(sb, "Training speed: " + String.format("%,d", model_info().get_processed_total() * 1000 / time_for_speed) + " samples/s");
    }
    DocGen.HTML.paragraph(sb, "Training time: " + PrettyPrint.msecs(time_so_far, true));
    if (progress > 0 && !isEnded)
      DocGen.HTML.paragraph(sb, "Estimated time left: " +PrettyPrint.msecs((long)(time_so_far*(1-progress)/progress), true));

    long score_train = error.score_training_samples;
    long score_valid = error.score_validation_samples;
    final boolean fulltrain = score_train==0 || score_train == training_rows;
    final boolean fullvalid = error.validation && get_params().n_folds == 0 && (score_valid==0 || score_valid == validation_rows);

    final String toolarge = " Confusion matrix not shown here - too large: number of classes (" + model_info.units[model_info.units.length-1]
            + ") is greater than the specified limit of " + get_params().max_confusion_matrix_size + ".";
    boolean smallenough = model_info.units[model_info.units.length-1] <= get_params().max_confusion_matrix_size;

    if (!error.validation) {
      if (_have_cv_results) {
        String cmTitle = "<div class=\"alert\">Scoring results reported for " + error.num_folds + "-fold cross-validated training data " + Inspect2.link(_dataKey) + ":</div>";
        sb.append("<h5>" + cmTitle);
        sb.append("</h5>");
      }
      else {
        String cmTitle = "<div class=\"alert\">Scoring results reported on training data " + Inspect2.link(_dataKey) + (fulltrain ? "" : " (" + score_train + " samples)") + ":</div>";
        sb.append("<h5>" + cmTitle);
        sb.append("</h5>");
      }
    }
    else {
      RString v_rs = new RString("<a href='Inspect2.html?src_key=%$key'>%key</a>");
      String cmTitle = "<div class=\"alert\">Scoring results reported on validation data " + Inspect2.link(_validationKey) + (fullvalid ? "" : " (" + score_valid + " samples)") + ":</div>";
      sb.append("<h5>" + cmTitle);
      sb.append("</h5>");
    }

    if (isClassifier()) {
      // print AUC
      if (error.validAUC != null) {
        error.validAUC.toHTML(sb);
      }
      else if (error.trainAUC != null) {
        error.trainAUC.toHTML(sb);
      }
      else {
        if (error.validation) {
          if (error.valid_confusion_matrix != null && smallenough) {
            error.valid_confusion_matrix.toHTML(sb);
          } else if (smallenough) sb.append("<h5>Confusion matrix on validation data is not yet computed.</h5>");
          else sb.append(toolarge);
        }
        else if (_have_cv_results) {
          if (error.valid_confusion_matrix != null && smallenough) {
            error.valid_confusion_matrix.toHTML(sb);
          } else if (smallenough) sb.append("<h5>Confusion matrix on " + error.num_folds + "-fold cross-validated training data is not yet computed.</h5>");
          else sb.append(toolarge);
        }
        else {
          if (error.train_confusion_matrix != null && smallenough) {
            error.train_confusion_matrix.toHTML(sb);
          } else if (smallenough) sb.append("<h5>Confusion matrix on training data is not yet computed.</h5>");
          else sb.append(toolarge);
        }
      }
    }

    // Hit ratio
    if (error.valid_hitratio != null) {
      error.valid_hitratio.toHTML(sb);
    } else if (error.train_hitratio != null) {
      error.train_hitratio.toHTML(sb);
    }

    // Variable importance
    if (error.variable_importances != null) {
      error.variable_importances.toHTML(this, sb);
    }

    printCrossValidationModelsHTML(sb);

    DocGen.HTML.title(sb, "Scoring history");
    if (errors.length > 1) {
      DocGen.HTML.paragraph(sb, "Time taken for last scoring and diagnostics: " + PrettyPrint.msecs(errors[errors.length-1].scoring_time, true));
      // training
      {
        final long pts = fulltrain ? training_rows : score_train;
        String training = "Number of training data samples for scoring: " + (fulltrain ? "all " : "") + pts;
        if (pts < 1000 && training_rows >= 1000) training += " (low, scoring might be inaccurate -> consider increasing this number in the expert mode)";
        if (pts > 100000 && errors[errors.length-1].scoring_time > 10000) training += " (large, scoring can be slow -> consider reducing this number in the expert mode or scoring manually)";
        DocGen.HTML.paragraph(sb, training);
      }
      // validation
      if (error.validation) {
        final long ptsv = fullvalid ? validation_rows : score_valid;
        String validation = "Number of validation data samples for scoring: " + (fullvalid ? "all " : "") + ptsv;
        if (ptsv < 1000 && validation_rows >= 1000) validation += " (low, scoring might be inaccurate -> consider increasing this number in the expert mode)";
        if (ptsv > 100000 && errors[errors.length-1].scoring_time > 10000) validation += " (large, scoring can be slow -> consider reducing this number in the expert mode or scoring manually)";
        DocGen.HTML.paragraph(sb, validation);
      }

      if (isClassifier() && nclasses() != 2 /*binary classifier has its own conflicting D3 object (AUC)*/) {
        // Plot training error
        float[] err = new float[errors.length];
        float[] samples = new float[errors.length];
        for (int i=0; i<err.length; ++i) {
          err[i] = (float)errors[i].train_err;
          samples[i] = errors[i].training_samples;
        }
        new D3Plot(samples, err, "training samples", "classification error",
                "classification error on training data").generate(sb);

        // Plot validation error
        if (error.validation) {
          for (int i=0; i<err.length; ++i) {
            err[i] = (float)errors[i].valid_err;
          }
          new D3Plot(samples, err, "training samples", "classification error",
                  "classification error on validation set").generate(sb);
        }
      }
      // regression
      else if (!isClassifier()) {
        // Plot training MSE
        float[] err = new float[errors.length-1];
        float[] samples = new float[errors.length-1];
        for (int i=0; i<err.length; ++i) {
          err[i] = (float)errors[i+1].train_mse;
          samples[i] = errors[i+1].training_samples;
        }
        new D3Plot(samples, err, "training samples", "MSE",
                "regression error on training data").generate(sb);

        // Plot validation MSE
        if (error.validation) {
          for (int i=0; i<err.length; ++i) {
            err[i] = (float)errors[i+1].valid_mse;
          }
          new D3Plot(samples, err, "training samples", "MSE",
                  "regression error on validation data").generate(sb);
        }
      }
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
      if (nclasses()==2) sb.append("<th>Training AUC</th>");
    } else {
      sb.append("<th>Training MSE</th>");
    }
    if (error.validation) {
      if (isClassifier()) {
//      sb.append("<th>Validation MCE</th>");
        sb.append("<th>Validation Error</th>");
        if (nclasses()==2) sb.append("<th>Validation AUC</th>");
      } else {
        sb.append("<th>Validation MSE</th>");
      }
    }
    else if (error.num_folds > 0) {
      if (isClassifier()) {
        sb.append("<th>Cross-Validation Error</th>");
        if (nclasses()==2) sb.append("<th>Cross-Validation AUC</th>");
      } else {
        sb.append("<th>Cross-Validation MSE</th>");
      }
    }
    sb.append("</tr>");
    for( int i = errors.length - 1; i >= 0; i-- ) {
      final Errors e = errors[i];
      sb.append("<tr>");
      sb.append("<td>" + PrettyPrint.msecs(e.training_time_ms, true) + "</td>");
      sb.append("<td>" + String.format("%g", e.epoch_counter) + "</td>");
      sb.append("<td>" + String.format("%,d", e.training_samples) + "</td>");
      if (isClassifier() && !get_params().autoencoder) {
        sb.append("<td>" + Utils.formatPct(e.train_err) + "</td>");
        if (nclasses()==2) {
          if (e.trainAUC != null) sb.append("<td>" + Utils.formatPct(e.trainAUC.AUC()) + "</td>");
          else sb.append("<td>" + "N/A" + "</td>");
        }
      } else {
        sb.append("<td>" + String.format(mse_format, e.train_mse) + "</td>");
      }
      if(e.validation) {
        if (isClassifier()) {
          sb.append("<td>" + Utils.formatPct(e.valid_err) + "</td>");
          if (nclasses()==2) {
            if (e.validAUC != null) sb.append("<td>" + Utils.formatPct(e.validAUC.AUC()) + "</td>");
            else sb.append("<td>" + "N/A" + "</td>");
          }
        } else {
          sb.append("<td>" + String.format(mse_format, e.valid_mse) + "</td>");
        }
      }
      else if(e.num_folds > 0) {
        if (i == errors.length - 1 && _have_cv_results) {
          if (isClassifier()) {
            sb.append("<td>" + Utils.formatPct(e.valid_err) + "</td>");
            if (nclasses() == 2) {
              if (e.validAUC != null) sb.append("<td>" + Utils.formatPct(e.validAUC.AUC()) + "</td>");
              else sb.append("<td>" + "N/A" + "</td>");
            }
          } else {
            sb.append("<td>" + String.format(mse_format, e.valid_mse) + "</td>");
          }
        }
        else {
          sb.append("<td>N/A</td>");
          if (nclasses() == 2) sb.append("<td>N/A</td>");
        }
      }
      sb.append("</tr>");
    }
    sb.append("</table>");
    return true;
  }

  @Override
  protected SB toJavaNCLASSES(SB sb) {
    return !get_params().autoencoder ? super.toJavaNCLASSES(sb) :
            JCodeGen.toStaticVar(sb, "NCLASSES", model_info.units[model_info.units.length-1], "Number of output features (same as features of training data).");
  }

  @Override
  protected void toJavaFillPreds0(SB bodySb) {
    if (!get_params().autoencoder) super.toJavaFillPreds0(bodySb);
  }

  public void toJavaHtml(StringBuilder sb) {
    sb.append("<br /><br /><div class=\"pull-right\"><a href=\"#\" onclick=\'$(\"#javaModel\").toggleClass(\"hide\");\'" +
            "class=\'btn btn-inverse btn-mini\'>Java Model</a></div><br /><div class=\"hide\" id=\"javaModel\">");

    boolean featureAllowed = true; //isFeatureAllowed();
    if (! featureAllowed) {
      sb.append("<br/><div id=\'javaModelWarningBlock\' class=\"alert\" style=\"background:#eedd20;color:#636363;text-shadow:none;\">");
      sb.append("<b>You have requested a premium feature and your H<sub>2</sub>O software is unlicensed.</b><br/><br/>");
      sb.append("Please enter your email address below, and we will send you a trial license shortly.<br/>");
      sb.append("This will also temporarily enable downloading Java models.<br/>");
      sb.append("<form class=\'form-inline\'><input id=\"emailForJavaModel\" class=\"span5\" type=\"text\" placeholder=\"Email\"/> ");
      sb.append("<a href=\"#\" onclick=\'processJavaModelLicense();\' class=\'btn btn-inverse\'>Send</a></form></div>");
      sb.append("<div id=\"javaModelSource\" class=\"hide\"><pre style=\"overflow-y:scroll;\"><code class=\"language-java\">");
      DocGen.HTML.escape(sb, toJava());
      sb.append("</code></pre></div>");
    }
    else if( model_info().size() > 100000 ) {
      String modelName = JCodeGen.toJavaId(_key.toString());
      sb.append("<pre style=\"overflow-y:scroll;\"><code class=\"language-java\">");
      sb.append("/* Java code is too large to display, download it directly.\n");
      sb.append("   To obtain the code please invoke in your terminal:\n");
      sb.append("     curl http:/").append(H2O.SELF.toString()).append("/h2o-model.jar > h2o-model.jar\n");
      sb.append("     curl http:/").append(H2O.SELF.toString()).append("/2/").append(this.getClass().getSimpleName()).append("View.java?_modelKey=").append(_key).append(" > ").append(modelName).append(".java\n");
      sb.append("     javac -cp h2o-model.jar -J-Xmx2g -J-XX:MaxPermSize=128m ").append(modelName).append(".java\n");
      sb.append("*/");
      sb.append("</code></pre>");
    } else {
      sb.append("<pre style=\"overflow-y:scroll;\"><code class=\"language-java\">");
      DocGen.HTML.escape(sb, toJava());
      sb.append("</code></pre>");
    }
    sb.append("</div>");
    sb.append("<script type=\"text/javascript\">$(document).ready(showOrHideJavaModel);</script>");
  }

  @Override protected SB toJavaInit(SB sb, SB fileContextSB) {
    sb = super.toJavaInit(sb, fileContextSB);
    if (model_info().data_info()._nums > 0) {
      JCodeGen.toStaticVar(sb, "NUMS", new double[model_info().data_info()._nums], "Workspace for storing numerical input variables.");
      JCodeGen.toStaticVar(sb, "NORMMUL", model_info().data_info()._normMul, "Standardization/Normalization scaling factor for numerical variables.");
      JCodeGen.toStaticVar(sb, "NORMSUB", model_info().data_info()._normSub, "Standardization/Normalization offset for numerical variables.");
    }
    if (model_info().data_info()._cats > 0) {
      JCodeGen.toStaticVar(sb, "CATS", new int[model_info().data_info()._cats], "Workspace for storing categorical input variables.");
    }
    JCodeGen.toStaticVar(sb, "CATOFFSETS", model_info().data_info()._catOffsets, "Workspace for categorical offsets.");
    if (model_info().data_info()._normRespMul != null) {
      JCodeGen.toStaticVar(sb, "NORMRESPMUL", model_info().data_info()._normRespMul, "Standardization/Normalization scaling factor for response.");
      JCodeGen.toStaticVar(sb, "NORMRESPSUB", model_info().data_info()._normRespSub, "Standardization/Normalization offset for response.");
    }
    if (get_params().hidden_dropout_ratios != null) {
      JCodeGen.toStaticVar(sb, "HIDDEN_DROPOUT_RATIOS", get_params().hidden_dropout_ratios, "Hidden layer dropout ratios.");
    }

    Neurons[] neurons = DeepLearningTask.makeNeuronsForTesting(model_info());
    int[] layers = new int[neurons.length];
    for (int i=0;i<neurons.length;++i)
      layers[i] = neurons[i].units;
    JCodeGen.toStaticVar(sb, "NEURONS", layers, "Number of neurons for each layer.");

    if (get_params().autoencoder) {
      sb.i(1).p("@Override public int getPredsSize() { return " + model_info.units[model_info.units.length-1] + "; }").nl();
      sb.i(1).p("@Override public boolean isAutoEncoder() { return true; }").nl();
      sb.i(1).p("@Override public String getHeader() { return \"" + getHeader() + "\"; }").nl();

    }

    // activation storage
    sb.i(1).p("// Storage for neuron activation values.").nl();
    sb.i(1).p("public static final float[][] ACTIVATION = new float[][] {").nl();
    for (int i=0; i<neurons.length; i++) {
      String colInfoClazz = "Activation_"+i;
      sb.i(2).p("/* ").p(neurons[i].getClass().getSimpleName()).p(" */ ");
      sb.p(colInfoClazz).p(".VALUES");
      if (i!=neurons.length-1) sb.p(',');
      sb.nl();
      fileContextSB.i().p("// Neuron activation values for ").p(neurons[i].getClass().getSimpleName()).p(" layer").nl();
      JCodeGen.toClassWithArray(fileContextSB, null, colInfoClazz, new float[layers[i]]);
    }
    sb.i(1).p("};").nl();

    // biases
    sb.i(1).p("// Neuron bias values.").nl();
    sb.i(1).p("public static final float[][] BIAS = new float[][] {").nl();
    for (int i=0; i<neurons.length; i++) {
      String colInfoClazz = "Bias_"+i;
      sb.i(2).p("/* ").p(neurons[i].getClass().getSimpleName()).p(" */ ");
      sb.p(colInfoClazz).p(".VALUES");
      if (i!=neurons.length-1) sb.p(',');
      sb.nl();
      fileContextSB.i().p("// Neuron bias values for ").p(neurons[i].getClass().getSimpleName()).p(" layer").nl();
      float[] bias = i == 0 ? null : new float[model_info().get_biases(i-1).size()];
      if (i>0) {
        for (int j=0; j<bias.length; ++j) bias[j] = model_info().get_biases(i-1).get(j);
      }
      JCodeGen.toClassWithArray(fileContextSB, null, colInfoClazz, bias);
    }
    sb.i(1).p("};").nl();

    // weights
    sb.i(1).p("// Connecting weights between neurons.").nl();
    sb.i(1).p("public static final float[][] WEIGHT = new float[][] {").nl();
    for (int i=0; i<neurons.length; i++) {
      String colInfoClazz = "Weight_"+i;
      sb.i(2).p("/* ").p(neurons[i].getClass().getSimpleName()).p(" */ ");
      sb.p(colInfoClazz).p(".VALUES");
      if (i!=neurons.length-1) sb.p(',');
      sb.nl();
      if (i > 0) {
        fileContextSB.i().p("// Neuron weights connecting ").
                p(neurons[i - 1].getClass().getSimpleName()).p(" and ").
                p(neurons[i].getClass().getSimpleName()).
                p(" layer").nl();
      }
      float[] weights = i == 0 ? null : new float[model_info().get_weights(i-1).rows()*model_info().get_weights(i-1).cols()];
      if (i>0) {
        final int rows = model_info().get_weights(i-1).rows();
        final int cols = model_info().get_weights(i-1).cols();
        for (int j=0; j<rows; ++j)
          for (int k=0; k<cols; ++k)
            weights[j*cols+k] = model_info().get_weights(i-1).get(j,k);
      }
      JCodeGen.toClassWithArray(fileContextSB, null, colInfoClazz, weights);
    }
    sb.i(1).p("};").nl();

    return sb;
  }

  @Override protected void toJavaPredictBody( final SB bodySb, final SB classCtxSb, final SB fileCtxSb) {
    SB model = new SB();
    bodySb.i().p("java.util.Arrays.fill(preds,0f);").nl();
    final int cats = model_info().data_info()._cats;
    final int nums = model_info().data_info()._nums;
    // initialize input layer
    if (nums > 0) bodySb.i().p("java.util.Arrays.fill(NUMS,0f);").nl();
    if (cats > 0) bodySb.i().p("java.util.Arrays.fill(CATS,0);").nl();
    bodySb.i().p("int i = 0, ncats = 0;").nl();
    if (cats > 0) {
      bodySb.i().p("for(; i<"+cats+"; ++i) {").nl();
      bodySb.i(1).p("if (!Double.isNaN(data[i])) {").nl();
      bodySb.i(2).p("int c = (int) data[i];").nl();
      if (model_info().data_info()._useAllFactorLevels)
        bodySb.i(2).p("CATS[ncats++] = c + CATOFFSETS[i];").nl();
      else
        bodySb.i(2).p("if (c != 0) CATS[ncats++] = c + CATOFFSETS[i] - 1;").nl();
      bodySb.i(1).p("}").nl();
      bodySb.i().p("}").nl();
    }
    if (nums > 0) {
      bodySb.i().p("final int n = data.length;").nl();
      bodySb.i().p("for(; i<n; ++i) {").nl();
        bodySb.i(1).p("NUMS[i" + (cats > 0 ? "-" + cats : "") + "] = Double.isNaN(data[i]) ? 0 : ");
      if (model_info().data_info()._normMul != null) {
        bodySb.p("(data[i] - NORMSUB[i" + (cats > 0 ? "-" + cats : "") + "])*NORMMUL[i" + (cats > 0 ? "-" + cats : "") + "];").nl();
      } else {
        bodySb.p("data[i];").nl();
      }
      bodySb.i(0).p("}").nl();
    }
    bodySb.i().p("java.util.Arrays.fill(ACTIVATION[0],0);").nl();
    if (cats > 0) {
      bodySb.i().p("for (i=0; i<ncats; ++i) ACTIVATION[0][CATS[i]] = 1f;").nl();
    }
    if (nums > 0) {
      bodySb.i().p("for (i=0; i<NUMS.length; ++i) {").nl();
        bodySb.i(1).p("ACTIVATION[0][CATOFFSETS[CATOFFSETS.length-1] + i] = Double.isNaN(NUMS[i]) ? 0f : (float) NUMS[i];").nl();
      bodySb.i().p("}").nl();
    }

    boolean tanh=(get_params().activation == DeepLearning.Activation.Tanh || get_params().activation == DeepLearning.Activation.TanhWithDropout);
    boolean relu=(get_params().activation == DeepLearning.Activation.Rectifier || get_params().activation == DeepLearning.Activation.RectifierWithDropout);
    boolean maxout=(get_params().activation == DeepLearning.Activation.Maxout || get_params().activation == DeepLearning.Activation.MaxoutWithDropout);

    final String stopping = get_params().autoencoder ? "(i<=ACTIVATION.length-1)" : "(i<ACTIVATION.length-1)";

    // make prediction: forward propagation
    bodySb.i().p("for (i=1; i<ACTIVATION.length; ++i) {").nl();
    bodySb.i(1).p("java.util.Arrays.fill(ACTIVATION[i],0f);").nl();
    if (maxout) {
      bodySb.i(1).p("float rmax = 0;").nl();
    }
    bodySb.i(1).p("for (int r=0; r<ACTIVATION[i].length; ++r) {").nl();
    bodySb.i(2).p("final int cols = ACTIVATION[i-1].length;").nl();
    if (maxout) {
      bodySb.i(2).p("float cmax = Float.NEGATIVE_INFINITY;").nl();
    }
    bodySb.i(2).p("for (int c=0; c<cols; ++c) {").nl();
    if (!maxout) {
      bodySb.i(3).p("ACTIVATION[i][r] += ACTIVATION[i-1][c] * WEIGHT[i][r*cols+c];").nl();
    } else {
      bodySb.i(3).p("if " + stopping + " cmax = Math.max(ACTIVATION[i-1][c] * WEIGHT[i][r*cols+c], cmax);").nl();
      bodySb.i(3).p("else ACTIVATION[i][r] += ACTIVATION[i-1][c] * WEIGHT[i][r*cols+c];").nl();
    }
    bodySb.i(2).p("}").nl();
    if (maxout) {
      bodySb.i(2).p("if "+ stopping +" ACTIVATION[i][r] = Float.isInfinite(cmax) ? 0f : cmax;").nl();
    }
    bodySb.i(2).p("ACTIVATION[i][r] += BIAS[i][r];").nl();
    if (maxout) {
      bodySb.i(2).p("if " + stopping + " rmax = Math.max(rmax, ACTIVATION[i][r]);").nl();
    }
    bodySb.i(1).p("}").nl();

    if (!maxout) bodySb.i(1).p("if " + stopping + " {").nl();
    bodySb.i(2).p("for (int r=0; r<ACTIVATION[i].length; ++r) {").nl();
    if (tanh) {
      bodySb.i(3).p("ACTIVATION[i][r] = 1f - 2f / (1f + (float)Math.exp(2*ACTIVATION[i][r]));").nl();
    } else if (relu) {
      bodySb.i(3).p("ACTIVATION[i][r] = Math.max(0f, ACTIVATION[i][r]);").nl();
    } else if (maxout) {
      bodySb.i(3).p("if (rmax > 1 ) ACTIVATION[i][r] /= rmax;").nl();
    }
    if (get_params().hidden_dropout_ratios != null) {
      if (maxout) bodySb.i(1).p("if " + stopping + " {").nl();
      bodySb.i(3).p("ACTIVATION[i][r] *= HIDDEN_DROPOUT_RATIOS[i-1];").nl();
      if (maxout) bodySb.i(1).p("}").nl();
    }
    bodySb.i(2).p("}").nl();
    if (!maxout) bodySb.i(1).p("}").nl();
    if (isClassifier()) {
      bodySb.i(1).p("if (i == ACTIVATION.length-1) {").nl();
      // softmax
      bodySb.i(2).p("float max = ACTIVATION[i][0];").nl();
      bodySb.i(2).p("for (int r=1; r<ACTIVATION[i].length; r++) {").nl();
      bodySb.i(3).p("if (ACTIVATION[i][r]>max) max = ACTIVATION[i][r];").nl();
      bodySb.i(2).p("}").nl();
      bodySb.i(2).p("float scale = 0f;").nl();
      bodySb.i(2).p("for (int r=0; r<ACTIVATION[i].length; r++) {").nl();
      bodySb.i(3).p("ACTIVATION[i][r] = (float) Math.exp(ACTIVATION[i][r] - max);").nl();
      bodySb.i(3).p("scale += ACTIVATION[i][r];").nl();
      bodySb.i(2).p("}").nl();
      bodySb.i(2).p("for (int r=0; r<ACTIVATION[i].length; r++) {").nl();
      bodySb.i(3).p("if (Float.isNaN(ACTIVATION[i][r]))").nl();
      bodySb.i(4).p("throw new RuntimeException(\"Numerical instability, predicted NaN.\");").nl();
      bodySb.i(3).p("ACTIVATION[i][r] /= scale;").nl();
      bodySb.i(3).p("preds[r+1] = ACTIVATION[i][r];").nl();
      bodySb.i(2).p("}").nl();
      bodySb.i(1).p("}").nl();
      bodySb.i().p("}").nl();
    } else if (!get_params().autoencoder) { //Regression
      bodySb.i(1).p("if (i == ACTIVATION.length-1) {").nl();
      // regression: set preds[1], FillPreds0 will put it into preds[0]
      if (model_info().data_info()._normRespMul != null) {
        bodySb.i(2).p("preds[1] = (float) (ACTIVATION[i][0] / NORMRESPMUL[0] + NORMRESPSUB[0]);").nl();
      }
      else {
        bodySb.i(2).p("preds[1] = ACTIVATION[i][0];").nl();
      }
      bodySb.i(2).p("if (Float.isNaN(preds[1])) throw new RuntimeException(\"Predicted regression target NaN!\");").nl();
      bodySb.i(1).p("}").nl();
      bodySb.i().p("}").nl();
    } else { //AutoEncoder
      bodySb.i(1).p("if (i == ACTIVATION.length-1) {").nl();
      bodySb.i(2).p("for (int r=0; r<ACTIVATION[i].length; r++) {").nl();
      bodySb.i(3).p("if (Float.isNaN(ACTIVATION[i][r]))").nl();
      bodySb.i(4).p("throw new RuntimeException(\"Numerical instability, reconstructed NaN.\");").nl();
      bodySb.i(3).p("preds[r] = ACTIVATION[i][r];").nl();
      bodySb.i(2).p("}").nl();
      if (model_info().data_info()._nums > 0) {
        int ns = model_info().data_info().numStart();
        bodySb.i(2).p("for (int k=" + ns + "; k<" + model_info().data_info().fullN() + "; ++k) {").nl();
        bodySb.i(3).p("preds[k] = preds[k] / (float)NORMMUL[k-" + ns + "] + (float)NORMSUB[k-" + ns + "];").nl();
        bodySb.i(2).p("}").nl();
      }
      bodySb.i(1).p("}").nl();
      bodySb.i().p("}").nl();
      // DEBUGGING
//      bodySb.i().p("System.out.println(java.util.Arrays.toString(data));").nl();
//      bodySb.i().p("System.out.println(java.util.Arrays.toString(ACTIVATION[0]));").nl();
//      bodySb.i().p("System.out.println(java.util.Arrays.toString(ACTIVATION[ACTIVATION.length-1]));").nl();
//      bodySb.i().p("System.out.println(java.util.Arrays.toString(preds));").nl();
//      bodySb.i().p("System.out.println(\"\");").nl();
    }
    fileCtxSb.p(model);
    toJavaUnifyPreds(bodySb);
    toJavaFillPreds0(bodySb);
  }

  // helper to push this model to another key (for keeping good models)
  private void putMeAsBestModel(Key bestModelKey) {
    final Key job = null;
    final DeepLearningModel cp = this;
    DeepLearningModel bestModel = new DeepLearningModel(cp, bestModelKey, job, model_info().data_info());
    bestModel.get_params().state = Job.JobState.DONE;
    bestModel.get_params().job_key = get_params().self();
    bestModel.delete_and_lock(job);
    bestModel.unlock(job);
    assert (UKV.get(bestModelKey) != null);
    assert (bestModel.compareTo(this) <= 0);
    assert (((DeepLearningModel) UKV.get(bestModelKey)).error() == _bestError);
  }

  public void delete_best_model( ) {
    if (actual_best_model_key != null && actual_best_model_key != _key) DKV.remove(actual_best_model_key);
  }

  public void delete_xval_models( ) {
    if (get_params().xval_models != null) {
      for (Key k : get_params().xval_models) {
        UKV.<DeepLearningModel>get(k).delete_best_model();
        UKV.<DeepLearningModel>get(k).delete();
      }
    }
  }

  transient private final String unstable_msg = "Job was aborted due to observed numerical instability (exponential growth)."
          + "\nTry a different initial distribution, a bounded activation function or adding"
          + "\nregularization with L1, L2 or max_w2 and/or use a smaller learning rate or faster annealing.";

}

