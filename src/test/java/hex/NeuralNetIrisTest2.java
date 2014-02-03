package hex;

import hex.nn.NN;
import hex.nn.NNModel;
import hex.nn.NNTask;
import hex.nn.Neurons;
import junit.framework.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import water.JUnitRunnerDebug;
import water.Key;
import water.TestUtil;
import water.UKV;
import water.fvec.Frame;
import water.fvec.NFSFileVec;
import water.fvec.ParseDataset2;
import water.util.Log;
import water.util.Utils;

import java.util.Random;

public class NeuralNetIrisTest2 extends TestUtil {
  static final String PATH = "smalldata/iris/iris.csv";
  Frame _train, _test;

  @BeforeClass public static void stall() {
    stall_till_cloudsize(JUnitRunnerDebug.NODES);
  }

  void compareVal(double a, double b, double abseps, double releps) {
    // check for equality
    if (Double.compare(a, b) == 0) {
      return;
    }
    // check for small relative error
    else if (Math.abs(a-b)/Math.max(a,b) < releps) {
      return;
    }
    // check for small absolute error
    else if (Math.abs(a - b) <= abseps) {
      return;
    }
    // fail
    else Assert.failNotEquals("Not equal: ", new Double(a), new Double(b));
  }

  @Test public void compare() throws Exception {

    // Testing different things
    // Note: Microsoft reference implementation is only for Tanh + MSE, rectifier and MCE are implemented by 0xdata (trivial).
    // Note: Initial weight distributions are copied, but what is tested is the stability behavior.
    NN.Activation[] activations = { NN.Activation.Tanh, NN.Activation.Rectifier };
    NN.Loss[] losses = { NN.Loss.MeanSquare, NN.Loss.CrossEntropy };
    NN.InitialWeightDistribution[] dists = {
            NN.InitialWeightDistribution.Normal,
            //NN.InitialWeightDistribution.Uniform,
            NN.InitialWeightDistribution.UniformAdaptive
            };
    double[] initial_weight_scales = { 0.0258 };
    double[] holdout_ratios = { 0.8 };


    int hogwild_runs = 0;
    int hogwild_errors = 0;
    for (NN.Activation activation : activations) {
      for (NN.Loss loss : losses) {
        for (NN.InitialWeightDistribution dist : dists) {
          for (double scale : initial_weight_scales) {
            for (double holdout_ratio : holdout_ratios) {

              Log.info("");
              Log.info("STARTING.");
              Log.info("Running with " + activation.name() + " activation function and " + loss.name() + " loss function.");
              Log.info("Initialization with " + dist.name() + " distribution and " + scale + " scale, holdout ratio " + holdout_ratio);

              NeuralNetMLPReference2 ref = new NeuralNetMLPReference2();

              final long seed = new Random().nextLong(); //Actually change the seed every time!
              Log.info("Using seed " + seed);
              ref.init(activation, Utils.getDeterRNG(seed), holdout_ratio);

              // Parse Iris and shuffle the same way as ref
              Key file = NFSFileVec.make(find_test_file(PATH));
              Frame frame = ParseDataset2.parse(Key.make("iris_nn2"), new Key[] { file });

              double[][] rows = new double[(int) frame.numRows()][frame.numCols()];
              String[] names = new String[frame.numCols()];
              for( int c = 0; c < frame.numCols(); c++ ) {
                names[c] = "ColumnName" + c;
                for( int r = 0; r < frame.numRows(); r++ )
                  rows[r][c] = frame.vecs()[c].at(r);
              }

              Random rand = Utils.getDeterRNG(seed);
              for( int i = rows.length - 1; i >= 0; i-- ) {
                int shuffle = rand.nextInt(i + 1);
                double[] row = rows[shuffle];
                rows[shuffle] = rows[i];
                rows[i] = row;
              }

              int limit = (int) (frame.numRows() * holdout_ratio);
              _train = frame(names, Utils.subarray(rows, 0, limit));
              _test = frame(names, Utils.subarray(rows, limit, (int) frame.numRows() - limit));
              frame.delete();

              NN p = new NN();
              p.seed = seed;
              p.hidden = new int[]{7};
              p.rate = 0.01;
              p.activation = activation;
              p.max_w2 = Double.MAX_VALUE;
              p.rate = 0.01;
              p.epochs = 13*17;
              p.activation = activation;
              p.input_dropout_ratio = 0;
              p.rate_annealing = 0;
              p.l1 = 0;
              p.loss = loss;
              p.l2 = 0;
              p.momentum_start = 0;
              p.momentum_ramp = 0;
              p.momentum_stable = 0;
              p.initial_weight_distribution = dist;
              p.initial_weight_scale = scale;
              p.classification = true;
              p.source = _train;
              p.validation = null;
              p.response = _train.lastVec();
              p.destination_key = Key.make("iris_test.model");
              p.ignored_cols = null;

              Frame fr = FrameTask.DataInfo.prepareFrame(p.source, p.response, p.ignored_cols, true);
              p._dinfo = new FrameTask.DataInfo(fr, 1, true);
              p.initModel(); //randomize weights, but don't start training yet

              NNModel mymodel = UKV.get(p.dest()); //get the model
              Neurons[] neurons = NNTask.makeNeurons(p._dinfo, mymodel.model_info());

              // use the same random weights for the reference implementation
              Neurons l = neurons[1];
              for( int o = 0; o < l._a.length; o++ ) {
                for( int i = 0; i < l._previous._a.length; i++ )
                  ref._nn.ihWeights[i][o] = l._w[o * l._previous._a.length + i];
                ref._nn.hBiases[o] = l._b[o];
              }
              l = neurons[2];
              for( int o = 0; o < l._a.length; o++ ) {
                for( int i = 0; i < l._previous._a.length; i++ )
                  ref._nn.hoWeights[i][o] = l._w[o * l._previous._a.length + i];
                ref._nn.oBiases[o] = l._b[o];
              }

              // Train the Reference
              ref.train((int)p.epochs, p.rate, loss);
              final double trainAcc = ref._nn.Accuracy(ref._trainData);
              final double testAcc = ref._nn.Accuracy(ref._testData);

              // Train H2O
              p.trainModel(false);
              mymodel = UKV.get(p.dest()); //get the trained model
              final double myTrainAcc = mymodel.classificationError(_train, "Final training error:", true);
              final double myTestAcc  = mymodel.classificationError(_test,  "Final testing error:",  true);

//              double abseps = 1e-15;
//              double releps = 1e-12;

              /**
               * Compare classification errors
               */
              final boolean hogwild_error = (trainAcc != myTrainAcc || testAcc != myTestAcc);
              hogwild_errors += hogwild_error == true ? 1 : 0;
              hogwild_runs++;

              /**
               * Compare weights
               */
              neurons = NNTask.makeNeurons(p._dinfo, mymodel.model_info()); //link the weights to the neurons, for easy access
              double weight_mse = 0;
              l = neurons[1];
              for( int o = 0; o < l._a.length; o++ ) {
                for( int i = 0; i < l._previous._a.length; i++ ) {
                  double a = ref._nn.ihWeights[i][o];
                  double b = l._w[o * l._previous._a.length + i];
                  weight_mse += (a-b) * (a-b);
                }
              }
              weight_mse /= l._a.length * l._previous._a.length;

              /**
               * Report accuracy
               */
              Log.info("DONE. " + (hogwild_error ? "Multithreading resulted in errors due to race conditions (Hogwild!)." : ""));
              Log.info("MSE of H2O weights: " + weight_mse + ".");
              Log.info("H2O  training error : " + myTrainAcc*100 + "%, test error: " + myTestAcc*100 + "%" +
                      (trainAcc != myTrainAcc|| testAcc != myTestAcc? " HOGWILD! " : ""));
              Log.info("REF  training error : " + trainAcc*100 + "%, test error: " + testAcc*100 + "%");

              // cleanup
              mymodel.delete();
              _train.delete();
              _test.delete();
              fr.delete();
            }
          }
        }
      }
    }

    Log.info("===============================================================");
    Log.info("Number of differences due to Hogwild: " + hogwild_errors + " (out of " + hogwild_runs + " runs).");
    Log.info("===============================================================");
  }
}
