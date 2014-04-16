package hex;

import static hex.Layer.Rectifier;
import static hex.Layer.Tanh;
import hex.Layer.VecSoftmax;
import hex.Layer.VecsInput;
import hex.NeuralNet.Loss;
import junit.framework.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import water.JUnitRunnerDebug;
import water.Key;
import water.TestUtil;
import water.fvec.Frame;
import water.fvec.NFSFileVec;
import water.fvec.ParseDataset2;
import water.fvec.Vec;
import water.util.Log;
import water.util.Utils;

import java.util.Random;

public class NeuralNetIrisTest extends TestUtil {
  static final String PATH = "smalldata/iris/iris.csv";
  Frame _train, _test;

  @BeforeClass public static void stall() {
    stall_till_cloudsize(JUnitRunnerDebug.NODES);
  }

  void compareVal(double a, double b, double abseps, double releps) {
    // check for equality
    if (Double.compare(a, b) == 0) {
    }
    // check for small relative error
    else if (Math.abs(a-b)/Math.max(a,b) < releps) {
    }
    // check for small absolute error
    else if (Math.abs(a - b) <= abseps) {
    }
    // fail
    else Assert.failNotEquals("Not equal: ", new Double(a), new Double(b));
  }

  @Test
  public void compare() throws Exception {

    // Testing different things
    // Note: Microsoft reference implementation is only for Tanh + MSE, rectifier and MCE are implemented by 0xdata (trivial).
    // Note: Initial weight distributions are copied, but what is tested is the stability behavior.
    NeuralNet.Activation[] activations = { NeuralNet.Activation.Tanh, NeuralNet.Activation.Rectifier };
    Loss[] losses = { NeuralNet.Loss.MeanSquare, NeuralNet.Loss.CrossEntropy };
    NeuralNet.InitialWeightDistribution[] dists = {
            NeuralNet.InitialWeightDistribution.Normal,
            //NeuralNet.InitialWeightDistribution.Uniform,
            NeuralNet.InitialWeightDistribution.UniformAdaptive };
    double[] initial_weight_scales = { 0.0258 };
    double[] holdout_ratios = { 0.8 };
    double[] epochs = { 1, 13*17 };
    double[] rates = { 0.01 };

    NeuralNet.ExecutionMode[] trainers = {
            NeuralNet.ExecutionMode.SingleThread,
//            NeuralNet.ExecutionMode.SingleNode,
//            NeuralNet.ExecutionMode.MapReduce
    };

    final long seed0 = 0xDECAF;

    int count = 0;
    int hogwild_runs = 0;
    int hogwild_errors = 0;
    for (NeuralNet.ExecutionMode trainer : trainers) {
      for (NeuralNet.Activation activation : activations) {
        for (Loss loss : losses) {
          for (NeuralNet.InitialWeightDistribution dist : dists) {
            for (double scale : initial_weight_scales) {
              for (double holdout_ratio : holdout_ratios) {
                for (double epoch : epochs) {
                  for (double rate : rates) {
                    Log.info("");
                    Log.info("STARTING.");
                    Log.info("Running in " + trainer.name() + " mode with " + activation.name() + " activation function and " + loss.name() + " loss function.");
                    Log.info("Initialization with " + dist.name() + " distribution and " + scale + " scale, holdout ratio " + holdout_ratio);

                    NeuralNetMLPReference ref = new NeuralNetMLPReference();

                    final long seed = seed0 + count;
                    Log.info("Using seed " + seed);
                    ref.init(activation, water.util.Utils.getDeterRNG(seed), holdout_ratio);

                    // Parse Iris and shuffle the same way as ref
                    Key file = NFSFileVec.make(find_test_file(PATH));
                    Frame frame = ParseDataset2.parse(Key.make(), new Key[] { file });

                    double[][] rows = new double[(int) frame.numRows()][frame.numCols()];
                    for( int c = 0; c < frame.numCols(); c++ )
                      for( int r = 0; r < frame.numRows(); r++ )
                        rows[r][c] = frame.vecs()[c].at(r);

                    Random rand = water.util.Utils.getDeterRNG(seed);
                    for( int i = rows.length - 1; i >= 0; i-- ) {
                      int shuffle = rand.nextInt(i + 1);
                      double[] row = rows[shuffle];
                      rows[shuffle] = rows[i];
                      rows[i] = row;
                    }

                    int limit = (int) (frame.numRows() * holdout_ratio);
                    _train = frame(null, Utils.subarray(rows, 0, limit));
                    _test = frame(null, Utils.subarray(rows, limit, (int) frame.numRows() - limit));


                    Vec[] data = Utils.remove(_train.vecs(), _train.vecs().length - 1);
                    Vec labels = _train.vecs()[_train.vecs().length - 1];

                    NeuralNet p = new NeuralNet();
                    p.seed = seed;
                    p.rate = rate;
                    p.activation = activation;
                    p.max_w2 = Double.MAX_VALUE;
                    p.epochs = epoch;
                    p.activation = activation;
                    p.input_dropout_ratio = 0;
                    p.rate_annealing = 0;
                    p.l1 = 0;
                    p.l2 = 0;
                    p.momentum_start = 0;
                    p.momentum_ramp = 0;
                    p.momentum_stable = 0;
                    p.initial_weight_distribution = dist;
                    p.initial_weight_scale = scale;
                    p.diagnostics = true;
                    p.fast_mode = false;
                    p.loss = loss;

                    Layer[] ls = new Layer[3];
                    ls[0] = new VecsInput(data, null);
                    if (activation == NeuralNet.Activation.Tanh) {
                      ls[1] = new Tanh(7);
                    }
                    else if (activation == NeuralNet.Activation.TanhWithDropout) {
                      ls[1] = new Layer.TanhDropout(7);
                    }
                    else if (activation == NeuralNet.Activation.Rectifier) {
                      ls[1] = new Rectifier(7);
                    }
                    else if (activation == NeuralNet.Activation.RectifierWithDropout) {
                      ls[1] = new Layer.RectifierDropout(7);
                    }
                    ls[2] = new VecSoftmax(labels, null);

                    for( int i = 0; i < ls.length; i++ ) {
                      ls[i].init(ls, i, p);
                    }

                    // use the same random weights for the reference implementation
                    Layer l = ls[1];
                    for( int o = 0; o < l._a.length; o++ ) {
                      for( int i = 0; i < l._previous._a.length; i++ ) {
//                    System.out.println("initial weight[" + o + "]=" + l._w[o * l._previous._a.length + i]);
                        ref._nn.ihWeights[i][o] = l._w[o * l._previous._a.length + i];
                      }
                      ref._nn.hBiases[o] = l._b[o];
//                  System.out.println("initial bias[" + o + "]=" + l._b[o]);
                    }
                    l = ls[2];
                    for( int o = 0; o < l._a.length; o++ ) {
                      for( int i = 0; i < l._previous._a.length; i++ ) {
//                    System.out.println("initial weight[" + o + "]=" + l._w[o * l._previous._a.length + i]);
                        ref._nn.hoWeights[i][o] = l._w[o * l._previous._a.length + i];
                      }
                      ref._nn.oBiases[o] = l._b[o];
//                  System.out.println("initial bias[" + o + "]=" + l._b[o]);
                    }

                    // Reference
                    ref.train((int)p.epochs, p.rate, loss);

                    // H2O
                    if (trainer == NeuralNet.ExecutionMode.SingleThread) {
                      new Trainer.Direct(ls, p.epochs, null).run();
                    } else if (trainer == NeuralNet.ExecutionMode.SingleNode) {
                      new Trainer.Threaded(ls, p.epochs, null, -1).run();
                    } else {
                      new Trainer.MapReduce(ls, p.epochs, null).run();
                    }

                    // tiny absolute and relative tolerances for single threaded mode
                    double abseps = 1e-4;
                    double releps = 1e-4; // relative error check only triggers if abs(a-b) > abseps
                    double weight_mse = 0;

                    // Make sure weights are equal
                    l = ls[1];
                    for( int o = 0; o < l._a.length; o++ ) {
                      for( int i = 0; i < l._previous._a.length; i++ ) {
                        double a = ref._nn.ihWeights[i][o];
                        double b = l._w[o * l._previous._a.length + i];
                        if (trainer == NeuralNet.ExecutionMode.SingleThread) {
                          compareVal(a, b, abseps, releps);
//                      System.out.println("weight[" + o + "]=" + b);
                        } else {
                          weight_mse += (a-b) * (a-b);
                        }
                      }
                    }
                    weight_mse /= l._a.length * l._previous._a.length;

                    // Make sure output layer (predictions) are equal
                    for( int o = 0; o < ls[2]._a.length; o++ ) {
                      double a = ref._nn.outputs[o];
                      double b = ls[2]._a[o];
                      if (trainer == NeuralNet.ExecutionMode.SingleThread) {
                        compareVal(a, b, abseps, releps);
                      }
                    }

                    // Make sure overall classification accuracy is equal
                    NeuralNet.Errors train = NeuralNet.eval(ls, 0, null);
                    data = Utils.remove(_test.vecs(), _test.vecs().length - 1);
                    labels = _test.vecs()[_test.vecs().length - 1];
                    VecsInput input = (VecsInput) ls[0];
                    input.vecs = data;
                    input._len = data[0].length();
                    ((VecSoftmax) ls[2]).vec = labels;
                    NeuralNet.Errors test = NeuralNet.eval(ls, 0, null);
                    double trainAcc = ref._nn.Accuracy(ref._trainData);
                    double testAcc = ref._nn.Accuracy(ref._testData);

                    if (trainer == NeuralNet.ExecutionMode.SingleThread) {
                      compareVal(trainAcc, train.classification, abseps, releps);
                      compareVal(testAcc, test.classification, abseps, releps);
                      Log.info("DONE. Single-threaded mode shows exact agreement with reference results.");
                    }
                    else {
                      final boolean hogwild_error = (trainAcc != train.classification || testAcc != test.classification);
                      Log.info("DONE. " + (hogwild_error ? "Threaded mode resulted in errors due to Hogwild." : ""));
                      Log.info("MSE of Hogwild H2O weights: " + weight_mse + ".");
                      hogwild_errors += hogwild_error ? 1 : 0;
                    }
                    Log.info("H2O  training error : " + train.classification*100 + "%, test error: " + test.classification*100 + "%" +
                            (trainAcc != train.classification || testAcc != test.classification ? " HOGWILD! " : ""));
                    Log.info("REF  training error : " + trainAcc*100 + "%, test error: " + testAcc*100 + "%");

                    frame.delete();
                    for (Layer l1 : ls) l1.close();
                    _train.delete();
                    _test.delete();

                    if (trainer != NeuralNet.ExecutionMode.SingleThread) {
                      hogwild_runs++;
                    }
                    count++;
                  }
                }
              }
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
