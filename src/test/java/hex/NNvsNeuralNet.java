package hex;

import hex.nn.NN;
import hex.nn.NNModel;
import hex.nn.NNTask;
import hex.nn.Neurons;
import org.junit.BeforeClass;
import org.junit.Test;
import water.JUnitRunnerDebug;
import water.Key;
import water.TestUtil;
import water.UKV;
import water.fvec.Frame;
import water.fvec.NFSFileVec;
import water.fvec.ParseDataset2;
import water.fvec.Vec;
import water.util.Log;
import water.util.Utils;

import java.util.Random;

import static hex.NeuralNet.*;
import static water.util.MRUtils.sampleFrame;

public class NNvsNeuralNet extends TestUtil {
  Frame _train, _test;
  Frame _train2, _test2;

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
    else {
//      Thread.dumpStack();
      Log.err("Not close enough: " + a + " " + b);
//      System.exit(0);
    }
  }

  @Test public void compare() throws Exception {

    // Testing different things
    // Note: Microsoft reference implementation is only for Tanh + MSE, rectifier and MCE are implemented by 0xdata (trivial).
    // Note: Initial weight distributions are copied, but what is tested is the stability behavior.

//    NN.Activation[] activations = { NN.Activation.Rectifier };
    NN.Activation[] activations = { NN.Activation.RectifierWithDropout };
    NN.Loss[] losses = { NN.Loss.CrossEntropy };
    NN.InitialWeightDistribution[] dists = { NN.InitialWeightDistribution.UniformAdaptive };
    double[] initial_weight_scales = { 1e-4 + new Random().nextDouble() };
    double[] holdout_ratios = { 0.8 };
    int[][] hiddens = { {20,20} };
    double[] rates = { 0.01 };

    String[] files = { "smalldata/mnist/train.csv" };
    int[] epochs = { 3 };
    int num_repeats = 10;

//    String[] files = { "smalldata/iris/iris.csv" };
//    int[] epochs = { 100 };
//    int num_repeats = 100;

//    String[] files = { "smalldata/logreg/prostate.csv" };
//    int[] epochs = { 100 };
//    int num_repeats = 10;

    //set parameters
    double p0 = 0.5;
    long pR = 100000;
    double p1 = 0.9;
    double l1 = 1e-5;
    double l2 = 0;
    double max_w2 = 20;
    double input_dropout = 0.3;
    double rate_annealing = 1e-6;

    for (NN.Activation activation : activations) {
      for (NN.Loss loss : losses) {
        for (NN.InitialWeightDistribution dist : dists) {
          for (double scale : initial_weight_scales) {
            for (double holdout_ratio : holdout_ratios) {
              for (int[] hidden : hiddens) {
                for (int epoch : epochs) {
                  for (double rate : rates) {
                    for (String file : files) {
                      double referror=0, myerror=0;
                      double[] a = new double[hidden.length+2];
                      double[] b = new double[hidden.length+2];
                      double[] ba = new double[hidden.length+2];
                      double[] bb = new double[hidden.length+2];
                      for (int repeat = 0; repeat < num_repeats; ++repeat) {
                        long seed = new Random().nextLong();
                        Log.info("");
                        Log.info("STARTING.");
                        Log.info("Running with " + activation.name() + " activation function and " + loss.name() + " loss function.");
                        Log.info("Initialization with " + dist.name() + " distribution and " + scale + " scale, holdout ratio " + holdout_ratio);
                        Log.info("Using seed " + seed);

                        Key kfile = NFSFileVec.make(find_test_file(file));
                        Frame frame = ParseDataset2.parse(Key.make(), new Key[]{kfile});

                        _train = sampleFrame(frame, (long)(frame.numRows()*holdout_ratio), seed);
                        _train2 = sampleFrame(frame, (long)(frame.numRows()*holdout_ratio), seed);

                        _test = sampleFrame(frame, (long)(frame.numRows()*(1-holdout_ratio)), seed+1);
                        _test2 = sampleFrame(frame, (long)(frame.numRows()*(1-holdout_ratio)), seed+1);

                        if (input_dropout > 0 && activation != NN.Activation.RectifierWithDropout)
                          input_dropout = 0; //old NeuralNet code cannot do input dropout unless RectifierWithDropout is used.

                        // Train new NN
                        Neurons[] neurons = null;
                        NNModel mymodel = null;
                        {
                          NN p = new NN();
                          p.source = (Frame)_train.clone();
                          p.response = _train.lastVec();
                          p.ignored_cols = null;
                          p.ignore_const_cols = true;
                          Frame fr = FrameTask.DataInfo.prepareFrame(p.source, p.response, p.ignored_cols, true, p.ignore_const_cols);
                          p._dinfo = new FrameTask.DataInfo(fr, 1, true);
                          p.seed = seed;
                          p.hidden = hidden;
                          p.rate = rate;
                          p.activation = activation;
                          p.max_w2 = max_w2;
                          p.epochs = epoch;
                          p.input_dropout_ratio = input_dropout;
                          p.rate_annealing = rate_annealing;
                          p.loss = loss;
                          p.l1 = l1;
                          p.l2 = l2;
                          p.momentum_stable = p1;
                          p.momentum_start = p0;
                          p.momentum_ramp = pR;
                          p.initial_weight_distribution = dist;
                          p.initial_weight_scale = scale;
                          p.classification = true;
                          p.diagnostics = true;
                          p.validation = null;
                          p.fast_mode = true; //to be the same as old NeuralNet code
                          p.sync_samples = 0; //sync once per period
                          p.ignore_const_cols = false; //to be the same as old NeuralNet code
                          p.exec(); //randomize weights, but don't start training yet

                          mymodel = UKV.get(p.dest());
                          neurons = NNTask.makeNeuronsForTesting(mymodel.model_info());
                        }

                        // Reference: NeuralNet
                        Layer[] ls;
                        NeuralNetModel refmodel;
                        NeuralNet p = new NeuralNet();
                        {
                          Vec[] data = Utils.remove(_train2.vecs(), _train2.vecs().length - 1);
                          Vec labels = _train2.lastVec();

                          p.seed = seed;
                          p.rate = rate;
                          p.max_w2 = max_w2;
                          p.epochs = epoch;
                          p.input_dropout_ratio = input_dropout;
                          p.rate_annealing = rate_annealing;
                          p.l1 = l1;
                          p.l2 = l2;
                          p.momentum_start = p0;
                          p.momentum_ramp = pR;
                          p.momentum_stable = p1;
                          if (dist == NN.InitialWeightDistribution.Normal) p.initial_weight_distribution = InitialWeightDistribution.Normal;
                          else if (dist == NN.InitialWeightDistribution.Uniform) p.initial_weight_distribution = InitialWeightDistribution.Uniform;
                          else if (dist == NN.InitialWeightDistribution.UniformAdaptive) p.initial_weight_distribution = InitialWeightDistribution.UniformAdaptive;
                          p.initial_weight_scale = scale;
                          p.diagnostics = true;
                          p.classification = true;
                          if (loss == NN.Loss.MeanSquare) p.loss = Loss.MeanSquare;
                          else if (loss == NN.Loss.CrossEntropy) p.loss = Loss.CrossEntropy;

                          ls = new Layer[hidden.length+2];
                          ls[0] = new Layer.VecsInput(data, null);
                          for (int i=0; i<hidden.length; ++i) {
                            if (activation == NN.Activation.Tanh) {
                              assert(p.input_dropout_ratio == 0);
                              ls[1+i] = new Layer.Tanh(hidden[i]);
                            } else if (activation == NN.Activation.TanhWithDropout) {
                              ls[1+i] = new Layer.TanhDropout(hidden[i]);
                            } else if (activation == NN.Activation.Rectifier) {
                              assert(p.input_dropout_ratio == 0);
                              ls[1+i] = new Layer.Rectifier(hidden[i]);
                            } else if (activation == NN.Activation.RectifierWithDropout) {
                              ls[1+i] = new Layer.RectifierDropout(hidden[i]);
                            }
                          }
                          ls[ls.length-1] = new Layer.VecSoftmax(labels, null);
                          for (int i = 0; i < ls.length; i++) {
                            ls[i].init(ls, i, p);
                          }
                          Trainer.Threaded trainer = new Trainer.Threaded(ls, p.epochs, null, -1);

                          refmodel = new NeuralNetModel(null, null, _train2, ls, p);
                          trainer.run();
                        }


                        /**
                         * Compare MEAN weights and biases in hidden and output layer
                         */
                        for (int n=1; n<ls.length; ++n) {
                          Neurons l = neurons[n];
                          Layer ref = ls[n];
                          for (int o = 0; o < l._a.length; o++) {
                            for (int i = 0; i < l._previous._a.length; i++) {
                              a[n] += ref._w[o * l._previous._a.length + i];
                              b[n] += l._w[o * l._previous._a.length + i];
                            }
                            ba[n] += ref._b[o];
                            bb[n] += l._b[o];
                          }
                        }

                        /**
                         * Compare predictions
                         * Note: Reference and H2O each do their internal data normalization,
                         * so we must use their "own" test data, which is assumed to be created correctly.
                         */
                        // H2O predictions


                        // NN scoring
                        {
                          Frame fpreds = mymodel.score(_test); //[0] is label, [1]...[4] are the probabilities
                          water.api.ConfusionMatrix CM = new water.api.ConfusionMatrix();
                          CM.actual = _test;
                          CM.vactual = _test.lastVec();
                          CM.predict = fpreds;
                          CM.vpredict = fpreds.vecs()[0];
                          CM.serve();
                          StringBuilder sb = new StringBuilder();
                          myerror += CM.toASCII(sb);
                          for (String s : sb.toString().split("\n")) Log.info(s);
                          fpreds.delete();
                        }
                        // NeuralNet scoring
                        {
                          final Frame[] adapted = refmodel.adapt(_test2, false);
                          Vec[] data = Utils.remove(_test2.vecs(), _test2.vecs().length - 1);
                          Vec labels = _test.vecs()[_test.vecs().length - 1];
                          Layer.VecsInput input = (Layer.VecsInput) ls[0];
                          input.vecs = data;
                          input._len = data[0].length();
                          ((Layer.VecSoftmax) ls[ls.length-1]).vec = labels;
                          NeuralNet.Errors test = NeuralNet.eval(ls, 0, null);
                          referror += test.classification;
                          adapted[1].delete();
                        }


                        // cleanup
                        mymodel.delete();
                        refmodel.delete();
                        _train.delete();
                        _test.delete();
                        _train2.delete();
                        _test2.delete();
                        frame.delete();
                      }
                      myerror /= (double)num_repeats;
                      referror /= (double)num_repeats;

                      /**
                       * Tolerances
                       */
                      final double abseps = 0.01;
                      final double releps = 0.01;

                      // overal test set scoring
                      Log.info("NeuralNet test error " + referror);
                      Log.info("NN        test error " + myerror);
                      compareVal(referror, myerror, abseps, releps);

//                        // mean weights/biases
//                        for (int n=1; n<hidden.length+2; ++n) {
//                          Log.info("NeuralNet mean weight for layer " + n + ": " + a[n]);
//                          Log.info("NN        mean weight for layer " + n + ": " + b[n]);
//                          Log.info("NeuralNet mean bias for layer " + n + ": " + ba[n]);
//                          Log.info("NN        mean bias for layer " + n + ": " + bb[n]);
//                          compareVal(ba[n]/num_repeats, bb[n]/num_repeats, abseps, releps);
//                          compareVal(a[n]/num_repeats, b[n]/num_repeats, abseps, releps);
//                        }

                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}
