package hex;

import static hex.NeuralNet.*;
import static water.util.MRUtils.sampleFrame;
import hex.deeplearning.DeepLearning;
import hex.deeplearning.DeepLearningModel;
import hex.deeplearning.DeepLearningTask;
import hex.deeplearning.Neurons;
import junit.framework.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
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

public class DeepLearningVsNeuralNet extends TestUtil {
  Frame _train, _test;

  @BeforeClass public static void stall() {
    stall_till_cloudsize(JUnitRunnerDebug.NODES);
  }

  void compareVal(float a, float b, float abseps, float releps) {
    // check for equality
    if (Float.compare(a, b) == 0) {
    }
    // check for small relative error
    else if (Math.abs(a-b)/Math.max(a,b) < releps) {
    }
    // check for small absolute error
    else if (Math.abs(a - b) <= abseps) {
    }
    // fail
    else {
//      Log.err("Not close enough: " + a + " " + b);
      Assert.failNotEquals("Not equal: ", a, b);
    }
  }

  @Ignore
  @Test
  public void compare() throws Exception {
    final long seed = 0xc0ffee;
    Random rng = new Random(seed);

    DeepLearning.Activation[] activations = {
            DeepLearning.Activation.Maxout,
            DeepLearning.Activation.MaxoutWithDropout,
            DeepLearning.Activation.Tanh,
            DeepLearning.Activation.TanhWithDropout,
            DeepLearning.Activation.Rectifier,
            DeepLearning.Activation.RectifierWithDropout,
    };
    DeepLearning.Loss[] losses = {
            DeepLearning.Loss.MeanSquare,
            DeepLearning.Loss.CrossEntropy
    };
    DeepLearning.InitialWeightDistribution[] dists = {
            DeepLearning.InitialWeightDistribution.Normal,
            DeepLearning.InitialWeightDistribution.Uniform,
            DeepLearning.InitialWeightDistribution.UniformAdaptive
    };
    double[] initial_weight_scales = {
            1e-3 + 1e-2 * rng.nextFloat()
    };
    double[] holdout_ratios = {
            0.7 + 0.2 * rng.nextFloat()
    };
    int[][] hiddens = {
            {1},
            {1+rng.nextInt(50)},
            {17,13},
            {20,10,5}
    };
    double[] rates = {
            0.005 + 1e-2 * rng.nextFloat()
    };
    int[] epochs = {
            5 + rng.nextInt(5)
    };
    double[] input_dropouts = {
            0,
            rng.nextFloat() * 0.5
    };

    double p0 = 0.5 * rng.nextFloat();
    long pR = 1000 + rng.nextInt(1000);
    double p1 = 0.5 + 0.49 * rng.nextFloat();
    double l1 = 1e-5 * rng.nextFloat();
    double l2 = 1e-5 * rng.nextFloat();
    float max_w2 = Float.POSITIVE_INFINITY; // rng.nextInt(50);
    double rate_annealing = 1e-7 + rng.nextFloat() * 1e-6;



    boolean threaded = false;
    int num_repeats = 1;

    // TODO: test that Deep Learning and NeuralNet agree for Mnist dataset
//    String[] files = { "smalldata/mnist/train.csv" };
//    hiddens = new int[][]{ {50,50} };
//    threaded = true;
//    num_repeats = 5;

    // TODO: test that Deep Learning and NeuralNet agree for covtype dataset
//    String[] files = { "smalldata/covtype/covtype.20k.data.my" };
//    hiddens = new int[][]{ {100,100} };
//    epochs = new int[]{ 50 };
//    threaded = true;
//    num_repeats = 2;

    String[] files = { "smalldata/iris/iris.csv", "smalldata/neural/two_spiral.data" };

    for (DeepLearning.Activation activation : activations) {
      for (DeepLearning.Loss loss : losses) {
        for (DeepLearning.InitialWeightDistribution dist : dists) {
          for (double scale : initial_weight_scales) {
            for (double holdout_ratio : holdout_ratios) {
              for (double input_dropout : input_dropouts) {
                for (int[] hidden : hiddens) {
                  for (int epoch : epochs) {
                    for (double rate : rates) {
                      for (String file : files) {
                        for (boolean fast_mode : new boolean[]{true,false}) {
                          float reftrainerr=0, trainerr=0;
                          float reftesterr=0, testerr=0;
                          float[] a = new float[hidden.length+2];
                          float[] b = new float[hidden.length+2];
                          float[] ba = new float[hidden.length+2];
                          float[] bb = new float[hidden.length+2];
                          long numweights = 0, numbiases = 0;
                          for (int repeat = 0; repeat < num_repeats; ++repeat) {
                            long myseed = seed + repeat;
                            Log.info("");
                            Log.info("STARTING.");
                            Log.info("Running with " + activation.name() + " activation function and " + loss.name() + " loss function.");
                            Log.info("Initialization with " + dist.name() + " distribution and " + scale + " scale, holdout ratio " + holdout_ratio);
                            Log.info("Using seed " + seed);

                            Key kfile = NFSFileVec.make(find_test_file(file));
                            Frame frame = ParseDataset2.parse(Key.make(), new Key[]{kfile});

                            _train = sampleFrame(frame, (long)(frame.numRows()*holdout_ratio), seed);
                            _test = sampleFrame(frame, (long)(frame.numRows()*(1-holdout_ratio)), seed+1);

                            // Train new Deep Learning
                            Neurons[] neurons;
                            DeepLearningModel mymodel;
                            {
                              DeepLearning p = new DeepLearning();
                              p.source = (Frame)_train.clone();
                              p.response = _train.lastVec();
                              p.ignored_cols = null;
                              p.seed = myseed;
                              p.hidden = hidden;
                              p.adaptive_rate = false;
                              p.rho = 0;
                              p.epsilon = 0;
                              p.rate = rate;
                              p.activation = activation;
                              p.max_w2 = max_w2;
                              p.epochs = epoch;
                              p.input_dropout_ratio = input_dropout;
                              p.rate_annealing = rate_annealing;
                              p.loss = loss;
                              p.l1 = l1;
                              p.l2 = l2;
                              p.momentum_start = p0;
                              p.momentum_ramp = pR;
                              p.momentum_stable = p1;
                              p.initial_weight_distribution = dist;
                              p.initial_weight_scale = scale;
                              p.classification = true;
                              p.diagnostics = true;
                              p.validation = null;
                              p.quiet_mode = true;
                              p.fast_mode = fast_mode;
                              p.train_samples_per_iteration = 0; //sync once per period
                              p.ignore_const_cols = false; //same as old NeuralNet code
                              p.shuffle_training_data = false; //same as old NeuralNet code
                              p.nesterov_accelerated_gradient = true; //same as old NeuralNet code
                              p.classification_stop = -1; //don't stop early -> need to compare against old NeuralNet code, which doesn't stop either
                              p.force_load_balance = false; //keep 1 chunk for reproducibility
                              p.replicate_training_data = false;
                              p.single_node_mode = true;
                              p.invoke();

                              mymodel = UKV.get(p.dest());
                              neurons = DeepLearningTask.makeNeuronsForTesting(mymodel.model_info());
                            }

                            // Reference: NeuralNet
                            Layer[] ls;
                            NeuralNetModel refmodel;
                            NeuralNet p = new NeuralNet();
                            {
                              Vec[] data = Utils.remove(_train.vecs(), _train.vecs().length - 1);
                              Vec labels = _train.lastVec();

                              p.seed = myseed;
                              p.hidden = hidden;
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
                              if (dist == DeepLearning.InitialWeightDistribution.Normal) p.initial_weight_distribution = InitialWeightDistribution.Normal;
                              else if (dist == DeepLearning.InitialWeightDistribution.Uniform) p.initial_weight_distribution = InitialWeightDistribution.Uniform;
                              else if (dist == DeepLearning.InitialWeightDistribution.UniformAdaptive) p.initial_weight_distribution = InitialWeightDistribution.UniformAdaptive;
                              p.initial_weight_scale = scale;
                              p.diagnostics = true;
                              p.fast_mode = fast_mode;
                              p.classification = true;
                              if (loss == DeepLearning.Loss.MeanSquare) p.loss = Loss.MeanSquare;
                              else if (loss == DeepLearning.Loss.CrossEntropy) p.loss = Loss.CrossEntropy;

                              ls = new Layer[hidden.length+2];
                              ls[0] = new Layer.VecsInput(data, null);
                              for (int i=0; i<hidden.length; ++i) {
                                if (activation == DeepLearning.Activation.Tanh) {
                                  p.activation = NeuralNet.Activation.Tanh;
                                  ls[1+i] = new Layer.Tanh(hidden[i]);
                                } else if (activation == DeepLearning.Activation.TanhWithDropout) {
                                  p.activation = Activation.TanhWithDropout;
                                  ls[1+i] = new Layer.TanhDropout(hidden[i]);
                                } else if (activation == DeepLearning.Activation.Rectifier) {
                                  p.activation = Activation.Rectifier;
                                  ls[1+i] = new Layer.Rectifier(hidden[i]);
                                } else if (activation == DeepLearning.Activation.RectifierWithDropout) {
                                  p.activation = Activation.RectifierWithDropout;
                                  ls[1+i] = new Layer.RectifierDropout(hidden[i]);
                                } else if (activation == DeepLearning.Activation.Maxout) {
                                  p.activation = Activation.Maxout;
                                  ls[1+i] = new Layer.Maxout(hidden[i]);
                                } else if (activation == DeepLearning.Activation.MaxoutWithDropout) {
                                  p.activation = Activation.MaxoutWithDropout;
                                  ls[1+i] = new Layer.MaxoutDropout(hidden[i]);
                                }
                              }
                              ls[ls.length-1] = new Layer.VecSoftmax(labels, null);
                              for (int i = 0; i < ls.length; i++) {
                                ls[i].init(ls, i, p);
                              }
                              Trainer trainer;
                              if (threaded)
                                trainer = new Trainer.Threaded(ls, p.epochs, null, -1);
                              else
                                trainer = new Trainer.Direct(ls, p.epochs, null);
                              trainer.start();
                              trainer.join();

                              refmodel = new NeuralNetModel(null, null, _train, ls, p);
                            }


                            /**
                             * Compare MEAN weights and biases in hidden and output layer
                             */
                            for (int n=1; n<ls.length; ++n) {
                              Neurons l = neurons[n];
                              Layer ref = ls[n];
                              for (int o = 0; o < l._a.size(); o++) {
                                for (int i = 0; i < l._previous._a.size(); i++) {
                                  a[n] += ref._w[o * l._previous._a.size() + i];
                                  b[n] += l._w.raw()[o * l._previous._a.size() + i];
                                  numweights++;
                                }
                                ba[n] += ref._b[o];
                                bb[n] += l._b.get(o);
                                numbiases++;
                              }
                            }

                            /**
                             * Compare predictions
                             * Note: Reference and H2O each do their internal data normalization,
                             * so we must use their "own" test data, which is assumed to be created correctly.
                             */
                            water.api.ConfusionMatrix CM = new water.api.ConfusionMatrix();
                            // Deep Learning scoring
                            {
                              Frame fpreds = mymodel.score(_train); //[0] is label, [1]...[4] are the probabilities
                              CM = new water.api.ConfusionMatrix();
                              CM.actual = _train;
                              CM.vactual = _train.lastVec();
                              CM.predict = fpreds;
                              CM.vpredict = fpreds.vecs()[0];
                              CM.invoke();
                              StringBuilder sb = new StringBuilder();
                              trainerr += new ConfusionMatrix(CM.cm).err();
                              for (String s : sb.toString().split("\n")) Log.info(s);
                              fpreds.delete();

                              Frame fpreds2 = mymodel.score(_test); //[0] is label, [1]...[4] are the probabilities
                              CM = new water.api.ConfusionMatrix();
                              CM.actual = _test;
                              CM.vactual = _test.lastVec();
                              CM.predict = fpreds2;
                              CM.vpredict = fpreds2.vecs()[0];
                              CM.invoke();
                              sb = new StringBuilder();
                              CM.toASCII(sb);
                              testerr += new ConfusionMatrix(CM.cm).err();
                              for (String s : sb.toString().split("\n")) Log.info(s);
                              fpreds2.delete();
                            }
                            // NeuralNet scoring
                            long [][] cm;
                            {
                              Log.info("\nNeuralNet Scoring:");
                              //training set
                              NeuralNet.Errors train = NeuralNet.eval(ls, 0, null);
                              reftrainerr += train.classification;

                              //test set
                              final Frame[] adapted = refmodel.adapt(_test, false);
                              Vec[] data = Utils.remove(_test.vecs(), _test.vecs().length - 1);
                              Vec labels = _test.vecs()[_test.vecs().length - 1];
                              Layer.VecsInput input = (Layer.VecsInput) ls[0];
                              input.vecs = data;
                              input._len = data[0].length();
                              ((Layer.VecSoftmax) ls[ls.length-1]).vec = labels;
                              int classes = ls[ls.length - 1].units; //WARNING: only works if training set is large enough to have all classes
                              cm = new long[classes][classes];
                              NeuralNet.Errors test = NeuralNet.eval(ls, 0, cm);
                              Log.info("\nNeuralNet Confusion Matrix:");
                              Log.info(new ConfusionMatrix(cm).toString());
                              reftesterr += test.classification;
                              adapted[1].delete();
                            }
                            Assert.assertEquals(cm[0][0], CM.cm[0][0]);
                            Assert.assertEquals(cm[1][0], CM.cm[1][0]);
                            Assert.assertEquals(cm[0][1], CM.cm[0][1]);
                            Assert.assertEquals(cm[1][1], CM.cm[1][1]);

                            // cleanup
                            mymodel.delete();
                            refmodel.delete();
                            _train.delete();
                            _test.delete();
                            frame.delete();
                          }
                          trainerr /= (float)num_repeats;
                          reftrainerr /= (float)num_repeats;
                          testerr /= (float)num_repeats;
                          reftesterr /= (float)num_repeats;

                          /**
                           * Tolerances
                           */
                          final float abseps = threaded ? 1e-2f : 1e-7f;
                          final float releps = threaded ? 1e-2f : 1e-5f;

                          // training set scoring
                          Log.info("NeuralNet     train error " + reftrainerr);
                          Log.info("Deep Learning train error " + trainerr);
                          compareVal(reftrainerr, trainerr, abseps, releps);
                          // test set scoring
                          Log.info("NeuralNet     test error " + reftesterr);
                          Log.info("Deep Learning test error " + testerr);
                          compareVal(reftrainerr, trainerr, abseps, releps);

                          // mean weights/biases
                          for (int n=1; n<hidden.length+2; ++n) {
                            Log.info("NeuralNet     mean weight for layer " + n + ": " + a[n]/numweights);
                            Log.info("Deep Learning mean weight for layer " + n + ": " + b[n]/numweights);
                            Log.info("NeuralNet     mean bias for layer " + n + ": " + ba[n]/numbiases);
                            Log.info("Deep Learning mean bias for layer " + n + ": " + bb[n]/numbiases);
                            compareVal(a[n]/numweights, b[n]/numweights, abseps, releps);
                            compareVal(ba[n]/numbiases, bb[n]/numbiases, abseps, releps);
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
    }
  }
}
