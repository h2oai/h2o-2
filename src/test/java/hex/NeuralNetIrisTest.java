package hex;

import org.junit.BeforeClass;
import org.junit.Test;
import water.JUnitRunnerDebug;
import water.TestUtil;
import water.fvec.Frame;

public class NeuralNetIrisTest extends TestUtil {
  static final String PATH = "smalldata/iris/iris.csv";
  Frame _train, _test;

  @BeforeClass public static void stall() {
    stall_till_cloudsize(JUnitRunnerDebug.NODES);
  }

  @Test public void compare() throws Exception {

//    // Testing different things
//    // Note: Microsoft reference implementation is only for Tanh + MSE, rectifier and MCE are implemented by 0xdata (trivial).
//    // Note: Initial weight distributions are copied, but what is tested is the stability behavior.
//    NeuralNet.Activation[] activations = { NeuralNet.Activation.Tanh, NeuralNet.Activation.Rectifier };
//    Loss[] losses = { NeuralNet.Loss.MeanSquare, NeuralNet.Loss.CrossEntropy };
//    NeuralNet.InitialWeightDistribution[] dists = {
//            NeuralNet.InitialWeightDistribution.Normal,
//            //NeuralNet.InitialWeightDistribution.Uniform,
//            NeuralNet.InitialWeightDistribution.UniformAdaptive };
//    float[] initial_weight_scales = { 0.02f, 1.538f };
//    double[] holdout_ratios = { 0.8 };
//
//    for (NeuralNet.Activation activation : activations) {
//      for (Loss loss : losses) {
//        for (NeuralNet.InitialWeightDistribution dist : dists) {
//          for (float scale : initial_weight_scales) {
//            for (double holdout_ratio : holdout_ratios) {
//
//              Log.info("Testing " + activation.name() + " activation function with " + loss.name() + " loss function");
//              Log.info("Testing " + dist.name() + " distribution with " + scale + " scale");
//              Log.info("Testing holdout ratio " + holdout_ratio);
//
//              NeuralNetMLPReference ref = new NeuralNetMLPReference();
//
//              final long seed = 0xDEADBEEF;
//              Log.info("Using seed " + seed);
//              ref.init(activation, water.util.Utils.getDeterRNG(seed), holdout_ratio);
//
//              // Parse Iris and shuffle the same way as ref
//              Key file = NFSFileVec.make(new File(PATH));
//              Key pars = Key.make();
//              Frame frame = ParseDataset2.parse(pars, new Key[] { file });
//              UKV.remove(file);
//
//              double[][] rows = new double[(int) frame.numRows()][frame.numCols()];
//              for( int c = 0; c < frame.numCols(); c++ )
//                for( int r = 0; r < frame.numRows(); r++ )
//                  rows[r][c] = frame.vecs()[c].at(r);
//
//              Random rand = water.util.Utils.getDeterRNG(seed);
//              for( int i = rows.length - 1; i >= 0; i-- ) {
//                int shuffle = rand.nextInt(i + 1);
//                double[] row = rows[shuffle];
//                rows[shuffle] = rows[i];
//                rows[i] = row;
//              }
//
//              int limit = (int) (frame.numRows() * holdout_ratio);
//              _train = frame(null, Utils.subarray(rows, 0, limit));
//              _test = frame(null, Utils.subarray(rows, limit, (int) frame.numRows() - limit));
//              UKV.remove(pars);
//
//              Vec[] data = Utils.remove(_train.vecs(), _train.vecs().length - 1);
//              Vec labels = _train.vecs()[_train.vecs().length - 1];
//
//              NeuralNet p = new NeuralNet();
//              p.rate = 0.01f;
//              p.epochs = 1000;
//              p.activation = activation;
//              p.max_w2 = Float.MAX_VALUE;
//              p.rate = 0.01f;
//              p.epochs = 1000;
//              p.activation = activation;
//              p.max_w2 = Float.MAX_VALUE;
//              p.input_dropout_ratio = 0;
//              p.rate_annealing = 0;
//              p.l1 = 0;
//              p.l2 = 0;
//              p.momentum_start = 0;
//              p.momentum_ramp = 0;
//              p.momentum_stable = 0;
//              p.initial_weight_distribution = dist;
//              p.initial_weight_scale = scale;
//
//              Layer[] ls = new Layer[3];
//              ls[0] = new VecsInput(data, null);
//              if (activation == NeuralNet.Activation.Tanh) {
//                ls[1] = new Tanh(7);
//              }
//              else if (activation == NeuralNet.Activation.Rectifier) {
//                ls[1] = new Rectifier(7);
//              }
//              ls[2] = new VecSoftmax(labels, null, loss);
//
//              for( int i = 0; i < ls.length; i++ ) {
//                ls[i].init(ls, i, p);
//              }
//
//              // use the same random weights for the reference implementation
//              Layer l = ls[1];
//              for( int o = 0; o < l._a.length; o++ ) {
//                for( int i = 0; i < l._previous._a.length; i++ )
//                  ref._nn.ihWeights[i][o] = l._w[o * l._previous._a.length + i];
//                ref._nn.hBiases[o] = l._b[o];
//              }
//              l = ls[2];
//              for( int o = 0; o < l._a.length; o++ ) {
//                for( int i = 0; i < l._previous._a.length; i++ )
//                  ref._nn.hoWeights[i][o] = l._w[o * l._previous._a.length + i];
//                ref._nn.oBiases[o] = l._b[o];
//              }
//
//              // Reference
//              ref.train((int)p.epochs, (float)p.rate, loss);
//
//              // H2O
//              Trainer.Direct trainer = new Trainer.Direct(ls, p.epochs, null);
//              trainer.run();
//
//              float epsilon = 1e-4f;
//              for( int o = 0; o < ls[2]._a.length; o++ ) {
//                float a = ref._nn.outputs[o];
//                float b = ls[2]._a[o];
//                Assert.assertEquals(a, b, epsilon); //absolute error
//                if (a != b) Assert.assertTrue(Math.abs(a-b)/Math.max(a,b) < 10*epsilon); //relative error
//              }
//
//              // Make sure weights are equal
//              l = ls[1];
//              for( int o = 0; o < l._a.length; o++ ) {
//                for( int i = 0; i < l._previous._a.length; i++ ) {
//                  float a = ref._nn.ihWeights[i][o];
//                  float b = l._w[o * l._previous._a.length + i];
//                  Assert.assertEquals(a, b, epsilon); //absolute error
//                  if (a != b) Assert.assertTrue(Math.abs(a-b)/Math.max(a,b) < 10*epsilon); //relative error
//                }
//              }
//
//              // Make sure errors are equal
//              NeuralNet.Errors train = NeuralNet.eval(ls, 0, null);
//              data = Utils.remove(_test.vecs(), _test.vecs().length - 1);
//              labels = _test.vecs()[_test.vecs().length - 1];
//              VecsInput input = (VecsInput) ls[0];
//              input.vecs = data;
//              input._len = data[0].length();
//              ((VecSoftmax) ls[2]).vec = labels;
//              NeuralNet.Errors test = NeuralNet.eval(ls, 0, null);
//              float trainAcc = ref._nn.Accuracy(ref._trainData);
//              Assert.assertEquals(trainAcc, train.classification, epsilon);
//              float testAcc = ref._nn.Accuracy(ref._testData);
//              Assert.assertEquals(testAcc, test.classification, epsilon);
//
//              Log.info("H2O and Reference equal, train: " + train + ", test: " + test);
//
//              for( int i = 0; i < ls.length; i++ )
//                ls[i].close();
//              _train.remove();
//              _test.remove();
//            }
//          }
//        }
//      }
//    }
  }
}
