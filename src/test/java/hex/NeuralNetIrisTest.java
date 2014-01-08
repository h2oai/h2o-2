package hex;

import hex.Layer.Loss;
import hex.Layer.VecSoftmax;
import hex.Layer.VecsInput;
import hex.rng.MersenneTwisterRNG;

import java.io.File;

import junit.framework.Assert;

import org.junit.BeforeClass;
import org.junit.Test;

import water.*;
import water.fvec.*;
import water.util.Log;
import water.util.Utils;

public class NeuralNetIrisTest extends TestUtil {
  static final String PATH = "smalldata/iris/iris.csv";
  Frame _train, _test;

  @BeforeClass public static void stall() {
    stall_till_cloudsize(JUnitRunnerDebug.NODES);
  }

  @Test public void compare() throws Exception {
    NeuralNetMLPReference ref = new NeuralNetMLPReference();

    // Select tanh or rectifier
    ref.init(NeuralNetMLPReference.NeuralNetwork.Activation.tanh);
    //ref.init(NeuralNetMLPReference.NeuralNetwork.Activation.rectifier);

    // Parse Iris and shuffle the same way as ref
    Key file = NFSFileVec.make(new File(PATH));
    Key pars = Key.make();
    Frame frame = ParseDataset2.parse(pars, new Key[] { file });
    UKV.remove(file);

    double[][] rows = new double[(int) frame.numRows()][frame.numCols()];
    for( int c = 0; c < frame.numCols(); c++ )
      for( int r = 0; r < frame.numRows(); r++ )
        rows[r][c] = frame.vecs()[c].at(r);

    MersenneTwisterRNG rand = new MersenneTwisterRNG(MersenneTwisterRNG.SEEDS);
    for( int i = rows.length - 1; i >= 0; i-- ) {
      int shuffle = rand.nextInt(i + 1);
      double[] row = rows[shuffle];
      rows[shuffle] = rows[i];
      rows[i] = row;
    }

    int limit = (int) frame.numRows() * 80 / 100;
    _train = frame(null, Utils.subarray(rows, 0, limit));
    _test = frame(null, Utils.subarray(rows, limit, (int) frame.numRows() - limit));
    UKV.remove(pars);

    float rate = 0.01f;
    int epochs = 1000;
    Vec[] data = Utils.remove(_train.vecs(), _train.vecs().length - 1);
    Vec labels = _train.vecs()[_train.vecs().length - 1];

    Layer[] ls = new Layer[3];
    ls[0] = new VecsInput(data, null);
    ls[1] = new Layer.Tanh(7);
    //ls[1] = new Layer.Rectifier(7);
    ls[2] = new VecSoftmax(labels, null);

    for( int i = 0; i < ls.length; i++ ) {
      ls[i].rate = rate;
      ls[i].loss = Loss.MeanSquare;
      ls[i].max_w2 = Float.MAX_VALUE; //effectively turning it off
//      ls[i].initial_weight_distribution = Layer.InitialWeightDistribution.Uniform;
//      ls[i].initial_weight_scale = 0.01f;
      ls[i].init(ls, i);
    }

    // use the same random weights for the reference implementation
    Layer l = ls[1];
    for( int o = 0; o < l._a.length; o++ ) {
      for( int i = 0; i < l._previous._a.length; i++ )
        ref._nn.ihWeights[i][o] = l._w[o * l._previous._a.length + i];
      ref._nn.hBiases[o] = l._b[o];
    }
    l = ls[2];
    for( int o = 0; o < l._a.length; o++ ) {
      for( int i = 0; i < l._previous._a.length; i++ )
        ref._nn.hoWeights[i][o] = l._w[o * l._previous._a.length + i];
      ref._nn.oBiases[o] = l._b[o];
    }

    // Reference
    ref.train(epochs, rate);

    // H2O
    Trainer.Direct trainer = new Trainer.Direct(ls, epochs, null);
    trainer.run();

    // Make sure outputs are equal
    float epsilon = 1e-4f;
    for( int o = 0; o < ls[2]._a.length; o++ ) {
      float a = ref._nn.outputs[o];
      float b = ls[2]._a[o];
      Assert.assertEquals(a, b, epsilon);
    }

    // Make sure weights are equal
    l = ls[1];
    for( int o = 0; o < l._a.length; o++ ) {
      for( int i = 0; i < l._previous._a.length; i++ ) {
        float a = ref._nn.ihWeights[i][o];
        float b = l._w[o * l._previous._a.length + i];
        Assert.assertEquals(a, b, epsilon);
      }
    }

    // Make sure errors are equal
    NeuralNet.Errors train = NeuralNet.eval(ls, 0, null);
    data = Utils.remove(_test.vecs(), _test.vecs().length - 1);
    labels = _test.vecs()[_test.vecs().length - 1];
    VecsInput input = (VecsInput) ls[0];
    input.vecs = data;
    input._len = data[0].length();
    ((VecSoftmax) ls[2]).vec = labels;
    NeuralNet.Errors test = NeuralNet.eval(ls, 0, null);
    float trainAcc = ref._nn.Accuracy(ref._trainData);
    Assert.assertEquals(trainAcc, train.classification, epsilon);
    float testAcc = ref._nn.Accuracy(ref._testData);
    Assert.assertEquals(testAcc, test.classification, epsilon);

    Log.info("H2O and Reference equal, train: " + train + ", test: " + test);

    for( int i = 0; i < ls.length; i++ )
      ls[i].close();
    _train.remove();
    _test.remove();
  }
}
