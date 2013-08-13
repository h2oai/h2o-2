package hex;

import hex.Layer.FrameInput;
import hex.Layer.Softmax;
import hex.rng.MersenneTwisterRNG;

import java.io.File;
import java.util.UUID;

import junit.framework.Assert;

import org.junit.BeforeClass;
import org.junit.Test;

import water.*;
import water.fvec.*;
import water.util.Log;

public class NeuralNetIrisTest extends NeuralNetTest {
  static final String PATH = "smalldata/iris/iris.csv";
  FrameInput _train, _test;

  public static void main(String[] args) throws Exception {
    water.Boot.main(UserMain.class, args);
  }

  public static class UserMain {
    public static void main(String[] args) throws Exception {
      Sandbox.localCloud(1, true, args);

      NeuralNetIrisTest test = new NeuralNetIrisTest();
      test.load();
      test.run();
    }
  }

  static Frame frame(double[][] rows, int off, int len) {
    Vec[] vecs = new Vec[rows[0].length];
    for( int c = 0; c < vecs.length; c++ ) {
      AppendableVec vec = new AppendableVec(UUID.randomUUID().toString());
      NewChunk chunk = new NewChunk(vec, 0);
      for( int r = 0; r < len; r++ )
        chunk.addNum(rows[r + off][c]);
      chunk.close(0, null);
      vecs[c] = vec.close(null);
    }
    return new Frame(null, vecs);
  }

  @BeforeClass public void load() {
    Key file = NFSFileVec.make(new File(PATH));
    Frame frame = ParseDataset2.parse(Key.make(), new Key[] { file });
    UKV.remove(file);

    double[][] rows = new double[(int) frame.numRows()][frame.numCols()];
    for( int c = 0; c < frame.numCols(); c++ )
      for( int r = 0; r < frame.numRows(); r++ )
        rows[r][c] = frame._vecs[c].at(r);

    MersenneTwisterRNG rand = new MersenneTwisterRNG(MersenneTwisterRNG.SEEDS);
    for( int i = rows.length - 1; i >= 0; i-- ) {
      int shuffle = rand.nextInt(i + 1);
      double[] row = rows[shuffle];
      rows[shuffle] = rows[i];
      rows[i] = row;
    }

    int limit = (int) frame.numRows() * 80 / 100;
    _train = new FrameInput(frame(rows, 0, limit), false);
    _test = new FrameInput(frame(rows, limit, (int) frame.numRows() - limit), false);
  }

  @Test public void compare() throws Exception {
    float rate = 0.01f;
    // We think the reference implementation's momentum
    // approach is incorrect, turn it off
    float momentum = 0;
    int epochs = 1000;
    _ls = new Layer[3];
    _ls[0] = _train;
    _ls[1] = new Layer.Tanh(_ls[0], 7);
    _ls[1]._rate = rate;
    _ls[1]._rateAnnealing = 0;
    _ls[1]._momentum = momentum;
    _ls[1]._l2 = 0;
    _ls[2] = new Softmax(_ls[1], 3);
    _ls[2]._rate = rate;
    _ls[2]._rateAnnealing = 0;
    _ls[2]._momentum = momentum;
    _ls[2]._l2 = 0;
    for( int i = 0; i < _ls.length; i++ )
      _ls[i].init();

    NeuralNetMLPReference ref = new NeuralNetMLPReference();
    ref.init();
    Layer l = _ls[1];
    for( int o = 0; o < l._a.length; o++ ) {
      for( int i = 0; i < l._in._a.length; i++ )
        ref._nn.ihWeights[i][o] = l._w[o * l._in._a.length + i];
      ref._nn.hBiases[o] = l._b[o];
    }
    l = _ls[2];
    for( int o = 0; o < l._a.length; o++ ) {
      for( int i = 0; i < l._in._a.length; i++ )
        ref._nn.hoWeights[i][o] = l._w[o * l._in._a.length + i];
      ref._nn.oBiases[o] = l._b[o];
    }

    // Reference
    ref.train(epochs, rate, momentum);

    // H2O
    Trainer trainer = new Trainer.Direct(_ls);
    trainer._batches = epochs * (int) _train._frame.numRows();
    trainer._batch = 1;
    trainer.run();

    // Make sure outputs are equal
    float epsilon = 1e-4f;
    for( int o = 0; o < _ls[2]._a.length; o++ ) {
      float a = ref._nn.outputs[o];
      float b = _ls[2]._a[o];
      Assert.assertEquals(a, b, epsilon);
    }

    // Make sure weights are equal
    l = _ls[1];
    for( int o = 0; o < l._a.length; o++ ) {
      for( int i = 0; i < l._in._a.length; i++ ) {
        float a = ref._nn.ihWeights[i][o];
        float b = l._w[o * l._in._a.length + i];
        Assert.assertEquals(a, b, epsilon);
      }
    }

    // Make sure errors are equal
    Error train = eval(_train);
    Error test = eval(_test);
    float trainAcc = ref._nn.Accuracy(ref._trainData);
    Assert.assertEquals(trainAcc, train.Value, epsilon);
    float testAcc = ref._nn.Accuracy(ref._testData);
    Assert.assertEquals(testAcc, test.Value, epsilon);

    Log.info("H2O and Reference equal, train: " + train + ", test: " + test);
  }

  public void run() {
    for( int t = 0; t < 10; t++ ) {
      int epochs = 100;
      float m = (t + 0) / 10f;
      _ls = new Layer[3];
      _ls[0] = _train;
      _ls[1] = new Layer.Tanh(_ls[0], 7);
      _ls[1]._rate = 0.01f;
      _ls[1]._momentum = m;
      _ls[2] = new Softmax(_ls[1], 3);
      _ls[2]._rate = 0.01f;
      _ls[2]._momentum = m;
      for( int i = 0; i < _ls.length; i++ )
        _ls[i].init();

      Trainer trainer = new Trainer.Direct(_ls);
      int count = epochs * (int) _train._frame.numRows();
      trainer._batches = count / trainer._batch;
      trainer.run();
      Error train = eval(_train);
      Error test = eval(_test);
      Log.info("Iris run: train: " + train + ", test: " + test);
    }
  }
}