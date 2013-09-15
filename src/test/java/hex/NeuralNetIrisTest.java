package hex;

import hex.Layer.FrameInput;
import hex.Layer.Softmax;
import hex.NeuralNet.NeuralNetScore;
import hex.rng.MersenneTwisterRNG;

import java.io.File;
import java.util.UUID;

import junit.framework.Assert;

import org.junit.*;

import water.*;
import water.fvec.*;
import water.util.Log;

public class NeuralNetIrisTest extends TestUtil {
  static final String PATH = "smalldata/iris/iris.csv";
  Frame _train, _test;

  @BeforeClass public static void stall() {
    stall_till_cloudsize(3);
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

  @Before public void load() {
    Key file = NFSFileVec.make(new File(PATH));
    Key pars = Key.make();
    Frame frame = ParseDataset2.parse(pars, new Key[] { file });
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
    _train = frame(rows, 0, limit);
    _test = frame(rows, limit, (int) frame.numRows() - limit);
    UKV.remove(pars);
  }

  public Layer[] create(float rate) {
    Layer[] ls = new Layer[3];
    ls[0] = new FrameInput(_train, false);
    ls[0].init(null, 4);
    ls[1] = new Layer.Tanh();
    ls[1]._rate = rate;
    ls[1].init(ls[0], 7);
    ls[2] = new Softmax();
    ls[2]._rate = rate;
    ls[2].init(ls[1], 3);
    for( int i = 1; i < ls.length; i++ )
      ls[i].randomize();
    return ls;
  }

  @After public void clean() {
    _train.remove();
    _test.remove();
  }

  @Test public void compare() throws Exception {
    float rate = 0.01f;
    int epochs = 1000;
    Layer[] ls = create(rate);

    NeuralNetMLPReference ref = new NeuralNetMLPReference();
    ref.init();
    Layer l = ls[1];
    for( int o = 0; o < l._a.length; o++ ) {
      for( int i = 0; i < l._in._a.length; i++ )
        ref._nn.ihWeights[i][o] = l._w[o * l._in._a.length + i];
      ref._nn.hBiases[o] = l._b[o];
    }
    l = ls[2];
    for( int o = 0; o < l._a.length; o++ ) {
      for( int i = 0; i < l._in._a.length; i++ )
        ref._nn.hoWeights[i][o] = l._w[o * l._in._a.length + i];
      ref._nn.oBiases[o] = l._b[o];
    }

    // Reference
    ref.train(epochs, rate);

    // H2O
    Trainer.Direct trainer = new Trainer.Direct(ls);
    trainer._batches = epochs * (int) _train.numRows();
    trainer._batch = 1;
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
      for( int i = 0; i < l._in._a.length; i++ ) {
        float a = ref._nn.ihWeights[i][o];
        float b = l._w[o * l._in._a.length + i];
        Assert.assertEquals(a, b, epsilon);
      }
    }

    // Make sure errors are equal
    NeuralNet.Error train = NeuralNetScore.eval(ls, NeuralNet.EVAL_ROW_COUNT);
    ls[0] = new FrameInput(_test, false);
    ls[0].init(null, 4);
    ls[1]._in = ls[0];
    NeuralNet.Error test = NeuralNetScore.eval(ls, NeuralNet.EVAL_ROW_COUNT);
    float trainAcc = ref._nn.Accuracy(ref._trainData);
    Assert.assertEquals(trainAcc, train.Value, epsilon);
    float testAcc = ref._nn.Accuracy(ref._testData);
    Assert.assertEquals(testAcc, test.Value, epsilon);

    Log.info("H2O and Reference equal, train: " + train + ", test: " + test);
  }
}