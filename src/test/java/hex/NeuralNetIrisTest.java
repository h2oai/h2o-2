package hex;

import hex.Layer.FrameInput;
import hex.Layer.Softmax;
import hex.rng.MersenneTwisterRNG;

import java.io.File;
import java.util.UUID;

import water.*;
import water.fvec.*;

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

  public void load() {
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

  public void create(float rate, float momentum) {
    _ls = new Layer[3];
    _ls[0] = _train;
    _ls[1] = new Layer.Tanh(_ls[0], 7);
    _ls[1]._rate = rate;
    _ls[1]._momentum = 1 - momentum;
    _ls[1]._l2 = 0;
    _ls[2] = new Softmax(_ls[1], 3);
    _ls[2]._rate = rate;
    _ls[2]._momentum = 1 - momentum;
    _ls[2]._l2 = 0;
    for( int i = 0; i < _ls.length; i++ )
      _ls[i].init(false);
  }

  public void run() throws Exception {
    load();
    float rate = 0.001f;
    float momentum = .9f;
    int epochs = 100;
    create(rate, momentum);

    NeuralNetMLPReference cs = new NeuralNetMLPReference();
    cs.init();
    Layer l = _ls[1];
    for( int o = 0; o < l._a.length; o++ ) {
      for( int i = 0; i < l._in._a.length; i++ )
        cs._nn.ihWeights[i][o] = l._w[o * l._in._a.length + i];
      cs._nn.hBiases[o] = l._b[o];
    }
    l = _ls[2];
    for( int o = 0; o < l._a.length; o++ ) {
      for( int i = 0; i < l._in._a.length; i++ )
        cs._nn.hoWeights[i][o] = l._w[o * l._in._a.length + i];
      cs._nn.oBiases[o] = l._b[o];
    }
    long start = System.nanoTime();
    cs.train(epochs, rate, momentum);
    long ended = System.nanoTime();
    int ms = (int) ((ended - start) / 1e6);
    System.out.println("CSharp " + ms);

    //ParallelTrainers trainer = new ParallelTrainers(_ls, _train._labels);
    Trainer trainer = new Trainer.Direct(_ls);
    trainer._batches = (int) _train._frame.numRows();
    trainer._batch = 1;

    for( int i = 0; i < epochs; i++ ) {
      start = System.nanoTime();
      trainer.run();
      ended = System.nanoTime();
      ms = (int) ((ended - start) / 1e6);
      System.out.println(_ls[1]._w[0] + ", g: " + _ls[1]._gw[0]);
    }

    Error train = eval(_train);
    Error test = eval(_test);
    System.out.println("train: " + train + ", test: " + test);

    for( int o = 0; o < _ls[2]._a.length; o++ ) {
      float a = cs._nn.outputs[o];
      float b = _ls[2]._a[o];
      System.out.println(a - b);
    }

    l = _ls[1];
    for( int o = 0; o < l._a.length; o++ ) {
      for( int i = 0; i < l._in._a.length; i++ ) {
        float a = cs._nn.ihWeights[i][o];
        float b = l._w[o * l._in._a.length + i];
        System.out.println(a - b);
      }
    }

    cs.test();
  }
}