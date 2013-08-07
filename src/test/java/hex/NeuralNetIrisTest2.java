package hex;

import hex.Layer2.FrameInput;
import hex.Layer2.Input;
import hex.Layer2.Softmax;
import hex.rng.MersenneTwisterRNG;

import java.io.File;
import java.text.DecimalFormat;
import java.util.UUID;

import water.*;
import water.fvec.*;
import water.util.Utils;

public class NeuralNetIrisTest2 {
  static final String PATH = "smalldata/iris/iris.csv";
  static final DecimalFormat _format = new DecimalFormat("0.000");

  FrameInput _train, _test;
  Layer2[] _ls;

  public static void main(String[] args) throws Exception {
    water.Boot.main(UserMain.class, args);
  }

  public static class UserMain {
    public static void main(String[] args) throws Exception {
      Sandbox.localCloud(1, true, args);

      NeuralNetIrisTest2 test = new NeuralNetIrisTest2();
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

  public void run() throws Exception {
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

    System.out.println("Train:");
    for( int n = 0; n < 5; n++ ) {
      _train._n = n;
      _train.fprop();
      for( int i = 0; i < _train._a.length; i++ )
        System.out.print(_train._a[i] + ", ");
      System.out.println();
    }
    System.out.println("Test:");
    for( int n = 0; n < 3; n++ ) {
      _test._n = n;
      _test.fprop();
      for( int i = 0; i < _test._a.length; i++ )
        System.out.print(_test._a[i] + ", ");
      System.out.println();
    }
    _train._n = _test._n = 0;

    float rate = 0.01f;
    float momentum = .9f;
    int epochs = 100000;

    _ls = new Layer2[3];
    _ls[0] = _train;
    _ls[1] = new Layer2.Tanh(_ls[0], 7);
    _ls[1]._rate = rate;
    _ls[1]._oneMinusMomentum = 1 - momentum;
    _ls[1]._l2 = 0;
    _ls[2] = new Softmax(_ls[1], 3);
    _ls[2]._rate = rate;
    _ls[2]._oneMinusMomentum = 1 - momentum;
    _ls[2]._l2 = 0;
    for( int i = 0; i < _ls.length; i++ )
      _ls[i].init();

    CSharp cs = new CSharp();
    cs.init();
    Layer2 l = _ls[1];
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
    Trainer2 trainer = new Trainer2.Direct(_ls);
    trainer._batches = limit;
    trainer._batch = 1;

    for( int i = 0; i < epochs; i++ ) {
      start = System.nanoTime();
      trainer.run();
      ended = System.nanoTime();
      ms = (int) ((ended - start) / 1e6);
      System.out.println(_ls[1]._w[0] + ", last: " + _ls[1]._wLast[0]);
    }

    String train = test(_train, _train._count);
    String test = test(_test, _test._count);
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

  String test(FrameInput input, long count) {
    Layer2[] clones = new Layer2[_ls.length];
    for( int i = 1; i < _ls.length; i++ )
      clones[i] = Utils.deepClone(_ls[i], "_in");
    clones[0] = new FrameInput(input._frame, input._normalize);
    for( int i = 1; i < _ls.length; i++ )
      clones[i]._in = clones[i - 1];

    int correct = 0;
    for( int n = 0; n < count; n++ )
      if( test(clones, n) )
        correct++;
    String pct = _format.format(((count - correct) * 100f / count));
    return pct + "%";
  }

  static boolean test(Layer2[] ls, int n) {
    Input input = (Input) ls[0];
    input._n = n;
    for( int i = 0; i < ls.length; i++ )
      ls[i].fprop();

    float[] out = ls[ls.length - 1]._a;
    float max = Float.MIN_VALUE;
    int idx = -1;
    for( int i = 0; i < out.length; i++ ) {
      if( out[i] > max ) {
        max = out[i];
        idx = i;
      }
    }
    return idx == input.label();
  }
}