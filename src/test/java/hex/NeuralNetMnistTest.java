package hex;

import hex.Layer.Input;
import hex.rng.MersenneTwisterRNG;

import java.io.*;
import java.util.zip.GZIPInputStream;

import water.util.Utils;

public class NeuralNetMnistTest extends NeuralNetTest {
  static final int PIXELS = 784, EDGE = 28;
  static final String PATH = "smalldata/mnist70k/";

  MnistInput _train, _test;

  public static class MnistInput extends Input {
    float[] _images;
    byte[] _labels;

    public MnistInput() {
      super(PIXELS);
    }

    @Override int label() {
      return _labels[(int) _n];
    }

    @Override void fprop(int off, int len) {
      System.arraycopy(_images, (int) _n * PIXELS, _a, 0, PIXELS);
    }
  }

  void load() {
    _train = loadZip(PATH + "train-images-idx3-ubyte.gz", PATH + "train-labels-idx1-ubyte.gz");
    _test = loadZip(PATH + "t10k-images-idx3-ubyte.gz", PATH + "t10k-labels-idx1-ubyte.gz");
  }

  public void run() throws Exception {
    load();

    boolean load = false, save = false;
    boolean pretrain = false;
    boolean rectifier = false;
    {
      _ls = new Layer[3];
      _ls[0] = _train;
      if( rectifier ) {
        _ls[1] = new Layer.Rectifier(_ls[0], 500);
        _ls[2] = new Layer.Rectifier(_ls[1], 10);
      } else {
        _ls[1] = new Layer.Tanh(_ls[0], 1000);
        _ls[1]._rate = 0.01f;
        _ls[2] = new Layer.Softmax(_ls[1], 10);
        _ls[2]._rate = 0.01f;
      }
    }
    for( int i = 0; i < _ls.length; i++ )
      _ls[i].init(false);

    if( load ) {
      long time = System.nanoTime();
      for( int i = 0; i < _ls.length; i++ ) {
        String json = Utils.readFile(new File("layer" + i + ".json"));
        _ls[i] = Utils.json(json, Layer.class);
      }
      System.out.println("load: " + (int) ((System.nanoTime() - time) / 1e6) + " ms");
    }

    //ParallelTrainers trainer = new ParallelTrainers(_ls, _train._labels);
    Trainer trainer = new Trainer.Direct(_ls);

    if( pretrain ) {
      for( int i = 0; i < _ls.length; i++ ) {
        System.out.println("Training level " + i);
        long time = System.nanoTime();
        preTrain(trainer, i);
        System.out.println((int) ((System.nanoTime() - time) / 1e6) + " ms");
      }
    }
    train(trainer);

    if( save ) {
      long time = System.nanoTime();
      for( int i = 0; i < _ls.length; i++ ) {
        String json = Utils.json(_ls[i]);
        Utils.writeFile(new File("rbm" + i + ".json"), json);
      }
      System.out.println("save: " + (int) ((System.nanoTime() - time) / 1e6) + " ms");
    }
  }

  void preTrain(Trainer trainer, int upTo) {
    float[][] inputs = new float[trainer._batch][];
    float[][] tester = new float[100][];
    int n = 0;
    float trainError = 0, testError = 0, trainEnergy = 0, testEnergy = 0;
    for( int i = 0; i < 10000; i++ ) {
      n = up(_train, n, inputs, upTo);

      for( int b = 0; b < trainer._batch; b++ )
        _ls[upTo].contrastiveDivergence(inputs[b]);
      _ls[upTo].adjust(n);

      if( i % 100 == 0 ) {
        up(_train, 0, tester, upTo);
        for( int b = 0; b < tester.length; b++ ) {
          trainError += _ls[upTo].error(tester[b]);
          trainEnergy += _ls[upTo].freeEnergy(tester[b]);
        }

        up(_train, 0, tester, upTo);
        for( int b = 0; b < tester.length; b++ ) {
          testError += _ls[upTo].error(tester[b]);
          testEnergy += _ls[upTo].freeEnergy(tester[b]);
        }

        StringBuilder sb = new StringBuilder();
        sb.append(i);
        sb.append(", train err: ");
        sb.append(_format.format(trainError));
        sb.append(", test err: ");
        sb.append(_format.format(testError));
        sb.append(", train egy: ");
        sb.append(_format.format(trainEnergy));
        sb.append(", test egy: ");
        sb.append(_format.format(testEnergy));
        System.out.println(sb.toString());
        trainError = testError = trainEnergy = testEnergy = 0;
      }
    }
  }

  int up(MnistInput data, int n, float[][] batch, int upTo) {
//    for( int b = 0; b < batch.length; b++ ) {
//      batch[b] = data._images[n];
//      n = n == data._labels.length - 1 ? 0 : n + 1;
//
//      for( int level = 0; level < upTo; level++ ) {
//        float[] t = new float[_ls[level]._b.length];
//        _ls[level].fprop(batch[b], t);
//        batch[b] = t;
//      }
//    }
    return n;
  }

  void train(final Trainer trainer) {
    Thread thread = new Thread() {
      @Override public void run() {
        trainer.run();
      }
    };
    thread.start();

    long start = System.nanoTime();
    long lastTime = start;
    int n = 0, lastItems = 0;
    for( ;; ) {
      try {
        Thread.sleep(3000);
      } catch( InterruptedException e ) {
        throw new RuntimeException(e);
      }

      Error train = eval(_train);
      Error test = eval(_test);
      long time = System.nanoTime();
      int ms = (int) ((time - lastTime) / 1e6);
      lastTime = time;
      int items = trainer._count.get();
      int ps = (items - lastItems) * 1000 / ms;
      lastItems = items;
      String s = ms + " ms " + (ps) + "/s, train: " + train + ", test: " + test;

      Layer layer = _ls[1];
      double sqr = 0;
      int zeros = 0;
      for( int o = 0; o < layer._a.length; o++ ) {
        for( int i = 0; i < layer._in._a.length; i++ ) {
          float d = layer._gw[o * layer._in._a.length + i];
          sqr += d * d;
          zeros += d == 0 ? 1 : 0;
        }
      }
      s += ", gw: " + sqr + " (" + (zeros * 100 / layer._gw.length) + "% 0)";
      sqr = 0;
      for( int o = 0; o < layer._a.length; o++ ) {
        for( int i = 0; i < layer._in._a.length; i++ ) {
          float d = layer._w[o * layer._in._a.length + i];
          sqr += d * d;
        }
      }
      s += ", w: " + sqr;
      sqr = 0;
      for( int o = 0; o < layer._a.length; o++ ) {
        float d = layer._a[o];
        sqr += d * d;
      }
      System.out.println(s + ", a: " + sqr);

      if( n != 0 && n % 10 == 0 )
        System.out.println("All: " + eval(_train));
    }
  }

  static float convert(byte b) {
    return (b & 0xff) / 255f;
  }

  static MnistInput loadZip(String images, String labels) {
    DataInputStream imagesBuf = null, labelsBuf = null;
    try {
      imagesBuf = new DataInputStream(new GZIPInputStream(new FileInputStream(new File(images))));
      labelsBuf = new DataInputStream(new GZIPInputStream(new FileInputStream(new File(labels))));

      imagesBuf.readInt(); // Magic
      int count = imagesBuf.readInt();
      labelsBuf.readInt(); // Magic
      if( count != labelsBuf.readInt() )
        throw new RuntimeException();
      imagesBuf.readInt(); // Rows
      imagesBuf.readInt(); // Cols

      System.out.println("Count=" + count);
      byte[][] rawI = new byte[count][PIXELS];
      byte[] rawL = new byte[count];
      for( int n = 0; n < count; n++ ) {
        imagesBuf.readFully(rawI[n]);
        rawL[n] = labelsBuf.readByte();
      }

      MersenneTwisterRNG rand = new MersenneTwisterRNG(MersenneTwisterRNG.SEEDS);
      for( int i = count - 1; i >= 0; i-- ) {
        int shuffle = rand.nextInt(i + 1);
        byte[] image = rawI[shuffle];
        rawI[shuffle] = rawI[i];
        rawI[i] = image;
        byte label = rawL[shuffle];
        rawL[shuffle] = rawL[i];
        rawL[i] = label;
      }

      MnistInput input = new MnistInput();
      input._images = new float[count * PIXELS];
      input._labels = rawL;
      input._count = rawL.length;
      for( int n = 0; n < count; n++ )
        for( int i = 0; i < PIXELS; i++ )
          input._images[n * PIXELS + i] = convert(rawI[n][i]);
      return input;
    } catch( Exception e ) {
      throw new RuntimeException(e);
    } finally {
      Utils.close(labelsBuf, imagesBuf);
    }
  }
}