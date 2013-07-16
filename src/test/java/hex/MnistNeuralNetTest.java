package hex;

import java.io.*;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.zip.GZIPInputStream;

import water.util.Utils;

public class MnistNeuralNetTest {
  static final int PIXELS = 784, EDGE = 28;

  static class Data {
    float[] _images;
    byte[] _labels;
  }

  Data _train, _test;
  Layer[] _ls;
  DecimalFormat _format = new DecimalFormat("0.00");

  public static void main(String[] args) throws Exception {
    MnistNeuralNetTest mnist = new MnistNeuralNetTest();
    mnist.run();
  }

  void run() throws Exception {
    {
      long time = System.nanoTime();
      String f = "smalldata/mnist70k/";
      _train = loadData(f + "train-images-idx3-ubyte.gz", f + "train-labels-idx1-ubyte.gz");
      _test = loadData(f + "t10k-images-idx3-ubyte.gz", f + "t10k-labels-idx1-ubyte.gz");
      System.out.println("load: " + (int) ((System.nanoTime() - time) / 1e6) + " ms");
    }

    boolean load = false;
    boolean pretrain = false;
    boolean rectifier = false;
    {
      _ls = new Layer[3];
      _ls[0] = new Layer.Input(_train._images, PIXELS);
      if( rectifier ) {
        _ls[1] = new Layer.Rectifier(_ls[0], 500);
        _ls[2] = new Layer.Rectifier(_ls[1], 10);
      } else {
        _ls[1] = new Layer.Tanh(_ls[0], 500);
        _ls[2] = new Layer.Tanh(_ls[1], 10);
      }
    }

    if( load ) {
      long time = System.nanoTime();
      for( int i = 0; i < _ls.length; i++ ) {
        String json = Utils.readFile(new File("layer" + i + ".json"));
        _ls[i] = Utils.json(json, Layer.class);
      }
      System.out.println("json: " + (int) ((System.nanoTime() - time) / 1e6) + " ms");
    }

    if( pretrain ) {
      for( int i = 0; i < _ls.length; i++ ) {
        System.out.println("Training level " + i);
        long time = System.nanoTime();
        preTrain(i);
        System.out.println((int) ((System.nanoTime() - time) / 1e6) + " ms");
      }
    }

    // Basic visualization of images and weights

//    JFrame frame = new JFrame("RBM");
//    frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
//    MnistCanvas canvas = new MnistCanvas(_ls, _train._images, _train._labels);
//    frame.setContentPane(canvas.init());
//    frame.pack();
//    frame.setLocationRelativeTo(null);
//    frame.setVisible(true);

    //

    train();

//    long time = System.nanoTime();
//    for( int i = 0; i < _rbms.length; i++ ) {
//      String json = Utils.json(_rbms[i]);
//      Utils.writeFile(new File("rbm" + i + ".json"), json);
//    }
//    System.out.println("save: " + (int) ((System.nanoTime() - time) / 1e6) + " ms");
  }

  void preTrain(int upTo) {
    float[][] inputs = new float[Layer.BATCH][];
    float[][] tester = new float[100][];
    int n = 0;
    float trainError = 0, testError = 0, trainEnergy = 0, testEnergy = 0;
    for( int i = 0; i < 10000; i++ ) {
      n = up(_train, n, inputs, upTo);

      for( int b = 0; b < Layer.BATCH; b++ )
        _ls[upTo].contrastiveDivergence(inputs[b]);
      _ls[upTo].adjust();

      if( i % 100 == 0 ) {
        up(_train, 0, tester, upTo);
        for( int b = 0; b < tester.length; b++ ) {
          trainError += _ls[upTo].error(tester[b]);
          trainEnergy += _ls[upTo].freeEnergy(tester[b]);
        }

        up(_test, 0, tester, upTo);
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

  int up(Data data, int n, float[][] batch, int upTo) {
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

  void train() {
    Trainer trainer = new Trainer.Direct(_ls);
    int n = 0;
    for( ;; ) {
      for( int b = 0; b < Layer.BATCH; b++ ) {
        _ls[0]._off = n * PIXELS;
        trainer.fprop();

        for( int i = 1; i < _ls.length; i++ )
          Arrays.fill(_ls[i]._e, 0);
        float[] err = _ls[_ls.length - 1]._e;
        err[_train._labels[n]] = 1.0f;
        for( int i = 0; i < err.length; i++ )
          err[i] -= _ls[_ls.length - 1]._a[i];

        trainer.bprop();
        n = n == _train._labels.length - 1 ? 0 : n + 1;
      }

      for( int i = 1; i < _ls.length; i++ )
        _ls[i].adjust();

      if( n % 100 == 0 ) {
        String train = test(_train, 100);
        String test = test(_test, 100);
        String s = "Train: " + train + ", test: " + test;

        Layer layer = _ls[1];
        double sqr = 0;
        int zeros = 0;
        for( int o = 0; o < layer._a.length; o++ ) {
          for( int i = 0; i < layer._in._len; i++ ) {
            float d = layer._gw[o * layer._in._len + i];
            sqr += d * d;
            zeros += d == 0 ? 1 : 0;
          }
        }
        s += ", gw: " + sqr + " (" + (zeros * 100 / layer._gw.length) + "% 0)";
        sqr = 0;
        for( int o = 0; o < layer._a.length; o++ ) {
          for( int i = 0; i < layer._in._len; i++ ) {
            float d = layer._w[o * layer._in._len + i];
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
      }

      if( n != 0 && n % 10000 == 0 ) {
//        for( int i = 0; i < _gs.length; i++ ) {
//          _gs[i].wRate *= 0.9;
//          _gs[i].bRate *= 0.9;
//          _gs[i].wl1 *= 0.9;
//          _gs[i].bl1 *= 0.9;
//        }

        System.out.println("All: " + test(_test, _test._labels.length));
      }
    }
  }

  static class Error {
    double Value;
  }

  String test(Data data, int length) {
    Layer[] clones = new Layer[_ls.length];
    for( int i = 0; i < _ls.length; i++ ) {
//      _ls[i]._forward.get(_ls[i]._w);
      clones[i] = Utils.deepClone(_ls[i]);
    }
    for( int i = 1; i < _ls.length; i++ )
      clones[i]._in = clones[i - 1];

    Error error = new Error();
    int correct = 0;
    for( int n = 0; n < length; n++ ) {
      if( MnistNeuralNetTest.test(clones, data, n, error) ) correct++;
    }
    String pct = _format.format(((length - correct) * 100f / length));
    return "err " + _format.format(error.Value / length) + " (" + pct + "%)";
  }

  static boolean test(Layer[] clones, Data data, int n, Error error) {
    clones[0]._a = data._images;
    clones[0]._off = n * PIXELS;

    for( int i = 1; i < clones.length; i++ )
      clones[i].fprop(0, clones[i]._a.length);

    float[] out = clones[clones.length - 1]._a;
    for( int i = 0; i < out.length; i++ ) {
      float t = i == data._labels[n] ? 1 : 0;
      float d = t - out[i];
      error.Value += d * d;
    }

    float max = Float.MIN_VALUE;
    int idx = -1;
    for( int i = 0; i < out.length; i++ ) {
      if( out[i] > max ) {
        max = out[i];
        idx = i;
      }
    }
    return idx == data._labels[n];
  }

  static float convert(byte b) {
    return (b & 0xff) / 255f;
  }

  static void convert(byte[] bytes, float[] floats) {
    assert bytes.length == floats.length;
    for( int i = 0; i < floats.length; i++ )
      floats[i] = convert(bytes[i]);
  }

  static Data loadData(String images, String labels) {
    DataInputStream imagesBuf = null, labelsBuf = null;
    try {
      imagesBuf = new DataInputStream(new GZIPInputStream(new FileInputStream(new File(images))));
      labelsBuf = new DataInputStream(new GZIPInputStream(new FileInputStream(new File(labels))));

      imagesBuf.readInt(); // Magic
      int count = imagesBuf.readInt();
      labelsBuf.readInt(); // Magic
      if( count != labelsBuf.readInt() ) throw new RuntimeException();
      imagesBuf.readInt(); // Rows
      imagesBuf.readInt(); // Cols

      System.out.println("Count=" + count);
      byte[][] rawI = new byte[count][PIXELS];
      byte[] rawL = new byte[count];
      for( int n = 0; n < count; n++ ) {
        imagesBuf.readFully(rawI[n]);
        rawL[n] = labelsBuf.readByte();
      }

      int todo;
//      MersenneTwisterRNG rand = new MersenneTwisterRNG(MersenneTwisterRNG.SEEDS);
//      for( int i = 0; i < count; i++ ) {
//        int shuffle = rand.nextInt(count);
//        byte[] image = rawI[shuffle];
//        rawI[shuffle] = rawI[i];
//        rawI[i] = image;
//        byte label = rawL[shuffle];
//        rawL[shuffle] = rawL[i];
//        rawL[i] = label;
//      }

      Data data = new Data();
      data._images = new float[count * PIXELS];
      data._labels = rawL;
      for( int n = 0; n < count; n++ )
        for( int i = 0; i < PIXELS; i++ )
          data._images[n * PIXELS + i] = convert(rawI[n][i]);
      return data;
    } catch( Exception e ) {
      throw new RuntimeException(e);
    } finally {
      Utils.close(labelsBuf, imagesBuf);
    }
  }
}