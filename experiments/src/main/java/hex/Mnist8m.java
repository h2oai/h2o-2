package hex;

import hex.Layer.Input;
import hex.rng.MersenneTwisterRNG;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.*;
import java.util.*;

import javax.swing.JFrame;

import water.Sample07_NeuralNet_Mnist;
import water.TestUtil;
import water.fvec.Frame;
import water.util.Log;
import water.util.Utils;

public class Mnist8m extends Sample07_NeuralNet_Mnist {
  static final String IMAGES_SOURCE = "../datasets/mnist8m/train8m-images-idx3-ubyte";
  static final String LABELS_SOURCE = "../datasets/mnist8m/train8m-labels-idx1-ubyte";
  static final String IMAGES_SHUFFLED = "../mnist8m/shuffled-images";
  static final String LABELS_SHUFFLED = "../mnist8m/shuffled-labels";
  static final int COUNT = 8100000;
  static ByteBuffer[] _images = new ByteBuffer[16]; // Too big for ByteBuffer
  static final byte[] _labels = new byte[COUNT];
  static final long PAGE_SIZE = COUNT / _images.length * PIXELS;

  public static void main(String[] args) throws Exception {
    // /home/0xdiag/home-0xdiag-datasets/mnist/mnist8m.csv
    load();
    // One shot
    // normalize();
    //TODO try subtract mean for each sample before col norm
    Mnist8m mnist = new Mnist8m();
    mnist.run();
  }

  public static class Train8mInput extends Input {
    public Train8mInput() {
      _len = COUNT;
    }

    @Override int label() {
      return _labels[(int) _row];
    }

    @Override void fprop() {
      long offset = _row * PIXELS;
      int page = (int) (offset / PAGE_SIZE);
      int indx = (int) (offset % PAGE_SIZE);
      ByteBuffer buffer = _images[page];
      for( int i = 0; i < _a.length; i++ ) {
        double d = buffer.get(indx + i) & 0xff / 256;
        d -= Mnist8mNorm.MEANS[i];
        d = Mnist8mNorm.SIGMS[i] > 1e-4 ? d / Mnist8mNorm.SIGMS[i] : d;
        _a[i] = (float) d;
      }
    }
  }

  todo testinput with same normalization

  public void run() throws Exception {
    boolean load = false;
    boolean deep = false;
    {
      if( deep ) {
        _ls = new Layer[5];
        _ls[0] = new Train8mInput();
        _ls[1] = new Layer.Rectifier(_ls[0], 1000);
        _ls[2] = new Layer.Rectifier(_ls[1], 1000);
        _ls[3] = new Layer.Rectifier(_ls[2], 1000);
        _ls[4] = new Layer.Rectifier(_ls[3], 10);
      } else {
        _ls = new Layer[3];
        _ls[0] = new Train8mInput();
        _ls[1] = new Layer.Tanh(_ls[0], 1000);
        _ls[1]._rate = 0.001f;
        _ls[2] = new Layer.Tanh(_ls[1], 10);
        _ls[2]._rate = 0.00005f;
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

    if( deep ) {
      for( int i = 0; i < _ls.length; i++ ) {
        System.out.println("Training level " + i);
        long time = System.nanoTime();
        //preTrain(i);
        System.out.println((int) ((System.nanoTime() - time) / 1e6) + " ms");
      }
    }

    if( pretrain ) {
      for( int i = 0; i < _ls.length; i++ ) {
        System.out.println("Training level " + i);
        long time = System.nanoTime();
        preTrain(_trainer, i);
        System.out.println((int) ((System.nanoTime() - time) / 1e6) + " ms");
      }
    }

    ParallelTrainers trainer = new ParallelTrainers(_ls);
    trainer.start();
    //Trainer trainer = new Trainer.Direct(_ls, _labels);
    monitor(trainer);

//    long time = System.nanoTime();
//    for( int i = 0; i < _rbms.length; i++ ) {
//      String json = Utils.json(_rbms[i]);
//      Utils.writeFile(new File("rbm" + i + ".json"), json);
//    }
//    System.out.println("save: " + (int) ((System.nanoTime() - time) / 1e6) + " ms");
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

  void monitor(Trainer trainer) {
    // Basic visualization of images and weights
    JFrame frame = new JFrame("H2O");
    frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
    MnistCanvas canvas = new MnistCanvas(trainer, new Train8mInput());
    frame.setContentPane(canvas.init());
    frame.pack();
    frame.setLocationRelativeTo(null);
    frame.setVisible(true);

    long start = System.nanoTime();
    long lastTime = start;
    int n = 0, lastItems = 0;
    for( ;; ) {
      try {
        Thread.sleep(3000);
      } catch( InterruptedException e ) {
        throw new RuntimeException(e);
      }

      String train = test(_train, 100);
      String test = test(_test, 100);
      long time = System.nanoTime();
      int items = trainer._count.get();
      int ps = (int) ((items - lastItems) * (long) 1e9 / (time - lastTime));
      lastTime = time;
      lastItems = items;
      String m = Log.padRight((int) ((time - start) / 1e9) + "s ", 6);
      m += Log.padRight("" + trainer._count.get(), 8) + " " + Log.padRight(ps + "/s", 7);
      m += ", train: " + train + ", test: " + test;

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
      m += ", gw: " + sqr + " (" + (zeros * 100 / layer._gw.length) + "% 0)";
      sqr = 0;
      for( int o = 0; o < layer._a.length; o++ ) {
        for( int i = 0; i < layer._in._a.length; i++ ) {
          float d = layer._w[o * layer._in._a.length + i];
          sqr += d * d;
        }
      }
      m += ", w: " + sqr;
      sqr = 0;
      for( int o = 0; o < layer._a.length; o++ ) {
        float d = layer._a[o];
        sqr += d * d;
      }
      System.out.println(m + ", a: " + sqr);

      if( n != 0 && n % 10 == 0 ) {
        System.out.println("All: " + test(_test, _test._labels.length));
      }
    }
  }

  static void shuffle() throws Exception {
    RandomAccessFile images = new RandomAccessFile(IMAGES_SOURCE, "r");
    RandomAccessFile labels = new RandomAccessFile(LABELS_SOURCE, "r");
    images.readInt(); // Magic
    int count = images.readInt(); // Count
    images.readInt(); // Rows
    images.readInt(); // Cols
    labels.readInt(); // Magic
    int count2 = labels.readInt();
    assert count == _labels.length && count2 == _labels.length;

    int[] indexes = new int[count];
    for( int i = 0; i < count; i++ )
      indexes[i] = i;
    MersenneTwisterRNG rand = new MersenneTwisterRNG(MersenneTwisterRNG.SEEDS);
    for( int i = 0; i < count; i++ ) {
      int shuffle = rand.nextInt(count);
      int index = indexes[shuffle];
      indexes[shuffle] = indexes[i];
      indexes[i] = index;
    }

    File f = new File(IMAGES_SHUFFLED);
    f.getParentFile().mkdirs();
    f.createNewFile();
    RandomAccessFile imagesShuffled = new RandomAccessFile(f, "rw");
    f = new File(LABELS_SHUFFLED);
    f.getParentFile().mkdirs();
    f.createNewFile();
    RandomAccessFile labelsShuffled = new RandomAccessFile(f, "rw");
    byte[] buffer = new byte[PIXELS];
    long imagesStart = images.getFilePointer();
    long labelsStart = labels.getFilePointer();
    for( int n = 0; n < count; n++ ) {
      images.seek(imagesStart + indexes[n] * (long) PIXELS);
      labels.seek(labelsStart + indexes[n]);
      images.readFully(buffer);
      byte label = labels.readByte();
      imagesShuffled.write(buffer);
      labelsShuffled.write(label);
    }
    Utils.close(images, labels, imagesShuffled, labelsShuffled);
  }

  static void load() throws Exception {
    if( !new File(IMAGES_SHUFFLED).exists() )
      shuffle();

    FileSystem fs = FileSystems.getDefault();
    Set<StandardOpenOption> options = EnumSet.of(StandardOpenOption.READ);
    FileChannel images = FileChannel.open(fs.getPath(IMAGES_SHUFFLED), options);
    for( int i = 0; i < _images.length; i++ )
      _images[i] = images.map(MapMode.READ_ONLY, i * PAGE_SIZE, PAGE_SIZE);

    RandomAccessFile labels = new RandomAccessFile(LABELS_SHUFFLED, "r");
    for( int n = 0; n < _labels.length; n++ )
      _labels[n] = labels.readByte();
    Utils.close(labels);
  }

  static void normalize() throws Exception {
    Train8mInput input = new Train8mInput();

    double[] means = new double[PIXELS];
    for( input._n = 0; input._n < COUNT; input._n++ ) {
      input.fprop(0, input._a.length);
      for( int i = 0; i < input._a.length; i++ )
        means[i] += input._a[i];
    }
    for( int i = 0; i < means.length; i++ )
      means[i] /= COUNT;

    double[] sigmas = new double[PIXELS];
    for( input._n = 0; input._n < COUNT; input._n++ ) {
      input.fprop(0, input._a.length);
      for( int i = 0; i < input._a.length; i++ ) {
        double d = input._a[i] - means[i];
        sigmas[i] += d * d;
      }
    }
    for( int i = 0; i < means.length; i++ )
      sigmas[i] = Math.sqrt(sigmas[i] / (COUNT - 1));

    System.out.println(Arrays.toString(means));
    System.out.println(Arrays.toString(sigmas));
  }
}