package hex;

import hex.Layer.Input;
import hex.Trainer.ParallelTrainers;
import hex.rng.MersenneTwisterRNG;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.*;
import java.util.*;

import javax.swing.JFrame;

import water.util.Log;
import water.util.Utils;

public class Mnist8m extends NeuralNetMnistTest {
  static final String IMAGES_SOURCE = "../datasets/mnist8m/train8m-images-idx3-ubyte";
  static final String LABELS_SOURCE = "../datasets/mnist8m/train8m-labels-idx1-ubyte";
  static final String IMAGES_SHUFFLED = "../mnist8m/shuffled-images";
  static final String LABELS_SHUFFLED = "../mnist8m/shuffled-labels";
  static final int COUNT = 8100000;
  static ByteBuffer[] _images = new ByteBuffer[16]; // Too big for ByteBuffer
  static final byte[] _labels = new byte[COUNT];
  static final long PAGE_SIZE = COUNT / _images.length * (long) PIXELS;

  public static void main(String[] args) throws Exception {
    load();
    // One shot
    // normalize();
    //TODO try subtract mean for each sample before col norm
    Mnist8m mnist = new Mnist8m();
    mnist.run();
  }

  public static class Train8mInput extends Input {
    public Train8mInput() {
      super(PIXELS);
      _count = COUNT;
    }

    @Override int label() {
      return _labels[(int) _n];
    }

    @Override void fprop() {
      long offset = _n * PIXELS;
      int page = (int) (offset / PAGE_SIZE);
      int indx = (int) (offset % PAGE_SIZE);
      ByteBuffer buffer = _images[page];
      for( int i = 0; i < _a.length; i++ ) {
        double d = convert(buffer.get(indx + i));
        d -= Mnist8mNorm.MEANS[i];
        d = Mnist8mNorm.SIGMS[i] > 1e-4 ? d / Mnist8mNorm.SIGMS[i] : d;
        _a[i] = (float) d;
      }
    }
  }

  public static class TestInput extends Input {
    MnistInput _raw = loadZip(PATH + "t10k-images-idx3-ubyte.gz", PATH + "t10k-labels-idx1-ubyte.gz");

    public TestInput() {
      super(PIXELS);
      _count = _raw._labels.length;
    }

    @Override int label() {
      return _raw._labels[(int) _n];
    }

    @Override void fprop() {
      for( int i = 0; i < _a.length; i++ ) {
        double d = _raw._images[(int) _n * PIXELS + i];
        d -= Mnist8mNorm.MEANS[i];
        d = Mnist8mNorm.SIGMS[i] > 1e-4 ? d / Mnist8mNorm.SIGMS[i] : d;
        _a[i] = (float) d;
      }
    }
  }

  @Override public void run() throws Exception {
    String f = "smalldata/mnist70k/";
    _test =

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