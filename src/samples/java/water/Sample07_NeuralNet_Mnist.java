package water;

import hex.*;
import hex.Layer.FrameInput;
import hex.Layer.Input;
import hex.NeuralNet.Error;
import hex.NeuralNet.NeuralNetScore;
import hex.rng.MersenneTwisterRNG;

import java.io.*;
import java.util.UUID;
import java.util.zip.GZIPInputStream;

import water.fvec.*;
import water.util.Utils;

/**
 * Runs a neural network on the MNIST dataset.
 */
public class Sample07_NeuralNet_Mnist {
  public static final int PIXELS = 784;

  public static void main(String[] args) throws Exception {
    water.Boot.main(UserCode.class, "-beta");
  }

  public static class UserCode {
    public static void userMain(String[] args) throws Exception {
      H2O.main(args);
      new Sample07_NeuralNet_Mnist().run();
    }
  }

  Layer[] build(Input input) {
    Layer[] ls = new Layer[3];
    ls[0] = input;
    ls[1] = new Layer.Tanh();
    ls[2] = new Layer.Softmax();
    ls[1]._rate = .05f;
    ls[2]._rate = .02f;
    ls[1]._l2 = .0001f;
    ls[2]._l2 = .0001f;
    ls[1]._rateAnnealing = 1 / 2e6f;
    ls[2]._rateAnnealing = 1 / 2e6f;
    ls[1].init(ls[0], 500);
    ls[2].init(ls[1], 10);

    for( int i = 1; i < ls.length; i++ )
      ls[i].randomize();
    return ls;
  }

  public void run() {
    Frame trainFrame = TestUtil.parseFrame("smalldata/mnist/train.csv.gz");
    Frame testFrame = TestUtil.parseFrame("smalldata/mnist/test.csv.gz");
    trainFrame = NeuralNet.reChunk(trainFrame);

    // Use train normalization stats for both frames
    FrameInput train = new FrameInput(trainFrame);
    FrameInput test = new FrameInput(testFrame, train._means, train._sigmas);
    train.init(null, PIXELS);
    test.init(null, PIXELS);

    Layer[] ls = build(train);
    final Trainer trainer = new Trainer.MapReduce(ls);
    trainer.start();

    long start = System.nanoTime();
    long lastTime = start;
    long lastItems = 0;
    for( ;; ) {
      try {
        Thread.sleep(2000);
      } catch( InterruptedException e ) {
        throw new RuntimeException(e);
      }

      Layer[] clones1 = Layer.clone(ls, train, 0);
      Error trainE = NeuralNetScore.eval(clones1, NeuralNet.EVAL_ROW_COUNT, null);
      Layer[] clones2 = Layer.clone(ls, test, 0);
      Error testE = NeuralNetScore.eval(clones2, NeuralNet.EVAL_ROW_COUNT, null);
      long time = System.nanoTime();
      double delta = (time - lastTime) / 1e9;
      double total = (time - start) / 1e9;
      lastTime = time;
      long steps = trainer.items();
      int ps = (int) ((steps - lastItems) / delta);

      lastItems = steps;
      String m = (int) total + "s, " + steps + " steps (" + (ps) + "/s) ";
      m += "train: " + trainE + ", test: " + testE;
      System.out.println(m);
    }
  }

  // Was used to shuffle & convert to CSV

  static void csv() throws Exception {
    csv("smalldata/mnist/train.csv", "train-images-idx3-ubyte.gz", "train-labels-idx1-ubyte.gz");
    csv("smalldata/mnist/test.csv", "t10k-images-idx3-ubyte.gz", "t10k-labels-idx1-ubyte.gz");
  }

  static void csv(String dest, String images, String labels) throws Exception {
    DataInputStream imagesBuf = new DataInputStream(new GZIPInputStream(new FileInputStream(new File(images))));
    DataInputStream labelsBuf = new DataInputStream(new GZIPInputStream(new FileInputStream(new File(labels))));

    imagesBuf.readInt(); // Magic
    int count = imagesBuf.readInt();
    labelsBuf.readInt(); // Magic
    assert count == labelsBuf.readInt();
    imagesBuf.readInt(); // Rows
    imagesBuf.readInt(); // Cols

    System.out.println("Count=" + count);
    count = 500 * 1000;
    byte[][] rawI = new byte[count][PIXELS];
    byte[] rawL = new byte[count];
    for( int n = 0; n < count; n++ ) {
      imagesBuf.readFully(rawI[n]);
      rawL[n] = labelsBuf.readByte();
    }

    MersenneTwisterRNG rand = new MersenneTwisterRNG(MersenneTwisterRNG.SEEDS);
    for( int n = count - 1; n >= 0; n-- ) {
      int shuffle = rand.nextInt(n + 1);
      byte[] image = rawI[shuffle];
      rawI[shuffle] = rawI[n];
      rawI[n] = image;
      byte label = rawL[shuffle];
      rawL[shuffle] = rawL[n];
      rawL[n] = label;
    }

    Vec[] vecs = new Vec[PIXELS + 1];
    NewChunk[] chunks = new NewChunk[vecs.length];
    for( int v = 0; v < vecs.length; v++ ) {
      vecs[v] = new AppendableVec(UUID.randomUUID().toString());
      chunks[v] = new NewChunk(vecs[v], 0);
    }
    for( int n = 0; n < count; n++ ) {
      for( int v = 0; v < vecs.length - 1; v++ )
        chunks[v].addNum(rawI[n][v] & 0xff, 0);
      chunks[chunks.length - 1].addNum(rawL[n], 0);
    }
    for( int v = 0; v < vecs.length; v++ ) {
      chunks[v].close(0, null);
      vecs[v] = ((AppendableVec) vecs[v]).close(null);
    }

    Frame frame = new Frame(null, vecs);
    Utils.writeFileAndClose(new File(dest), frame.toCSV(false));
    imagesBuf.close();
    labelsBuf.close();
  }
}
