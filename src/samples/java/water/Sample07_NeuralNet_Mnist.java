package water;

import hex.*;
import hex.Layer.Tanh;
import hex.Layer.VecSoftmax;
import hex.Layer.VecsInput;
import hex.NeuralNet.Error;
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

  public Layer[] build(Vec[] data, Vec labels, VecsInput stats) {
    Layer[] ls = new Layer[3];
    ls[0] = new VecsInput(data, stats);
    ls[1] = new Tanh(500);
    ls[2] = new VecSoftmax(labels);
    ls[1]._rate = .05f;
    ls[2]._rate = .02f;
    ls[1]._l2 = .0001f;
    ls[2]._l2 = .0001f;
    ls[1]._rateAnnealing = 1 / 2e6f;
    ls[2]._rateAnnealing = 1 / 2e6f;
    for( int i = 0; i < ls.length; i++ )
      ls[i].init(ls, i);
    return ls;
  }

  public void run() {
    // Load data
    Vec[] train = TestUtil.parseFrame("smalldata/mnist/train.csv.gz").vecs();
    Vec[] test = TestUtil.parseFrame("smalldata/mnist/test.csv.gz").vecs();
    NeuralNet.reChunk(train);

    // Labels are on last column for this dataset
    Vec trainLabels = train[train.length - 1];
    Vec testLabels = test[test.length - 1];
    train = Utils.remove(train, train.length - 1);
    test = Utils.remove(test, test.length - 1);
    trainLabels.asEnum();
    testLabels.asEnum();

    // Build net and start training
    Layer[] ls = build(train, trainLabels, null);
    Trainer trainer = new Trainer.MapReduce(ls);
    trainer.start();

    // Monitor training
    long start = System.nanoTime();
    long lastTime = start;
    long lastItems = 0;
    for( ;; ) {
      try {
        Thread.sleep(2000);
      } catch( InterruptedException e ) {
        throw new RuntimeException(e);
      }

      long time = System.nanoTime();
      double delta = (time - lastTime) / 1e9;
      double total = (time - start) / 1e9;
      long steps = trainer.items();
      int ps = (int) ((steps - lastItems) / delta);
      String text = (int) total + "s, " + steps + " steps (" + (ps) + "/s) ";
      lastTime = time;
      lastItems = steps;

      // Build separate nets for scoring purposes, use same normalization stats as for training
      Layer[] temp = build(train, trainLabels, (VecsInput) ls[0]);
      Layer.copyWeights(ls, temp);
      Error error = NeuralNet.eval(temp, NeuralNet.EVAL_ROW_COUNT, null);
      text += "train: " + error;

      temp = build(test, testLabels, (VecsInput) ls[0]);
      Layer.copyWeights(ls, temp);
      error = NeuralNet.eval(temp, NeuralNet.EVAL_ROW_COUNT, null);
      text += ", test: " + error;

      System.out.println(text);
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
