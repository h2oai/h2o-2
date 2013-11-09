package samples;

import hex.*;
import hex.Layer.Tanh;
import hex.Layer.VecSoftmax;
import hex.Layer.VecsInput;
import hex.NeuralNet.Error;
import hex.rng.MersenneTwisterRNG;

import java.io.*;
import java.util.UUID;
import java.util.zip.GZIPInputStream;

import water.Job;
import water.TestUtil;
import water.fvec.*;
import water.util.Utils;

/**
 * Runs a neural network on the MNIST dataset.
 */
public class NeuralNetMnist extends Job {
  public static void main(String[] args) throws Exception {
    samples.launchers.CloudLocal.launch(1, NeuralNetMnist.class);
    // samples.launchers.CloudProcess.launch(4, NeuralNetMnist.class);
    // samples.launchers.CloudRemote.launchDefaultIPs(NeuralNetMnist.class);
    // samples.launchers.CloudConnect.launch("localhost:54321", NeuralNetMnist.class);
  }

  protected Vec[] train, test;

  public void load() {
    train = TestUtil.parseFromH2OFolder("smalldata/mnist/train.csv.gz").vecs();
    test = TestUtil.parseFromH2OFolder("smalldata/mnist/test.csv.gz").vecs();
    NeuralNet.reChunk(train);
  }

  protected Layer[] build(Vec[] data, Vec labels, VecsInput inputStats, VecSoftmax outputStats) {
    Layer[] ls = new Layer[3];
    ls[0] = new VecsInput(data, inputStats);
    ls[1] = new Tanh(500);
    ls[2] = new VecSoftmax(labels, outputStats);
    for( int i = 0; i < ls.length; i++ ) {
      ls[i].rate = .005f;
      ls[i].rate_annealing = 1 / 1e6f;
      ls[i].l2 = .001f;
      ls[i].init(ls, i);
    }
    return ls;
  }

  protected Trainer startTraining(Layer[] ls) {
    Trainer trainer = new Trainer.MapReduce(ls, 0, self());
    //Trainer trainer = new Trainer.Direct(ls);
    trainer.start();
    return trainer;
  }

  @Override protected void exec() {
    load();
    System.out.println("Loaded data");

    // Labels are on last column for this dataset
    Vec trainLabels = train[train.length - 1];
    train = Utils.remove(train, train.length - 1);
    Vec testLabels = test[test.length - 1];
    test = Utils.remove(test, test.length - 1);

    Layer[] ls = build(train, trainLabels, null, null);
    Trainer trainer = startTraining(ls);

    // Monitor training
    long start = System.nanoTime();
    while( !cancelled() ) {
      try {
        Thread.sleep(2000);
      } catch( InterruptedException e ) {
        throw new RuntimeException(e);
      }

      double time = (System.nanoTime() - start) / 1e9;
      long samples = trainer.samples();
      int ps = (int) (samples / time);
      String text = (int) time + "s, " + samples + " samples (" + (ps) + "/s) ";

      // Build separate nets for scoring purposes, use same normalization stats as for training
      Layer[] temp = build(train, trainLabels, (VecsInput) ls[0], (VecSoftmax) ls[ls.length - 1]);
      Layer.shareWeights(ls, temp);
      Error error = NeuralNet.eval(temp, NeuralNet.EVAL_ROW_COUNT, null);
      text += "train: " + error;

      temp = build(test, testLabels, (VecsInput) ls[0], (VecSoftmax) ls[ls.length - 1]);
      Layer.shareWeights(ls, temp);
      error = NeuralNet.eval(temp, NeuralNet.EVAL_ROW_COUNT, null);
      text += ", test: " + error;
      text += ", rates: ";
      for( int i = 1; i < ls.length; i++ )
        text += String.format("%.3g", ls[i].rate(samples)) + ", ";

      System.out.println(text);
    }
  }

  // Remaining code was used to shuffle & convert to CSV

  public static final int PIXELS = 784;

  static void csv() throws Exception {
    csv("../smalldata/mnist/train.csv", "train-images-idx3-ubyte.gz", "train-labels-idx1-ubyte.gz");
    csv("../smalldata/mnist/test.csv", "t10k-images-idx3-ubyte.gz", "t10k-labels-idx1-ubyte.gz");
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
