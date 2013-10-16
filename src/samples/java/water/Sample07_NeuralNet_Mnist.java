package water;

import hex.*;
import hex.Layer.Tanh;
import hex.Layer.VecSoftmax;
import hex.Layer.VecsInput;
import hex.NeuralNet.Error;
import hex.NeuralNet.NeuralNetTrain;
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
  public static void main(String[] args) throws Exception {
    water.Boot.main(UserCode.class, "-beta");
  }

  public static class UserCode {
    public static void userMain(String[] args) throws Exception {
      H2O.main(args);

      Frame train = TestUtil.parseFrame("smalldata/mnist/train.csv.gz");
      Frame test = TestUtil.parseFrame("smalldata/mnist/test.csv.gz");
      train(train, test);
    }
  }

  public static class Job extends NeuralNetTrain {
    @Override protected Model newModel() {
      return new Model();
    }
  }

  public static class Model extends NeuralNet {
    @Override public Layer[] build(Vec[] vecs, Vec response) {
      Layer[] ls = new Layer[3];
      ls[0] = new VecsInput(vecs);
      ls[1] = new Tanh(500);
      ls[2] = new VecSoftmax(response);
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
  }

  public static void train(Frame train, Frame test) {
    Job job = new Job();
    NeuralNet model = (NeuralNet) job._model;
    job.source = train;
    job.response = train.vecs()[train.vecs().length - 1];
    job.start();

    // Monitor training
    test = model.adapt(test, false, true)[0];
    long start = System.nanoTime();
    for( ;; ) {
      try {
        Thread.sleep(2000);
      } catch( InterruptedException e ) {
        throw new RuntimeException(e);
      }

      Error trErr = model.evalAdapted(train, NeuralNet.EVAL_ROW_COUNT, null);
      Error tsErr = model.evalAdapted(test, NeuralNet.EVAL_ROW_COUNT, null);

      double time = (System.nanoTime() - start) / 1e9;
      String text = (int) time + "s, " + model.items + " steps (" + (model.items_per_second) + "/s) ";
      text += "train: " + trErr;
      text += ", test: " + tsErr;
      System.out.println(text);
    }
  }

  // Was used to shuffle & convert to CSV
  static final int PIXELS = 784;

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
