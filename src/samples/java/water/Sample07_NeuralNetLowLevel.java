package water;

import hex.*;
import hex.Layer.Tanh;
import hex.Layer.VecSoftmax;
import hex.Layer.VecsInput;
import hex.NeuralNet.Error;
import water.fvec.Vec;
import water.util.Utils;

/**
 * Runs a neural network on the MNIST dataset.
 */
public class Sample07_NeuralNetLowLevel {
  public static final int PIXELS = 784;

  public static void main(String[] args) throws Exception {
    water.Boot.main(UserCode.class, "-beta");
  }

  public static class UserCode {
    public static void userMain(String[] args) throws Exception {
      H2O.main(args);
      new Sample07_NeuralNetLowLevel().run();
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
    trainLabels.asEnum();
    train = Utils.remove(train, train.length - 1);
    Vec testLabels = test[test.length - 1];
    test = Utils.remove(test, test.length - 1);

    // Build net and start training
    Layer[] ls = build(train, trainLabels, null);
    Trainer trainer = new Trainer.MapReduce(ls);
    trainer.start();

    // Monitor training
    long start = System.nanoTime();
    for( ;; ) {
      try {
        Thread.sleep(2000);
      } catch( InterruptedException e ) {
        throw new RuntimeException(e);
      }

      double time = (System.nanoTime() - start) / 1e9;
      long steps = trainer.items();
      int ps = (int) (steps / time);
      String text = (int) time + "s, " + steps + " steps (" + (ps) + "/s) ";

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
}
