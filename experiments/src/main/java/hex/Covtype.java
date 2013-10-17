package hex;

import hex.Layer.Tanh;
import hex.Layer.VecSoftmax;
import hex.Layer.VecsInput;
import hex.NeuralNet.Error;
import water.H2O;
import water.TestUtil;
import water.api.FrameSplit;
import water.fvec.Frame;
import water.fvec.Vec;
import water.util.Utils;

public class Covtype {
  public static void main(String[] args) throws Exception {
    water.Boot.main(UserCode.class, "-beta");
  }

  public static class UserCode {
    public static void userMain(String[] args) throws Exception {
      H2O.main(args);
      new Covtype().run();
    }
  }

  public Layer[] build(Vec[] data, Vec labels, VecsInput stats) {
    Layer[] ls = new Layer[3];
    ls[0] = new VecsInput(data, stats);
    ls[1] = new Tanh(1000);
    ls[2] = new VecSoftmax(labels);
    ls[1]._rate = .05f;
    ls[2]._rate = .02f;
    ls[1]._l2 = .0001f;
    ls[2]._l2 = .0001f;
    for( int i = 0; i < ls.length; i++ )
      ls[i].init(ls, i);
    return ls;
  }

  public void run() {
    // Load data
    //Frame frame = TestUtil.parseFrame("smalldata/covtype/covtype.20k.data");
    Frame frame = TestUtil.parseFrame("smalldata/cars.csv");
    Frame[] frames = new FrameSplit().splitFrame(frame, new double[] { .8, .1, .1 });
    Vec[] train = frames[0].vecs();
    Vec[] valid = frames[1].vecs();
    Vec[] test_ = frames[2].vecs();
    NeuralNet.reChunk(train);

    // Labels are on last column for this dataset
    Vec trainLabels = train[train.length - 1];
    Vec validLabels = valid[valid.length - 1];
    Vec test_Labels = test_[test_.length - 1];
    train = Utils.remove(train, train.length - 1);
    valid = Utils.remove(valid, valid.length - 1);
    test_ = Utils.remove(test_, test_.length - 1);

    // Test is classification so make sure number is interpreted as enum
    trainLabels.asEnum();
    validLabels.asEnum();
    test_Labels.asEnum();

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

      temp = build(valid, validLabels, (VecsInput) ls[0]);
      Layer.copyWeights(ls, temp);
      error = NeuralNet.eval(temp, NeuralNet.EVAL_ROW_COUNT, null);
      text += ", valid: " + error;

      System.out.println(text);
    }
  }
}
