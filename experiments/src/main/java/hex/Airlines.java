package hex;

import hex.Layer.VecsInput;
import hex.NeuralNet.Error;
import hex.NeuralNet.NeuralNetTrain;
import water.*;
import water.fvec.Frame;

public class Airlines {
  public static void main(String[] args) throws Exception {
    water.Boot.main(UserCode.class, "-beta");
  }

  public static class UserCode {
    public static void userMain(String[] args) throws Exception {
      H2O.main(args);
      new Airlines().run();
    }
  }

  public void run() {
    // Load data
    Sandbox.airlines();
    Frame train = UKV.get(Key.make("train.hex"));

    NeuralNetTrain job = new NeuralNetTrain();
    NeuralNet model = (NeuralNet) job._model;
    job.source = train;
    job.response = train.vecs()[train.vecs().length - 1];
    job.start();

    // Monitor training
    long start = System.nanoTime();
    for( ;; ) {
      try {
        Thread.sleep(2000);
      } catch( InterruptedException e ) {
        throw new RuntimeException(e);
      }

      long time = System.nanoTime();
      double total = (time - start) / 1e9;
      String text = (int) total + "s, " + model.items + " steps (" + (model.items_per_second) + "/s) ";

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
