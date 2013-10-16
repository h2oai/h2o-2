package hex;

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
    model.rate = .0001;
    model.l2 = 0;
    job.start();

    // Monitor training
    Frame test = model.adapt((Frame) UKV.get(Key.make("test.hex")), false, true)[0];
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
}
