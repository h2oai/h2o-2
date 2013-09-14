package hex;

import hex.Layer.FrameInput;
import hex.Layer.Input;
import hex.Layer.Softmax;
import hex.NeuralNet.Error;
import hex.NeuralNet.NeuralNetScore;
import hex.NeuralNet.Weights;

import java.text.DecimalFormat;

import water.Sandbox;
import water.util.Log;

public class IrisDist extends NeuralNetIrisTest {
  static final DecimalFormat _format = new DecimalFormat("0.000");

  public static void main(String[] args) throws Exception {
    water.Boot.main(UserCode.class, "-beta");
  }

  public static class UserCode {
    public static void userMain(String[] args) throws Exception {
      Sandbox.localCloud(2, true, args);
      IrisDist test = new IrisDist();
      test.run();
    }
  }

  public void run() {
    load();
    _train = Trainer.reChunk(_train);

    Layer[] ls = new Layer[3];
    ls[0] = new FrameInput(_train);
    ls[1] = new Layer.Tanh();
    ls[2] = new Softmax();
    ls[1]._rate = 0.01f;
    ls[2]._rate = 0.01f;
    ls[1]._l2 = .001f;
    ls[2]._l2 = .001f;
    ls[0].init(null, 4);
    ls[1].init(ls[0], 7);
    ls[2].init(ls[1], 3);
    for( int i = 1; i < ls.length; i++ )
      ls[i].randomize();

//    final Trainer.Direct trainer = new Trainer.Direct(ls);
//    Trainer.Threaded trainer = new Trainer.Threaded(ls, 1000, 1);
    //final Trainer trainer = new Trainer.MR(ls, 0);
    //Trainer.MRAsync trainer = new Trainer.MRAsync(ls, 0);
    Trainer.MR2 trainer = new Trainer.MR2(ls, 0);
//  final Trainer trainer = new Trainer.OpenCL(_ls);
    trainer.start();


    long start = System.nanoTime();
    long lastTime = start;
    long lastItems = 0;
    for( ;; ) {
      try {
        Thread.sleep(1000);
      } catch( InterruptedException e ) {
        throw new RuntimeException(e);
      }

      Layer[] clones1 = Layer.clone(ls, _train);
      Error trainE = NeuralNetScore.eval(clones1, NeuralNet.EVAL_ROW_COUNT);
      Layer[] clones2 = Layer.clone(ls, _test);
      Error testE = NeuralNetScore.eval(clones2, NeuralNet.EVAL_ROW_COUNT);
      long time = System.nanoTime();
      double delta = (time - lastTime) / 1e9;
      double total = (time - start) / 1e9;
      lastTime = time;
      long steps = trainer.steps();
      int ps = (int) ((steps - lastItems) / delta);

      lastItems = steps;
      String m = _format.format(total) + "s, " + steps + " steps (" + (ps) + "/s) ";
      m += "train: " + trainE + ", test: " + testE;
      Log.info(m);
    }
  }
}