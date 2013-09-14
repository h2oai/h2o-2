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

public class IrisMisc extends NeuralNetIrisTest {
  static final DecimalFormat _format = new DecimalFormat("0.000");

  public static void main(String[] args) throws Exception {
    water.Boot.main(UserCode.class, "-beta");
  }

  public static class UserCode {
    public static void userMain(String[] args) throws Exception {
      Sandbox.localCloud(1, true, args);
      final IrisMisc test1 = new IrisMisc();
      final IrisMisc test2 = new IrisMisc();

      Thread t1 = new Thread() {
        public void run() {
          test1.run();
        }
      };
      Thread t2 = new Thread() {
        public void run() {
          test2.run();
        }
      };

      t1.start();
//      t2.start();
      sync(test1, test2);
    }
  }

  public void run() {
    load();

    Layer[] ls = new Layer[3];
    ls[0] = new FrameInput(_train);
    ls[1] = new Layer.Tanh();
    ls[2] = new Softmax();
    ls[1]._rate = 0.99f;
    ls[2]._rate = 0.99f;
    ls[1]._l2 = .001f;
    ls[2]._l2 = .001f;
    ls[0].init(null, 4);
    ls[1].init(ls[0], 7);
    ls[2].init(ls[1], 3);
    for( int i = 1; i < ls.length; i++ )
      ls[i].randomize();

    final Trainer.Direct trainer = new Trainer.Direct(ls);
//    Trainer.Threaded trainer = new Trainer.Threaded(ls, 1000, 1);
    //final Trainer trainer = new Trainer.MR(ls, 0);
    //Trainer.MRAsync trainer = new Trainer.MRAsync(ls, 0);
    //Trainer.MR2 trainer = new Trainer.MR2(ls, 0);
//  final Trainer trainer = new Trainer.OpenCL(_ls);

    // Basic visualization of images and weights
//    JFrame frame = new JFrame("H2O");
//    frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
//    MnistCanvas canvas = new MnistCanvas(trainer);
//    frame.setContentPane(canvas.init());
//    frame.pack();
//    frame.setLocationRelativeTo(null);
//    frame.setVisible(true);

    //trainer.start();
    Input input = (Input) ls[0];
    for( int s = 0; s < 1000000; s++ ) {
      trainer.step();
      input.move();
    }
    Weights a = Weights.get(ls, true);
    eval("a", ls);

    for( int s = 0; s < 100000; s++ ) {
      trainer.step();
      input.move();
    }
    Weights b = Weights.get(ls, true);
    eval("b", ls);

    for( int s = 0; s < 100000; s++ ) {
      trainer.step();
      input.move();
    }
    Weights c = Weights.get(ls, true);
    eval("c", ls);

    b.set(ls);
    eval("b", ls);
    Weights w = Weights.get(ls, true);
    for( int y = 1; y < ls.length; y++ ) {
      for( int i = 0; i < ls[y]._w.length; i++ )
        w._ws[y][i] += b._ws[y][i] - a._ws[y][i];
      for( int i = 0; i < ls[y]._b.length; i++ )
        w._bs[y][i] += b._bs[y][i] - a._bs[y][i];
    }
    w.set(ls);
    eval("w", ls);

    Log.info("Done!");
    System.exit(0);
  }

  void eval(String tag, Layer[] ls) {
    Layer[] clones1 = Layer.clone(ls, _train);
    Error trainE = NeuralNetScore.eval(clones1, NeuralNet.EVAL_ROW_COUNT);
    Layer[] clones2 = Layer.clone(ls, _test);
    Error testE = NeuralNetScore.eval(clones2, NeuralNet.EVAL_ROW_COUNT);
    Log.info(tag + ": train: " + trainE + ", test: " + testE);
  }

  private static void sync(IrisMisc test1, IrisMisc test2) {
  }
}