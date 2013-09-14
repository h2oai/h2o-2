package hex;

import hex.Layer.FrameInput;
import hex.NeuralNet.Error;
import hex.NeuralNet.NeuralNetScore;

import java.text.DecimalFormat;

import water.*;
import water.fvec.Frame;
import water.util.Log;

public class MnistDist {
  static final DecimalFormat _format = new DecimalFormat("0.000");

  public static void main(String[] args) throws Exception {
    water.Boot.main(UserCode.class, "-beta");
  }

  public static class UserCode {
    public static void userMain(String[] args) throws Exception {
      try {
        Sandbox.localCloud(2, true, args);
        MnistDist test = new MnistDist();
        test.run();
      } catch( Throwable t ) {
        Log.info(t);
      }
    }
  }

  public void run() {
    Frame train = TestUtil.parseFrame("smalldata/mnist/train.csv.gz");
    Frame test = TestUtil.parseFrame("smalldata/mnist/test.csv.gz");
    train = Trainer.reChunk(train);

    for( int i = 0; i < train._vecs.length; i++ )
      train._vecs[i].min();
    for( int i = 0; i < test._vecs.length; i++ )
      test._vecs[i].min();

    for( int i = 0; i < train.firstReadable().nChunks(); i++ ) {
      Value v = train.firstReadable().chunkIdx(i);
      Log.info("chunk " + i + "," + v._key.home_node().index());
    }

    Layer[] ls = new Layer[3];
    ls[0] = new FrameInput(train);
    ls[0].init(null, 784, true);
    ls[1] = new Layer.Tanh();
    ls[2] = new Layer.Softmax();
    ls[1]._rate = .05f;
    ls[2]._rate = .02f;
    ls[1]._l2 = .0001f;
    ls[2]._l2 = .0001f;
    ls[1].init(ls[0], 500, true);
    ls[2].init(ls[1], 10, true);

    for( int i = 1; i < ls.length; i++ )
      ls[i].randomize();

//  final Trainer trainer = new Trainer.Direct(ls);
    //Trainer.Threaded trainer = new Trainer.Threaded(ls);
    //final Trainer trainer = new Trainer.MR(ls, 0);
    //Trainer.MRAsync trainer = new Trainer.MRAsync(ls, 0);
    Trainer.MR2 trainer = new Trainer.MR2(ls, 0);
//  final Trainer trainer = new Trainer.OpenCL(_ls);

    // Basic visualization of images and weights
//    JFrame frame = new JFrame("H2O");
//    frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
//    MnistCanvas canvas = new MnistCanvas(trainer);
//    frame.setContentPane(canvas.init());
//    frame.pack();
//    frame.setLocationRelativeTo(null);
//    frame.setVisible(true);

    trainer.start();

    long start = System.nanoTime();
    long lastTime = start;
    long lastItems = 0;
    for( ;; ) {
      try {
        Thread.sleep(3000);
      } catch( InterruptedException e ) {
        throw new RuntimeException(e);
      }

      Layer[] clones1 = Layer.clone(ls, train);
      Error trainE = NeuralNetScore.eval(clones1, NeuralNet.EVAL_ROW_COUNT);
      Layer[] clones2 = Layer.clone(ls, test);
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
      System.out.println(m);

      System.out.println("Counts: " + trainer._counts.toString());
    }
  }
}