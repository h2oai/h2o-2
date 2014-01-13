package hex;

import hex.Layer.VecSoftmax;
import hex.Layer.VecsInput;
import hex.NeuralNet.Errors;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import water.JUnitRunner.Nightly;
import water.JUnitRunnerDebug;
import water.Key;
import water.TestUtil;
import water.deploy.VM;
import water.fvec.Frame;
import water.fvec.Vec;
import water.util.Utils;

import java.io.File;

@Nightly
@Ignore
public class NeuralNetSpiralsTest extends TestUtil {
  @BeforeClass public static void stall() {
    stall_till_cloudsize(JUnitRunnerDebug.NODES);
  }

  @Test public void run() throws Exception {
    File file = new File(VM.h2oFolder(), "smalldata/neural/two_spiral.data");
    Key key = Key.make(file.getName());
    Frame frame = parseFrame(key, file);
    NeuralNet.reChunk(frame.vecs());
    Vec[] data = Utils.remove(frame.vecs(), frame.vecs().length - 1);
    Vec labels = frame.vecs()[frame.vecs().length - 1];

    NeuralNet p = new NeuralNet();
    p.rate = 0.005f;
    p.epochs = 1000;
    p.activation = NeuralNet.Activation.Tanh;
    p.max_w2 = Float.MAX_VALUE;
//    p.initial_weight_distribution = Layer.InitialWeightDistribution.Uniform;
//    p.initial_weight_scale = 0.01f;

    Layer[] ls = new Layer[3];
    VecsInput input = new VecsInput(data, null);
    VecSoftmax output = new VecSoftmax(labels, null, NeuralNet.Loss.MeanSquare);
    ls[0] = input;
    ls[1] = new Layer.Tanh(50);
    ls[2] = output;
    for( int i = 0; i < ls.length; i++ )
      ls[i].init(ls, i, p);

    ls[2].rate = .0005f; //overwrite

//    for( ;; ) {
    //Trainer.Direct trainer = new Trainer.Direct(ls, 1000, null);
    //trainer.run();
    Trainer.MapReduce trainer = new Trainer.MapReduce(ls, 100000, null);
    trainer.start();

//      NeuralNet.Error train = NeuralNet.eval(ls, (int) frame.numRows(), null);
//      Log.info("H2O and Reference equal, train: " + train);
//    }

    long start = System.nanoTime();
    for( ;; ) {
      try {
        Thread.sleep(2000);
      } catch( InterruptedException e ) {
        throw new RuntimeException(e);
      }

      double time = (System.nanoTime() - start) / 1e9;
      long processed = trainer.processed();
      int ps = (int) (processed / time);
      String text = (int) time + "s, " + processed + " samples (" + (ps) + "/s) ";

      Errors error = NeuralNet.eval(ls, data, labels, 0, null);
      text += "train: " + error;
      text += ", rates: ";
      for( int i = 1; i < ls.length; i++ )
        text += String.format("%.3g", ls[i].rate(processed)) + ", ";

      System.out.println(text);
    }

//    for( int i = 0; i < ls.length; i++ )
//      ls[i].close();
//    UKV.remove(key);
  }
}
