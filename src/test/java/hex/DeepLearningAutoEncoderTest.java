package hex;

import hex.deeplearning.*;
import org.junit.BeforeClass;
import org.junit.Test;
import water.JUnitRunnerDebug;
import water.Key;
import water.TestUtil;
import water.UKV;
import water.fvec.Frame;
import water.fvec.NFSFileVec;
import water.fvec.ParseDataset2;
import water.util.Log;

public class DeepLearningAutoEncoderTest extends TestUtil {
  static final String PATH = "smalldata/iris/iris.csv";

  @BeforeClass public static void stall() {
    stall_till_cloudsize(JUnitRunnerDebug.NODES);
  }

  @Test
  public void run() {
    long seed = 0xDECAF;

    Key file = NFSFileVec.make(find_test_file(PATH));
    Frame frame = ParseDataset2.parse(Key.make("iris_nn2"), new Key[]{file});

    DeepLearning p = new DeepLearning();
    p.source = frame;
    p.autoencoder = true;
    p.response = frame.lastVec();

    // regular parameters
    p.seed = seed;
    p.hidden = new int[]{1000};
    p.adaptive_rate = true;
    p.activation = DeepLearning.Activation.Tanh;
    p.epochs = 1000;
    p.force_load_balance = false; //keep just 1 chunk for reproducibility
    p.invoke();

    DeepLearningModel mymodel = UKV.get(p.dest());
    Frame reconstructed = mymodel.score(frame);

    // TODO: check reconstruction accuracy

    // cleanup
    mymodel.delete();
    frame.delete();
    p.delete();
    reconstructed.delete();
  }
}

