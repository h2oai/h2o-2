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

    p.seed = seed;
    p.hidden = new int[]{4};
    p.adaptive_rate = true;
//    p.rate = 1e-4;
    p.activation = DeepLearning.Activation.Tanh;
//    p.initial_weight_distribution = DeepLearning.InitialWeightDistribution.Normal;
//    p.initial_weight_scale = 1e-3;
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

