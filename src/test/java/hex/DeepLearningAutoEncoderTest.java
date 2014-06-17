package hex;

import hex.deeplearning.*;
import org.junit.BeforeClass;
import org.junit.Test;
import water.*;
import water.api.QuantilesPage;
import water.exec.Env;
import water.exec.Exec2;
import water.fvec.*;
import water.util.FrameUtils;
import water.util.Log;
import water.util.MRUtils;

public class DeepLearningAutoEncoderTest extends TestUtil {
  static final String PATH = "smalldata/logreg/prostate.csv";

  @BeforeClass public static void stall() {
    stall_till_cloudsize(JUnitRunnerDebug.NODES);
  }

  @Test
  public void run() {
    long seed = 0xDECAF;

    Key file = NFSFileVec.make(find_test_file(PATH));
    Frame frame = ParseDataset2.parse(Key.make(), new Key[]{file});

    DeepLearning p = new DeepLearning();
    p.source = frame;
    p.autoencoder = true;
    p.response = frame.vecs()[0]; //easy way to ignore ID (col 0)

    p.seed = seed;
    p.hidden = new int[]{10,5};
    p.adaptive_rate = true;
    p.l1 = 1e-4;
    p.l2 = 1e-4;
//    p.rate = 1e-5;
    p.activation = DeepLearning.Activation.Tanh;
    p.loss = DeepLearning.Loss.MeanSquare;
//    p.initial_weight_distribution = DeepLearning.InitialWeightDistribution.Normal;
//    p.initial_weight_scale = 1e-3;
    p.epochs = 1000;
//    p.shuffle_training_data = true;
    p.force_load_balance = true;
    p.invoke();

    DeepLearningModel mymodel = UKV.get(p.dest());

    // Reconstruct data
    double quantile = 0.99;
//    final Vec l2 = frame.lastVec();
    final Vec l2 = mymodel.scoreAutoEncoder(frame);
    double thres = mymodel.calcOutlierThreshold(l2, quantile);

    // print stats and outliers
    Log.info("L2 norm of reconstruction error (in normalized space):");
    StringBuilder sb = new StringBuilder();
    sb.append("Mean reconstruction error: " + l2.mean() + "\n");
    sb.append("The following points are reconstructed with an error above the " + quantile*100 + "-th percentile - outliers?\n");
    for( long i=0; i<l2.length(); i++ ) {
      if (l2.at(i) > thres) {
        sb.append(String.format("row %d : l2 error = %5f\n", i, l2.at(i)));
      }
    }
    Log.info(sb);

    assert(DKV.get(frame._key) != null);
    // cleanup
    Log.info("before l2: " + H2O.store_size());
    Futures fs = new Futures();
    l2.remove(fs);
    fs.blockForPending();
    Log.info("after l2: " + H2O.store_size());
    mymodel.delete();
    Log.info("before frame: " + H2O.store_size());
    frame.delete();
    Log.info("after frame: " + H2O.store_size());
    p.delete();
  }
}

