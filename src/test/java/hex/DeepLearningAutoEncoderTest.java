package hex;

import hex.deeplearning.*;
import org.junit.Assert;
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
  static final String PATH = "smalldata/anomaly/ecg_discord.csv";

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
    p.response = frame.vecs()[0]; //ignored anyway
    p.classification = false;

    p.seed = seed;
    p.hidden = new int[]{20};
    p.adaptive_rate = true;
    p.l1 = 1e-4;
//    p.l2 = 1e-4;
//    p.rate = 1e-5;
    p.activation = DeepLearning.Activation.Tanh;
    p.loss = DeepLearning.Loss.MeanSquare;
//    p.initial_weight_distribution = DeepLearning.InitialWeightDistribution.Normal;
//    p.initial_weight_scale = 1e-3;
    p.epochs = 100;
//    p.shuffle_training_data = true;
    p.force_load_balance = false;
    p.invoke();

    DeepLearningModel mymodel = UKV.get(p.dest());

    // Verification of results

    StringBuilder sb = new StringBuilder();
    sb.append("Verifying results.");

    // Reconstruct data using the same helper functions and verify that self-reported MSE agrees
    double quantile = 0.95;
    final Frame l2_frame = mymodel.scoreAutoEncoder(frame);
    final Vec l2 = l2_frame.anyVec();
    double thres = mymodel.calcOutlierThreshold(l2, quantile);
    sb.append("Mean reconstruction error: " + l2.mean() + "\n");
    Assert.assertEquals(mymodel.mse(), l2.mean(), 1e-6);

    // manually compute L2
    Frame reconstr = mymodel.score(frame);
    double mean_l2 = 0;
    for (int r=0; r<reconstr.numRows(); ++r) {
      double my_l2 = 0;
      for (int c = 0; c < reconstr.numCols(); ++c) {
        my_l2 += Math.pow((reconstr.vec(c).at(r) - frame.vec(c).at(r)) * mymodel.model_info().data_info()._normMul[c], 2);
      }
      mean_l2 += my_l2;
    }
    mean_l2 /= reconstr.numRows();
    reconstr.delete();
    sb.append("Mean reconstruction error (check): " + l2.mean() + "\n");
    Assert.assertEquals(mymodel.mse(), mean_l2, 1e-6);

    // print stats and potential outliers
    sb.append("The following points are reconstructed with an error above the " + quantile*100 + "-th percentile - candidates for outliers.\n");
    for( long i=0; i<l2.length(); i++ ) {
      if (l2.at(i) > thres) {
        sb.append(String.format("row %d : l2 error = %5f\n", i, l2.at(i)));
      }
    }
    Log.info(sb);

    // cleanup
    p.delete();
    mymodel.delete();
    frame.delete();
    l2_frame.delete();
  }
}

