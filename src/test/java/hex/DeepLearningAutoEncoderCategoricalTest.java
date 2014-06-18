package hex;

import hex.deeplearning.DeepLearning;
import hex.deeplearning.DeepLearningModel;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import water.JUnitRunnerDebug;
import water.Key;
import water.TestUtil;
import water.UKV;
import water.fvec.Frame;
import water.fvec.NFSFileVec;
import water.fvec.ParseDataset2;
import water.fvec.Vec;
import water.util.Log;

import java.util.HashSet;

public class DeepLearningAutoEncoderCategoricalTest extends TestUtil {
  static final String PATH = "smalldata/airlines/AirlinesTrain.csv.zip";

  @BeforeClass public static void stall() {
    stall_till_cloudsize(JUnitRunnerDebug.NODES);
  }

  @Test
  @Ignore
  public void run() {
    long seed = 0xDECAF;

    Key file_train = NFSFileVec.make(find_test_file(PATH));
    Frame train = ParseDataset2.parse(Key.make(), new Key[]{file_train});

    DeepLearning p = new DeepLearning();
    p.source = train;
    p.autoencoder = true;
    p.response = train.vecs()[0]; //ignored anyway
    p.classification = true; //has to be consistent with ignored response

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
    p.epochs = 10;
//    p.shuffle_training_data = true;
    p.force_load_balance = false;
    p.invoke();

    DeepLearningModel mymodel = UKV.get(p.dest());

    // Verification of results

    StringBuilder sb = new StringBuilder();
    sb.append("Verifying results.");

    // Training data

    // Reconstruct data using the same helper functions and verify that self-reported MSE agrees
    double quantile = 0.95;
    final Frame l2_frame_train = mymodel.scoreAutoEncoder(train);
    final Vec l2_train = l2_frame_train.anyVec();
    double thresh_train = mymodel.calcOutlierThreshold(l2_train, quantile);
    sb.append("Mean reconstruction error: " + l2_train.mean() + "\n");
    Assert.assertEquals(mymodel.mse(), l2_train.mean(), 1e-6);

    // manually compute L2
    Frame reconstr = mymodel.score(train);
    double mean_l2 = 0;
    for (int r=0; r<reconstr.numRows(); ++r) {
      double my_l2 = 0;
      for (int c = 0; c < reconstr.numCols(); ++c) {
        my_l2 += Math.pow((reconstr.vec(c).at(r) - train.vec(c).at(r)) * mymodel.model_info().data_info()._normMul[c], 2);
      }
      mean_l2 += my_l2;
    }
    mean_l2 /= reconstr.numRows();
    reconstr.delete();
    sb.append("Mean reconstruction error (train): " + l2_train.mean() + "\n");
    Assert.assertEquals(mymodel.mse(), mean_l2, 1e-6);

    // print stats and potential outliers
    sb.append("The following training points are reconstructed with an error above the " + quantile*100 + "-th percentile - check for \"goodness\" of training data.\n");
    for( long i=0; i<l2_train.length(); i++ ) {
      if (l2_train.at(i) > thresh_train) {
        sb.append(String.format("row %d : l2_train error = %5f\n", i, l2_train.at(i)));
      }
    }

    // cleanup
    p.delete();
    mymodel.delete();
    train.delete();
    l2_frame_train.delete();
  }
}

