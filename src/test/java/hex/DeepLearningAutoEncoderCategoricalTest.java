package hex;

import hex.deeplearning.DeepLearning;
import hex.deeplearning.DeepLearningModel;
import org.junit.Assert;
import org.junit.BeforeClass;
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

public class DeepLearningAutoEncoderCategoricalTest extends TestUtil {
  static final String PATH = "smalldata/airlines/AirlinesTrain.csv.zip";

  @BeforeClass public static void stall() {
    stall_till_cloudsize(JUnitRunnerDebug.NODES);
  }

  @Test
  public void run() {
    long seed = 0xDECAF;

    Key file_train = NFSFileVec.make(find_test_file(PATH));
    Frame train = ParseDataset2.parse(Key.make(), new Key[]{file_train});

    DeepLearning p = new DeepLearning();
    p.source = train;
    p.autoencoder = true;
    p.response = train.lastVec();
    p.seed = seed;
    p.hidden = new int[]{100, 50, 20};
//    p.ignored_cols = new int[]{0,1,2,3,6,7,8,10}; //Optional: ignore all categoricals
//    p.ignored_cols = new int[]{4,5,9}; //Optional: ignore all numericals
    p.adaptive_rate = true;
    p.l1 = 1e-4;
    p.activation = DeepLearning.Activation.Tanh;
    p.train_samples_per_iteration = -1;
    p.loss = DeepLearning.Loss.MeanSquare;
    p.epochs = 2;
//    p.shuffle_training_data = true;
    p.force_load_balance = true;
    p.score_training_samples = 0;
    p.score_validation_samples = 0;
//    p.reproducible = true;
    p.invoke();

    // Verification of results

    StringBuilder sb = new StringBuilder();
    sb.append("Verifying results.\n");
    DeepLearningModel mymodel = UKV.get(p.dest());
    sb.append("Reported mean reconstruction error: " + mymodel.mse() + "\n");

    // Training data
    // Reconstruct data using the same helper functions and verify that self-reported MSE agrees
    final Frame l2 = mymodel.scoreAutoEncoder(train);
    final Vec l2vec = l2.anyVec();
    sb.append("Actual   mean reconstruction error: " + l2vec.mean() + "\n");

    // print stats and potential outliers
    double quantile = 1 - 5. / train.numRows();
    sb.append("The following training points are reconstructed with an error above the "
            + quantile * 100 + "-th percentile - potential \"outliers\" in testing data.\n");
    double thresh = mymodel.calcOutlierThreshold(l2vec, quantile);
    for (long i = 0; i < l2vec.length(); i++) {
      if (l2vec.at(i) > thresh) {
        sb.append(String.format("row %d : l2vec error = %5f\n", i, l2vec.at(i)));
      }
    }
    Log.info(sb.toString());

    Assert.assertEquals(mymodel.mse(), l2vec.mean(), 1e-8);

    // Create reconstruction
    Log.info("Creating full reconstruction.");
    final Frame recon_train = mymodel.score(train);

    // cleanup
    recon_train.delete();
    train.delete();
    p.delete();
    mymodel.delete();
    l2.delete();
  }
}

