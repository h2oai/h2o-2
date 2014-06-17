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
    Frame frame = ParseDataset2.parse(Key.make("Original"), new Key[]{file});

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
    Vec resp = frame.remove(0); //remove response
    final Frame reconstructed = mymodel.score(frame);
    // put Frame into K-V store
    Frame recon = new Frame(Key.make("Reconstruction"), reconstructed.names(), reconstructed.vecs());
    recon.delete_and_lock(null);
    recon.unlock(null);

    // compute reconstruction error
    Env ev = Exec2.exec("Difference = Original - Reconstruction");
    Frame diff = ev.popAry();
    ev.remove_and_unlock();

    // compute L2 norm of each reconstructed row (scaled back to normalized variables)
    final Vec l2 = MRUtils.getL2(diff, mymodel.model_info().data_info()._normMul);

    // compute the 95% quantile to have a threshold (in reconstruction error) to find the outliers
    Frame l2_frame = new Frame(Key.make("l2_frame"), new String[]{"L2"}, new Vec[]{l2});
    QuantilesPage qp = new QuantilesPage();
    qp.column = l2_frame.vec(0);
    qp.source_key = l2_frame;
    double quantile = 0.95;
    qp.quantile = quantile;
    qp.invoke();
    double thres = qp.result;

    // print stats and outliers
    Log.info("L2 norm of reconstruction error (in normalized space):");
    StringBuilder sb = new StringBuilder();
    sb.append("Mean reconstruction error: " + l2.mean() + "\n");
    sb.append("The following points are reconstructed with an error above the " + quantile*100 + "-th percentile - outliers?\n");
    for( int i=0; i<l2.length(); i++ ) {
      if (l2.at(i) > thres) {
        sb.append(String.format("row %d : l2 error = %5f\n", i, l2.at(i)));
      }
    }
    Log.info(sb);

    // cleanup
    mymodel.delete();
    frame.add("dummy", resp);
    frame.delete();
    p.delete();
    l2_frame.delete();
    recon.delete();
    reconstructed.delete();
    ((Frame)DKV.get(Key.make("Difference")).get()).delete();
    diff.delete();
  }
}

