package hex;

import hex.deeplearning.DeepLearning;
import hex.deeplearning.DeepLearningModel;
import junit.framework.Assert;
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

public class DeepLearningSpiralsTest extends TestUtil {
  @BeforeClass public static void stall() {
    stall_till_cloudsize(JUnitRunnerDebug.NODES);
  }

  @Test public void run() {
    Key file = NFSFileVec.make(find_test_file("smalldata/neural/two_spiral.data"));
    Frame frame = ParseDataset2.parse(Key.make(), new Key[]{file});

    Key dest = Key.make("spirals2");

    // build the model
    {
      DeepLearning p = new DeepLearning();
      p.seed = 0xbabe;
      p.rate = 0.007;
      p.rate_annealing = 0;
      p.epochs = 5000;
      p.hidden = new int[]{100};
      p.activation = DeepLearning.Activation.Tanh;
      p.max_w2 = Double.MAX_VALUE;
      p.l1 = 0;
      p.l2 = 0;
      p.momentum_start = 0;
      p.momentum_ramp = 0;
      p.momentum_stable = 0;
      p.initial_weight_distribution = DeepLearning.InitialWeightDistribution.Normal;
      p.initial_weight_scale = 2.5;
      p.loss = DeepLearning.Loss.CrossEntropy;
      p.source = frame;
      p.response = frame.lastVec();
      p.validation = null;
      p.score_interval = 10;
      p.ignored_cols = null;
      p.mini_batch = 0; //sync once per period
      p.quiet_mode = true;
      p.fast_mode = true;
      p.ignore_const_cols = true;
      p.nesterov_accelerated_gradient = true;
      p.classification = true;
      p.diagnostics = true;
      p.expert_mode = true;
      p.score_training_samples = 1000;
      p.score_validation_samples = 10000;
      p.shuffle_training_data = false;
      //p.force_load_balance = true; //multi-threaded
      p.force_load_balance = false; //single-threaded (1 chunk)
      p.replicate_training_data = false;
      p.destination_key = dest;
      p.adaptive_rate = true;
      p.rho = 0.99;
      p.epsilon = 5e-3;
      p.execImpl();
    }

    // score and check result
    {
      DeepLearningModel mymodel = UKV.get(dest); //this actually *requires* frame to also still be in UKV (because of DataInfo...)
      Frame pred = mymodel.score(frame);
      water.api.ConfusionMatrix CM = new water.api.ConfusionMatrix();
      CM.actual = frame;
      CM.vactual = frame.lastVec();
      CM.predict = pred;
      CM.vpredict = pred.vecs()[0];
      CM.serve();
      StringBuilder sb = new StringBuilder();
      CM.toASCII(sb);
      double error = new ConfusionMatrix(CM.cm).err();
      Log.info(sb);
      if (error != 0) {
        Assert.fail("Classification error is not 0, but " + error + ".");
      }
      pred.delete();
      mymodel.delete();
    }

    frame.delete();
  }
}
