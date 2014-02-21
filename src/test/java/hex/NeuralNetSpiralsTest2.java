package hex;

import hex.nn.NN;
import hex.nn.NNModel;
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
import water.util.Log;

public class NeuralNetSpiralsTest2 extends TestUtil {
  @BeforeClass public static void stall() {
    stall_till_cloudsize(JUnitRunnerDebug.NODES);
  }

  @Test public void run() {
    Key file = NFSFileVec.make(find_test_file("smalldata/neural/two_spiral.data"));
    Frame frame = ParseDataset2.parse(Key.make(), new Key[]{file});

    Key dest = Key.make("spirals2");

    // build the model
    {
      NN p = new NN();
      p.seed = 7401699394609084302l;
      p.rate = 0.007;
      p.rate_annealing = 0;
      p.epochs = 11000;
      p.hidden = new int[]{100};
      p.activation = NN.Activation.Tanh;
      p.max_w2 = Double.MAX_VALUE;
      p.l1 = 0;
      p.l2 = 0;
      p.momentum_start = 0;
      p.momentum_ramp = 0;
      p.momentum_stable = 0;
      p.initial_weight_distribution = NN.InitialWeightDistribution.Normal;
      p.initial_weight_scale = 2.5;
      p.loss = NN.Loss.CrossEntropy;
      p.source = frame;
      p.response = frame.lastVec();
      p.validation = null;
      p.score_interval = 10;
      p.ignored_cols = null;
      p.sync_samples = 0;
      p.quiet_mode = true;
      p.fast_mode = true; //same as old NeuralNet code
      p.ignore_const_cols = false; //same as old NeuralNet code
      p.shuffle_training_data = false; //same as old NeuralNet code
      p.nesterov_accelerated_gradient = true; //same as old NeuralNet code
      p.classification = true;
      p.diagnostics = true;
      p.expert_mode = true;
      p.score_training_samples = 1000;
      p.score_validation_samples = 10000;
      p.destination_key = dest;
      p.exec();
    }

    // score and check result
    {
      NNModel mymodel = UKV.get(dest); //this actually *requires* frame to also still be in UKV (because of DataInfo...)
      Frame pred = mymodel.score(frame);
      water.api.ConfusionMatrix CM = new water.api.ConfusionMatrix();
      CM.actual = frame;
      CM.vactual = frame.lastVec();
      CM.predict = pred;
      CM.vpredict = pred.vecs()[0];
      CM.serve();
      StringBuilder sb = new StringBuilder();
      double error = CM.toASCII(sb);
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
