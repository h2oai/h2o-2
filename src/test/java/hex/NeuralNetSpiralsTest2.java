package hex;

import hex.nn.NN;
import hex.nn.NNModel;
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

public class NeuralNetSpiralsTest2 extends TestUtil {
  @BeforeClass public static void stall() {
    stall_till_cloudsize(JUnitRunnerDebug.NODES);
  }

  @Test public void run() {
    Key file = NFSFileVec.make(find_test_file("smalldata/neural/two_spiral.data"));
    Key parse = Key.make();
    Frame frame = ParseDataset2.parse(parse, new Key[]{file});

    NN p = new NN();
    p.seed = 7401699394609084302l;
    p.rate = 0.007;
    p.rate_annealing = 0;
    p.epochs = 20000;
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
    p.exec();

    NNModel mymodel = UKV.get(p.dest());
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
      Assert.fail("Classification error is not 0, but " + error);
    }
    frame.delete();
    mymodel.delete();
  }
}
