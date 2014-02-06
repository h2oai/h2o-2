package samples;

import hex.FrameTask;
import hex.nn.NN;
import water.Job;
import water.Key;
import water.TestUtil;
import water.api.FrameSplit;
import water.fvec.Frame;
import water.util.Log;

/**
 * Runs a neural network on the MNIST dataset.
 */
public class NeuralNetMnist2 extends Job {
  public static void main(String[] args) throws Exception {
    Class job = NeuralNetMnist2.class;
    samples.launchers.CloudLocal.launch(job, 1);
//    samples.launchers.CloudProcess.launch(job, 4);
    //samples.launchers.CloudConnect.launch(job, "localhost:54321");
//    samples.launchers.CloudRemote.launchIPs(job, "192.168.1.171", "192.168.1.172", "192.168.1.173", "192.168.1.174", "192.168.1.175");
//    samples.launchers.CloudRemote.launchIPs(job, "192.168.1.161", "192.168.1.162", "192.168.1.163", "192.168.1.164");
//    samples.launchers.CloudRemote.launchIPs(job, "192.168.1.161", "192.168.1.162");
//    samples.launchers.CloudRemote.launchIPs(job, "192.168.1.161");
//    samples.launchers.CloudRemote.launchEC2(job, 4);
  }

  @Override protected Status exec() {
    Log.info("Parsing data.");
    long seed = 0xC0FFEE;
    double fraction = 1.0;
    Frame trainf = TestUtil.parseFromH2OFolder("smalldata/mnist/train.csv");
    Frame testf = TestUtil.parseFromH2OFolder("smalldata/mnist/test.csv");
    if (fraction < 1) {
      System.out.println("Sampling " + fraction*100 + "% of data with random seed: " + seed + ".");
      FrameSplit split = new FrameSplit();
      final double[] ratios = {fraction, 1-fraction};
      trainf = split.splitFrame(trainf, ratios, seed)[0];
//      testf = split.splitFrame(testf, ratios, seed)[0];
    }
    Log.info("Done.");

    NN p = new NN();
    // Hinton parameters -> should lead to ~1 % test error after ~ 10M training points
    p.seed = seed;
    p.hidden = new int[]{1024,1024,2048};
//    p.hidden = new int[]{128,128,256};
    p.rate = 0.01;
    p.activation = NN.Activation.RectifierWithDropout;
    p.loss = NN.Loss.CrossEntropy;
    p.input_dropout_ratio = 0.2;
    p.max_w2 = 15;
    p.epochs = 200;
    p.rate_annealing = 1e-6;
    p.l1 = 1e-5;
    p.l2 = 0;
    p.momentum_stable = 0.99;
    p.momentum_start = 0.5;
    p.momentum_ramp = 1800000;
    p.initial_weight_distribution = NN.InitialWeightDistribution.UniformAdaptive;
//    p.initial_weight_scale = 0.01
    p.classification = true;
    p.diagnostics = true;
    p.expert_mode = true;
    p.score_training = 1000;
    p.score_validation = 10000;
    p.validation = testf;
    p.source = trainf;
    p.response = trainf.lastVec();
    p.ignored_cols = null;
    p.destination_key = Key.make("mnist.model");
    p._dinfo = new FrameTask.DataInfo(FrameTask.DataInfo.prepareFrame(
            p.source, p.response, p.ignored_cols, true), 1, true);
    return p.exec();
  }
}
