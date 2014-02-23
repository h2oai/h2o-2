package samples;

import hex.nn.NN;
import water.Job;
import water.TestUtil;
import water.fvec.Frame;
import water.util.Log;

import java.util.Random;

import static water.util.MRUtils.sampleFrame;

/**
 * Runs a neural network on the MNIST dataset.
 */
public class NeuralNetMnist2 extends Job {
  public static void main(String[] args) throws Exception {
    Class job = NeuralNetMnist2.class;
//    samples.launchers.CloudLocal.launch(job, 1);
//    samples.launchers.CloudProcess.launch(job, 4);
    //samples.launchers.CloudConnect.launch(job, "localhost:54321");
//    samples.launchers.CloudRemote.launchIPs(job, "192.168.1.171", "192.168.1.172", "192.168.1.173", "192.168.1.174", "192.168.1.175");
    samples.launchers.CloudRemote.launchIPs(job, "192.168.1.161", "192.168.1.162", "192.168.1.163", "192.168.1.164");
//    samples.launchers.CloudRemote.launchIPs(job, "192.168.1.161", "192.168.1.162");
//    samples.launchers.CloudRemote.launchIPs(job, "192.168.1.161");
//    samples.launchers.CloudRemote.launchEC2(job, 4);
  }

  @Override protected JobState exec() {
    Log.info("Parsing data.");
    //long seed = 0xC0FFEE;
    long seed = new Random().nextLong();
    double fraction = 1.0;
    Frame trainf = sampleFrame(TestUtil.parseFromH2OFolder("smalldata/mnist/train10x.csv"), (long)(600000*fraction), seed);
//    Frame trainf = sampleFrame(TestUtil.parseFromH2OFolder("smalldata/mnist/train.csv"), (long)(60000*fraction), seed);
    Frame testf = sampleFrame(TestUtil.parseFromH2OFolder("smalldata/mnist/test.csv"), (long)(10000*fraction), seed+1);
    Log.info("Done.");

    NN p = new NN();
    // Hinton parameters -> should lead to ~1 % test error after a few dozen million samples
    p.seed = seed;
    p.hidden = new int[]{1024,1024,2048};
//    p.hidden = new int[]{128,128,256};
    p.rate = 0.01;
    p.rate_annealing = 1e-6;
    p.activation = NN.Activation.RectifierWithDropout;
    p.loss = NN.Loss.CrossEntropy;
    p.input_dropout_ratio = 0.2;
    p.max_w2 = 15;
    p.epochs = 10;
    p.l1 = 1e-5;
    p.l2 = 0;
    p.momentum_start = 0.5;
    p.momentum_ramp = 1800000;
    p.momentum_stable = 0.99;
    p.initial_weight_distribution = NN.InitialWeightDistribution.UniformAdaptive;
//    p.initial_weight_scale = 0.01
    p.classification = true;
    p.diagnostics = true;
    p.expert_mode = true;
    p.score_training_samples = 1000;
    p.score_validation_samples = 10000;
    p.validation = testf;
    p.source = trainf;
    p.response = trainf.lastVec();
    p.ignored_cols = null;
    p.mini_batch = 60000;
    p.score_interval = 600;

    p.fast_mode = true; //to match old NeuralNet behavior
    p.ignore_const_cols = true;
    p.shuffle_training_data = true;
    return p.exec();
  }
}
