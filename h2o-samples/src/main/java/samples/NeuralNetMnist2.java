package samples;

import hex.FrameTask;
import hex.nn.NN;
import water.Job;
import water.Key;
import water.TestUtil;
import water.fvec.Frame;
import water.util.Log;

/**
 * Runs a neural network on the MNIST dataset.
 */
public class NeuralNetMnist2 extends Job {
  public static void main(String[] args) throws Exception {
    Class job = NeuralNetMnist2.class;
    samples.launchers.CloudLocal.launch(job, 2);
//    samples.launchers.CloudProcess.launch(job, 4);
    //samples.launchers.CloudConnect.launch(job, "localhost:54321");
//    samples.launchers.CloudRemote.launchIPs(job, "192.168.1.171", "192.168.1.172", "192.168.1.173", "192.168.1.174", "192.168.1.175");
//    samples.launchers.CloudRemote.launchIPs(job, "192.168.1.161", "192.168.1.163", "192.168.1.164");
//    samples.launchers.CloudRemote.launchIPs(job, "192.168.1.161");
    //samples.launchers.CloudRemote.launchEC2(job, 4);
  }

  @Override protected Status exec() {
    final long seed = 0xC0FFEE;

    Log.info("Parsing data.");
//    Frame trainf = TestUtil.parseFromH2OFolder("smalldata/mnist/train10x.csv.gz");
    Frame trainf = TestUtil.parseFromH2OFolder("smalldata/mnist/train.csv.gz");
    Frame testf = TestUtil.parseFromH2OFolder("smalldata/mnist/test.csv.gz");
    Log.info("Done.");

    NN p = new NN();
    // Hinton parameters -> should lead to ~1 % test error after ~ 10M training points
    p.seed = seed;
    //p.hidden = new int[]{1024,1024,2048};
    p.hidden = new int[]{128,128,256};
    p.rate = 0.003;
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
    p.diagnostics = false;
    p.validation = testf;
    p.source = trainf;
    p.response = trainf.lastVec();
    p.ignored_cols = null;
    p.destination_key = Key.make("mnist.model");

    Frame fr = FrameTask.DataInfo.prepareFrame(p.source, p.response, p.ignored_cols, true);
    p._dinfo = new FrameTask.DataInfo(fr, 1, true);

    p.initModel();
    p.trainModel(true);
    return Status.Running;
  }
}
