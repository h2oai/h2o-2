package samples.expert;

import hex.Layer;
import hex.Layer.VecSoftmax;
import hex.Layer.VecsInput;
import hex.NeuralNet;
import hex.Trainer;
import water.fvec.Vec;

/**
 * Same as previous MNIST sample but using Rectifier units and Dropout. Also deprecated.
 */
public class NeuralNetMnistDrednet extends NeuralNetMnist {
  public static void main(String[] args) throws Exception {
    Class job = Class.forName(Thread.currentThread().getStackTrace()[1].getClassName());
    samples.launchers.CloudLocal.launch(job, 1);
//    samples.launchers.CloudProcess.launch(job, 3);
    //samples.launchers.CloudRemote.launchIPs(job, "192.168.1.161", "192.168.1.162", "192.168.1.163", "192.168.1.164");
//    samples.launchers.CloudRemote.launchIPs(job, "192.168.1.164");
//  samples.launchers.CloudRemote.launchIPs(job, "192.168.1.161", "192.168.1.163", "192.168.1.164");
    //samples.launchers.CloudRemote.launchEC2(job, 8);
  }

  @Override protected Layer[] build(Vec[] data, Vec labels, VecsInput inputStats, VecSoftmax outputStats) {
    NeuralNet p = new NeuralNet();
    Layer[] ls = new Layer[5];
    p.hidden = new int[]{1024,1024,2048};
//    p.hidden = new int[]{128,128,256};
    ls[0] = new VecsInput(data, inputStats);
    for( int i = 1; i < ls.length-1; i++ ) ls[i] = new Layer.RectifierDropout(p.hidden[i-1]);
    ls[4] = new VecSoftmax(labels, outputStats);
    p.rate = 0.01f;
    p.rate_annealing = 1e-6f;
    p.epochs = 1000;
    p.activation = NeuralNet.Activation.RectifierWithDropout;
    p.input_dropout_ratio = 0.2;
    p.loss = NeuralNet.Loss.CrossEntropy;
    p.max_w2 = 15;
    p.momentum_start = 0.5f;
    p.momentum_ramp = 1800000;
    p.momentum_stable = 0.99f;
    p.score_training = 1000;
    p.score_validation = 10000;
    p.l1 = .00001f;
    p.l2 = .00f;
    p.initial_weight_distribution = NeuralNet.InitialWeightDistribution.UniformAdaptive;
    p.score_interval = 30;
    // Hinton
//  p.initial_weight_distribution = Layer.InitialWeightDistribution.Normal;
//  p.initial_weight_scale = 0.01;


    for( int i = 0; i < ls.length; i++ ) {
      ls[i].init(ls, i, p);
    }


    return ls;
  }

  @Override protected void startTraining(Layer[] ls) {
    // Initial training on one thread to increase stability
    // If the net still produces NaNs, reduce learning rate //TODO: Automate this
//    System.out.println("Initial single-threaded training");
//    _trainer = new Trainer.Direct(ls, 0.1, self());
//    _trainer.start();
//    _trainer.join();

    System.out.println("Main training");

    System.out.println("Multi-threaded");
    _trainer = new Trainer.Threaded(ls, 0, self(), -1);
    _trainer.start();

//    System.out.println("MapReduce");
//    _trainer = new Trainer.MapReduce(ls, 0, self());
//    _trainer.start();
  }
}
