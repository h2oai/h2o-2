package samples;


import hex.Layer;
import hex.Layer.VecSoftmax;
import hex.Layer.VecsInput;
import hex.MnistCanvas;
import hex.NeuralNet;
import hex.Trainer;
import samples.expert.NeuralNetMnist;
import water.fvec.Vec;

import javax.swing.*;

public class NeuralNetMnistPretrain extends NeuralNetMnist {
  public static void main(String[] args) throws Exception {
    Class job = Class.forName(Thread.currentThread().getStackTrace()[1].getClassName());
    samples.launchers.CloudLocal.launch(job, 1);
  }

  @Override protected Layer[] build(Vec[] data, Vec labels, VecsInput inputStats, VecSoftmax outputStats) {
    Layer[] ls = new Layer[4];
    ls[0] = new VecsInput(data, inputStats);
//    ls[1] = new Layer.RectifierDropout(1024);
//    ls[2] = new Layer.RectifierDropout(1024);
    ls[1] = new Layer.Tanh(50);
    ls[2] = new Layer.Tanh(50);
    ls[3] = new VecSoftmax(labels, outputStats);

    // Parameters for MNIST run
    NeuralNet p = new NeuralNet();
    p.rate = 0.01; //only used for NN run after pretraining
    p.activation = NeuralNet.Activation.Tanh;
    p.loss = NeuralNet.Loss.CrossEntropy;
//    p.rate_annealing = 1e-6f;
//    p.max_w2 = 15;
//    p.momentum_start = 0.5f;
//    p.momentum_ramp = 60000 * 300;
//    p.momentum_stable = 0.99f;
//    p.l1 = .00001f;
//    p.l2 = .00f;
    p.initial_weight_distribution = NeuralNet.InitialWeightDistribution.UniformAdaptive;
//    p.initial_weight_scale = 1;

    for( int i = 0; i < ls.length; i++ ) {
      ls[i].init(ls, i, p);
    }
    return ls;
  }

  @Override protected void startTraining(Layer[] ls) {
    int pretrain_epochs = 2;
    preTrain(ls, pretrain_epochs);

    // actual run
    int epochs = 0;

    if (epochs > 0) {
//    _trainer = new Trainer.Direct(ls, epochs, self());
      _trainer = new Trainer.Threaded(ls, epochs, self(), -1);
          // Basic visualization of images and weights

      JFrame frame = new JFrame("H2O Training");
      frame.setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);
      MnistCanvas canvas = new MnistCanvas(_trainer);
      frame.setContentPane(canvas.init());
      frame.pack();
      frame.setLocationRelativeTo(null);
      frame.setVisible(true);//_trainer = new Trainer.MapReduce(ls, epochs, self());

      _trainer.start();
      _trainer.join();
    }
  }

  final private void preTrain(Layer[] ls, int epochs) {
    for( int i = 1; i < ls.length - 1; i++ ) {
      System.out.println("Pre-training level " + i);
      long time = System.nanoTime();
      preTrain(ls, i, epochs);
      System.out.println((int) ((System.nanoTime() - time) / 1e6) + " ms");
    }
  }

  final private void preTrain(Layer[] ls, int index, int epochs) {
    // Build a network with same layers below 'index', and an auto-encoder at the top
    Layer[] pre = new Layer[index + 2];
    VecsInput input = (VecsInput) ls[0];
    pre[0] = new VecsInput(input.vecs, input);
    pre[0].init(pre, 0, ls[0].params); //clone the parameters
    for( int i = 1; i < index; i++ ) {
      //pre[i] = new Layer.Rectifier(ls[i].units);
      pre[i] = new Layer.Tanh(ls[i].units);
      Layer.shareWeights(ls[i], pre[i]);
      pre[i].init(pre, i, ls[i].params); //share the parameters
      pre[i].params.rate = 0; //turn off training for these layers
    }

    // Auto-encoder is a layer and a reverse layer on top
    //pre[index] = new Layer.Rectifier(ls[index].units);
    //pre[index + 1] = new Layer.RectifierPrime(ls[index - 1].units);
    pre[index] = new Layer.Tanh(ls[index].units);
    pre[index].init(pre, index, ls[index].params);
    pre[index].params.rate = 1e-5;

    pre[index+1] = new Layer.TanhPrime(ls[index-1].units);
    pre[index+1].init(pre, index + 1, pre[index].params);
    pre[index+1].params.rate = 1e-5;

    Layer.shareWeights(ls[index], pre[index]);
    Layer.shareWeights(ls[index], pre[index+1]);

    _trainer = new Trainer.Direct(pre, epochs, self());

    // Basic visualization of images and weights
    JFrame frame = new JFrame("H2O Pre-Training");
    frame.setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);
    MnistCanvas canvas = new MnistCanvas(_trainer);
    frame.setContentPane(canvas.init());
    frame.pack();
    frame.setLocationRelativeTo(null);
    frame.setVisible(true);

    _trainer.start();
    _trainer.join();
  }
}
