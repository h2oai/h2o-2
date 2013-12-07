package samples;

import hex.*;
import hex.Layer.Tanh;
import hex.Layer.TanhPrime;
import hex.Layer.VecSoftmax;
import hex.Layer.VecsInput;
import water.fvec.Vec;

/**
 * Same as previous MNIST sample but with a drednet and pre-training.
 */
public class NeuralNetMnistDeep extends NeuralNetMnist {
  public static void main(String[] args) throws Exception {
    Class c = Class.forName(Thread.currentThread().getStackTrace()[1].getClassName());
    samples.launchers.CloudLocal.launch(1, c);
    //samples.launchers.CloudProcess.launch(4, c);
    //samples.launchers.CloudConnect.launch("localhost:54321", c);
    //samples.launchers.CloudRemote.launchIPs(c, "192.168.1.161", "192.168.1.162");
    //samples.launchers.CloudRemote.launchEC2(c, 4);
  }

  @Override protected Layer[] build(Vec[] data, Vec labels, VecsInput inputStats, VecSoftmax outputStats) {
    Layer[] ls = new Layer[5];
    ls[0] = new VecsInput(data, inputStats);
    ls[1] = new Layer.RectifierDropout(1024);
    ls[2] = new Layer.RectifierDropout(1024);
    ls[3] = new Layer.RectifierDropout(2048);
    ls[4] = new VecSoftmax(labels, outputStats);
    for( int i = 0; i < ls.length; i++ ) {
      ls[i].rate = .01f;
      ls[i].rate_annealing = 1e-6f;
      ls[i].momentum_start = .5f;
      ls[i].momentum_ramp = 60000 * 300;
      ls[i].momentum_stable = .99f;
      ls[i].init(ls, i);
    }
    return ls;
  }

  @Override protected Trainer startTraining(Layer[] ls) {
    // Uncomment for pre-training
    // preTrain(ls);

    //Trainer trainer = new Trainer.Direct(ls, 0, self());
    //Trainer trainer = new Trainer.Threaded(ls, 0, self());
    Trainer trainer = new Trainer.MapReduce(ls, 0, self());
    trainer.start();
    return trainer;
  }

  protected void preTrain(Layer[] ls) {
    for( int i = 1; i < ls.length - 1; i++ ) {
      System.out.println("Pre-training level " + i);
      long time = System.nanoTime();
      preTrain(ls, i);
      System.out.println((int) ((System.nanoTime() - time) / 1e6) + " ms");
    }
  }

  protected void preTrain(Layer[] ls, int index) {
    // Build a network with same layers below 'index', and an auto-encoder at the top
    Layer[] pre = new Layer[index + 2];
    VecsInput input = (VecsInput) ls[0];
    pre[0] = new VecsInput(input.vecs, input);
    for( int i = 1; i < index; i++ ) {
      pre[i] = new Tanh(ls[i].units);
      pre[i].rate = 0;
      pre[i].l2 = .01f;
      Layer.shareWeights(ls[i], pre[i]);
    }

    // Auto-encoder is a layer and a reverse layer on top
    pre[index] = new Tanh(ls[index].units);
    pre[index].rate = .001f;
    pre[index].l2 = 1f;
    pre[index + 1] = new TanhPrime(ls[index - 1].units);
    pre[index + 1].rate = .001f;
    pre[index + 1].l2 = 1f;
    Layer.shareWeights(ls[index], pre[index]);
    Layer.shareWeights(ls[index], pre[index + 1]);
    for( int i = 0; i < pre.length; i++ )
      pre[i].init(pre, i, false, 0, null);

    // Pre-train on subset of dataset
    // TODO try training only the last layer
    Trainer trainer = new Trainer.Threaded(pre, .1, self());
    trainer.start();
    trainer.join();
  }
}
