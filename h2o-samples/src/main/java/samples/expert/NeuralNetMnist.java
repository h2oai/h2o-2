package samples.expert;

import hex.Layer;
import hex.Layer.VecSoftmax;
import hex.Layer.VecsInput;
import hex.NeuralNet;
import hex.NeuralNet.Errors;
import hex.Trainer;
import hex.rng.MersenneTwisterRNG;
import water.Job;
import water.Key;
import water.TestUtil;
import water.fvec.AppendableVec;
import water.fvec.Frame;
import water.fvec.NewChunk;
import water.fvec.Vec;
import water.util.Utils;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPInputStream;

/**
 * Runs a neural network (deprecated - use Deep Learning instead) on the MNIST dataset.
 */
public class NeuralNetMnist extends Job {
  public static void main(String[] args) throws Exception {
    Class job = NeuralNetMnist.class;
    samples.launchers.CloudLocal.launch(job, 1);
//    samples.launchers.CloudProcess.launch(job, 4);
    //samples.launchers.CloudConnect.launch(job, "localhost:54321");
//    samples.launchers.CloudRemote.launchIPs(job, "192.168.1.163");
//    samples.launchers.CloudRemote.launchIPs(job, "192.168.1.161", "192.168.1.162", "192.168.1.163", "192.168.1.164");
//    samples.launchers.CloudRemote.launchIPs(job, "192.168.1.161", "192.168.1.163", "192.168.1.164");
    //samples.launchers.CloudRemote.launchEC2(job, 4);
  }

  private Vec[] train, test;
  protected transient volatile Trainer _trainer;

  protected Layer[] build(Vec[] data, Vec labels, VecsInput inputStats, VecSoftmax outputStats) {
    //same parameters as in test_NN_mnist.py
    Layer[] ls = new Layer[5];
    ls[0] = new VecsInput(data, inputStats);
    ls[1] = new Layer.RectifierDropout(117);
    ls[2] = new Layer.RectifierDropout(131);
    ls[3] = new Layer.RectifierDropout(129);
    ls[ls.length-1] = new VecSoftmax(labels, outputStats);

    NeuralNet p = new NeuralNet();
    p.seed = 98037452452l;
    p.rate = 0.005;
    p.rate_annealing = 1e-6;
    p.activation = NeuralNet.Activation.RectifierWithDropout;
    p.loss = NeuralNet.Loss.CrossEntropy;
    p.input_dropout_ratio = 0.2;
    p.max_w2 = 15;
    p.epochs = 2;
    p.l1 = 1e-5;
    p.l2 = 0.0000001;
    p.momentum_start = 0.5;
    p.momentum_ramp = 100000;
    p.momentum_stable = 0.99;
    p.initial_weight_distribution = NeuralNet.InitialWeightDistribution.UniformAdaptive;
    p.classification = true;
    p.diagnostics = true;
    p.expert_mode = true;

    for( int i = 0; i < ls.length; i++ ) {
      ls[i].init(ls, i, p);
    }
    return ls;
  }

  protected void startTraining(Layer[] ls) {
    // Single-thread SGD
//    System.out.println("Single-threaded\n");
//    _trainer = new Trainer.Direct(ls, epochs, self());

    // Single-node parallel
    System.out.println("Multi-threaded\n");
    _trainer = new Trainer.Threaded(ls, ls[0].params.epochs, self(), -1);

    // Distributed parallel
//    System.out.println("MapReduce\n");
//    _trainer = new Trainer.MapReduce(ls, epochs, self()); //this will call cancel() and abort the whole run

    _trainer.start();
  }

  @Override protected void execImpl() {
    Frame trainf = TestUtil.parseFromH2OFolder("smalldata/mnist/train.csv.gz");
    Frame testf = TestUtil.parseFromH2OFolder("smalldata/mnist/test.csv.gz");
    train = trainf.vecs();
    test = testf.vecs();

    // Labels are on last column for this dataset
    final Vec trainLabels = train[train.length - 1];
    train = Utils.remove(train, train.length - 1);
    final Vec testLabels = test[test.length - 1];
    test = Utils.remove(test, test.length - 1);

    final Layer[] ls = build(train, trainLabels, null, null);

    // Monitor training
    final Timer timer = new Timer();
    final long start = System.nanoTime();
    final AtomicInteger evals = new AtomicInteger(1);
    timer.schedule(new TimerTask() {
      @Override public void run() {
        if( !Job.isRunning(self()) )
          timer.cancel();
        else {
          double time = (System.nanoTime() - start) / 1e9;
          Trainer trainer = _trainer;
          long processed = trainer == null ? 0 : trainer.processed();
          int ps = (int) (processed / time);
          String text = (int) time + "s, " + processed + " samples (" + (ps) + "/s) ";

          // Build separate nets for scoring purposes, use same normalization stats as for training
          Layer[] temp = build(train, trainLabels, (VecsInput) ls[0], (VecSoftmax) ls[ls.length - 1]);
          Layer.shareWeights(ls, temp);
          // Estimate training error on subset of dataset for speed
          Errors e = NeuralNet.eval(temp, 1000, null);
          text += "train: " + e;
          text += ", rate: ";
          text += String.format("%.5g", ls[0].rate(processed));
          text += ", momentum: ";
          text += String.format("%.5g", ls[0].momentum(processed));
          System.out.println(text);
          if( (evals.incrementAndGet() % 1) == 0 ) {
            System.out.println("Computing test error");
            temp = build(test, testLabels, (VecsInput) ls[0], (VecSoftmax) ls[ls.length - 1]);
            Layer.shareWeights(ls, temp);
            e = NeuralNet.eval(temp, 0, null);
            System.out.println("Test error: " + e);
          }
        }
      }
    }, 0, 10);
    startTraining(ls);
  }

  // Remaining code was used to shuffle & convert to CSV

  public static final int PIXELS = 784;

  static void csv() throws Exception {
    csv("../smalldata/mnist/train.csv", "train-images-idx3-ubyte.gz", "train-labels-idx1-ubyte.gz");
    csv("../smalldata/mnist/test.csv", "t10k-images-idx3-ubyte.gz", "t10k-labels-idx1-ubyte.gz");
  }

  private static void csv(String dest, String images, String labels) throws Exception {
    DataInputStream imagesBuf = new DataInputStream(new GZIPInputStream(new FileInputStream(new File(images))));
    DataInputStream labelsBuf = new DataInputStream(new GZIPInputStream(new FileInputStream(new File(labels))));

    imagesBuf.readInt(); // Magic
    int count = imagesBuf.readInt();
    labelsBuf.readInt(); // Magic
    assert count == labelsBuf.readInt();
    imagesBuf.readInt(); // Rows
    imagesBuf.readInt(); // Cols

    System.out.println("Count=" + count);
    byte[][] rawI = new byte[count][PIXELS];
    byte[] rawL = new byte[count];
    for( int n = 0; n < count; n++ ) {
      imagesBuf.readFully(rawI[n]);
      rawL[n] = labelsBuf.readByte();
    }

    MersenneTwisterRNG rand = new MersenneTwisterRNG(MersenneTwisterRNG.SEEDS);
    for( int n = count - 1; n >= 0; n-- ) {
      int shuffle = rand.nextInt(n + 1);
      byte[] image = rawI[shuffle];
      rawI[shuffle] = rawI[n];
      rawI[n] = image;
      byte label = rawL[shuffle];
      rawL[shuffle] = rawL[n];
      rawL[n] = label;
    }

    Vec[] vecs = new Vec[PIXELS + 1];
    NewChunk[] chunks = new NewChunk[vecs.length];
    for( int v = 0; v < vecs.length; v++ ) {
      vecs[v] = new AppendableVec(Key.make(UUID.randomUUID().toString()));
      chunks[v] = new NewChunk(vecs[v], 0);
    }
    for( int n = 0; n < count; n++ ) {
      for( int v = 0; v < vecs.length - 1; v++ )
        chunks[v].addNum(rawI[n][v] & 0xff, 0);
      chunks[chunks.length - 1].addNum(rawL[n], 0);
    }
    for( int v = 0; v < vecs.length; v++ ) {
      chunks[v].close(0, null);
      vecs[v] = ((AppendableVec) vecs[v]).close(null);
    }

    Frame frame = new Frame(null, vecs);
    Utils.writeFileAndClose(new File(dest), frame.toCSV(false));
    imagesBuf.close();
    labelsBuf.close();
  }
}
