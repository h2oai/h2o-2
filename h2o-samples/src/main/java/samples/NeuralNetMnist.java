package samples;

import hex.Layer;
import hex.Layer.VecSoftmax;
import hex.Layer.VecsInput;
import hex.NeuralNet;
import hex.NeuralNet.Errors;
import hex.Trainer;
import hex.rng.MersenneTwisterRNG;
import water.Job;
import water.TestUtil;
import water.UKV;
import water.api.FrameSplit;
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
 * Runs a neural network on the MNIST dataset.
 */
public class NeuralNetMnist extends Job {
  public static void main(String[] args) throws Exception {
    Class job = NeuralNetMnist.class;
    samples.launchers.CloudLocal.launch(job, 1);
//    samples.launchers.CloudProcess.launch(job, 4);
    //samples.launchers.CloudConnect.launch(job, "localhost:54321");
//    samples.launchers.CloudRemote.launchIPs(job, "192.168.1.161", "192.168.1.162", "192.168.1.163", "192.168.1.164");
    //samples.launchers.CloudRemote.launchEC2(job, 4);
  }

  private Vec[] train, test;
  protected transient volatile Trainer _trainer;

  void load(double fraction, long seed) {
    assert(fraction > 0 && fraction <= 1);
    Frame trainf = TestUtil.parseFromH2OFolder("smalldata/mnist/train.csv.gz");
    Frame testf = TestUtil.parseFromH2OFolder("smalldata/mnist/test.csv.gz");
    if (fraction < 1) {
      System.out.println("Sampling " + fraction*100 + "% of data with random seed: " + seed + ".");
      FrameSplit split = new FrameSplit();
      final double[] ratios = {fraction, 1-fraction};
      trainf = split.splitFrame(trainf, ratios, seed)[0];
      testf = split.splitFrame(testf, ratios, seed)[0];

      // for debugging only
      if (false) {
        UKV.put(water.Key.make("train"+fraction), trainf);
        UKV.put(water.Key.make("test"+fraction), testf);
        //try { Thread.sleep(10000000); } catch (Exception _) {}
      }
    }
    train = trainf.vecs();
    test = testf.vecs();
    NeuralNet.reChunk(train);
  }

  protected Layer[] build(Vec[] data, Vec labels, VecsInput inputStats, VecSoftmax outputStats) {
    Layer[] ls = new Layer[3];
    ls[0] = new VecsInput(data, inputStats, 0.2);
//    ls[1] = new Layer.Tanh(500);
//    ls[1] = new Layer.TanhDropout(500);
    ls[1] = new Layer.RectifierDropout(500);
    ls[2] = new VecSoftmax(labels, outputStats);
    for( int i = 0; i < ls.length; i++ ) {
      ls[i].initial_weight_distribution = Layer.InitialWeightDistribution.Normal;
      ls[i].initial_weight_scale = 0.01;
      ls[i].rate = .005f;
      ls[i].rate_annealing = 1 / 1e6f;
      ls[i].l2 = .001f;
      ls[i].max_w2 = 15; //cf. hinton for Mnist
      ls[i].loss = Layer.Loss.CrossEntropy;
      ls[i].init(ls, i);
    }
    return ls;
  }

  protected void startTraining(Layer[] ls) {
    // Single-thread SGD
//    _trainer = new Trainer.Direct(ls, 0, self());

    // Single-node parallel
    _trainer = new Trainer.Threaded(ls, 0, self());

    // Distributed parallel
//    _trainer = new Trainer.MapReduce(ls, 0, self());
    _trainer.start();
  }

  @Override protected Status exec() {
    final double fraction = 1.0;
    final long seed = 0xC0FFEE;
    load(fraction, seed);

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
        if( NeuralNetMnist.this.cancelled() )
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
          if( (evals.incrementAndGet() % 5) == 0 ) {
            System.out.println("Computing test error");
            temp = build(test, testLabels, (VecsInput) ls[0], (VecSoftmax) ls[ls.length - 1]);
            Layer.shareWeights(ls, temp);
            e = NeuralNet.eval(temp, 0, null);
            System.out.println("Test error: " + e);
          }
        }
      }
    }, 0, 10000);
    startTraining(ls);
    return Status.Running;
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
      vecs[v] = new AppendableVec(UUID.randomUUID().toString());
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
