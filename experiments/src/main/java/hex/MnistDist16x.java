package hex;

import java.io.File;

import water.*;
import water.deploy.Cloud;
import water.fvec.*;
import water.util.Log;
import water.util.Utils;

public class MnistDist16x {
  public static void main(String[] args) throws Exception {
    Cloud cloud = new Cloud();
    for( int i = LOW; i < LOW + LEN; i++ )
      cloud.publicIPs.add("192.168.1." + (161 + i));
    cloud.clientRSyncIncludes.add("smalldata");
    cloud.clientRSyncIncludes.add("experiments/target");
    cloud.fannedRSyncIncludes.add("smalldata");
    cloud.jdk = "../libs/jdk";
    String java = "-ea -Xmx120G -Dh2o.debug";
    String node = "-mainClass " + UserCode.class.getName() + " -beta";
    cloud.start(java.split(" "), node.split(" "));
  }

  static int LOW = 0, LEN = 4;

  public static class UserCode {
    public static void userMain(String[] args) throws Exception {
      H2O.main(args);

      Log.info("java: " + System.getProperty("java.home"));

      TestUtil.stall_till_cloudsize(LEN);
      //Sample08_DeepNeuralNet_EC2.run();
      //Sample07_NeuralNet_Mnist8m.run();
      //new Sample07_NeuralNetLowLevel().run();

      Key fkey = NFSFileVec.make(new File("/home/0xdiag/home-0xdiag-datasets/mnist/mnist8m.csv"));
      Key mnist8m = Key.make("mnist8m.csv");
      Frame frame = ParseDataset2.parse(mnist8m, new Key[] { fkey });

      Vec response = frame.vecs()[0];
      Vec[] vecs = Utils.remove(frame.vecs(), 0);
      Key train = Key.make("train.hex");
      UKV.put(train, new Frame(frame.names(), Utils.append(vecs, response)));

      Key dest = Key.make("test.hex");
      Key ftest = NFSFileVec.make(new File("smalldata/mnist/test.csv.gz"));
      ParseDataset2.parse(dest, new Key[] { ftest });

      // Basic visualization of images and weights
//      JFrame frame = new JFrame("H2O");
//      frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
//      MnistDist canvas = new MnistDist();
//      frame.setContentPane(canvas.init());
//      frame.pack();
//      frame.setLocationRelativeTo(null);
//      frame.setVisible(true);
      Log.info("Ready");
    }
  }
}