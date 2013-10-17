package hex;

import java.io.File;

import water.*;
import water.deploy.Cloud;
import water.fvec.NFSFileVec;
import water.fvec.ParseDataset2;
import water.util.Log;

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

      Log.info("blah: " + System.getProperty("java.home"));

      TestUtil.stall_till_cloudsize(LEN);
      //Sample08_DeepNeuralNet_EC2.run();
      //Sample07_NeuralNet_Mnist8m.run();
      new Sample07_NeuralNetLowLevel().run();

      File f = new File("smalldata/mnist/train.csv.gz");
      Key dest = Key.make("train.hex");
      Key fkey = NFSFileVec.make(f);
      ParseDataset2.parse(dest, new Key[] { fkey });

      f = new File("smalldata/mnist/test.csv.gz");
      dest = Key.make("test.hex");
      fkey = NFSFileVec.make(f);
      ParseDataset2.parse(dest, new Key[] { fkey });

      // Basic visualization of images and weights
//      JFrame frame = new JFrame("H2O");
//      frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
//      MnistDist canvas = new MnistDist();
//      frame.setContentPane(canvas.init());
//      frame.pack();
//      frame.setLocationRelativeTo(null);
//      frame.setVisible(true);
    }
  }
}