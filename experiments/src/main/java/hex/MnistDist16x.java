package hex;

import water.*;
import water.deploy.Cloud;
import water.util.Log;

public class MnistDist16x extends MnistCanvas {
  public static void main(String[] args) throws Exception {
    //water.Boot.main(UserMain.class, args);
    cloud4();
  }

  public static class UserMain {
    public static void main(String[] args) throws Exception {
      H2O.main(args);
      TestUtil.stall_till_cloudsize(1);
      Log.info("blah");
      // localCloud();

      NeuralNetMnistTest mnist = new NeuralNetMnistTest();
      mnist.init();
      _test = mnist;

      // Basic visualization of images and weights
//      JFrame frame = new JFrame("H2O");
//      frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
//      MnistDist canvas = new MnistDist();
//      frame.setContentPane(canvas.init());
//      frame.pack();
//      frame.setLocationRelativeTo(null);
//      frame.setVisible(true);

      mnist.run();
    }
  }

  static void localCloud() {
    Sandbox.localCloud(2, true, new String[0]);
  }

  static void cloud4() {
    Cloud cloud = new Cloud();
    for( int i = 0; i < 1; i++ )
      cloud._publicIPs.add("192.168.1." + (161 + i));
    cloud._clientRSyncIncludes.add("../libs/jdk");
    cloud._clientRSyncIncludes.add("smalldata");
    cloud._clientRSyncIncludes.add("experiments/target");
    cloud._fannedRSyncIncludes.add("jdk");
    cloud._fannedRSyncIncludes.add("smalldata");
    String java = "-ea -Xmx12G -Dh2o.debug";
    String args = "-mainClass " + MnistDist16x.UserMain.class.getName();
    cloud.start(java.split(" "), args.split(" "));
  }
}