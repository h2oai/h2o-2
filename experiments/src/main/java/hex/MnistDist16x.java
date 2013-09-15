package hex;

import water.H2O;
import water.TestUtil;
import water.deploy.Cloud;
import water.util.Log;

public class MnistDist16x {
  public static void main(String[] args) throws Exception {
    Cloud cloud = new Cloud();
    for( int i = LOW; i < LOW + LEN; i++ )
      cloud._publicIPs.add("192.168.1." + (161 + i));
    cloud._clientRSyncIncludes.add("../libs/jdk");
    cloud._clientRSyncIncludes.add("smalldata");
    cloud._clientRSyncIncludes.add("experiments/target");
    cloud._fannedRSyncIncludes.add("jdk");
    cloud._fannedRSyncIncludes.add("smalldata");
    String java = "-ea -Xmx12G -Dh2o.debug";
    String node = "-mainClass " + MnistDist16x.UserCode.class.getName();
    cloud.start(java.split(" "), node.split(" "));
  }

  static int LOW = 1, LEN = 3;

  public static class UserCode {
    public static void userMain(String[] args) throws Exception {
      H2O.main(args);

      Log.info("blah: " + System.getProperty("java.home"));

      TestUtil.stall_till_cloudsize(LEN);

      MnistDist mnist = new MnistDist();
      mnist.run();

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