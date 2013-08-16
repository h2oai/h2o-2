package hex;

import java.util.ArrayList;

import water.Sandbox;
import water.deploy.Cloud;

public class MnistDist extends MnistCanvas {
  public static void main(String[] args) throws Exception {
    //water.Boot.main(UserMain.class, args);
    cloud4();
  }

  public static class UserMain {
    public static void main(String[] args) throws Exception {
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
    ArrayList<String> list = new ArrayList<String>();
    for( int i = 0; i < 4; i++ )
      list.add("192.168.1." + (161 + i));
    Cloud c = new Cloud(list.toArray(new String[0]), list.toArray(new String[0]));
    String args = "-mainclass " + MnistDist.UserMain.class.getName();
    c.start(null, null, new String[0], args.split(" "));
  }
}