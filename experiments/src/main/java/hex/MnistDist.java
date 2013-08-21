package hex;

import water.Sandbox;

public class MnistDist extends MnistCanvas {
  public static void main(String[] args) throws Exception {
    water.Boot.main(UserMain.class, args);
  }

  public static class UserMain {
    public static void main(String[] args) throws Exception {
      Sandbox.localCloud(2, true, new String[0]);

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
}