package hex;

import javax.swing.JFrame;

public class Mnist extends Mnist8m {
  public static void main(String[] args) throws Exception {
    load();
    // One shot
    // normalize();
    //TODO try subtract mean for each sample before col norm
    Mnist mnist = new Mnist();
    mnist.run();
  }

  @Override void monitor(Trainer trainer) {
    // Basic visualization of images and weights
    JFrame frame = new JFrame("H2O");
    frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
    MnistCanvas canvas = new MnistCanvas(trainer, new Train8mInput());
    frame.setContentPane(canvas.init());
    frame.pack();
    frame.setLocationRelativeTo(null);
    frame.setVisible(true);
    super.monitor(trainer);
  }
}