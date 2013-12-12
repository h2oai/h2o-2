package samples;

import hex.*;

import javax.swing.JFrame;

public class NeuralNetViz extends NeuralNetMnist {
  public static void main(String[] args) throws Exception {
    Class job = Class.forName(Thread.currentThread().getStackTrace()[1].getClassName());
    samples.launchers.CloudLocal.launch(job, 1);
  }

  protected void startTraining(Layer[] ls) {
    _trainer = new Trainer.MapReduce(ls, 0, self());
    //_trainer = new Trainer.Direct(ls);

    // Basic visualization of images and weights
    JFrame frame = new JFrame("H2O");
    frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
    MnistCanvas canvas = new MnistCanvas(_trainer);
    frame.setContentPane(canvas.init());
    frame.pack();
    frame.setLocationRelativeTo(null);
    frame.setVisible(true);

    //preTrain(ls);
    _trainer.start();
  }
}
