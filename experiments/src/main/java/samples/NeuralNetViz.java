package samples;

import hex.*;

import javax.swing.JFrame;

public class NeuralNetViz extends NeuralNetMnist {
  public static void main(String[] args) throws Exception {
    Class c = Class.forName(Thread.currentThread().getStackTrace()[1].getClassName());
    samples.launchers.CloudLocal.launch(1, c);
    // samples.launchers.CloudProcess.launch(4, c);
    // samples.launchers.CloudRemote.launchIPs(c);
    // samples.launchers.CloudConnect.launch("localhost:54321", c);
  }

  protected Trainer startTraining(Layer[] ls) {
    Trainer trainer = new Trainer.MapReduce(ls, 0, self());
    //Trainer trainer = new Trainer.Direct(ls);

    // Basic visualization of images and weights
    JFrame frame = new JFrame("H2O");
    frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
    MnistCanvas canvas = new MnistCanvas(trainer);
    frame.setContentPane(canvas.init());
    frame.pack();
    frame.setLocationRelativeTo(null);
    frame.setVisible(true);

    //preTrain(ls);
    trainer.start();
    return trainer;
  }
}
