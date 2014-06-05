package samples.expert;

import hex.deeplearning.DeepLearningModel;
import hex.deeplearning.DeepLearningTask;
import hex.deeplearning.Neurons;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.image.BufferedImage;
import java.awt.image.WritableRaster;

public class DeepLearningVisualization extends Canvas {

  static int _level = 1;
  Neurons[] _neurons;

  public DeepLearningVisualization(Neurons[] neurons) {
    _neurons = neurons;
  }

  public JPanel init() {
    JToolBar bar = new JToolBar();
    bar.add(new JButton("refresh") {
      @Override protected void fireActionPerformed(ActionEvent event) {
        DeepLearningVisualization.this.repaint();
      }
    });
    bar.add(new JButton("++") {
      @Override protected void fireActionPerformed(ActionEvent event) {
        if (_level < _neurons.length-2) _level++;
      }
    });
    bar.add(new JButton("--") {
      @Override protected void fireActionPerformed(ActionEvent event) {
        if (_level > 1) _level--;
      }
    });
    JPanel pane = new JPanel();
    BorderLayout bord = new BorderLayout();
    pane.setLayout(bord);
    pane.add("North", bar);
    setSize(1024, 1024);
    pane.add(this);
    return pane;
  }

  @Override public void paint(Graphics g) {
    Neurons layer = _neurons[_level];
    int edge = 56, pad = 10;
    final int EDGE = (int) Math.ceil(Math.sqrt(layer._previous._a.size()));
    assert (layer._previous._a.size() <= EDGE * EDGE);

    int offset = pad;
    int buf = EDGE + pad + pad;
    double mean = 0;
    long n = layer._w.size();
    for (int i = 0; i < n; i++)
      mean += layer._w.raw()[i];
    mean /= layer._w.size();
    double sigma = 0;
    for (int i = 0; i < layer._w.size(); i++) {
      double d = layer._w.raw()[i] - mean;
      sigma += d * d;
    }
    sigma = Math.sqrt(sigma / (layer._w.size() - 1));

    for (int o = 0; o < layer._a.size(); o++) {
      if (o % 10 == 0) {
        offset = pad;
        buf += pad + edge;
      }

      int[] pic = new int[EDGE * EDGE];
      for (int i = 0; i < layer._previous._a.size(); i++) {
        double w = layer._w.get(o, i);
        w = ((w - mean) / sigma) * 200;
        if (w >= 0)
          pic[i] = ((int) Math.min(+w, 255)) << 8; //GREEN
        else
          pic[i] = ((int) Math.min(-w, 255)) << 16; //RED
      }

      BufferedImage out = new BufferedImage(EDGE, EDGE, BufferedImage.TYPE_INT_RGB);
      WritableRaster r = out.getRaster();
      r.setDataElements(0, 0, EDGE, EDGE, pic);

      BufferedImage resized = new BufferedImage(edge, edge, BufferedImage.TYPE_INT_RGB);
      Graphics2D g2 = resized.createGraphics();
      try {
        g2.setRenderingHint(RenderingHints.KEY_INTERPOLATION, RenderingHints.VALUE_INTERPOLATION_BICUBIC);
        g2.clearRect(0, 0, edge, edge);
        g2.drawImage(out, 0, 0, edge, edge, null);
      } finally {
        g2.dispose();
      }
      g.drawImage(resized, buf, offset, null);

      offset += pad + edge;
    }
  }

  static JFrame frame = new JFrame("H2O Deep Learning");
  static public void visualize(final DeepLearningModel dlm) {
    Neurons[] neurons = DeepLearningTask.makeNeuronsForTesting(dlm.model_info());
    frame.dispose();
    frame.setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE);
    DeepLearningVisualization canvas = new DeepLearningVisualization(neurons);
    frame.setContentPane(canvas.init());
    frame.pack();
    frame.setLocationRelativeTo(null);
    frame.setVisible(true);
  }
}
