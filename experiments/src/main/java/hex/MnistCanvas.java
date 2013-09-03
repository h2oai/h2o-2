package hex;

import hex.Layer.FrameInput;

import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.image.BufferedImage;
import java.awt.image.WritableRaster;
import java.util.Random;

import javax.swing.*;

import water.H2O;
import water.Sample07_NeuralNet_Mnist;

public class MnistCanvas extends Canvas {
  static NeuralNetTest _test;
  static Random _rand = new Random();
  static int _level = 1;

  public static void main(String[] args) throws Exception {
    water.Boot.main(UserCode.class, args);
  }

  public static class UserCode {
    // Entry point can be called 'main' or 'userMain'
    public static void userMain(String[] args) throws Exception {
      H2O.main(args);
      Sample07_NeuralNet_Mnist mnist = new Sample07_NeuralNet_Mnist();
      mnist.init();
      _test = mnist;

      // Basic visualization of images and weights
      JFrame frame = new JFrame("H2O");
      frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
      MnistCanvas canvas = new MnistCanvas();
      frame.setContentPane(canvas.init());
      frame.pack();
      frame.setLocationRelativeTo(null);
      frame.setVisible(true);

      mnist.run();
    }
  }

  JPanel init() {
    JToolBar bar = new JToolBar();
    bar.add(new JButton("refresh") {
      @Override protected void fireActionPerformed(ActionEvent event) {
        MnistCanvas.this.repaint();
      }
    });
    bar.add(new JButton("++") {
      @Override protected void fireActionPerformed(ActionEvent event) {
        _level++;
      }
    });
    bar.add(new JButton("--") {
      @Override protected void fireActionPerformed(ActionEvent event) {
        _level--;
      }
    });
    bar.add(new JButton("histo") {
      @Override protected void fireActionPerformed(ActionEvent event) {
        Histogram.initFromSwingThread();
        Histogram.build(_test._trainer.layers());
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
    water.fvec.Frame frame = ((FrameInput) _test._ls[0])._frame;
    int edge = 56, pad = 10;
    int rand = _rand.nextInt((int) frame.numRows());

    // Side
    {
      BufferedImage in = new BufferedImage(Sample07_NeuralNet_Mnist.EDGE, Sample07_NeuralNet_Mnist.EDGE, BufferedImage.TYPE_INT_RGB);
      WritableRaster r = in.getRaster();

      // Input
      int[] pix = new int[Sample07_NeuralNet_Mnist.PIXELS];
      for( int i = 0; i < pix.length; i++ )
        pix[i] = (int) (frame._vecs[i].at8(rand));
      r.setDataElements(0, 0, Sample07_NeuralNet_Mnist.EDGE, Sample07_NeuralNet_Mnist.EDGE, pix);
      g.drawImage(in, pad, pad, null);

      // Labels
      g.drawString("" + frame._vecs[Sample07_NeuralNet_Mnist.PIXELS].at8(rand), 10, 50);
      g.drawString("RBM " + _level, 10, 70);
    }

    // Outputs
    int offset = pad;
//    float[] visible = new float[MnistNeuralNetTest.PIXELS];
//    System.arraycopy(_images, rand * MnistNeuralNetTest.PIXELS, visible, 0, MnistNeuralNetTest.PIXELS);
//    for( int i = 0; i <= _level; i++ ) {
//      for( int pass = 0; pass < 10; pass++ ) {
//        if( i == _level ) {
//          int[] output = new int[visible.length];
//          for( int v = 0; v < visible.length; v++ )
//            output[v] = (int) Math.min(visible[v] * 255, 255);
//          BufferedImage out = new BufferedImage(MnistNeuralNetTest.EDGE, MnistNeuralNetTest.EDGE,
//              BufferedImage.TYPE_INT_RGB);
//          WritableRaster r = out.getRaster();
//          r.setDataElements(0, 0, MnistNeuralNetTest.EDGE, MnistNeuralNetTest.EDGE, output);
//          BufferedImage image = new BufferedImage(edge, edge, BufferedImage.TYPE_INT_RGB);
//          Graphics2D ig = image.createGraphics();
//          ig.setRenderingHint(RenderingHints.KEY_INTERPOLATION, RenderingHints.VALUE_INTERPOLATION_BICUBIC);
//          ig.clearRect(0, 0, edge, edge);
//          ig.drawImage(out, 0, 0, edge, edge, null);
//          ig.dispose();
//          g.drawImage(image, pad * 2 + MnistNeuralNetTest.EDGE, offset, null);
//          offset += pad + edge;
//        }
//        if( _ls[i]._v != null ) {
//          float[] hidden = new float[_ls[i]._b.length];
//          _ls[i].forward(visible, hidden);
//          visible = _ls[i].generate(hidden);
//        }
//      }
//      float[] t = new float[_ls[i]._b.length];
//      _ls[i].forward(visible, t);
//      visible = t;
//    }

    // Weights
    int buf = Sample07_NeuralNet_Mnist.EDGE + pad + pad;
    Layer layer = _test._trainer.layers()[_level];
    double mean = 0;
    int n = layer._w.length;
    for( int i = 0; i < n; i++ )
      mean += layer._w[i];
    mean /= layer._w.length;
    double sigma = 0;
    for( int i = 0; i < layer._w.length; i++ ) {
      double d = layer._w[i] - mean;
      sigma += d * d;
    }
    sigma = Math.sqrt(sigma / (layer._w.length - 1));

    for( int o = 0; o < layer._b.length; o++ ) {
      if( o % 10 == 0 ) {
        offset = pad;
        buf += pad + edge;
      }

      int[] start = new int[layer._in._a.length];
      for( int i = 0; i < layer._in._a.length; i++ ) {
        double w = layer._w[o * layer._in._a.length + i];
        w = ((w - mean) / sigma) * 200;
        if( w >= 0 )
          start[i] = ((int) Math.min(+w, 255)) << 8;
        else
          start[i] = ((int) Math.min(-w, 255)) << 16;
      }

      BufferedImage out = new BufferedImage(Sample07_NeuralNet_Mnist.EDGE, Sample07_NeuralNet_Mnist.EDGE,
          BufferedImage.TYPE_INT_RGB);
      WritableRaster r = out.getRaster();
      r.setDataElements(0, 0, Sample07_NeuralNet_Mnist.EDGE, Sample07_NeuralNet_Mnist.EDGE, start);

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
}
