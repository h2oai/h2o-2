package hex;

import hex.Layer.Input;

import java.text.DecimalFormat;

import water.util.Utils;

public class NeuralNetTest {
  static final DecimalFormat _format = new DecimalFormat("0.000");
  static final int MAX_EVAL_COUNT = 1000;
  Layer[] _ls;
  Trainer _trainer;

  void init() {
  }

  void run() {
  }

  static class Error {
    double Value;
    double SqrDist;

    @Override public String toString() {
      return _format.format(100 * Value) + "% (d²:" + SqrDist + ")";
    }
  }

  static Error eval(Layer[] ls) {
    return eval(ls, (Input) ls[0]);
  }

  static Error eval(Layer[] ls, Input input) {
    Layer[] clones = new Layer[ls.length];
    clones[0] = Utils.clone(input);
    for( int i = 1; i < ls.length; i++ )
      clones[i] = Utils.deepClone(ls[i], "_in");
    for( int i = 1; i < ls.length; i++ )
      clones[i]._in = clones[i - 1];

    Error error = new Error();
    eval(clones, error);
    return error;
  }

  static void eval(Layer[] ls, Error error) {
    Input input = (Input) ls[0];
    long off = input._off;
    long len = (int) Math.min(input._len, MAX_EVAL_COUNT);
    int correct = 0;
    for( input._row = off; input._row < off + len; input._row++ )
      if( correct(ls, error) )
        correct++;
    error.Value = (len - (double) correct) / len;
  }

  private static boolean correct(Layer[] ls, Error error) {
    Input input = (Input) ls[0];
    for( int i = 0; i < ls.length; i++ )
      ls[i].fprop();
    float[] out = ls[ls.length - 1]._a;
    error.SqrDist = 0;
    for( int i = 0; i < out.length; i++ ) {
      float t = i == input.label() ? 1 : 0;
      float d = t - out[i];
      error.SqrDist += d * d;
    }
    float max = Float.MIN_VALUE;
    int idx = -1;
    for( int i = 0; i < out.length; i++ ) {
      if( out[i] > max ) {
        max = out[i];
        idx = i;
      }
    }
    return idx == input.label();
  }
}