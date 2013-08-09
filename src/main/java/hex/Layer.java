package hex;

import hex.Layer2.Input;
import hex.rng.MersenneTwisterRNG;

import java.util.Random;

import water.fvec.Frame;
import water.fvec.Vec;

/**
 * Neural network layer, can be used as one level of Perceptron, AA or RBM.
 */
public abstract class Layer {
  float _rate = .0001f;
  float _momentum = .1f; // 1 - value, as parameter search is 0 based
  float _annealing = .0001f;
  float _l2 = .0001f;
  boolean _auto = false;
  float _autoDelta = .2f;

  // Previous layer
  transient Layer _in;

  // Weights, biases, activity, error
  transient float[] _w, _b, _a, _e;

  // Gradients & auto rate data
  transient float[] _gw, _gwMult, _gwMtum;
  transient float[] _gb, _gbMult, _gbMtum;

  // Optional visible units bias, e.g. for pre-training
  transient float[] _v, _gv;

  public Layer() {
  }

  public Layer(Layer in, int len) {
    _w = new float[len * in._a.length];
    _b = new float[len];
    _a = new float[len];
    _e = new float[len];
    _gw = new float[len * in._a.length];
    _gb = new float[len];

    _in = in;
  }

  void init(boolean skipWeights) {
    // deeplearning.net tutorial (TODO figure out one for rectifiers)
    Random rand = new MersenneTwisterRNG(MersenneTwisterRNG.SEEDS);
    float min = (float) -Math.sqrt(6. / (_in._a.length + _a.length));
    float max = (float) +Math.sqrt(6. / (_in._a.length + _a.length));
    for( int i = 0; i < _w.length; i++ )
      _w[i] = rand(rand, min, max);

    if( _auto ) {
      _gwMult = new float[_w.length];
      _gwMtum = new float[_w.length];
      for( int i = 0; i < _gwMult.length; i++ )
        _gwMult[i] = 1;
      _gbMult = new float[_b.length];
      _gbMtum = new float[_b.length];
      for( int i = 0; i < _gbMult.length; i++ )
        _gbMult[i] = 1;
    }
  }

  abstract void fprop(int off, int len);

  abstract void bprop(int off, int len);

  public final void adjust(long n) {
    float rate = _rate / (1 + _annealing * n);

    for( int w = 0; w < _gw.length; w++ ) {
      _gw[w] -= _l2 * _w[w];
      _w[w] += rate * delta(_gw, _gwMult, _gwMtum, w);
    }

    for( int b = 0; b < _gb.length; b++ ) {
      _gb[b] -= _l2 * _b[b];
      _b[b] += rate * delta(_gb, _gbMult, _gbMtum, b);
    }
  }

  /**
   * Adjusts learning rate using a multiplier per parameter.
   */
  private final float delta(float[] g, float[] mult, float[] mtum, int i) {
    float res;
    if( _auto ) {
      boolean sign = g[i] > 0;
      boolean last = mult[i] > 0;
      float abs = Math.abs(mult[i]);
      // If the gradient kept its sign, increase
      if( sign == last ) {
        if( abs < 4 )
          abs += _autoDelta;
      } else {
        abs *= 1 - _autoDelta;
      }
      mtum[i] += g[i];
      res = mtum[i] * abs;
      mtum[i] *= 1 - _momentum;
      mult[i] = sign ? abs : -abs;
      g[i] = 0;
    } else {
      res = g[i];
      g[i] *= 1 - _momentum;
    }
    return res;
  }

  public static abstract class Input extends Layer {
    long _count;
    long _n;

    public Input() {
    }

    public Input(int len) {
      _a = new float[len];
    }

    @Override void init(boolean skipWeights) {
    }

    abstract int label();

    @Override void bprop(int off, int len) {
      throw new UnsupportedOperationException();
    }
  }

  public static class FrameInput extends Input {
    final Frame _frame;
    final boolean _normalize;

    public FrameInput(Frame frame, boolean normalize) {
      super(frame.numCols() - 1);
      _frame = frame;
      _normalize = normalize;
      _count = frame.numRows();
    }

    @Override int label() {
      return (int) _frame._vecs[_frame.numCols() - 1].at8(_n);
    }

    @Override void fprop(int off, int len) {
      for( int i = 0; i < _a.length; i++ ) {
        Vec v = _frame._vecs[i];
        double d = v.at(_n);
        if( _normalize )
          d = (d - v.mean()) / v.sigma();
        _a[i] = (float) d;
      }
    }
  }

  public static class Softmax extends Layer {
    public Softmax() {
    }

    public Softmax(Layer in, int len) {
      super(in, len);
    }

    @Override void fprop(int off, int len) {
      float max = Float.NEGATIVE_INFINITY;
      for( int o = 0; o < _a.length; o++ ) {
        _a[o] = 0;
        for( int i = 0; i < _in._a.length; i++ )
          _a[o] += _w[o * _in._a.length + i] * _in._a[i];
        _a[o] += _b[o];
        if( max < _a[o] )
          max = _a[o];
      }
      float scale = 0;
      for( int o = 0; o < _a.length; o++ ) {
        _a[o] = (float) Math.exp(_a[o] - max);
        scale += _a[o];
      }
      for( int o = 0; o < _a.length; o++ )
        _a[o] /= scale;
    }

    @Override void bprop(int off, int len) {
      for( int o = 0; o < _a.length; o++ ) {
        // Gradient is error * derivative of Softmax: (1 - x) * x
        float g = _e[o] * (1 - _a[o]) * _a[o];
        for( int i = 0; i < _in._a.length; i++ ) {
          int w = o * _in._a.length + i;
          _in._e[i] += g * _w[w];
          _gw[w] += g * _in._a[i];
        }
        _gb[o] += g;
      }
    }
  }

  public static class Tanh extends Layer {
    public Tanh() {
    }

    public Tanh(Layer in, int len) {
      super(in, len);
    }

    @Override void fprop(int off, int len) {
      for( int o = off; o < off + len; o++ ) {
        _a[o] = 0;
        for( int i = 0; i < _in._a.length; i++ )
          _a[o] += _w[o * _in._a.length + i] * _in._a[i];
        _a[o] += _b[o];
        //_a[o] = (float) (1.7159 * Math.tanh(0.66666667 * _a[o]));
        _a[o] = (float) Math.tanh(_a[o]);
      }
    }

    @Override void bprop(int off, int len) {
      for( int o = off; o < off + len; o++ ) {
        float g = _e[o] * (1 - _a[o] * _a[o]);
        // float d = (float) (0.66666667 / 1.7159 * (1.7159 + _a[o]) * (1.7159 - _a[o]));
        for( int i = 0; i < _in._a.length; i++ ) {
          _gw[o * _in._a.length + i] += g * _in._a[i];
          if( _in._e != null ) {
            _in._e[i] += g * _w[o * _in._a.length + i];
          }
        }
        _gb[o] += g;
      }
    }
  }

  public static class Rectifier extends Layer {
    public Rectifier() {
    }

    public Rectifier(Layer in, int len) {
      super(in, len);
    }

    @Override void init(boolean skipWeights) {
      super.init(skipWeights);

      for( int i = 0; i < _b.length; i++ )
        _b[i] = 1;
      for( int i = 0; _v != null && i < _v.length; i++ )
        _v[i] = 1;
    }

    @Override void fprop(int off, int len) {
      for( int o = off; o < off + len; o++ ) {
        _a[o] = 0;
        for( int i = 0; i < _in._a.length; i++ )
          _a[o] += _w[o * _in._a.length + i] * _in._a[i];
        _a[o] += _b[o];
        if( _a[o] < 0 ) {
          _a[o] = 0;
        }
      }
    }

    @Override void bprop(int off, int len) {
      for( int o = off; o < off + len; o++ ) {
        float d = _e[o];
        if( _a[o] < 0 )
          d = 0;
        for( int i = 0; i < _in._a.length; i++ ) {
          _gw[o * _in._a.length + i] += d * _in._a[i];
          if( _in._e != null ) {
            _in._e[i] += d * _w[o * _in._a.length + i];
          }
        }
        _gb[o] += d;
      }
    }
  }

  // If layer is a RBM

  /**
   * TODO inject noise in units <br>
   * mean 0 and variance 1 / ( 1 + e-x )
   */
  void contrastiveDivergence(float[] in) {
    float[] v1 = in;
    float[] h1 = new float[_b.length];
//    fprop(v1, h1);
    float[] v2 = generate(h1);
    float[] h2 = new float[_b.length];
//    fprop(v2, h2);

    for( int o = 0; o < _b.length; o++ )
      for( int i = 0; i < _v.length; i++ )
        _gw[o * _v.length + i] += _rate * ((h1[o] * v1[i]) - (h2[o] * v2[i]));

    for( int o = 0; o < _gb.length; o++ )
      _gb[o] += _rate * (h1[o] - h2[o]);

    for( int i = 0; i < _gv.length; i++ )
      _gv[i] += _rate * (v1[i] - v2[i]);
  }

  final void adjustVisible() {
    if( _gv != null ) {
      for( int v = 0; v < _gv.length; v++ ) {
        _v[v] += _gv[v];
        _gv[v] *= 1 - _momentum;
      }
    }
  }

  float[] generate(float[] hidden) {
    assert hidden.length == _b.length;
    float[] visible = new float[_v.length];
    for( int o = 0; o < hidden.length; o++ )
      for( int i = 0; i < _in._a.length; i++ )
        visible[i] += _w[o * _in._a.length + i] * hidden[o];
    for( int i = 0; i < visible.length; i++ ) {
      visible[i] += _v[i];
      if( visible[i] < 0 )
        visible[i] = 0;
    }
    return visible;
  }

  float error(float[] in1) {
    float[] out1 = new float[_b.length];
//    fprop(in1, out1);
    float[] in2 = generate(out1);
    float error = 0;
    for( int i = 0; i < in1.length; i++ ) {
      float d = in2[i] - in1[i];
      error += d * d;
    }
    return error;
  }

  float freeEnergy(float[] in) {
    float energy = 0.0f;
    for( int i = 0; i < in.length; i++ )
      energy -= in[i] * _v[i];
    for( int o = 0; o < _b.length; o++ ) {
      float out = 0;
      for( int i = 0; i < in.length; i++ )
        out += _w[o * in.length + i] * in[i];
      out += _b[o];
      energy -= Math.log(1 + Math.exp(out));
    }
    return energy;
  }

  //

  private static float rand(Random rand, float min, float max) {
    return min + rand.nextFloat() * (max - min);
  }
}