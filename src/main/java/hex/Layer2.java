package hex;

import hex.rng.MersenneTwisterRNG;

import java.util.Random;

import water.fvec.Frame;
import water.fvec.Vec;

/**
 * Neural network layer, can be used as one level of Perceptron, AA or RBM.
 */
public abstract class Layer2 {
  // Initial parameters
  float _rate = .001f;
  float _oneMinusMomentum = 1f; // 1 - value, as parameter search is 0 based
  float _annealing = .0f;
  float _l2 = .0001f;
  boolean _auto = false;
  float _autoDelta = .2f;

  // Current rate and momentum
  float _r, _m;

  // Weights, biases, activity, error
  float[] _w, _b, _a, _e;

  // Last weights & auto rate data
  float[] _wPrev, _wInit, _wMult;
  float[] _bPrev, _bInit, _bMult;

  // Previous layer
  Layer2 _in;

  // Optional visible units bias, e.g. for pre-training
  float[] _v, _gv;

  public Layer2() {
  }

  public Layer2(Layer2 in, int len) {
    _w = new float[len * in._a.length];
    _b = new float[len];
    _a = new float[len];
    _e = new float[len];

    _wPrev = new float[_w.length];
    _bPrev = new float[_b.length];

    _in = in;
  }

  void init() {
    // deeplearning.net tutorial (TODO figure out one for rectifiers)
    Random rand = new MersenneTwisterRNG(MersenneTwisterRNG.SEEDS);
    float min = (float) -Math.sqrt(6. / (_in._a.length + _a.length));
    float max = (float) +Math.sqrt(6. / (_in._a.length + _a.length));
    for( int i = 0; i < _w.length; i++ ) {
      _w[i] = rand(rand, min, max);
      _wPrev[i] = _w[i];
    }

    if( _auto ) {
      _wInit = new float[_w.length];
      _wMult = new float[_w.length];
      for( int i = 0; i < _w.length; i++ ) {
        _wInit[i] = _w[i];
        _wMult[i] = 1;
      }
      _bInit = new float[_b.length];
      _bMult = new float[_b.length];
      for( int i = 0; i < _b.length; i++ ) {
        _bInit[i] = _b[i];
        _bMult[i] = 1;
      }
    }

    adjust(0);
  }

  abstract void fprop();

  abstract void bprop();

  public final void adjust(long n) {
    for( int i = 0; i < _w.length; i++ )
      adjust(i, _w, _wPrev, _wInit, _wMult);

    for( int i = 0; i < _b.length; i++ )
      adjust(i, _b, _bPrev, _bInit, _bMult);

    _r = _rate;
    _m = 1 - _oneMinusMomentum;
//    _r = _rate / (1 + _annealing * n);
//    _m = (1 - _oneMinusMomentum) * n / (n + 3);
  }

  private final void adjust(int i, float[] w, float[] prev, float[] init, float[] mult) {
    w[i] *= 1 - _l2;
    if( !_auto ) {
      // Nesterov Accelerated Gradient
      float v = (w[i] - prev[i]) * _m;
      prev[i] = w[i];
      w[i] += v;
    } else {
      float g = w[i] - init[i];
      boolean sign = g > 0;
      boolean last = mult[i] > 0;
      float abs = Math.abs(mult[i]);
      // If the gradient kept its sign, increase
      if( sign == last ) {
        if( abs < 4 )
          abs += _autoDelta;
      } else
        abs *= 1 - _autoDelta;
      mult[i] = sign ? abs : -abs;
      w[i] = init[i] + abs * g;
      float v = (w[i] - prev[i]) * _m;
      prev[i] = w[i];
      w[i] += abs * v;
      init[i] = w[i];
    }
  }

  public static abstract class Input extends Layer2 {
    long _count;
    long _n;

    public Input() {
    }

    public Input(int len) {
      _a = new float[len];
    }

    @Override void init() {
      //
    }

    abstract int label();

    @Override void bprop() {
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

    @Override void fprop() {
      for( int i = 0; i < _a.length; i++ ) {
        Vec v = _frame._vecs[i];
        double d = v.at(_n);
        if( _normalize )
          d = (d - v.mean()) / v.sigma();
        _a[i] = (float) d;
      }
    }
  }

  public static class Softmax extends Layer2 {
    public Softmax() {
    }

    public Softmax(Layer2 in, int len) {
      super(in, len);
    }

    @Override void fprop() {
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

    @Override void bprop() {
      for( int o = 0; o < _a.length; o++ ) {
        // Gradient is error * derivative of Softmax: (1 - x) * x
        float g = _e[o] * (1 - _a[o]) * _a[o];
        float u = _r * g;
        for( int i = 0; i < _in._a.length; i++ ) {
          int w = o * _in._a.length + i;
          _in._e[i] += g * _w[w];
          _w[w] += u * _in._a[i];
        }
        _b[o] += u;
      }
    }
  }

  public static class Tanh extends Layer2 {
    public Tanh() {
    }

    public Tanh(Layer2 in, int len) {
      super(in, len);
    }

    @Override void fprop() {
      for( int o = 0; o < _a.length; o++ ) {
        _a[o] = 0;
        for( int i = 0; i < _in._a.length; i++ )
          _a[o] += _w[o * _in._a.length + i] * _in._a[i];
        _a[o] += _b[o];
        _a[o] = (float) Math.tanh(_a[o]);
      }
    }

    @Override void bprop() {
      for( int o = 0; o < _a.length; o++ ) {
        // Gradient is error * derivative of hyperbolic tangent: (1 - x^2)
        float g = _e[o] * (1 - _a[o] * _a[o]);
        float u = _r * g;
        for( int i = 0; i < _in._a.length; i++ ) {
          int w = o * _in._a.length + i;
          _w[w] += u * _in._a[i];
          if( _in._e != null )
            _in._e[i] += g * _w[w];
        }
        _b[o] += u;
      }
    }
  }

  public static class Rectifier extends Layer2 {
    public Rectifier() {
    }

    public Rectifier(Layer2 in, int len) {
      super(in, len);
    }

    @Override void init() {
      super.init();

      for( int i = 0; i < _b.length; i++ )
        _b[i] = 1;
      for( int i = 0; _v != null && i < _v.length; i++ )
        _v[i] = 1;
    }

    @Override void fprop() {
      for( int o = 0; o < _a.length; o++ ) {
        _a[o] = 0;
        for( int i = 0; i < _in._a.length; i++ )
          _a[o] += _w[o * _in._a.length + i] * _in._a[i];
        _a[o] += _b[o];
        if( _a[o] < 0 )
          _a[o] = 0;
      }
    }

    @Override void bprop() {
      for( int o = 0; o < _a.length; o++ ) {
        float g = _e[o];
        if( _a[o] < 0 )
          g = 0;
        float u = _r * g;
        for( int i = 0; i < _in._a.length; i++ ) {
          int w = o * _in._a.length + i;
          _w[w] += u * _in._a[i];
          if( _in._e != null )
            _in._e[i] += g * _w[w];
        }
        _b[o] += u;
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

//    for( int o = 0; o < _b.length; o++ )
//      for( int i = 0; i < _v.length; i++ )
//        _gw[o * _v.length + i] += _rate * ((h1[o] * v1[i]) - (h2[o] * v2[i]));
//
//    for( int o = 0; o < _gb.length; o++ )
//      _gb[o] += _rate * (h1[o] - h2[o]);

    for( int i = 0; i < _gv.length; i++ )
      _gv[i] += _rate * (v1[i] - v2[i]);
  }

  final void adjustVisible() {
    if( _gv != null ) {
      for( int v = 0; v < _gv.length; v++ ) {
        _v[v] += _gv[v];
        _gv[v] *= 1 - _oneMinusMomentum;
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