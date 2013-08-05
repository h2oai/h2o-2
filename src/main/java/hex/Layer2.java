package hex;

import hex.rng.MersenneTwisterRNG;

import java.util.Random;

import water.fvec.Frame;

/**
 * Neural network layer, can be used as one level of Perceptron, AA or RBM.
 */
public abstract class Layer2 {
  // Initial parameters
  float _rate = .0005f;
  float _momentum = .1f; // 1 - value, as parameter search is 0 based
  float _annealing = .00001f;
  float _l2 = .0001f;
  boolean _auto = false;
  float _autoDelta = .2f;

  // Current rate
  float _r;

  // Weights, biases, activity, error
  float[] _w, _b, _a, _e;

  // Last weights & auto rate data
  float[] _wLast, _wInit, _wMult;
  float[] _bLast, _bInit, _bMult;

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

    _wLast = new float[_w.length];
    _bLast = new float[_b.length];

    _in = in;
  }

  void init() {
    // deeplearning.net tutorial (TODO figure out one for rectifiers)
    Random rand = new MersenneTwisterRNG(MersenneTwisterRNG.SEEDS);
    float min = (float) -Math.sqrt(6. / (_in._a.length + _a.length));
    float max = (float) +Math.sqrt(6. / (_in._a.length + _a.length));
    for( int i = 0; i < _w.length; i++ )
      _w[i] = rand(rand, min, max);

    if( _auto ) {
      _wInit = new float[_w.length];
      _wMult = new float[_w.length];
      for( int i = 0; i < _wMult.length; i++ )
        _wMult[i] = 1;
      _bInit = new float[_b.length];
      _bMult = new float[_b.length];
      for( int i = 0; i < _bMult.length; i++ )
        _bMult[i] = 1;
    }
  }

  abstract void fprop(int off, int len);

  abstract void bprop(int off, int len);

  public final void adjust(long n) {
    for( int i = 0; i < _w.length; i++ )
      adjust(i, _w, _wLast, _wInit, _wMult);

    for( int i = 0; i < _b.length; i++ )
      adjust(i, _b, _bLast, _bInit, _bMult);

    _r = _rate / (1 + _annealing * n);
  }

  private final void adjust(int i, float[] w, float[] last, float[] init, float[] mult) {
    w[i] *= 1 - _l2;
    if( !_auto ) {
      float m = (w[i] - last[i]) * (1 - _momentum);
      last[i] = w[i];
      w[i] += m;
    } else {
      float g = w[i] - init[i];
      boolean sign = g > 0;
      boolean prev = mult[i] > 0;
      float abs = Math.abs(mult[i]);
      // If the gradient kept its sign, increase
      if( sign == prev ) {
        if( abs < 4 )
          abs += _autoDelta;
      } else
        abs *= 1 - _autoDelta;
      mult[i] = sign ? abs : -abs;
      w[i] = init[i] + abs * g;
      float m = (w[i] - last[i]) * (1 - _momentum);
      last[i] = w[i];
      w[i] += abs * m;
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

    @Override void bprop(int off, int len) {
      throw new UnsupportedOperationException();
    }
  }

  public static class FrameInput extends Input {
    private final Frame _frame;

    public FrameInput(Frame frame) {
      super(frame.numCols());
      _frame = frame;
      _count = frame.numRows();
    }

    @Override int label() {
      return (int) _frame._vecs[_frame._vecs.length - 1].at8(_n);
    }

    @Override void fprop(int off, int len) {
      for( int i = 0; i < _frame._vecs.length - 1; i++ )
        _a[i] = _frame._vecs[i].at8(_n);
    }
  }

  public static class Tanh extends Layer2 {
    public Tanh() {
    }

    public Tanh(Layer2 in, int len) {
      super(in, len);
    }

    @Override void fprop(int off, int len) {
      for( int o = off; o < off + len; o++ ) {
        _a[o] = 0;
        for( int i = 0; i < _in._a.length; i++ )
          _a[o] += _w[o * _in._a.length + i] * _in._a[i];
        _a[o] += _b[o];
        _a[o] = (float) Math.tanh(_a[o]);
      }
    }

    @Override void bprop(int off, int len) {
      for( int o = off; o < off + len; o++ ) {
        float d = _e[o] * (1 - _a[o] * _a[o]);
        float u = _r * d;
        for( int i = 0; i < _in._a.length; i++ ) {
          _w[o * _in._a.length + i] += u * _in._a[i];
          if( _in._e != null ) {
            _in._e[i] += d * _w[o * _in._a.length + i];
          }
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
        float u = _r * d;
        for( int i = 0; i < _in._a.length; i++ ) {
          _w[o * _in._a.length + i] += u * _in._a[i];
          if( _in._e != null ) {
            _in._e[i] += d * _w[o * _in._a.length + i];
          }
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