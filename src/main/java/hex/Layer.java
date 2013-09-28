package hex;

import hex.rng.MersenneTwisterRNG;

import java.util.Random;

import water.Iced;
import water.fvec.Chunk;
import water.fvec.Frame;

/**
 * Neural network layer.
 */
public abstract class Layer extends Iced {
  // Initial parameters
  public float _rate;
  public float _rateAnnealing;

  @ParamsSearch.Info(origin = 1)
  public float _momentum;
  public float _momentumAnnealing;

  public float _perWeight;
  public float _perWeightAnnealing;

  public float _l2;

  // Current rate and momentum
  transient float _r, _m;

  // Weights, biases, activity, error
  transient float[] _w, _b, _a, _e;

  // Last weights & per-weight rate data
  transient float[] _wPrev, _wInit, _wMult;
  transient float[] _bPrev, _bInit, _bMult;

  transient float[] _wSpeed, _bSpeed;

  // Previous layer
  transient Layer _in;

  // Optional visible units bias, e.g. for pre-training
  transient float[] _v, _gv;

  public final void init(Layer in, int units) {
    init(in, units, true, 0);
  }

  public void init(Layer in, int units, boolean weights, long step) {
    _a = new float[units];
    _e = new float[units];
    _in = in;

    if( weights ) {
      _w = new float[units * in._a.length];
      _b = new float[units];
    }

    if( _momentum != 0 ) {
      _wPrev = new float[_w.length];
      _bPrev = new float[_b.length];
      for( int i = 0; i < _w.length; i++ )
        _wPrev[i] = _w[i];
      _wSpeed = new float[_w.length];
      _bSpeed = new float[_b.length];
    }

    if( _perWeight != 0 ) {
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

    anneal(step);
  }

  public void randomize() {
    // deeplearning.net tutorial (TODO figure out one for rectifiers)
    Random rand = new MersenneTwisterRNG(MersenneTwisterRNG.SEEDS);
    float min = (float) -Math.sqrt(6. / (_in._a.length + _a.length));
    float max = (float) +Math.sqrt(6. / (_in._a.length + _a.length));
    for( int i = 0; i < _w.length; i++ )
      _w[i] = rand(rand, min, max);
  }

  abstract void fprop();

  abstract void bprop();

  public final void anneal(long n) {
    _r = _rate / (1 + _rateAnnealing * n);
    _m = _momentum * (n + 1) / ((n + 1) + _momentumAnnealing);
  }

  public final void momentum(long n) {
    for( int i = 0; i < _w.length; i++ )
      adjust(i, _w, _wPrev, _wInit, _wMult);

    for( int i = 0; i < _b.length; i++ )
      adjust(i, _b, _bPrev, _bInit, _bMult);
  }

  private final void adjust(int i, float[] w, float[] prev, float[] init, float[] mult) {
    w[i] *= 1 - _l2;
    float coef = 1;

    if( init != null ) {
      float g = w[i] - init[i];
      boolean sign = g > 0;
      boolean last = mult[i] > 0;
      coef = Math.abs(mult[i]);
      // If the gradient kept its sign, increase
      if( sign == last ) {
        if( coef < 4 )
          coef += _perWeight;
      } else
        coef *= 1 - _perWeight;
      mult[i] = sign ? coef : -coef;
      w[i] = init[i] + coef * g;
    }

    if( prev != null ) {
      // Nesterov's Accelerated Gradient
      float v = (w[i] - prev[i]) * _m;
      prev[i] = w[i];
      w[i] += coef * v;
      if( w == _w )
        _wSpeed[i] = v;
      else
        _bSpeed[i] = v;
    }

    if( init != null )
      init[i] = w[i];
  }

  public static abstract class Input extends Layer {
    long _row, _len;

    @Override public void init(Layer in, int units, boolean weights, long step) {
      _a = new float[units];
    }

    abstract int label();

    @Override void bprop() {
      throw new UnsupportedOperationException();
    }

    public final long move() {
      return _row = _row == _len - 1 ? 0 : _row + 1;
    }
  }

  public static class FrameInput extends Input {
    public Frame _frame;
    transient Chunk[] _caches;

    // TODO temp until stats are propagated with vecs
    public float[] _means, _sigmas;

    public FrameInput() {
    }

    public FrameInput(Frame frame) {
      this(frame, null, null);
    }

    public FrameInput(Frame frame, float[] means, float[] sigmas) {
      _frame = frame;
      _len = frame.numRows();

      if( means != null ) {
        _means = means;
        _sigmas = sigmas;
      } else {
        _means = new float[frame.numCols() - 1];
        _sigmas = new float[_means.length];
        for( int i = 0; i < _means.length; i++ ) {
          _means[i] = (float) frame.vecs()[i].mean();
          _sigmas[i] = (float) frame.vecs()[i].sigma();
        }
      }
    }

    @Override int label() {
      return (int) _frame.vecs()[_frame.numCols() - 1].at8(_row);
    }

    @Override void fprop() {
      for( int i = 0; i < _a.length; i++ ) {
        Chunk chunk = chunk(i, _row);
        double d = chunk.at(_row);
        d -= _means[i];
        d = _sigmas[i] > 1e-4 ? d / _sigmas[i] : d;
        _a[i] = (float) d;
      }
    }

    private final Chunk chunk(int i, long n) {
      if( _caches == null )
        _caches = new Chunk[_frame.vecs().length];
      Chunk c = _caches[i];
      if( c != null && c._start <= n && n < c._start + c._len )
        return c;
      return _caches[i] = _frame.vecs()[i].chunk(n);
    }
  }

  public static class ChunksInput extends Input {
    transient Chunk[] _chunks;
    float[] _means, _sigmas;

    public ChunksInput() {
    }

    public ChunksInput(Chunk[] chunks, float[] means, float[] sigmas) {
      _chunks = chunks;
      _means = means;
      _sigmas = sigmas;
    }

    @Override int label() {
      return (int) _chunks[_chunks.length - 1].at80((int) _row);
    }

    @Override void fprop() {
      for( int i = 0; i < _a.length; i++ ) {
        double d = _chunks[i].at0((int) _row);
        d -= _means[i];
        d = _sigmas[i] > 1e-4 ? d / _sigmas[i] : d;
        _a[i] = (float) d;
      }
    }
  }

  public static class Softmax extends Layer {
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

  public static class Tanh extends Layer {
    @Override void fprop() {
      for( int o = 0; o < _a.length; o++ ) {
        _a[o] = 0;
        for( int i = 0; i < _in._a.length; i++ )
          _a[o] += _w[o * _in._a.length + i] * _in._a[i];
        _a[o] += _b[o];

        // tanh approx, slightly faster, untested
        // float a = Math.abs(_a[o]);
        // float b = 12 + a * (6 + a * (3 + a));
        // _a[o] = (_a[o] * b) / (a * b + 24);

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
          if( _in._e != null )
            _in._e[i] += g * _w[w];
          _w[w] += u * _in._a[i];
        }
        _b[o] += u;
      }
    }
  }

  public static class Rectifier extends Layer {
    @Override public void randomize() {
      super.randomize();

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
          if( _in._e != null )
            _in._e[i] += g * _w[w];
          _w[w] += u * _in._a[i];
        }
        _b[o] += u;
      }
    }
  }

  // Cloning

  @Override public Layer clone() {
    return (Layer) super.clone();
  }

  public static Layer[] clone(Layer[] ls, Input input, long step) {
    Layer[] clones = new Layer[ls.length];
    clones[0] = input;
    for( int y = 1; y < ls.length; y++ ) {
      clones[y] = ls[y].clone();
      clones[y].init(clones[y - 1], ls[y]._b.length, false, step);
    }
    return clones;
  }

  // If layer is a RBM

  /**
   * TODO inject noise in units <br>
   * mean 0 and variance 1 / ( 1 + e-x )
   */
  void contrastiveDivergence(float[] in) {
//    float[] v1 = in;
//    float[] h1 = new float[_b.length];
//    fprop(v1, h1);
//    float[] v2 = generate(h1);
//    float[] h2 = new float[_b.length];
//    fprop(v2, h2);

//    for( int o = 0; o < _b.length; o++ )
//      for( int i = 0; i < _v.length; i++ )
//        _gw[o * _v.length + i] += _rate * ((h1[o] * v1[i]) - (h2[o] * v2[i]));
//
//    for( int o = 0; o < _gb.length; o++ )
//      _gb[o] += _rate * (h1[o] - h2[o]);

//    for( int i = 0; i < _gv.length; i++ )
//      _gv[i] += _rate * (v1[i] - v2[i]);
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

  private static float rand(Random rand, float min, float max) {
    return min + rand.nextFloat() * (max - min);
  }
}