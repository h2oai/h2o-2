package hex;

import hex.rng.MersenneTwisterRNG;

import java.lang.reflect.Field;
import java.util.Random;

import water.Iced;
import water.fvec.*;

import com.google.gson.*;

/**
 * Neural network layer, can be used as one level of Perceptron, AA or RBM.
 */
public abstract class Layer extends Iced {
  // Initial parameters
  float _rate = .01f;
  float _rateAnnealing = 0;

  @ParamsSearch.Info(origin = 1)
  float _momentum = 0;
  float _momentumAnnealing = 0;

  float _perWeight = 0;
  float _perWeightAnnealing = 0;

  float _l2 = .0001f;

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

  void init(Layer in, int len) {
    _w = new float[len * in._a.length];
    _b = new float[len];
    _a = new float[len];
    _e = new float[len];
    _wSpeed = new float[_w.length];
    _bSpeed = new float[_b.length];
    _in = in;

    if( _momentum != 0 ) {
      _wPrev = new float[_w.length];
      _bPrev = new float[_b.length];
      for( int i = 0; i < _w.length; i++ )
        _wPrev[i] = _w[i];
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

    adjust(0);
  }

  void randomize() {
    // deeplearning.net tutorial (TODO figure out one for rectifiers)
    Random rand = new MersenneTwisterRNG(MersenneTwisterRNG.SEEDS);
    float min = (float) -Math.sqrt(6. / (_in._a.length + _a.length));
    float max = (float) +Math.sqrt(6. / (_in._a.length + _a.length));
    for( int i = 0; i < _w.length; i++ )
      _w[i] = rand(rand, min, max);
  }

  abstract void fprop();

  abstract void bprop();

  public final void adjust(long n) {
    for( int i = 0; i < _w.length; i++ )
      adjust(i, _w, _wPrev, _wInit, _wMult);

    for( int i = 0; i < _b.length; i++ )
      adjust(i, _b, _bPrev, _bInit, _bMult);

    _r = _rate / (1 + _rateAnnealing * n);
    _m = _momentum * (n + 1) / ((n + 1) + _momentumAnnealing);
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
      // Nesterov Accelerated Gradient
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
    long _count;
    long _n;

    @Override void init(Layer in, int len) {
      _a = new float[len];
    }

    abstract int label();

    @Override void bprop() {
      throw new UnsupportedOperationException();
    }
  }

  public static class FrameInput extends Input {
    Frame _frame;
    boolean _normalize;
    transient Chunk[] _caches;

    public FrameInput() {
    }

    public FrameInput(Frame frame, boolean normalize) {
      _frame = frame;
      _normalize = normalize;
      _count = frame.numRows();
    }

    @Override int label() {
      return (int) _frame._vecs[_frame.numCols() - 1].at8(_n);
    }

    @Override void fprop() {
      for( int i = 0; i < _a.length; i++ ) {
        Chunk chunk = chunk(i, _n);
        double d = chunk.at(_n);
        if( _normalize ) {
          Vec v = _frame._vecs[i];
          d = (d - v.mean()) / v.sigma();
        }
        _a[i] = (float) d;
      }
    }

    private final Chunk chunk(int i, long n) {
      if( _caches == null )
        _caches = new Chunk[_frame._vecs.length];
      Chunk c = _caches[i];
      if( c != null && c._start <= n && n < c._start + c._len )
        return c;
      return _caches[i] = _frame._vecs[i].chunk(n);
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
          _w[w] += u * _in._a[i];
        }
        _b[o] += u;
      }
    }
  }

  public static class TanhDeep extends Tanh {
    @Override void bprop() {
      for( int o = 0; o < _a.length; o++ ) {
        float g = _e[o] * (1 - _a[o] * _a[o]);
        float u = _r * g;
        for( int i = 0; i < _in._a.length; i++ ) {
          int w = o * _in._a.length + i;
          _in._e[i] += g * _w[w]; // Deep: also set previous error
          _w[w] += u * _in._a[i];
        }
        _b[o] += u;
      }
    }
  }

  public static class Rectifier extends Layer {
    @Override void randomize() {
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

  //

  public static String json(Object o) {
    Gson gson = new GsonBuilder().setFieldNamingStrategy(new FieldNamingStrategy() {
      @Override public String translateName(Field f) {
        String result = "";
        String name = f.getName().substring(1);
        for( char ch : name.toCharArray() )
          result += ((Character.isUpperCase(ch)) ? "_" : "") + Character.toLowerCase(ch);
        return result;
      }
    }).setPrettyPrinting().create();
    return gson.toJson(o);
  }

  public static <T> T json(String json, Class<T> c) {
    Gson gson = new GsonBuilder().setFieldNamingStrategy(new FieldNamingStrategy() {
      @Override public String translateName(Field f) {
        String result = "";
        String name = f.getName().substring(1);
        for( char ch : name.toCharArray() )
          result += ((Character.isUpperCase(ch)) ? "_" : "") + Character.toLowerCase(ch);
        return result;
      }
    }).setPrettyPrinting().create();
    return gson.fromJson(json, c);
  }
}