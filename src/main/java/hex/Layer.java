package hex;

import hex.rng.MersenneTwisterRNG;

import java.util.Random;

import com.amd.aparapi.Kernel;
import com.amd.aparapi.Range;

/**
 * Neural network layer, can be used as one level of Perceptron, AA or RBM.
 */
public abstract class Layer {
  static final int BATCH = 16;
  static final boolean AUTO_RATE = false;

  float momentum = 0.9f;
  float rate = 0.0001f;
  float l2 = 1f * rate;
  float minMult = 0, maxMult = 50;

  // Weights, biases, activity, error
  float[] _w, _b, _a, _e;

  // Gradients, last gradients, auto rate multipliers
  float[] _gw, _gwLast, _gwMult;
  float[] _gb, _gbLast, _gbMult;

  // In case _a is a dataset (Aparapi needs flat arrays)
  int _off, _len;

  // Previous layer
  Layer _in;

  // Optional visible units bias, e.g. for pre-training
  float[] _v, _gv;

  public Layer() {}

  public Layer(Layer in, int len) {
    _w = new float[len * in._len];
    _b = new float[len];
    _a = new float[len];
    _e = new float[len];

    _gw = new float[_w.length];
    _gb = new float[_b.length];

    _off = 0;
    _len = len;
    _in = in;

    if( AUTO_RATE ) {
      _gwLast = new float[_w.length];
      _gwMult = new float[_w.length];
      _gbLast = new float[_b.length];
      _gbMult = new float[_b.length];
      for( int i = 0; i < _gwMult.length; i++ )
        _gwMult[i] = 1;
      for( int i = 0; i < _gbMult.length; i++ )
        _gbMult[i] = 1;
    }
  }

  abstract void fprop(int off, int len);

  abstract void bprop(int off, int len);

  public final void adjust() {
    for( int w = 0; w < _gw.length; w++ ) {
      _gw[w] -= l2 * _w[w];
      _w[w] += rate * delta(_gw, _gwLast, _gwMult, w);
      _gw[w] *= momentum;
    }

    for( int b = 0; b < _gb.length; b++ ) {
      _b[b] += rate * delta(_gb, _gbLast, _gbMult, b);
      _gb[b] *= momentum;
    }
  }

  /**
   * Adjusts learning rate using a multiplier per parameter.
   */
  private final float delta(float[] g, float[] gLast, float[] gMult, int i) {
    float res = g[i];
    if( AUTO_RATE ) {
      // If the gradient kept its sign, increase
      if( g[i] * gLast[i] > 0 ) {
        gMult[i] += 0.05f;
        gMult[i] = Math.min(gMult[i], maxMult);
      }
      if( g[i] * gLast[i] < 0 ) {
        gMult[i] *= 0.95f;
        gMult[i] = Math.max(gMult[i], minMult);
      }
      gLast[i] = g[i];
      res *= gMult[i];
    }
    return res;
  }

  public static class Input extends Layer {
    public Input() {}

    public Input(float[] a, int len) {
      _a = a;
      _len = len;
    }

    @Override void fprop(int off, int len) {
      throw new UnsupportedOperationException();
    }

    @Override void bprop(int off, int len) {
      throw new UnsupportedOperationException();
    }
  }

  public static class Tanh extends Layer {
    public Tanh() {}

    public Tanh(Layer in, int len) {
      super(in, len);

      // deeplearning.net tutorial
      Random rand = new MersenneTwisterRNG(MersenneTwisterRNG.SEEDS);
      float min = (float) -Math.sqrt(6. / (in._len + len));
      float max = (float) +Math.sqrt(6. / (in._len + len));
      for( int i = 0; i < _w.length; i++ )
        _w[i] = rand(rand, min, max);
    }

    @Override void fprop(int off, int len) {
      for( int o = off; o < off + len; o++ ) {
        _a[o] = 0;
        for( int i = 0; i < _in._len; i++ )
          _a[o] += _w[o * _in._len + i] * _in._a[_in._off + i];
        _a[o] += _b[o];
        _a[o] = (float) Math.tanh(_a[o]);
      }
    }

    @Override void bprop(int off, int len) {
      for( int o = off; o < off + len; o++ ) {
        float d = _e[o] * (1 - _a[o] * _a[o]);
        for( int i = 0; i < _in._len; i++ ) {
          _gw[o * _in._len + i] += d * _in._a[_in._off + i];
          if( _in._e != null ) {
            _in._e[i] += d * _w[o * _in._len + i];
          }
        }
        _gb[o] += d;
      }
    }
  }

  public static class Rectifier extends Layer {
    public Rectifier() {}

    public Rectifier(Layer in, int len) {
      super(in, len);

      // TODO ?
      MersenneTwisterRNG rand = new MersenneTwisterRNG(MersenneTwisterRNG.SEEDS);
      float min = (float) -Math.sqrt(6. / (in._len + len));
      float max = (float) +Math.sqrt(6. / (in._len + len));
      for( int i = 0; i < _w.length; i++ )
        _w[i] = rand(rand, min, max);
      for( int i = 0; i < _b.length; i++ )
        _b[i] = 1;
      for( int i = 0; _v != null && i < _v.length; i++ )
        _v[i] = 1;
    }

    @Override void fprop(int off, int len) {
      for( int o = off; o < off + len; o++ ) {
        _a[o] = 0;
        for( int i = 0; i < _in._len; i++ )
          _a[o] += _w[o * _in._len + i] * _in._a[_in._off + i];
        _a[o] += _b[o];
        if( _a[o] < 0 ) {
          _a[o] = 0;
        }
      }
    }

    @Override void bprop(int off, int len) {
      for( int o = off; o < off + len; o++ ) {
        float d = _e[o];
        if( _a[o] < 0 ) d = 0;
        for( int i = 0; i < _in._len; i++ ) {
          _gw[o * _in._len + i] += d * _in._a[_in._off + i];
          if( _in._e != null ) {
            _in._e[i] += d * _w[o * _in._len + i];
          }
        }
        _gb[o] += d;
      }
    }
  }

  public class RectifierGPU extends Rectifier {
    static final boolean EXPLICIT = true;
    Kernel _fprop, _bprop;
    int[] _indexes = new int[2];

    public RectifierGPU(Layer in, int len) {
      super(in, len);

      _fprop = new Kernel() {
        float[] w = _w, b = _b, a = _a, inA = _in._a;
        // @Local
        int[] indexes = _indexes;
        int inLen = _in._len;

        @Override public void run() {
          int o = getGlobalId(0);
          int offset = indexes[0];
          int length = indexes[1];
          a[o] = 0;
          for( int i = 0; i < length; i++ )
            a[o] += w[o * inLen + i] * inA[offset + i];
          a[o] += b[o];
          a[o] = max(0, a[o]);
        }
      };
      _fprop.setExplicit(EXPLICIT);

      _bprop = new Kernel() {
        float[] w = _w, gw = _gw, gb = _gb, a = _a, e = _e, inA = _in._a, inE = _in._e;
        int inOff = _in._off;
        int inLen = _in._len;

        @Override public void run() {
          int o = getGlobalId(0);
          float d = e[o];
          if( a[o] < 0 ) d = 0;
          for( int i = 0; i < inLen; i++ ) {
            gw[o * inLen + i] += d * inA[inOff + i];
            if( inE != null ) {
              inE[i] += d * w[o * inLen + i];
            }
          }
          gb[o] += d;
        }
      };
      _bprop.setExplicit(EXPLICIT);
    }

    void close() {
      _fprop.dispose();
    }

    @Override void fprop(int off, int len) {
      _indexes[0] = off;
      _indexes[1] = len;
      _fprop.put(_indexes);
      _fprop.execute(Range.create(_a.length));
    }

    @Override void bprop(int off, int len) {
      _bprop.execute(Range.create(_in._e.length));
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
        _gw[o * _v.length + i] += rate * ((h1[o] * v1[i]) - (h2[o] * v2[i]));

    for( int o = 0; o < _gb.length; o++ )
      _gb[o] += rate * (h1[o] - h2[o]);

    for( int i = 0; i < _gv.length; i++ )
      _gv[i] += rate * (v1[i] - v2[i]);
  }

  final void adjustVisible() {
    if( _gv != null ) {
      for( int v = 0; v < _gv.length; v++ ) {
        _v[v] += _gv[v];
        _gv[v] *= momentum;
      }
    }
  }

  float[] generate(float[] hidden) {
    assert hidden.length == _b.length;
    float[] visible = new float[_v.length];
    for( int o = 0; o < hidden.length; o++ )
      for( int i = 0; i < _in._len; i++ )
        visible[i] += _w[o * _in._len + i] * hidden[o];
    for( int i = 0; i < visible.length; i++ ) {
      visible[i] += _v[i];
      if( visible[i] < 0 ) visible[i] = 0;
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