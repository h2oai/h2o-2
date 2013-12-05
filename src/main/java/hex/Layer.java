package hex;

import hex.rng.MersenneTwisterRNG;
import hex.rng.XorShiftRNG;

import java.util.Random;

import water.*;
import water.api.DocGen;
import water.api.Request.API;
import water.fvec.Chunk;
import water.fvec.Vec;

/**
 * Neural network layer.
 *
 * @author cypof
 */
public abstract class Layer extends Iced {
  static final int API_WEAVER = 1;
  public static DocGen.FieldDoc[] DOC_FIELDS;

  @API(help = "Number of neurons")
  public int units;

  @API(help = "Learning rate")
  public float rate;

  @API(help = "Learning rate annealing")
  public float rate_annealing;

  @API(help = "L1 regularisation")
  public float l1;

  @API(help = "L2 regularisation")
  public float l2;

  @API(help = "Initial momentum value")
  public float momentum_start;

  @API(help = "Number of samples during which momentum value varies")
  @ParamsSearch.Info(origin = 1)
  public long momentum_ramp;

  @API(help = "Momentum value once ramp is over")
  public float momentum_stable;

  // TODO
  public float _perWeight;
  public float _perWeightAnnealing;

  // Weights, biases, activity, error
  // TODO hold transients only for current two layers
  protected transient float[] _w, _b, _a, _e;

  // Momentum for weights and biases
  protected transient float[] _wm, _bm;

  // Per-weight acceleration
  protected transient float[] _wp, _bp;

  // Previous and input layers
  transient Layer _previous;
  transient Input _input;

  /**
   * Start of refactoring in specification & running data, for layers and trainers.
   */
  static abstract class Training {
    abstract long processed();
  }

  transient Training _training;

  public final void init(Layer[] ls, int index) {
    init(ls, index, true, 0, new MersenneTwisterRNG(MersenneTwisterRNG.SEEDS));
  }

  public void init(Layer[] ls, int index, boolean weights, long step, Random rand) {
    _a = new float[units];
    _e = new float[units];
    _previous = ls[index - 1];
    _input = (Input) ls[0];

    if( weights ) {
      _w = new float[units * _previous.units];
      _b = new float[units];
      if( momentum_start != 0 || momentum_stable != 0 ) {
        _wm = new float[_w.length];
        _bm = new float[_b.length];
      }
    }

    if( _perWeight != 0 ) {
//      _wInit = new float[_w.length];
//      _wMult = new float[_w.length];
//      for( int i = 0; i < _w.length; i++ ) {
//        _wInit[i] = _w[i];
//        _wMult[i] = 1;
//      }
//      _bInit = new float[_b.length];
//      _bMult = new float[_b.length];
//      for( int i = 0; i < _b.length; i++ ) {
//        _bInit[i] = _b[i];
//        _bMult[i] = 1;
//      }
    }
  }

  public void close() {
  }

  protected abstract void fprop(boolean training);

  protected abstract void bprop();

  /**
   * Apply gradient g to unit u with rate r and momentum m.
   */
  protected final void bprop(int u, float g, float r, float m) {
    float r2 = 0;
    for( int i = 0; i < _previous._a.length; i++ ) {
      int w = u * _previous._a.length + i;
      if( _previous._e != null )
        _previous._e[i] += g * _w[w];
      float d = g * _previous._a[i] - _w[w] * l2 - Math.signum(_w[w]) * l1;
      if( _wm != null ) {
        _wm[w] *= m;
        _wm[w] = d = _wm[w] + d;
      }
      _w[w] += r * d;
      r2 += _w[w] * _w[w];
    }
    if( r2 > 15 ) { // C.f. Improving neural networks by preventing co-adaptation of feature detectors
      float scale = (float) Math.sqrt(15 / r2);
      for( int i = 0; i < _previous._a.length; i++ ) {
        int w = u * _previous._a.length + i;
        _w[w] *= scale;
      }
    }
    float d = g;
    if( _bm != null ) {
      _bm[u] *= m;
      _bm[u] = d = _bm[u] + d;
    }
    _b[u] += r * d;
  }

  public float rate(long n) {
    return rate / (1 + rate_annealing * n);
  }

  public float momentum(long n) {
    float m = momentum_start;
    if( momentum_ramp > 0 ) {
      if( n >= momentum_ramp )
        m = momentum_stable;
      else
        m += (momentum_stable - momentum_start) * n / momentum_ramp;
    }
    return m;
  }

  private final void adjust(int i, float[] w, float[] prev, float[] init, float[] mult) {
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
//      float v = (w[i] - prev[i]) * _m;
//      prev[i] = w[i];
//      w[i] += coef * v;
//      if( w == _w )
//        _wSpeed[i] = v;
//      else
//        _bSpeed[i] = v;
    }

    if( init != null )
      init[i] = w[i];
  }

  public static abstract class Input extends Layer {
    protected long _pos, _len;

    @Override public void init(Layer[] ls, int index, boolean weights, long step, Random rand) {
      _a = new float[units];
    }

    @Override protected void bprop() {
      throw new UnsupportedOperationException();
    }

    public final long move() {
      return _pos = _pos == _len - 1 ? 0 : _pos + 1;
    }
  }

  public static class VecsInput extends Input {
    static final int API_WEAVER = 1;
    public static DocGen.FieldDoc[] DOC_FIELDS;

    public Vec[] vecs;

    @API(help = "Categorical classes identified on the training set")
    int[] categoricals_lens;

    @API(help = "Categorical minimums identified on the training set")
    int[] categoricals_mins;

    @API(help = "Normalisation stats used during training")
    float[] subs, muls;

    transient Chunk[] _chunks;

    VecsInput() {
    }

    public VecsInput(Vec[] vecs, VecsInput train) {
      units = train != null ? train.subs.length : expand(vecs);
      this.vecs = vecs;
      _len = vecs[0].length();

      if( train != null ) {
        int a = train.categoricals_lens.length;
        int b = vecs.length;
        assert a == b;
        categoricals_lens = train.categoricals_lens;
        categoricals_mins = train.categoricals_mins;
        assert train.subs.length == units;
        subs = train.subs;
        muls = train.muls;
      } else {
        categoricals_lens = new int[vecs.length];
        categoricals_mins = new int[vecs.length];
        for( int i = 0; i < vecs.length; i++ ) {
          categoricals_lens[i] = categories(vecs[i]);
          categoricals_mins[i] = (int) vecs[i].min();
        }
        subs = new float[units];
        muls = new float[units];
        stats(vecs);
      }
    }

    static int categories(Vec vec) {
      String [] dom = vec.domain();
      return dom == null?1:dom.length-1;
    }

    static int expand(Vec[] vecs) {
      int n = 0;
      for( int i = 0; i < vecs.length; i++ )
        n += categories(vecs[i]);
      return n;
    }

    private void stats(Vec[] vecs) {
      Stats stats = new Stats();
      stats._units = units;
      stats._categoricals_lens = categoricals_lens;
      stats._categoricals_mins = categoricals_mins;
      stats.doAll(vecs);
      for( int i = 0; i < vecs.length; i++ ) {
        subs[i] = (float) stats._means[i];
        double sigma = Math.sqrt(stats._sigms[i] / (stats._rows - 1));
        muls[i] = (float) (sigma > 1e-6 ? 1 / sigma : 1);
      }
    }

    @Override protected void fprop(boolean training) {
      if( _chunks == null )
        _chunks = new Chunk[vecs.length];
      for( int i = 0; i < vecs.length; i++ ) {
        Chunk c = _chunks[i];
        if( c == null || c._vec != vecs[i] || _pos < c._start || _pos >= c._start + c._len )
          _chunks[i] = vecs[i].chunk(_pos);
      }
      ChunksInput.set(_chunks, _a, (int) (_pos - _chunks[0]._start), subs, muls, categoricals_lens, categoricals_mins);
    }
  }

  /**
   * Stats with expanded categoricals.
   */
  static class Stats extends MRTask2<Stats> {
    int _units;
    int[] _categoricals_lens, _categoricals_mins;
    double[] _means, _sigms;
    long _rows;
    transient float[] _subs, _muls;

    @Override protected void setupLocal() {
      _subs = new float[_units];
      _muls = new float[_units];
      for( int i = 0; i < _muls.length; i++ )
        _muls[i] = 1;
    }

    @Override public void map(Chunk[] cs) {
      _means = new double[_units];
      _sigms = new double[_units];
      float[] a = new float[_means.length];
      for( int r = 0; r < cs[0]._len; r++ ) {
        ChunksInput.set(cs, a, r, _subs, _muls, _categoricals_lens, _categoricals_mins);
        for( int c = 0; c < a.length; c++ )
          _means[c] += a[c];
      }
      for( int c = 0; c < a.length; c++ )
        _means[c] /= cs[0]._len;
      for( int r = 0; r < cs[0]._len; r++ ) {
        ChunksInput.set(cs, a, r, _subs, _muls, _categoricals_lens, _categoricals_mins);
        for( int c = 0; c < a.length; c++ )
          _sigms[c] += (a[c] - _means[c]) * (a[c] - _means[c]);
      }
      _rows += cs[0]._len;
    }

    @Override public void reduce(Stats rs) {
      reduce(_means, _sigms, _rows, rs._means, rs._sigms, rs._rows);
      _rows += rs._rows;
    }

    static void reduce(double[] ma, double[] sa, long ra, double[] mb, double[] sb, long rb) {
      for( int c = 0; c < ma.length; c++ ) {
        double delta = ma[c] - mb[c];
        ma[c] = (ma[c] * ra + mb[c] * rb) / (ra + rb);
        sa[c] = sa[c] + sb[c] + delta * delta * ra * rb / (ra + rb);
      }
    }

    @Override public boolean logVerbose() {
      return !H2O.DEBUG;
    }
  }

  static class ChunksInput extends Input {
    transient Chunk[] _chunks;
    float[] _subs, _muls;
    int[] _categoricals_lens;
    int[] _categoricals_mins;

    public ChunksInput(Chunk[] chunks, VecsInput stats) {
      units = stats.subs.length;
      _chunks = chunks;
      _subs = stats.subs;
      _muls = stats.muls;
      _categoricals_lens = stats.categoricals_lens;
      _categoricals_mins = stats.categoricals_mins;
    }

    @Override protected void fprop(boolean training) {
      set(_chunks, _a, (int) _pos, _subs, _muls, _categoricals_lens, _categoricals_mins);
    }

    static void set(Chunk[] chunks, float[] a, int row, float[] subs, float[] muls, int[] catLens, int[] catMins) {
      int n = 0;
      for( int i = 0; i < catLens.length; i++ ) {
        double d = chunks[i].at0(row);
        d = Double.isNaN(d) ? 0 : d;
        if( catLens[i] == 1 ) {
          d -= subs[n];
          d *= muls[n];
          a[n++] = (float) d;
        } else {
          int cat = catLens[i];
          for( int c = 0; c < cat; c++ )
            a[n + c] = -subs[n + c];
          int c = (int) d - catMins[i] - 1;
          if( c >= 0 )
            a[n + c] = (1 - subs[n + c]) * muls[n + c];
          n += cat;
        }
      }
      assert n == a.length;
    }
  }

  public static abstract class Output extends Layer {
    static final int API_WEAVER = 1;
    public static DocGen.FieldDoc[] DOC_FIELDS;

    public enum Loss {
      MeanSquare, CrossEntropy
    }

    @API(help = "Loss function")
    public Loss loss = Loss.CrossEntropy;

    protected final long pos() {
      return _input._pos;
    }
  }

  public static abstract class Softmax extends Output {
    protected abstract int target();

    @Override public void init(Layer[] ls, int index, boolean weights, long step, Random rand) {
      super.init(ls, index, weights, step, rand);
      if( weights ) {
        float min = (float) -Math.sqrt(6. / (_previous.units + units));
        float max = (float) +Math.sqrt(6. / (_previous.units + units));
        for( int i = 0; i < _w.length; i++ )
          _w[i] = rand(rand, min, max);
      }
    }

    @Override protected void fprop(boolean training) {
      float max = Float.NEGATIVE_INFINITY;
      for( int o = 0; o < _a.length; o++ ) {
        _a[o] = 0;
        for( int i = 0; i < _previous._a.length; i++ )
          _a[o] += _w[o * _previous._a.length + i] * _previous._a[i];
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

    @Override protected void bprop() {
      long processed = _training.processed();
      float m = momentum(processed);
      float r = rate(processed) * (1 - m);
      int label = target();
      for( int u = 0; u < _a.length; u++ ) {
        float t = u == label ? 1 : 0;
        float e = t - _a[u];
        float g = e;
        if( loss == Loss.MeanSquare )
          g *= (1 - _a[u]) * _a[u];
        bprop(u, g, r, m);
      }
    }
  }

  public static class VecSoftmax extends Softmax {
    public Vec vec;
    private Vec _toClose;

    VecSoftmax() {
    }

    public VecSoftmax(Vec vec, VecSoftmax stats) {
// Waiting for Michal stuff, for now enum must start at 0
//      if( vec.domain() == null ) {
//        vec = vec.toEnum();
//        _toClose = vec;
//      }
      this.units = stats != null ? stats.units : (int) (vec.max() + 1);
      this.vec = vec;
    }

    @Override protected int target() {
      if( vec.isNA(_input._pos) )
        return -2;
      return (int) vec.at8(_input._pos);
    }

    @Override public void close() {
      super.close();
      if( _toClose != null )
        UKV.remove(_toClose._key);
    }
  }

  static class ChunkSoftmax extends Softmax {
    transient Chunk _chunk;

    public ChunkSoftmax(Chunk chunk, VecSoftmax stats) {
      units = stats.units;
      _chunk = chunk;

      // TODO extract layer info in separate Ice
      rate = stats.rate;
      rate_annealing = stats.rate_annealing;
      momentum_start = stats.momentum_start;
      momentum_stable = stats.momentum_stable;
      momentum_ramp = stats.momentum_ramp;
      _perWeight = stats._perWeight;
      _perWeightAnnealing = stats._perWeightAnnealing;
      l2 = stats.l2;
    }

    @Override protected int target() {
      if( _chunk.isNA0((int) _input._pos) )
        return -2;
      return (int) _chunk.at80((int) _input._pos);
    }
  }

  public static abstract class Linear extends Output {
    abstract float[] target();

    @Override protected void fprop(boolean training) {
      for( int o = 0; o < _a.length; o++ ) {
        _a[o] = 0;
        for( int i = 0; i < _previous._a.length; i++ )
          _a[o] += _w[o * _previous._a.length + i] * _previous._a[i];
        _a[o] += _b[o];
      }
    }

    @Override protected void bprop() {
      long processed = _training.processed();
      float m = momentum(processed);
      float r = rate(processed) * (1 - m);
      float[] v = target();
      for( int u = 0; u < _a.length; u++ ) {
        float e = v[u] - _a[u];
        float g = e * (1 - _a[u]) * _a[u]; // Square error
        bprop(u, g, r, m);
      }
    }
  }

  public static class VecLinear extends Linear {
    Vec _vec;
    transient float[] _values;

    VecLinear() {
    }

    public VecLinear(Vec vec, VecLinear stats) {
      this.units = stats != null ? stats.units : 1;
      _vec = vec;
    }

    @Override float[] target() {
      if( _values == null )
        _values = new float[units];
      double d = _vec.at(_input._pos);
      _values[0] = Double.isNaN(d) ? 0 : (float) d;
      return _values;
    }
  }

  static class ChunkLinear extends Linear {
    transient Chunk _chunk;
    transient float[] _values;

    public ChunkLinear(Chunk chunk, VecLinear stats) {
      units = stats.units;
      _chunk = chunk;

      // TODO extract layer info in separate Ice
      rate = stats.rate;
      rate_annealing = stats.rate_annealing;
      momentum_start = stats.momentum_start;
      momentum_stable = stats.momentum_stable;
      momentum_ramp = stats.momentum_ramp;
      _perWeight = stats._perWeight;
      _perWeightAnnealing = stats._perWeightAnnealing;
      l2 = stats.l2;
    }

    @Override float[] target() {
      if( _values == null )
        _values = new float[units];
      double d = _chunk.at0((int) _input._pos);
      _values[0] = Double.isNaN(d) ? 0 : (float) d;
      return _values;
    }
  }

  public static class Tanh extends Layer {
    Tanh() {
    }

    public Tanh(int units) {
      this.units = units;
    }

    @Override public void init(Layer[] ls, int index, boolean weights, long step, Random rand) {
      super.init(ls, index, weights, step, rand);
      if( weights ) {
        // C.f. deeplearning.net tutorial
        float min = (float) -Math.sqrt(6. / (_previous.units + units));
        float max = (float) +Math.sqrt(6. / (_previous.units + units));
        for( int i = 0; i < _w.length; i++ )
          _w[i] = rand(rand, min, max);
      }
    }

    @Override protected void fprop(boolean training) {
      for( int o = 0; o < _a.length; o++ ) {
        _a[o] = 0;
        for( int i = 0; i < _previous._a.length; i++ )
          _a[o] += _w[o * _previous._a.length + i] * _previous._a[i];
        _a[o] += _b[o];

        // tanh approx, slightly faster, untested
        // float a = Math.abs(_a[o]);
        // float b = 12 + a * (6 + a * (3 + a));
        // _a[o] = (_a[o] * b) / (a * b + 24);
        _a[o] = (float) Math.tanh(_a[o]);
      }
    }

    @Override protected void bprop() {
      long processed = _training.processed();
      float m = momentum(processed);
      float r = rate(processed) * (1 - m);
      for( int u = 0; u < _a.length; u++ ) {
        // Gradient is error * derivative of hyperbolic tangent: (1 - x^2)
        float g = _e[u] * (1 - _a[u] * _a[u]);
        bprop(u, g, r, m);
      }
    }
  }

  /**
   * Apply tanh to the weights' transpose. Used for auto-encoders.
   */
  public static class TanhPrime extends Tanh {
    TanhPrime() {
    }

    public TanhPrime(int units) {
      this.units = units;
    }

    @Override public void init(Layer[] ls, int index, boolean weights, long step, Random rand) {
      super.init(ls, index, weights, step, rand);
      // Auto encoder has it's own bias vector
      _b = new float[units];
    }

    @Override protected void fprop(boolean training) {
      for( int o = 0; o < _a.length; o++ ) {
        _a[o] = 0;
        for( int i = 0; i < _previous._a.length; i++ )
          _a[o] += _w[i * _a.length + o] * _previous._a[i];
        _a[o] += _b[o];
        _a[o] = (float) Math.tanh(_a[o]);
      }
    }

    @Override protected void bprop() {
      long processed = _training.processed();
      float m = momentum(processed);
      float r = rate(processed) * (1 - m);
      for( int o = 0; o < _a.length; o++ ) {
        assert _previous._previous.units == units;
        float e = _previous._previous._a[o] - _a[o];
        float g = e; // * (1 - _a[o]) * _a[o]; // Square error
        for( int i = 0; i < _previous._a.length; i++ ) {
          int w = i * _a.length + o;
          if( _previous._e != null )
            _previous._e[i] += g * _w[w];
          _w[w] += r * (g * _previous._a[i] - _w[w] * l2);
        }
        _b[o] += r * g;
      }
    }
  }

  public static class Maxout extends Layer {
    transient Random _rand;
    transient byte[] _bits;

    Maxout() {
    }

    public Maxout(int units) {
      this.units = units;
    }

    @Override public void init(Layer[] ls, int index, boolean weights, long step, Random rand) {
      super.init(ls, index, weights, step, rand);
      if( weights ) {
//        int count = Math.min(15, _previous.units);
//        //float min = -.1f, max = +.1f;
//        float min = -1f, max = +1f;
//        for( int o = 0; o < units; o++ ) {
//          for( int n = 0; n < count; n++ ) {
//            int i = rand.nextInt(_previous.units);
//            int w = o * _previous.units + i;
//            _w[w] = rand(rand, min, max);
//          }
//        }
        float min = (float) -Math.sqrt(6. / (_previous.units + units));
        float max = (float) +Math.sqrt(6. / (_previous.units + units));
        for( int i = 0; i < _w.length; i++ )
          _w[i] = rand(rand, min, max);
        for( int i = 0; i < _b.length; i++ )
          _b[i] = 1;
      }
    }

    @Override protected void fprop(boolean training) {
      if( _rand == null ) {
        _rand = new MersenneTwisterRNG(MersenneTwisterRNG.SEEDS);
        _bits = new byte[units / 8 + 1];
      }
      _rand.nextBytes(_bits);
      float max = 0;
      for( int o = 0; o < _a.length; o++ ) {
        _a[o] = 0;
        boolean b = (_bits[o >> 3] & (1 << o)) != 0;
        if( !training || b ) {
          _a[o] = Float.NEGATIVE_INFINITY;
          for( int i = 0; i < _previous._a.length; i++ )
            _a[o] = Math.max(_a[o], _w[o * _previous._a.length + i] * _previous._a[i]);
          _a[o] += _b[o];
          if( !training )
            _a[o] *= .5f;
          if( max < _a[o] )
            max = _a[o];
        }
      }
      if( max > 1 )
        for( int o = 0; o < _a.length; o++ )
          _a[o] /= max;
    }

    @Override protected void bprop() {
      long processed = _training.processed();
      float m = momentum(processed);
      float r = rate(processed) * (1 - m);
      for( int u = 0; u < _a.length; u++ ) {
        float g = _e[u];
//                if( _a[o] < 0 )   Not sure if we should be using maxout with a hard zero bottom
//                    g = 0;
        bprop(u, g, r, m);
      }
    }
  }

  public static class Rectifier extends Layer {
    Rectifier() {
    }

    public Rectifier(int units) {
      this.units = units;
    }

    @Override public void init(Layer[] ls, int index, boolean weights, long step, Random rand) {
      super.init(ls, index, weights, step, rand);
      if( weights ) {
        int count = Math.min(15, _previous.units);
        float min = -.1f, max = +.1f;
        //float min = -1f, max = +1f;
        for( int o = 0; o < units; o++ ) {
          for( int n = 0; n < count; n++ ) {
            int i = rand.nextInt(_previous.units);
            int w = o * _previous.units + i;
            _w[w] = rand(rand, min, max);
          }
        }
//        float min = (float) -Math.sqrt(6. / (_previous.units + units));
//        float max = (float) +Math.sqrt(6. / (_previous.units + units));
//        for( int i = 0; i < _w.length; i++ )
//          _w[i] = rand(rand, min, max);

//        for( int i = 0; i < _w.length; i++ )
//          _w[i] = rand(rand, -.01f, .01f);
        for( int i = 0; i < _b.length; i++ )
          _b[i] = 1;
      }
    }

    @Override protected void fprop(boolean training) {
      float max = 0;
      for( int o = 0; o < _a.length; o++ ) {
        _a[o] = 0;
        for( int i = 0; i < _previous._a.length; i++ )
          _a[o] += _w[o * _previous._a.length + i] * _previous._a[i];
        _a[o] += _b[o];
        if( _a[o] < 0 )
          _a[o] = 0;
        if( max < _a[o] )
          max = _a[o];
      }
      if( max > 1 )
        for( int o = 0; o < _a.length; o++ )
          _a[o] /= max;
    }

    @Override protected void bprop() {
      long processed = _training.processed();
      float m = momentum(processed);
      float r = rate(processed) * (1 - m);
      for( int u = 0; u < _a.length; u++ ) {
        float g = _e[u];
        int todo_test_ge;
        if( _a[u] > 0 )
          bprop(u, g, r, m);
      }
    }
  }

  public static class RectifierDropout extends Rectifier {
    transient Random _rand;
    transient byte[] _bits;

    RectifierDropout() {
    }

    public RectifierDropout(int units) {
      super(units);
    }

    @Override protected void fprop(boolean training) {
      if( _rand == null ) {
        //_rand = new MersenneTwisterRNG(MersenneTwisterRNG.SEEDS);
        _rand = new XorShiftRNG(0x9afa938d554f4e76L);
        _bits = new byte[units / 8 + 1];
      }
      _rand.nextBytes(_bits);
      float max = 0;
      for( int o = 0; o < _a.length; o++ ) {
        _a[o] = 0;
        boolean b = (_bits[o >> 3] & (1 << o)) != 0;
        if( !training || b ) {
          for( int i = 0; i < _previous._a.length; i++ )
            _a[o] += _w[o * _previous._a.length + i] * _previous._a[i];
          _a[o] += _b[o];
          if( _a[o] < 0 )
            _a[o] = 0;
          else if( !training )
            _a[o] *= .5f;
          if( max < _a[o] )
            max = _a[o];
        }
      }
      if( max > 1 )
        for( int o = 0; o < _a.length; o++ )
          _a[o] /= max;
    }
  }

  public static class RectifierPrime extends Rectifier {
    RectifierPrime() {
    }

    public RectifierPrime(int units) {
      this.units = units;
    }

    @Override public void init(Layer[] ls, int index, boolean weights, long step, Random rand) {
      super.init(ls, index, weights, step, rand);
      // Auto encoder has it's own bias vector
      _b = new float[units];
      for( int i = 0; i < _b.length; i++ )
        _b[i] = 1;
    }

    @Override protected void fprop(boolean training) {
      for( int o = 0; o < _a.length; o++ ) {
        _a[o] = 0;
        for( int i = 0; i < _previous._a.length; i++ )
          _a[o] += _w[i * _a.length + o] * _previous._a[i];
        _a[o] += _b[o];
        if( _a[o] < 0 )
          _a[o] = 0;
      }
    }

    @Override protected void bprop() {
      long processed = _training.processed();
      float m = momentum(processed);
      float r = rate(processed) * (1 - m);
      for( int o = 0; o < _a.length; o++ ) {
        assert _previous._previous.units == units;
        float e = _previous._previous._a[o] - _a[o];
        float g = e;//* (1 - _a[o]) * _a[o]; // Square error
        for( int i = 0; i < _previous._a.length; i++ ) {
          int w = i * _a.length + o;
          if( _previous._e != null )
            _previous._e[i] += g * _w[w];
          _w[w] += r * (g * _previous._a[i] - _w[w] * l2);
        }
        _b[o] += r * g;
      }
    }
  }

  //

  @Override public Layer clone() {
    return (Layer) super.clone();
  }

  public static void shareWeights(Layer src, Layer dst) {
    dst._w = src._w;
    dst._b = src._b;
    dst._wm = src._wm;
    dst._bm = src._bm;
  }

  public static void shareWeights(Layer[] src, Layer[] dst) {
    for( int y = 1; y < src.length; y++ )
      shareWeights(src[y], dst[y]);
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

//
//  final void adjustVisible() {
//    if( _gv != null ) {
//      for( int v = 0; v < _gv.length; v++ ) {
//        _v[v] += _gv[v];
//        _gv[v] *= 1 - _momentum;
//      }
//    }
//  }
//
//  float[] generate(float[] hidden) {
//    assert hidden.length == _b.length;
//    float[] visible = new float[_v.length];
//    for( int o = 0; o < hidden.length; o++ )
//      for( int i = 0; i < _in._a.length; i++ )
//        visible[i] += _w[o * _in._a.length + i] * hidden[o];
//    for( int i = 0; i < visible.length; i++ ) {
//      visible[i] += _v[i];
//      if( visible[i] < 0 )
//        visible[i] = 0;
//    }
//    return visible;
//  }

//  float freeEnergy(float[] in) {
//    float energy = 0.0f;
//    for( int i = 0; i < in.length; i++ )
//      energy -= in[i] * _v[i];
//    for( int o = 0; o < _b.length; o++ ) {
//      float out = 0;
//      for( int i = 0; i < in.length; i++ )
//        out += _w[o * in.length + i] * in[i];
//      out += _b[o];
//      energy -= Math.log(1 + Math.exp(out));
//    }
//    return energy;
//  }

  private static float rand(Random rand, float min, float max) {
    return min + rand.nextFloat() * (max - min);
  }

  @Override public AutoBuffer writeJSON(AutoBuffer bb) {
    bb.put1('{');
    bb.putJSONStr("type").put1(':').putJSONStr(getClass().getName());
    bb.put1(',');
    writeJSONFields(bb);
    bb.put1('}');
    return bb;
  }
}
