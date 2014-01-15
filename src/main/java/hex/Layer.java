package hex;

import water.*;
import water.api.DocGen;
import water.api.Request.API;
import water.fvec.Chunk;
import water.fvec.Vec;

import java.util.Random;

import static hex.NeuralNet.RNG.getRNG;

/**
 * Neural network layer.
 *
 * @author cypof
 */
public abstract class Layer extends Iced {
  static final int API_WEAVER = 1;
  public static DocGen.FieldDoc[] DOC_FIELDS;

  @API(help = "Number of neurons")
  @ParamsSearch.Ignore
  public int units;

  @API(help = "Initial Weight Distribution")
  public NeuralNet.InitialWeightDistribution initial_weight_distribution = NeuralNet.InitialWeightDistribution.UniformAdaptive;

  @API(help = "Initial weight (Uniform: amplitude, Normal: stddev)")
  @ParamsSearch.Info(origin = 0.01)
  public double initial_weight_scale;

  @API(help = "Learning rate")
  public float rate;

  @API(help = "Learning rate annealing")
  public float rate_annealing;

  @API(help = "L1 regularisation")
  public float l1;

  @API(help = "L2 regularisation")
  public float l2;

  @API(help = "Initial momentum value")
  @ParamsSearch.Info(origin = 1)
  public float momentum_start;

  @API(help = "Number of samples during which momentum value varies")
  public long momentum_ramp;

  @API(help = "Momentum value once ramp is over")
  @ParamsSearch.Info(origin = 1)
  public float momentum_stable;

  @API(help = "Constraint for squared sum of incoming weights per unit")
  public float max_w2;

  public volatile boolean unstable = false;

  public void transferParams(NeuralNet p) {
    initial_weight_distribution = p.initial_weight_distribution;
    initial_weight_scale = p.initial_weight_scale;
    rate = (float) p.rate;
    rate_annealing = (float) p.rate_annealing;
    l1 = (float) p.l1;
    l2 = (float) p.l2;
    momentum_start = (float) p.momentum_start;
    momentum_ramp = p.momentum_ramp;
    momentum_stable = (float) p.momentum_stable;
    max_w2 = p.max_w2;
  }

  public void transferParams(Layer p) {
    initial_weight_distribution = p.initial_weight_distribution;
    initial_weight_scale = p.initial_weight_scale;
    rate = p.rate;
    rate_annealing = p.rate_annealing;
    l1 = p.l1;
    l2 = p.l2;
    momentum_start = p.momentum_start;
    momentum_ramp = p.momentum_ramp;
    momentum_stable = p.momentum_stable;
    max_w2 = p.max_w2;
  }

  // Weights, biases, activity, error
  // TODO hold transients only for current two layers
  // TODO extract transients & code in separate one-shot trees to avoid cloning
  protected transient float[] _w, _b, _a, _e;

  // Momentum for weights and biases
  protected transient float[] _wm, _bm;

  // Previous and input layers
  protected transient Layer _previous;
  transient Input _input;

  // Dropout (for input + hidden layers)
  transient Dropout dropout;

  Dropout createDropout(int units) {
    return new Dropout(units);
  }

  /**
   * Start of refactoring in specification & running data, for layers and trainers.
   */
  static abstract class Training {
    abstract long processed();
  }

  transient Training _training;

  /**
   * Helper class for dropout, only to be used from within a Layer
   */
  private class Dropout {
    private transient Random _rand;
    private transient byte[] _bits;

    private Dropout(int units) {
      _bits = new byte[(units+7)/8];
      _rand = getRNG();
    }

    // for input layer
    private void clearSomeInput() {
      assert(_previous.isInput());
      final double rate = ((Input)_previous)._dropout_rate;
      for( int i = 0; i < _previous._a.length; i++ ) {
        if (_rand.nextFloat() < rate) _previous._a[i] = 0;
      }
    }

    // for hidden layers
    private void fillBytes() {
      _rand.nextBytes(_bits);
    }

    private boolean unit_active(int o) {
      return (_bits[o / 8] & (1 << (o % 8))) != 0;
    }
  }

  public final void init(Layer[] ls, int index, NeuralNet p) {
    transferParams(p);
    init(ls, index, true);
  }

  public void init(Layer[] ls, int index, boolean weights) {
    _a = new float[units];
    if (!(this instanceof Output)) {
      _e = new float[units];
    }
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
  }

  /**
   *
   // helper to initialize weights
   // adaptive initialization uses prefactor * sqrt(6 / (units_input_layer + units_this_layer))
   * @param rng random generator to use
   * @param prefactor prefactor for initialization (typical value: 1.0)
   */
  // cf. http://machinelearning.wustl.edu/mlpapers/paper_files/AISTATS2010_GlorotB10.pdf
  void randomize(Random rng, float prefactor) {
    if (_w == null) return;

    if (initial_weight_distribution == NeuralNet.InitialWeightDistribution.UniformAdaptive) {
      final float range = prefactor * (float)Math.sqrt(6. / (_previous.units + units));
      for( int i = 0; i < _w.length; i++ )
        _w[i] = uniformDist(rng, -range, range);
    }
    else {
      if (initial_weight_distribution == NeuralNet.InitialWeightDistribution.Uniform) {
        for (int i = 0; i < _w.length; i++) {
          _w[i] = uniformDist(rng, (float)-initial_weight_scale, (float)initial_weight_scale);
        }
      } else if (initial_weight_distribution == NeuralNet.InitialWeightDistribution.Normal) {
        for (int i = 0; i < _w.length; i++) {
          _w[i] = (float) (0 + rng.nextGaussian() * initial_weight_scale);
        }
      }
    }
  }

    // TODO: Add "subset randomize" function
//        int count = Math.min(15, _previous.units);
//        float min = -.1f, max = +.1f;
//        //float min = -1f, max = +1f;
//        for( int o = 0; o < units; o++ ) {
//          for( int n = 0; n < count; n++ ) {
//            int i = rand.nextInt(_previous.units);
//            int w = o * _previous.units + i;
//            _w[w] = uniformDist(rand, min, max);
//          }
//        }

  public void close() {
  }

  protected abstract void fprop(boolean training);

  protected abstract void bprop();

  boolean isInput() { return false; }

  /**
   * Apply gradient g to unit u with rate r and momentum m.
   */
  final void bprop(int u, float g, float r, float m) {
    double r2 = 0;
    for( int i = 0; i < _previous._a.length; i++ ) {
      int w = u * _previous._a.length + i;
      if( _previous._e != null )
        _previous._e[i] += g * _w[w];
      float d = g * _previous._a[i] - _w[w] * l2 - Math.signum(_w[w]) * l1;
      unstable |= Float.isNaN(d);

      // TODO finish per-weight acceleration, doesn't help for now
//      if( _wp != null && d != 0 ) {
//        boolean sign = _wp[w] >= 0;
//        float mult = Math.abs(_wp[w]);
//        // If the gradient kept its sign, increase
//        if( (d >= 0) == sign )
//          mult += .05f;
//        else {
//          if( mult > 1 )
//            mult *= .95f;
//          else
//            sign = !sign;
//        }
//        d *= mult;
//        _wp[w] = sign ? mult : -mult;
//      }

      if( _wm != null ) {
        _wm[w] *= m;
        _wm[w] = d = _wm[w] + d;
      }
      _w[w] += r * d;
      r2 += _w[w] * _w[w];
    }
    if( r2 > max_w2) { // C.f. Improving neural networks by preventing co-adaptation of feature detectors
      double scale = Math.sqrt(max_w2) / Math.sqrt(r2);
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

  public static abstract class Input extends Layer {
    @ParamsSearch.Ignore
    protected long _pos, _len;

    @Override public void init(Layer[] ls, int index, boolean weights) {
      _a = new float[units];
    }

    @Override protected void bprop() {
      throw new UnsupportedOperationException();
    }

    @Override
    protected boolean isInput() {
      return true;
    }

    @API(help = "Dropout rate for the input layer")
    double _dropout_rate;

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

    @Override public Layer clone() {
      VecsInput o = (VecsInput) super.clone();
      if( o._chunks != null )
        o._chunks = new Chunk[o._chunks.length];
      return o;
    }

    public VecsInput(Vec[] vecs, VecsInput train, double dropout_rate) {
      _dropout_rate = dropout_rate;
      Init(vecs, train);
    }

    public VecsInput(Vec[] vecs, VecsInput train) {
      Init(vecs, train);
    }

    public void Init(Vec[] vecs, VecsInput train) {
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
      String[] dom = vec.domain();
      return dom == null ? 1 : dom.length - 1;
    }

    static int expand(Vec[] vecs) {
      int n = 0;
      for (Vec vec : vecs) n += categories(vec);
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

    @API(help = "Loss function")
    public NeuralNet.Loss loss = NeuralNet.Loss.MeanSquare;

    public final void init(Layer[] ls, int index, NeuralNet p, NeuralNet.Loss l) {
      super.init(ls, index, p);
      loss = l;
    }

    protected final long pos() {
      return _input._pos;
    }
  }

  public static abstract class Softmax extends Output {
    protected abstract int target();

    @Override public void init(Layer[] ls, int index, boolean weights) {
      super.init(ls, index, weights);
      if( weights ) {
        randomize(getRNG(), 1.0f);
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
        final float targetval = (u == label ? 1 : 0);
        float g = targetval - _a[u];
        if (loss == NeuralNet.Loss.CrossEntropy) {
          //nothing else needed
        } else if (loss == NeuralNet.Loss.MeanSquare) {
          g *= (1 - _a[u]) * _a[u];
        }
        bprop(u, g, r, m);
      }
    }
  }

  public static class VecSoftmax extends Softmax {
    public Vec vec;
    private Vec _toClose;

    VecSoftmax() {
    }

    public VecSoftmax(Vec vec, VecSoftmax stats, NeuralNet.Loss l) {
// Waiting for Michal stuff, for now enum must start at 0
//      if( vec.domain() == null ) {
//        vec = vec.toEnum();
//        _toClose = vec;
//      }
      this.units = stats != null ? stats.units : (int) (vec.max() + 1);
      this.vec = vec;
      loss = l;
      if (stats != null) transferParams(stats);
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
      loss = stats.loss;
      transferParams(stats);
    }

    @Override protected int target() {
      if( _chunk.isNA0((int) _input._pos) )
        return -2;
      return (int) _chunk.at80((int) _input._pos);
    }
  }

  public static abstract class Linear extends Output {
    abstract float[] target();

    @Override public void init(Layer[] ls, int index, boolean weights) {
      super.init(ls, index, weights);
      if( weights ) {
        randomize(getRNG(), 1.0f);
      }
    }

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
      assert(loss == NeuralNet.Loss.MeanSquare);
      for( int u = 0; u < _a.length; u++ ) {
        float g = v[u] - _a[u];
        g *= (1 - _a[u]) * _a[u];
        bprop(u, g, r, m);
      }
    }
  }

  public static class VecLinear extends Linear {
    Vec _vec;
    transient float[] _values;

    public VecLinear(Vec vec, VecLinear stats) {
      this.units = stats != null ? stats.units : 1;
      _vec = vec;
      if (stats != null) transferParams(stats);
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
      loss = stats.loss;
      transferParams(stats);
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

    @Override public void init(Layer[] ls, int index, boolean weights) {
      super.init(ls, index, weights);
      if( weights ) {
        randomize(getRNG(), 1.0f);
      }
    }

    @Override protected void fprop(boolean training) {
      if (dropout != null && training) {
        dropout.fillBytes();
        if (_previous.isInput())
          dropout.clearSomeInput();
      }

      for( int o = 0; o < _a.length; o++ ) {
        _a[o] = 0;
        if( !training || dropout == null || dropout.unit_active(o) ) {
          for( int i = 0; i < _previous._a.length; i++ ) {
            _a[o] += _w[o * _previous._a.length + i] * _previous._a[i];
          }
          _a[o] += _b[o];

          // tanh approx, slightly faster, untested
          // float a = Math.abs(_a[o]);
          // float b = 12 + a * (6 + a * (3 + a));
          // _a[o] = (_a[o] * b) / (a * b + 24);

          // Other approx to try
          // _a[o] = -1 + (2 / (1 + Math.exp(-2 * _a[o])));

          _a[o] = (float) Math.tanh(_a[o]);

          if( !training && dropout != null ) {
            _a[o] *= .5f;
          }
        }
      }
    }

    @Override protected void bprop() {
      long processed = _training.processed();
      float m = momentum(processed);
      float r = rate(processed) * (1 - m);
      for( int u = 0; u < _a.length; u++ ) {
        // Gradient is error * derivative of hyperbolic tangent: (1 - x^2)
        float g = _e[u] * (1 - _a[u]) * (1 + _a[u]); //more numerically stable than 1-x^2
        bprop(u, g, r, m);
      }
    }
  }

  public static class TanhDropout extends Tanh {
    public TanhDropout(int units) {
      super(units);
      dropout = createDropout(units);
    }
  }

  /**
   * Apply tanh to the weights' transpose. Used for auto-encoders.
   */
  public static class TanhPrime extends Tanh {
    TanhPrime() {
    }

    public TanhPrime(int units) {
      super(units);
    }

    @Override public void init(Layer[] ls, int index, boolean weights) {
      super.init(ls, index, weights);
      // Auto encoder has its own bias vector
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
          _w[w] += r * (g * _previous._a[i] - _w[w] * l2 - Math.signum(_w[w]) * l1);
        }
        _b[o] += r * g;
      }
    }
  }

  public static class Maxout extends Layer {
    public Maxout(int units) {
      this.units = units;
      dropout = createDropout(units);
    }

    @Override public void init(Layer[] ls, int index, boolean weights) {
      super.init(ls, index, weights);
      if( weights ) {
        randomize(getRNG(), 1.0f);
        for( int i = 0; i < _b.length; i++ )
          _b[i] = 1;
      }
    }

    @Override protected void fprop(boolean training) {
      if (dropout != null && training) {
        dropout.fillBytes();
        if (_previous.isInput())
          dropout.clearSomeInput();
      }

      float max = 0;
      for( int o = 0; o < _a.length; o++ ) {
        _a[o] = 0;
        if( !training || dropout.unit_active(o) ) {
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

    @Override public void init(Layer[] ls, int index, boolean weights) {
      super.init(ls, index, weights);
      if( weights ) {
        randomize(getRNG(), 1.0f);
        for( int i = 0; i < _b.length; i++ )
          _b[i] = 1;
      }
    }

    @Override protected void fprop(boolean training) {
      if (dropout != null && training) {
        dropout.fillBytes();
        if (_previous.isInput())
          dropout.clearSomeInput();
      }

      for( int o = 0; o < _a.length; o++ ) {
        _a[o] = 0;
        if( !training || dropout == null || dropout.unit_active(o) ) {
          for( int i = 0; i < _previous._a.length; i++ )
            _a[o] += _w[o * _previous._a.length + i] * _previous._a[i];
          _a[o] += _b[o];
          if( _a[o] < 0 )
            _a[o] = 0;
          else if( !training && dropout != null )
            _a[o] *= .5f;
        }
        unstable |= Float.isNaN(_a[o]);
      }
    }

    @Override protected void bprop() {
      long processed = _training.processed();
      final float m = momentum(processed);
      final float r = rate(processed) * (1 - m);
      for( int u = 0; u < _a.length; u++ ) {
        //(d/dx)(max(0,x)) = 1 if x > 0, otherwise 0
        if( _a[u] > 0 ) { // don't use >=
          final float g = _e[u]; // * 1.0 (from derivative of rectifier)
          bprop(u, g, r, m);
        }
        // otherwise g = _e[u] * 0.0 = 0 and we don't allow other contributions by (and to) weights and momenta
      }
    }
  }

  public static class RectifierDropout extends Rectifier {
    public RectifierDropout(int units) {
      super(units);
      this.dropout = createDropout(units);
    }
  }

  public static class RectifierPrime extends Rectifier {
    RectifierPrime() {
    }

    public RectifierPrime(int units) {
      super(units);
    }

    @Override public void init(Layer[] ls, int index, boolean weights) {
      super.init(ls, index, weights);
      // Auto encoder has its own bias vector
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
      for( int u = 0; u < _a.length; u++ ) {
        assert _previous._previous.units == units;
        float e = _previous._previous._a[u] - _a[u];
        float g = e;//* (1 - _a[o]) * _a[o];
        //float g = e * (1 - _a[o]) * _a[o]; // Square error
        double r2 = 0;
        for( int i = 0; i < _previous._a.length; i++ ) {
          int w = i * _a.length + u;
          if( _previous._e != null )
            _previous._e[i] += g * _w[w];
          float d = g * _previous._a[i] - _w[w] * l2 - Math.signum(_w[w]) * l1;
          _w[w] += r * d;
          r2 += _w[w] * _w[w];
        }
        if( r2 >  max_w2) { // C.f. Improving neural networks by preventing co-adaptation of feature detectors
          final float scale = (float) Math.sqrt(max_w2 / r2);
          for( int i = 0; i < _previous._a.length; i++ ) {
            int w = i * _a.length + u;
            _w[w] *= scale;
          }
        }
        float d = g;
        _b[u] += r * d;
      }
    }
  }

  @Override public Layer clone() {
    Layer l = (Layer) super.clone();
    if (dropout != null) l.dropout = new Dropout(units);
    return l;
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

  private static float uniformDist(Random rand, float min, float max) {
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
