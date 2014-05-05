package hex;

import water.*;
import water.api.DocGen;
import water.api.Request.API;
import water.fvec.Chunk;
import water.fvec.Vec;
import water.util.Utils;

import java.util.Random;

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

  public NeuralNet params;

  // Layer state: activity, error
  protected transient float[] _a, _e;

  // Shared state: weights and biases (and their momenta)
  protected transient float[] _w, _wm;
  protected transient float[] _b, _bm;

  // Previous and input layers
  protected transient Layer _previous;
  transient Input _input;

  // Dropout (for input + hidden layers)
  transient Dropout dropout;

  /**
   * Start of refactoring in specification & running data, for layers and trainers.
   */
  static abstract class Training {
    abstract long processed();
  }

  transient Training _training;

  /**
   * We need a way to encode a missing value in the neural net forward/back-propagation scheme.
   * For simplicity and performance, we simply use the largest values to encode a missing value.
   * If we run into exactly one of those values with regular neural net updates, then we're very
   * likely also running into overflow problems, which will trigger a NaN somewhere, which will be
   * caught and lead to automatic job cancellation.
   */
  public static final int missing_int_value = Integer.MAX_VALUE; //encode missing label or target
  public static final float missing_float_value = Float.MAX_VALUE; //encode missing input

  /**
   * Helper class for dropout, only to be used from within a Layer
   */

  public class Dropout {
    private transient Random _rand;
    private transient byte[] _bits;

    @Override
    public String toString() {
      String s = "Dropout: " + super.toString();
      s += "\nRandom: " + _rand.toString();
      s += "\nbits: ";
      for (int i=0; i< _bits.length*8; ++i) s += unit_active(i) ? "1":"0";
      s += "\n";
      return s;
    }

    Dropout(int units) {
      _bits = new byte[(units+7)/8];
      _rand = new Random(0);
    }

    // for input layer
    public void randomlySparsifyActivation(float[] a, double rate, long seed) {
      if (rate == 0) return;
      setSeed(seed);
      for( int i = 0; i < a.length; i++ )
        if (_rand.nextFloat() < rate) a[i] = 0;
    }

    // for hidden layers
    public void fillBytes(long seed) {
      setSeed(seed);
      _rand.nextBytes(_bits);
    }

    public boolean unit_active(int o) {
      return (_bits[o / 8] & (1 << (o % 8))) != 0;
    }

    private void setSeed(long seed) {
      if ((seed >>> 32) < 0x0000ffffL)         seed |= 0x5b93000000000000L;
      if (((seed << 32) >>> 32) < 0x0000ffffL) seed |= 0xdb910000L;
      _rand.setSeed(seed);
    }
  }

  public final void init(Layer[] ls, int index, NeuralNet p) {
    params = (NeuralNet)p.clone();
    init(ls, index, true);
  }

  public void init(Layer[] ls, int index, boolean weights) {
    params.rate *= Math.pow(params.rate_decay, index-1);
    _a = new float[units];
    if (!(this instanceof Output) && !(this instanceof Input)) {
      _e = new float[units];
    }
    _previous = ls[index - 1];
    _input = (Input) ls[0];

    if (this instanceof MaxoutDropout || this instanceof TanhDropout || this instanceof RectifierDropout) {
      dropout = new Dropout(units);
    }

    if( weights ) {
      _w = new float[units * _previous.units];
      _b = new float[units];
      if( params.momentum_start != 0 || params.momentum_stable != 0 ) {
        _wm = new float[_w.length];
        _bm = new float[_b.length];
      }
    }
  }

  /**
   *
   // helper to initialize weights
   // adaptive initialization uses prefactor * sqrt(6 / (units_input_layer + units_this_layer))
   * @param seed random generator seed to use
   * @param prefactor prefactor for initialization (typical value: 1.0)
   */
  // cf. http://machinelearning.wustl.edu/mlpapers/paper_files/AISTATS2010_GlorotB10.pdf
  void randomize(long seed, double prefactor) {
    if (_w == null) return;
    final Random rng = water.util.Utils.getDeterRNG(seed);

    if (params.initial_weight_distribution == NeuralNet.InitialWeightDistribution.UniformAdaptive) {
      final double range = prefactor * Math.sqrt(6. / (_previous.units + units));
      for( int i = 0; i < _w.length; i++ )
        _w[i] = (float)uniformDist(rng, -range, range);
    }
    else {
      if (params.initial_weight_distribution == NeuralNet.InitialWeightDistribution.Uniform) {
        for (int i = 0; i < _w.length; i++) {
          _w[i] = (float)uniformDist(rng, -params.initial_weight_scale, params.initial_weight_scale);
        }
      } else if (params.initial_weight_distribution == NeuralNet.InitialWeightDistribution.Normal) {
        for (int i = 0; i < _w.length; i++) {
          _w[i] = (float) (0 + rng.nextGaussian() * params.initial_weight_scale);
        }
      }
    }
  }

    // TODO: Add "subset randomize" function
//        int count = Math.min(15, _previous.units);
//        double min = -.1f, max = +.1f;
//        //double min = -1f, max = +1f;
//        for( int o = 0; o < units; o++ ) {
//          for( int n = 0; n < count; n++ ) {
//            int i = rand.nextInt(_previous.units);
//            int w = o * _previous.units + i;
//            _w[w] = uniformDist(rand, min, max);
//          }
//        }

  public void close() {
  }

  protected abstract void fprop(long seed, boolean training);

  protected abstract void bprop();

  /**
   * Apply gradient g to unit u with rate r and momentum m.
   */
  final void bprop(int u, float g, float r, float m) {
    // only correct weights if the gradient is large enough
    if (params.fast_mode || (_w == null && params.l1 == 0.0 && params.l2 == 0.0)) {
      if (g == 0f) return;
    }

    final float l1 = (float)params.l1;
    final float l2 = (float)params.l2;
    double r2 = 0;
    final int off = u * _previous._a.length;
    for( int i = 0; i < _previous._a.length; i++ ) {
      int w = off + i;
      if( _previous._e != null ) _previous._e[i] += g * _w[w];
      if (params.fast_mode && _previous._a[i] == 0) continue;

      float d = g * _previous._a[i] - Math.signum(_w[w]) * l1 - _w[w] * l2;

      // TODO finish per-weight acceleration, doesn't help for now
//      if( _wp != null && d != 0 ) {
//        boolean sign = _wp[w] >= 0;
//        double mult = Math.abs(_wp[w]);
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
        _wm[w] += d;
        d = _wm[w];
      }
      _w[w] += r * d;
      if (params.max_w2 != Double.POSITIVE_INFINITY) r2 += _w[w] * _w[w];
    }
    if( params.max_w2 != Double.POSITIVE_INFINITY && r2 > params.max_w2 ) { // C.f. Improving neural networks by preventing co-adaptation of feature detectors
      final float scale = Utils.approxSqrt((float)(params.max_w2 / r2));
      for( int i = 0; i < _previous._a.length; i++ ) _w[off + i] *= scale;
    }
    float d = g;
    if( _bm != null ) {
      _bm[u] *= m;
      _bm[u] += d;
      d = _bm[u];
    }
    _b[u] += r * d;
  }

  public float rate(long n) {
    return (float)(params.rate / (1 + params.rate_annealing * n));
  }

  public float momentum(long n) {
    double m = params.momentum_start;
    if( params.momentum_ramp > 0 ) {
      if( n >= params.momentum_ramp )
        m = params.momentum_stable;
      else
        m += (params.momentum_stable - params.momentum_start) * n / params.momentum_ramp;
    }
    return (float)m;
  }

  public static abstract class Input extends Layer {
    @ParamsSearch.Ignore
    protected long _pos, _len;

    @Override public void init(Layer[] ls, int index, boolean weights) {
      _a = new float[units];
      dropout = new Dropout(units);
    }

    public void inputDropout(long seed) {
      double rate = params.input_dropout_ratio;
      seed += params.seed + 0x1337B4BE;
      dropout.randomlySparsifyActivation(_a, rate, seed);
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
    double[] subs, muls;

    transient Chunk[] _chunks;

    @Override public Layer clone() {
      VecsInput o = (VecsInput) super.clone();
      if( o._chunks != null )
        o._chunks = new Chunk[o._chunks.length];
      return o;
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
        subs = new double[units];
        muls = new double[units];
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
        subs[i] = stats._means[i];
        double sigma = Math.sqrt(stats._sigms[i] / (stats._rows - 1));
        muls[i] = sigma > 1e-6 ? 1 / sigma : 1;
      }
    }

    @Override protected void fprop(long seed, boolean training) {
      if( _chunks == null )
        _chunks = new Chunk[vecs.length];
      for( int i = 0; i < vecs.length; i++ ) {
        Chunk c = _chunks[i];
        if( c == null || c._vec != vecs[i] || _pos < c._start || _pos >= c._start + c._len )
          _chunks[i] = vecs[i].chunkForRow(_pos);
      }
      ChunksInput.set(_chunks, _a, (int) (_pos - _chunks[0]._start), subs, muls, categoricals_lens, categoricals_mins);
      if (training) inputDropout(seed);
    }
  }

  /**
   * Stats with expanded categoricals. Used to normalize the data in the input layer.
   */
  static class Stats extends MRTask2<Stats> {
    int _units;
    int[] _categoricals_lens, _categoricals_mins;
    double[] _means, _sigms;
    long _rows;
    transient double[] _subs, _muls;

    @Override protected void setupLocal() {
      _subs = new double[_units];
      _muls = new double[_units];
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

  /**
   * A ChunksInput layer populates the activation values from a FVec chunk.
   * Missing values will lead to a 0 activation value in the input layer, which is equivalent to
   * setting it to the *average* column value before normalizing. In effect, missing column values are ignored.
   */
  static class ChunksInput extends Input {
    transient Chunk[] _chunks;
    double[] _subs, _muls;
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

    /**
     * forward propagation means filling the activation values with all the row's column values
     */
    @Override protected void fprop(long seed, boolean training) {
      set(_chunks, _a, (int) _pos, _subs, _muls, _categoricals_lens, _categoricals_mins);
      if (training) inputDropout(seed);
    }

    static void set(Chunk[] chunks, float[] a, int row, double[] subs, double[] muls, int[] catLens, int[] catMins) {
      int n = 0;
      // loop over all columns
      for( int i = 0; i < catLens.length; i++ ) {
        final boolean missing = chunks[i].isNA0(row);
        double d = chunks[i].at0(row);
        if( catLens[i] == 1 ) {
          //numerical value: normalize
          d -= subs[n];
          d *= muls[n];
          a[n++] = missing ? 0f : (float)d;
        } else {
          // categorical values: use precomputed stats
          int cat = catLens[i];
          for( int c = 0; c < cat; c++ )
            a[n + c] = missing ? 0f : (float)-subs[n + c];
          int c = (int) d - catMins[i] - 1;
          if( c >= 0 )
            a[n + c] = missing ? 0f : (float)((1 - subs[n + c]) * muls[n + c]);
          n += cat;
        }
      }
      assert n == a.length;
    }
  }

  public static abstract class Output extends Layer {
    static final int API_WEAVER = 1;
    public static DocGen.FieldDoc[] DOC_FIELDS;

    protected final long pos() {
      return _input._pos;
    }
  }

  /**
   * Softmax output layer is used for classification
   * Rows with missing values in the response column will be ignored
   **/
  public static abstract class Softmax extends Output {
    protected abstract int target();

    @Override public void init(Layer[] ls, int index, boolean weights) {
      super.init(ls, index, weights);
      if( weights ) {
        randomize(params.seed + 0xBAD5EED + index, 4.0f);
      }
    }

    @Override protected void fprop(long seed, boolean training) {
      for( int o = 0; o < _a.length; o++ ) {
        _a[o] = 0;
        for( int i = 0; i < _previous._a.length; i++ )
          _a[o] += _w[o * _previous._a.length + i] * _previous._a[i];
        _a[o] += _b[o];
      }
      final float max = Utils.maxValue(_a);
      float scale = 0;
      for( int o = 0; o < _a.length; o++ ) {
        _a[o] = (float)Math.exp(_a[o] - max);
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
      if (label == missing_int_value) return; //ignore missing response values
      for( int u = 0; u < _a.length; u++ ) {
        final float targetval = (u == label ? 1f : 0f);
        float g = targetval - _a[u];
        if (params.loss == NeuralNet.Loss.CrossEntropy) {
          //nothing else needed
        } else if (params.loss == NeuralNet.Loss.MeanSquare) {
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

    public VecSoftmax(Vec vec, VecSoftmax stats) {
// Waiting for Michal stuff, for now enum must start at 0
//      if( vec.domain() == null ) {
//        vec = vec.toEnum();
//        _toClose = vec;
//      }
      this.units = stats != null ? stats.units : (int) (vec.max() + 1);
      this.vec = vec;
      params = stats != null ? (NeuralNet)stats.params.clone() : null;
    }

    @Override protected int target() {
      if( vec.isNA(_input._pos) )
        return missing_int_value;
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
      params = (NeuralNet)stats.params.clone();
    }

    @Override protected int target() {
      if( _chunk.isNA0((int) _input._pos) )
        return missing_int_value;
      return (int) _chunk.at80((int) _input._pos);
    }
  }

  /**
   * Linear output layer is used for regression
   * Rows with missing values in the response column will be ignored
   **/
  public static abstract class Linear extends Output {
    abstract float[] target();

    @Override public void init(Layer[] ls, int index, boolean weights) {
      super.init(ls, index, weights);
      if( weights ) {
        randomize(params.seed + 0xBAD5EED + index, 1.0f);
      }
    }

    @Override protected void fprop(long seed, boolean training) {
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
      assert(params.loss == NeuralNet.Loss.MeanSquare);
      for( int u = 0; u < _a.length; u++ ) {
        if (v[u] == missing_float_value) continue; //ignore missing regression targets
        float g = v[u] - _a[u];
        bprop(u, g, r, m);
      }
    }
  }

  public static class VecLinear extends Linear {
    Vec _vec;
    transient float[] _values;

    public VecLinear(Vec vec, VecLinear stats) {
      assert(stats == null || stats.units == 1);
      units = 1; //regression
      _vec = vec;
      params = stats != null ? (NeuralNet)stats.params.clone() : null;
    }

    @Override float[] target() {
      if( _values == null )
        _values = new float[units];
      long pos = _input._pos; //pos is a global index into the vector
      _values[0] = _vec.isNA(pos) ? missing_float_value : (float)_vec.at(pos);
      return _values;
    }
  }

  static class ChunkLinear extends Linear {
    transient Chunk _chunk;
    transient float[] _values;

    public ChunkLinear(Chunk chunk, VecLinear stats) {
      assert(stats == null || stats.units == 1);
      units = 1;
      _chunk = chunk;
      params = (NeuralNet) (stats != null ? stats.params.clone() : null);
    }

    @Override float[] target() {
      if( _values == null )
        _values = new float[units];
      int pos = (int)_input._pos; //pos is a local index for this chunk
      _values[0] = _chunk.isNA0(pos) ? missing_float_value : (float)_chunk.at0(pos);
      return _values;
    }
  }

  public static class Tanh extends Layer {
    public Tanh(int units) { this.units = units; }
    @Override public void init(Layer[] ls, int index, boolean weights) {
      super.init(ls, index, weights);
      if( weights ) {
        randomize(params.seed + 0xBAD5EED + index, 1.0f);
      }
    }

    @Override protected void fprop(long seed, boolean training) {
      for( int o = 0; o < _a.length; o++ ) {
        _a[o] = 0;
        if( !training || dropout == null || dropout.unit_active(o) ) {
          for( int i = 0; i < _previous._a.length; i++ ) {
            _a[o] += _w[o * _previous._a.length + i] * _previous._a[i];
          }
          _a[o] += _b[o];
          _a[o] = 1f - 2f / (1f + (float)Math.exp(2*_a[o])); //evals faster than tanh(x), but is slightly less numerically stable - OK
        }
      }
    }

    @Override protected void bprop() {
      long processed = _training.processed();
      float m = momentum(processed);
      float r = rate(processed) * (1 - m);
      for( int u = 0; u < _a.length; u++ ) {
        // Gradient is error * derivative of hyperbolic tangent: (1 - x^2)
        float g = _e[u] * (1f - _a[u] * _a[u]);
        bprop(u, g, r, m);
      }
    }
  }

  public static class TanhDropout extends Tanh {
    public TanhDropout(int units) { super(units); }
    @Override
    protected void fprop(long seed, boolean training) {
      if (training) {
        seed += params.seed + 0xDA7A6000;
        dropout.fillBytes(seed);
        super.fprop(seed, true);
      }
      else {
        super.fprop(seed, false);
        Utils.div(_a, 2.f);
      }
    }
  }

  /**
   * Apply tanh to the weights' transpose. Used for auto-encoders.
   */
  public static class TanhPrime extends Tanh {
    public TanhPrime(int units) {
      super(units);
    }
    @Override public void init(Layer[] ls, int index, boolean weights) {
      super.init(ls, index, weights);
      // Auto encoder has its own bias vector
      _b = new float[units];
    }

    @Override protected void fprop(long seed, boolean training) {
      for( int o = 0; o < _a.length; o++ ) {
        _a[o] = 0;
        for( int i = 0; i < _previous._a.length; i++ )
          _a[o] += _w[i * _a.length + o] * _previous._a[i];
        _a[o] += _b[o];
        _a[o] = (float)Math.tanh(_a[o]);
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
          _w[w] += r * (g * _previous._a[i] - _w[w] * params.l2 - Math.signum(_w[w]) * params.l1);
        }
        _b[o] += r * g;
      }
    }
  }

  public static class Maxout extends Layer {
    public Maxout(int units) { this.units = units; }
    @Override public void init(Layer[] ls, int index, boolean weights) {
      super.init(ls, index, weights);
      if( weights ) {
        randomize(params.seed + 0xBAD5EED + index, 1.0f);
        for( int i = 0; i < _b.length; i++ )
          _b[i] = index == 1 ? 0.5f : 1f;
      }
    }

    @Override protected void fprop(long seed, boolean training) {
      float max = 0;
      for( int o = 0; o < _a.length; o++ ) {
        _a[o] = 0;
        if( !training || dropout == null || dropout.unit_active(o)) {
          final int off = o * _previous._a.length;
          _a[o] = Float.NEGATIVE_INFINITY;
          for( int i = 0; i < _previous._a.length; i++ )
            _a[o] = Math.max(_a[o], _w[off+i] * _previous._a[i]);
          _a[o] += _b[o];
          max = Math.max(_a[o], max);
        }
      }
      if( max > 1 ) Utils.div(_a, max);
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

  public static class MaxoutDropout extends Maxout {
    public MaxoutDropout(int units) { super(units); }
    @Override protected void fprop(long seed, boolean training) {
      if (training) {
        seed += params.seed + 0x51C8D00D;
        dropout.fillBytes(seed);
        super.fprop(seed, true);
      }
      else {
        super.fprop(seed, false);
        Utils.div(_a, 2.f);
      }
    }
  }

  public static class Rectifier extends Layer {
    public Rectifier(int units) { this.units = units; }
    @Override public void init(Layer[] ls, int index, boolean weights) {
      super.init(ls, index, weights);
      if( weights ) {
        randomize(params.seed + 0xBAD5EED + index, 1.0f);
        for( int i = 0; i < _b.length; i++ )
          _b[i] = index == 1 ? 0.5f : 1f;
      }
    }

    @Override protected void fprop(long seed, boolean training) {
      for( int o = 0; o < _a.length; o++ ) {
        _a[o] = 0;
        if( !training || dropout == null || dropout.unit_active(o) ) {
          for( int i = 0; i < _previous._a.length; i++ )
            _a[o] += _w[o * _previous._a.length + i] * _previous._a[i];
          _a[o] += _b[o];
          _a[o] = Math.max(_a[o], 0f);
        }
      }
    }

    @Override protected void bprop() {
      long processed = _training.processed();
      final float m = momentum(processed);
      final float r = rate(processed) * (1 - m);
      for( int u = 0; u < _a.length; u++ ) {
        //(d/dx)(max(0,x)) = 1 if x > 0, otherwise 0
        final float g = _a[u] > 0 ? _e[u] : 0; // * 1.0 (from derivative of rectifier)
        bprop(u, g, r, m);
        // otherwise g = _e[u] * 0.0 = 0 and we don't allow other contributions by (and to) weights and momenta
      }
    }
  }

  public static class RectifierDropout extends Rectifier {
    public RectifierDropout(int units) { super(units); }
    @Override protected void fprop(long seed, boolean training) {
      if (training) {
        seed += params.seed + 0x3C71F1ED;
        dropout.fillBytes(seed);
        super.fprop(seed, true);
      }
      else {
        super.fprop(seed, false);
        Utils.div(_a, 2.f);
      }
    }
  }

  public static class RectifierPrime extends Rectifier {
    public RectifierPrime(int units) { super(units); }
    @Override public void init(Layer[] ls, int index, boolean weights) {
      super.init(ls, index, weights);
      // Auto encoder has its own bias vector
      _b = new float[units];
      for( int i = 0; i < _b.length; i++ )
        _b[i] = index == 1 ? 0.5f : 1f;
    }

    @Override protected void fprop(long seed, boolean training) {
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
          if( _previous._e != null ) _previous._e[i] += g * _w[w];
          float d = g * _previous._a[i] - (float)(_w[w] * params.l2) - (float)(Math.signum(_w[w]) * params.l1);
          _w[w] += r * d;
          if (params.max_w2 != Double.POSITIVE_INFINITY) r2 += _w[w] * _w[w];
        }
        if( params.max_w2 != Double.POSITIVE_INFINITY && r2 > params.max_w2 ) { // C.f. Improving neural networks by preventing co-adaptation of feature detectors
          final double scale = Math.sqrt(params.max_w2 / r2);
          for( int i = 0; i < _previous._a.length; i++ ) _w[i * _a.length + u] *= scale;
        }
        _b[u] += r * g;
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
    if (dst._b == null || dst._b.length == src._b.length) dst._b = src._b;
    dst._wm = src._wm;
    if (dst._bm == null || dst._bm.length == src._bm.length) dst._bm = src._bm;
  }

  public static void shareWeights(Layer[] src, Layer[] dst) {
    for( int y = 1; y < src.length; y++ )
      shareWeights(src[y], dst[y]);
  }

  private static double uniformDist(Random rand, double min, double max) {
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
