package hex.nn;

import hex.FrameTask;
import water.MemoryManager;
import water.api.DocGen;
import water.api.Request.API;
import water.util.Utils;

import java.util.Arrays;

import static hex.nn.NN.Loss;

/**
 * This class implements the concept of a Neuron layer in a Neural Network
 * During training, every MRTask2 F/J thread is expected to create these neurons for every map call (Cheap to make).
 * These Neurons are NOT sent over the wire.
 * The weights connecting the neurons are in a separate class (NNModel.NNModelInfo), and will be shared per node.
 */
public abstract class Neurons {
  static final int API_WEAVER = 1;
  public static DocGen.FieldDoc[] DOC_FIELDS;

  @API(help = "Number of neurons")
  protected int units;

  /**
   * Constructor of a Neuron Layer
   * @param units How many neurons are in this layer?
   */
  Neurons(int units) {
    this.units = units;
  }

  @Override
  public String toString() {
    String s = this.getClass().getSimpleName();
    s += "\nNumber of Neurons: " + units;
    s += "\nParameters:\n" + params.toString();
    if (dropout != null) s += "\nDropout:\n" + dropout.toString();
    return s;
  }

  /**
   * Parameters (deep-cloned() from the user input, can be modified here, e.g. learning rate decay)
   */
  protected NN params;

  /**
   * Layer state (one per neuron): activity, error
   */
  public transient double[] _a, _e;

  /**
   * References for feed-forward connectivity
   */
  public Neurons _previous; // previous layer of neurons
  NNModel.NNModelInfo _minfo; //reference to shared model info
  public float[] _w; //reference to _minfo.weights[layer] for convenience
  float[] _wm; //reference to _minfo.weights_momenta[layer] for convenience
  public double[] _b; //reference to _minfo.biases[layer] for convenience
  private double[] _bm; //reference to _minfo.biases_momenta[layer] for convenience

  /**
   * For Dropout training
   */
  protected Dropout dropout;

//  /**
//   * We need a way to encode a missing value in the neural net forward/back-propagation scheme.
//   * For simplicity and performance, we simply use the largest values to encode a missing value.
//   * If we run into exactly one of those values with regular neural net updates, then we're very
//   * likely also running into overflow problems, which will trigger a NaN somewhere, which will be
//   * caught and lead to automatic job cancellation.
//   */
//  public static final int missing_int_value = Integer.MAX_VALUE; //encode missing label or target
//  public static final double missing_double_value = Double.MAX_VALUE; //encode missing input

  /**
   * Initialization of the parameters and connectivity of a Neuron layer
   * @param neurons Array of all neuron layers, to establish feed-forward connectivity
   * @param index Which layer am I?
   * @param p User-given parameters (Job parental object hierarchy is not used)
   * @param minfo Model information (weights/biases and their momenta)
   * @param training Whether training is done or just testing (no need for dropout)
   */
  public final void init(Neurons[] neurons, int index, NN p, final NNModel.NNModelInfo minfo, boolean training) {
    params = (NN)p.clone();
    params.rate *= Math.pow(params.rate_decay, index-1);
    _a = new double[units];
    if (!(this instanceof Output) && !(this instanceof Input)) {
      _e = new double[units];
    }
    if (training && (this instanceof Maxout || this instanceof TanhDropout ||
                    this instanceof RectifierDropout || this instanceof Input) ) {
      dropout = new Dropout(units);
    }
    if (!(this instanceof Input)) {
      _previous = neurons[index-1]; //incoming neurons
      _minfo = minfo;
      _w = minfo.get_weights(index-1); //incoming weights
      _b = minfo.get_biases(index-1); //bias for this layer (starting at hidden layer)
      if (minfo.has_momenta()) {
        _wm = minfo.get_weights_momenta(index-1); //incoming weights
        _bm = minfo.get_biases_momenta(index-1); //bias for this layer (starting at hidden layer)
      }
    }
  }

  /**
   * Forward propagation
   * @param seed For seeding the RNG inside (for dropout)
   * @param training Whether training is done or just testing (no need for dropout)
   */
  protected abstract void fprop(long seed, boolean training);

  /**
   *  Back propagation
   */
  protected abstract void bprop();

  /**
   * Apply gradient g to unit u with rate r and momentum m.
   * @param u Unit (source of gradient correction)
   * @param g Gradient (derivative of the loss function with respect to changing the weight of this unit)
   * @param r Learning rate (as of this moment, in case there is learning rate annealing)
   * @param m Momentum (as of this moment, in case there is a momentum ramp)
   */
  final void bprop(int u, double g, double r, double m) {
    double r2 = 0;
    final int off = u * _previous._a.length;
    for( int i = 0; i < _previous._a.length; i++ ) {
      int w = off + i;
      if( _previous._e != null )
        _previous._e[i] += g * _w[w];
      double d = g * _previous._a[i] - _w[w] * params.l2 - Math.signum(_w[w]) * params.l1;
      final double delta = r * d;

      // TODO finish per-weight acceleration, doesn't help for now
//        if( _wp != null && d != 0 ) {
//          boolean sign = _wp[w] >= 0;
//          double mult = Math.abs(_wp[w]);
//          // If the gradient kept its sign, increase
//          if( (d >= 0) == sign )
//            mult += .05f;
//          else {
//            if( mult > 1 )
//              mult *= .95f;
//            else
//              sign = !sign;
//          }
//          d *= mult;
//          _wp[w] = sign ? mult : -mult;
//        }

      _w[w] += delta;
      if( _wm != null ) {
        _w[w] += m * _wm[w];
        _wm[w] = (float)(delta);
      }
      if (params.max_w2 != Double.POSITIVE_INFINITY)
        r2 += _w[w] * _w[w];
    }
    if( params.max_w2 != Double.POSITIVE_INFINITY && r2 > params.max_w2 ) { // C.f. Improving neural networks by preventing co-adaptation of feature detectors
      final double scale = Math.sqrt(params.max_w2 / r2);
      for( int i = 0; i < _previous._a.length; i++ ) _w[off + i] *= scale;
    }
    final double delta = r * g;
    _b[u] += delta;
    if( _bm != null ) {
      _b[u] += m * _bm[u];
      _bm[u] = delta;
    }
  }

  /**
   * The learning rate
   * @param n The number of training samples seen so far (for rate_annealing > 0)
   * @return Learning rate
   */
  public double rate(long n) {
    return params.rate / (1 + params.rate_annealing * n);
  }

  /**
   * The momentum - real number in [0, 1)
   * Can be a linear ramp from momentum_start to momentum_stable, over momentum_ramp training samples
   * @param n The number of training samples seen so far
   * @return momentum
   */
  public double momentum(long n) {
    double m = params.momentum_start;
    if( params.momentum_ramp > 0 ) {
      if( n >= params.momentum_ramp )
        m = params.momentum_stable;
      else
        m += (params.momentum_stable - params.momentum_start) * n / params.momentum_ramp;
    }
    return m;
  }

  /**
   * Input layer of the Neural Network
   * This layer is different from other layers as it has no incoming weights,
   * but instead gets its activation values from the training points.
   */
  public static class Input extends Neurons {

    private FrameTask.DataInfo _dinfo; //training data

    Input(int units, final FrameTask.DataInfo d) {
      super(units);
      _dinfo = d;
      _a = new double[units];
    }

    @Override protected void bprop() { throw new UnsupportedOperationException(); }
    @Override protected void fprop(long seed, boolean training) { throw new UnsupportedOperationException(); }

    /**
     * One of two methods to set layer input values. This one is for raw double data, e.g. for scoring
     * @param seed For seeding the RNG inside (for input dropout)
     * @param data Data (training columns and responses) to extract the training columns
     *             from to be mapped into the input neuron layer
     */
    public void setInput(long seed, final double[] data) {
      assert(_dinfo != null);
      double [] nums = MemoryManager.malloc8d(_dinfo._nums); // a bit wasteful - reallocated each time
      int    [] cats = MemoryManager.malloc4(_dinfo._cats); // a bit wasteful - reallocated each time
      int i = 0, ncats = 0;
      for(; i < _dinfo._cats; ++i){
        int c = (int)data[i];
        if(c != 0)cats[ncats++] = c + _dinfo._catOffsets[i] - 1;
      }
      final int n = data.length-_dinfo._responses;
      for(;i < n;++i){
        double d = data[i];
        if(_dinfo._normMul != null) d = (d - _dinfo._normSub[i-_dinfo._cats])*_dinfo._normMul[i-_dinfo._cats];
        nums[i-_dinfo._cats] = d;
      }
      setInput(seed, nums, ncats, cats);
    }

    /**
     * The second method used to set input layer values. This one is used directly by FrameTask.processRow() and by the method above.
     * @param seed For seeding the RNG inside (for input dropout)
     * @param nums Array containing numerical values
     * @param numcat Number of horizontalized categorical non-zero values (i.e., those not being the first factor of a class)
     * @param cats Array of indices, the first numcat values are the input layer unit (==column) indices for the non-zero categorical values
     *             (This allows this array to be re-usable by the caller, without re-allocating each time)
     */
    public void setInput(long seed, final double[] nums, final int numcat, final int[] cats) {
      Arrays.fill(_a, 0.);
      for (int i=0; i<numcat; ++i) _a[cats[i]] = 1.0;
      System.arraycopy(nums, 0, _a, _dinfo.numStart(), nums.length);
      final double rate = params.input_dropout_ratio;
      if (rate == 0 || seed < 0) return; // seed is set to -1 for testing (no input dropout done there)
      seed += params.seed + 0x1337B4BE;
      dropout.randomlySparsifyActivation(_a, rate, seed);
    }

  }

  public static class Tanh extends Neurons {
    public Tanh(int units) { super(units); }
    @Override protected void fprop(long seed, boolean training) {
      for( int o = 0; o < _a.length; o++ ) {
        _a[o] = 0;
        final int off = o * _previous._a.length;
        if( !training || dropout == null || dropout.unit_active(o) ) {
          for( int i = 0; i < _previous._a.length; i++ ) {
            _a[o] += _w[off+i] * _previous._a[i];
          }
          _a[o] += _b[o];

          // tanh approx, slightly faster, untested
//          double a = Math.abs(_a[o]);
//          double b = 12 + a * (6 + a * (3 + a));
//          _a[o] = (_a[o] * b) / (a * b + 24);

          // use this identity: tanh = 2*sigmoid(2*x) - 1, evaluates faster than tanh(x)
          _a[o] = -1 + (2 / (1 + Math.exp(-2 * _a[o])));

//          _a[o] = Math.tanh(_a[o]); //slow
        }
      }
    }
    @Override protected void bprop() {
      long processed = _minfo.get_processed_total();
      double m = momentum(processed);
      double r = rate(processed) * (1 - m);
      for( int u = 0; u < _a.length; u++ ) {
        // Gradient is error * derivative of hyperbolic tangent: (1 - x^2)
        double g = _e[u] * (1 - _a[u]) * (1 + _a[u]); //more numerically stable than 1-x^2
        bprop(u, g, r, m);
      }
    }
  }

  public static class TanhDropout extends Tanh {
    public TanhDropout(int units) {
      super(units);
    }

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

  public static class Maxout extends Neurons {
    public Maxout(int units) {
      super(units);
    }

    @Override protected void fprop(long seed, boolean training) {
      if (dropout != null && training) {
        dropout.fillBytes(seed);
      }

      double max = 0;
      for( int o = 0; o < _a.length; o++ ) {
        _a[o] = 0;
        if( !training || (dropout != null && dropout.unit_active(o))) {
          final int off = o * _previous._a.length;
          _a[o] = Double.NEGATIVE_INFINITY;
          for( int i = 0; i < _previous._a.length; i++ )
            _a[o] = Math.max(_a[o], _w[off+i] * _previous._a[i]);
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
      if (!training && dropout != null)
        Utils.div(_a, 2.f);
    }

    @Override protected void bprop() {
      long processed = _minfo.get_processed_total();
      double m = momentum(processed);
      double r = rate(processed) * (1 - m);
      for( int u = 0; u < _a.length; u++ ) {
        double g = _e[u];
//                if( _a[o] < 0 )   Not sure if we should be using maxout with a hard zero bottom
//                    g = 0;
        bprop(u, g, r, m);
      }
    }
  }

  public static class Rectifier extends Neurons {

    public Rectifier(int units) {
      super(units);
      this.units = units;
    }

    @Override protected void fprop(long seed, boolean training) {
      for( int o = 0; o < _a.length; o++ ) {
        _a[o] = 0;
        final int off = o * _previous._a.length;
        if( !training || dropout == null || dropout.unit_active(o) ) {
          for( int i = 0; i < _previous._a.length; i++ )
            _a[o] += _w[off+i] * _previous._a[i];
          _a[o] += _b[o];
          _a[o] = Math.max(_a[o], 0);
        }
      }
    }

    @Override protected void bprop() {
      long processed = _minfo.get_processed_total();
      final double m = momentum(processed);
      final double r = rate(processed) * (1 - m);
      for( int u = 0; u < _a.length; u++ ) {
        //(d/dx)(max(0,x)) = 1 if x > 0, otherwise 0

        // no need to update the weights if there are no momenta and l1=0 and l2=0
        if (params.fast_mode || (_wm == null && params.l1 == 0.0 && params.l2 == 0.0)) {
          if( _a[u] > 0 ) { // don't use >= (faster this way: lots of zeros)
            final double g = _e[u]; // * 1.0 (from derivative of rectifier)
            bprop(u, g, r, m);
          }
        }
        // if we have momenta or l1 or l2, then EVEN for g=0, there will be contributions to the weight updates
        // Note: this is slower than always doing the shortcut above, and might not affect the accuracy much
        else {
          final double g = _a[u] > 0 ? _e[u] : 0;
          bprop(u, g, r, m);
        }
      }
    }
  }

  public static class RectifierDropout extends Rectifier {
    public RectifierDropout(int units) {
      super(units);
    }
    @Override
    protected void fprop(long seed, boolean training) {
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

  public static abstract class Output extends Neurons {
    static final int API_WEAVER = 1;
    public static DocGen.FieldDoc[] DOC_FIELDS;

    Output(int units) { super(units); }

    protected abstract void fprop(); //don't differentiate between testing/training
    protected void fprop(long seed, boolean training) { throw new UnsupportedOperationException(); }
    protected void bprop() { throw new UnsupportedOperationException(); }
  }

  public static class Softmax extends Output {
    public Softmax(int units) { super(units); }

    @Override protected void fprop() {
      double max = Double.NEGATIVE_INFINITY;
      for( int o = 0; o < _a.length; o++ ) {
        _a[o] = 0;
        final int off = o * _previous._a.length;
        for( int i = 0; i < _previous._a.length; i++ )
          _a[o] += _w[off+i] * _previous._a[i];
        _a[o] += _b[o];
        if( max < _a[o] )
          max = _a[o];
      }
      double scale = 0;
      for( int o = 0; o < _a.length; o++ ) {
        _a[o] = Math.exp(_a[o] - max);
        scale += _a[o];
      }
      for( int o = 0; o < _a.length; o++ )
        _a[o] /= scale;
    }
    protected void bprop(int target) {
      long processed = _minfo.get_processed_total();
      double m = momentum(processed);
      double r = rate(processed) * (1 - m);
//      if (target == missing_int_value) return; //ignore missing response values
      for( int u = 0; u < _a.length; u++ ) {
        final double targetval = (u == target ? 1 : 0);
        double g = targetval - _a[u];
        if (params.loss == Loss.CrossEntropy) {
          //nothing else needed
        } else if (params.loss == Loss.MeanSquare) {
          g *= (1 - _a[u]) * _a[u];
        }
        bprop(u, g, r, m);
      }
    }
  }

  public static class Linear extends Output {
    public Linear(int units) { super(units); }
    @Override protected void fprop() {
      assert(_a.length == 1);
      int o = 0;
      _a[o] = 0;
      final int off = o * _previous._a.length;
      for( int i = 0; i < _previous._a.length; i++ )
        _a[o] += _w[off+i] * _previous._a[i];
      _a[o] += _b[o];
    }
    protected void bprop(double target) {
      long processed = _minfo.get_processed_total();
      double m = momentum(processed);
      double r = rate(processed) * (1 - m);
      assert(params.loss == Loss.MeanSquare);
      int u = 0;
//      if (target == missing_double_value) return;
      double g = target - _a[u];
      g *= (1 - _a[u]) * _a[u];
      bprop(u, g, r, m);
    }
  }

}
