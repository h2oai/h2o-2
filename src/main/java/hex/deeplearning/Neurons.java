package hex.deeplearning;

import static hex.deeplearning.DeepLearning.Loss;
import hex.FrameTask;
import org.junit.Ignore;
import org.junit.Test;
import water.MemoryManager;
import water.PrettyPrint;
import water.api.DocGen;
import water.api.Request.API;
import water.util.Utils;

import java.util.Arrays;

/**
 * This class implements the concept of a Neuron layer in a Neural Network
 * During training, every MRTask2 F/J thread is expected to create these neurons for every map call (Cheap to make).
 * These Neurons are NOT sent over the wire.
 * The weights connecting the neurons are in a separate class (DeepLearningModel.DeepLearningModelInfo), and will be shared per node.
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

  /**
   * Print the status of this neuron layer
   * @return populated String
   */
  @Override
  public String toString() {
    String s = this.getClass().getSimpleName();
    s += "\nNumber of Neurons: " + units;
    s += "\nParameters:\n" + params.toString();
    if (_dropout != null) s += "\nDropout:\n" + _dropout.toString();
    return s;
  }

  /**
   * Parameters (deep-cloned() from the user input, can be modified here, e.g. learning rate decay)
   */
  protected DeepLearning params;

  /**
   * Layer state (one per neuron): activity, error
   */
  public transient float[] _a, _e;

  /**
   * References for feed-forward connectivity
   */
  public Neurons _previous; // previous layer of neurons
  DeepLearningModel.DeepLearningModelInfo _minfo; //reference to shared model info
  public float[] _w; //reference to _minfo.weights[layer] for convenience
  public float[] _b; //reference to _minfo.biases[layer] for convenience

  // momentum
  float[] _wm; //reference to _minfo.weights_momenta[layer] for convenience
  private float[] _bm; //reference to _minfo.biases_momenta[layer] for convenience

  // ADADELTA
  private float[] _E_dx2; //reference to _minfo.E_dx2[layer] for convenience
  private float[] _E_g2; //reference to _minfo.E_g2[layer] for convenience

  /**
   * For Dropout training
   */
  protected Dropout _dropout;

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
   * Helper to check sanity of Neuron layers
   * @param training whether training or testing is done
   */
  void sanityCheck(boolean training) {
    if (this instanceof Input) {
      assert(_previous == null);
      assert (!training || _dropout != null);
    } else {
      assert(_previous != null);
      if (_minfo.has_momenta()) {
        assert(_wm != null);
        assert(_bm != null);
        assert(_E_dx2 == null);
        assert(_E_g2 == null);
      }
      if (_minfo.adaDelta()) {
        if (params.rho == 0) throw new IllegalArgumentException("rho must be > 0 if epsilon is >0.");
        if (params.epsilon == 0) throw new IllegalArgumentException("epsilon must be > 0 if rho is >0.");
        assert(_minfo.adaDelta());
        assert(_E_dx2 != null);
        assert(_E_g2 != null);
        assert(_wm == null);
        assert(_bm == null);
      }
      if (this instanceof MaxoutDropout || this instanceof TanhDropout || this instanceof RectifierDropout) {
        assert (!training || _dropout != null);
      }
    }
  }

  /**
   * Initialization of the parameters and connectivity of a Neuron layer
   * @param neurons Array of all neuron layers, to establish feed-forward connectivity
   * @param index Which layer am I?
   * @param p User-given parameters (Job parental object hierarchy is not used)
   * @param minfo Model information (weights/biases and their momenta)
   * @param training Whether training is done or just testing (no need for dropout)
   */
  public final void init(Neurons[] neurons, int index, DeepLearning p, final DeepLearningModel.DeepLearningModelInfo minfo, boolean training) {
    params = (DeepLearning)p.clone();
    params.rate *= Math.pow(params.rate_decay, index-1);
    _a = new float[units];
    if (!(this instanceof Output) && !(this instanceof Input)) {
      _e = new float[units];
    }
    if (training && (this instanceof MaxoutDropout || this instanceof TanhDropout
            || this instanceof RectifierDropout || this instanceof Input) ) {
      _dropout = new Dropout(units);
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
      if (minfo.adaDelta()) {
        _E_dx2 = minfo.get_E_dx2(index-1);
        _E_g2 = minfo.get_E_g2(index - 1);
      }
    }
    sanityCheck(training);
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
   * Backpropagation: w -= rate * dE/dw, where dE/dw = dE/dy * dy/dnet * dnet/dw
   * This method adds the dnet/dw = activation term per unit
   * @param u unit (which neuron)
   * @param g partial derivative dE/dnet = dE/dy * dy/net
   * @param r rate
   * @param m momentum
   */
  final void bprop(int u, float g, float r, float m) {
    // only correct weights if the gradient is large enough
    if (params.fast_mode || (
            // not doing fast mode, but also don't have anything else to update (neither momentum nor ADADELTA history), and no L1/L2
            !_minfo.get_params().adaptive_rate && !_minfo.has_momenta() && params.l1 == 0.0 && params.l2 == 0.0)) {
      if (Math.abs(g) <= 1e-10) return;
    }

//    Log.info("bprop(u=" + u + ", g=" + g + ", r=" + r + ", m=" + m);
    double r2 = 0;
    final int off = u * _previous._a.length;
    for( int i = 0; i < _previous._a.length; i++ ) {
      int w = off + i;
      // propagate the error dE/dnet to the previous layer, via connecting weights
      if( _previous._e != null ) _previous._e[i] += g * _w[w];

      //this is the actual gradient dE/dw
      float d = g * _previous._a[i] - (float)(_w[w] * params.l2) - (float)(Math.signum(_w[w]) * params.l1);

      // adaptive learning rate r from ADADELTA
      // http://www.matthewzeiler.com/pubs/googleTR2012/googleTR2012.pdf
      if (_E_dx2 != null && _E_g2 != null) {
        assert(_wm == null && _bm == null);
        final float grad = d;
        _E_g2[w] = (float)(params.rho * _E_g2[w] + (1.-params.rho)*grad*grad);
        final float RMS_dx = (float)Math.sqrt(_E_dx2[w]+params.epsilon);
        final float RMS_g = (float)Math.sqrt(_E_g2[w]+params.epsilon);
        r = RMS_dx/RMS_g;
        _E_dx2[w] = (float)(params.rho * _E_dx2[w] + (1.-params.rho)*(r*d)*(r*d));
      }

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

      if (!params.nesterov_accelerated_gradient) {
        final float delta = r * d;
        _w[w] += delta;
        if( _wm != null ) {
          _w[w] += m * _wm[w];
          _wm[w] = delta;
        }
      } else {
        if( _wm != null ) {
          _wm[w] *= m;
          _wm[w] += d;
          d = _wm[w];
        }
        _w[w] += r * d;
//        Log.info("w[" + w + "] += " + r + " * " + d + " = " + _w[w]);
      }
      if (params.max_w2 != Double.POSITIVE_INFINITY)
        r2 += _w[w] * _w[w];
    }
    if( params.max_w2 != Double.POSITIVE_INFINITY && r2 > params.max_w2 ) { // C.f. Improving neural networks by preventing co-adaptation of feature detectors
      final double scale = Math.sqrt(params.max_w2 / r2);
      for( int i = 0; i < _previous._a.length; i++ ) _w[off + i] *= scale;
    }

    if (!params.nesterov_accelerated_gradient) {
      final float delta = r * g;
      _b[u] += delta;
      if( _bm != null ) {
        _b[u] += m * _bm[u];
        _bm[u] = delta;
      }
    } else {
      float d = g;
      if( _bm != null ) {
        _bm[u] *= m;
        _bm[u] += d;
        d = _bm[u];
      }
      _b[u] += r * d;
    }
    if (Float.isInfinite(_b[u])) _minfo.set_unstable();
  }

  /**
   * The learning rate
   * @param n The number of training samples seen so far (for rate_annealing > 0)
   * @return Learning rate
   */
  public float rate(long n) {
    return (float)(params.rate / (1 + params.rate_annealing * n));
  }

  /**
   * The momentum - real number in [0, 1)
   * Can be a linear ramp from momentum_start to momentum_stable, over momentum_ramp training samples
   * @param n The number of training samples seen so far
   * @return momentum
   */
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
      _a = new float[units];
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
      final int n = data.length; // data contains only input features - no response is included
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
     * @param nums Array containing numerical values, can be NaN
     * @param numcat Number of horizontalized categorical non-zero values (i.e., those not being the first factor of a class)
     * @param cats Array of indices, the first numcat values are the input layer unit (==column) indices for the non-zero categorical values
     *             (This allows this array to be re-usable by the caller, without re-allocating each time)
     */
    public void setInput(long seed, final double[] nums, final int numcat, final int[] cats) {
      Arrays.fill(_a, 0f);
      for (int i=0; i<numcat; ++i) _a[cats[i]] = 1f;
      for (int i=0; i<nums.length; ++i) _a[_dinfo.numStart()+i] = Double.isNaN(nums[i]) ? 0f : (float)nums[i];

      // Input Dropout
      final double rate = params.input_dropout_ratio;
      if (rate == 0 || _dropout == null) return;
      seed += params.seed + 0x1337B4BE;
      _dropout.randomlySparsifyActivation(_a, rate, seed);
    }

  }

  /**
   * Tanh neurons - most common, most stable
   */
  public static class Tanh extends Neurons {
    public Tanh(int units) { super(units); }
    @Override protected void fprop(long seed, boolean training) {
      gemv(_a, _w, _previous._a, _b, _dropout != null ? _dropout.bits() : null);
      for( int o = 0; o < _a.length; o++ )
        _a[o] = 1f - 2f / (1f + (float)Math.exp(2*_a[o])); //evals faster than tanh(x), but is slightly less numerically stable - OK
    }
    @Override protected void bprop() {
      final long processed = _minfo.get_processed_total();
      float m = momentum(processed);
      float r = rate(processed) * (1 - m);
      for( int u = 0; u < _a.length; u++ ) {
        // Computing partial derivative g = dE/dnet = dE/dy * dy/dnet, where dE/dy is the backpropagated error
        // dy/dnet = (1 - a^2) for y(net) = tanh(net)
        float g = _e[u] * (1f - _a[u] * _a[u]);
        bprop(u, g, r, m);
      }
    }
  }

  /**
   * Tanh neurons with 50% dropout
   */
  public static class TanhDropout extends Tanh {
    public TanhDropout(int units) { super(units); }
    @Override protected void fprop(long seed, boolean training) {
      if (training) {
        seed += params.seed + 0xDA7A6000;
        _dropout.fillBytes(seed);
        super.fprop(seed, true);
      }
      else {
        super.fprop(seed, false);
        Utils.div(_a, 2.f);
      }
    }
  }

  /**
   * Maxout neurons
   */
  public static class Maxout extends Neurons {
    public Maxout(int units) { super(units); }
    @Override protected void fprop(long seed, boolean training) {
      float max = 0;
      for( int o = 0; o < _a.length; o++ ) {
        _a[o] = 0;
        if( !training || _dropout == null || _dropout.unit_active(o) ) {
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
      final long processed = _minfo.get_processed_total();
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

  /**
   * Maxout neurons with 50% dropout
   */
  public static class MaxoutDropout extends Maxout {
    public MaxoutDropout(int units) { super(units); }
    @Override protected void fprop(long seed, boolean training) {
      if (training) {
        seed += params.seed + 0x51C8D00D;
        _dropout.fillBytes(seed);
        super.fprop(seed, true);
      }
      else {
        super.fprop(seed, false);
        Utils.div(_a, 2.f);
      }
    }
  }

  /**
   * Rectifier linear unit (ReLU) neurons
   */
  public static class Rectifier extends Neurons {
    public Rectifier(int units) { super(units); }
    @Override protected void fprop(long seed, boolean training) {
      gemv(_a, _w, _previous._a, _b, _dropout != null ? _dropout.bits() : null);
      for( int o = 0; o < _a.length; o++ )
        _a[o] = Math.max(_a[o], 0f);
    }

    @Override protected void bprop() {
      final long processed = _minfo.get_processed_total();
      float m = momentum(processed);
      float r = rate(processed) * (1 - m);
      for( int u = 0; u < _a.length; u++ ) {
        //(d/dx)(max(0,x)) = 1 if x > 0, otherwise 0
        final float g = _a[u] > 0f ? _e[u] : 0;
        bprop(u, g, r, m);
      }
    }
  }

  /**
   * Rectifier linear unit (ReLU) neurons with 50% dropout
   */
  public static class RectifierDropout extends Rectifier {
    public RectifierDropout(int units) { super(units); }
    @Override protected void fprop(long seed, boolean training) {
      if (training) {
        seed += params.seed + 0x3C71F1ED;
        _dropout.fillBytes(seed);
        super.fprop(seed, true);
      }
      else {
        super.fprop(seed, false);
        Utils.div(_a, 2.f);
      }
    }
  }

  /**
   * Abstract class for Output neurons
   */
  public static abstract class Output extends Neurons {
    static final int API_WEAVER = 1;
    public static DocGen.FieldDoc[] DOC_FIELDS;
    Output(int units) { super(units); }
    protected abstract void fprop(); //don't differentiate between testing/training
    protected void fprop(long seed, boolean training) { throw new UnsupportedOperationException(); }
    protected void bprop() { throw new UnsupportedOperationException(); }
  }

  /**
   * Output neurons for classification - Softmax
   */
  public static class Softmax extends Output {
    public Softmax(int units) { super(units); }
    @Override protected void fprop() {
      gemv(_a, _w, _previous._a, _b, null);
      final float max = Utils.maxValue(_a);
      float scale = 0;
      for( int o = 0; o < _a.length; o++ ) {
        _a[o] = (float)Math.exp(_a[o] - max);
        scale += _a[o];
      }
      for( int o = 0; o < _a.length; o++ ) {
        if (Float.isNaN(_a[o]))
          throw new RuntimeException("Numerical instability, predicted NaN.");
        _a[o] /= scale;
      }
    }

    /**
     * Backpropagation for classification
     * Update every weight as follows: w += -rate * dE/dw
     * Compute dE/dw via chain rule: dE/dw = dE/dy * dy/dnet * dnet/dw, where net = sum(xi*wi)+b and y = activation function
     * @param target actual class label
     */
    protected void bprop(int target) {
//      if (target == missing_int_value) return; //ignore missing response values
      final long processed = _minfo.get_processed_total();
      float m = momentum(processed);
      float r = rate(processed) * (1 - m);
      float g; //partial derivative dE/dy * dy/dnet
      for( int u = 0; u < _a.length; u++ ) {
        final float t = (u == target ? 1 : 0);
        final float y = _a[u];
        //dy/dnet = derivative of softmax = (1-y)*y
        if (params.loss == Loss.CrossEntropy) {
          //nothing else needed, -dCE/dy * dy/dnet = target - y
          //cf. http://www.stanford.edu/group/pdplab/pdphandbook/handbookch6.html
          g = t - y;
        } else {
          assert(params.loss == Loss.MeanSquare);
          //-dMSE/dy = target-y
          g = (t - y) * (1 - y) * y;
        }
        // this call expects dE/dnet
        bprop(u, g, r, m);
      }
    }
  }

  /**
   * Output neurons for regression - Softmax
   */
  public static class Linear extends Output {
    public Linear(int units) { super(units); }
    @Override protected void fprop() {
      gemv(_a, _w, _previous._a, _b, null);
    }

    /**
     * Backpropagation for regression
     * @param target floating-point target value
     */
    protected void bprop(float target) {
//      if (target == missing_double_value) return;
      if (params.loss != Loss.MeanSquare) throw new UnsupportedOperationException("Regression is only implemented for MeanSquare error.");
      final int u = 0;
      // Computing partial derivative: dE/dnet = dE/dy * dy/dnet = dE/dy * 1
      final float g = target - _a[u]; //for MSE -dMSE/dy = target-y
      final long processed = _minfo.get_processed_total();
      float m = momentum(processed);
      float r = rate(processed) * (1 - m);
      bprop(u, g, r, m);
    }
  }

  /**
   * Mat-Vec Plus Add (with optional row dropout)
   * @param res = a*x+y (pre-allocated, will be overwritten)
   * @param a matrix of size rows x cols
   * @param x vector of length cols
   * @param y vector of length rows
   * @param row_bits if not null, check bits of this byte[] to determine whether a row is used or not
   */
  static void gemv_naive(final float[] res, final float[] a, final float[] x, final float[] y, byte[] row_bits) {
    final int cols = x.length;
    final int rows = y.length;
    assert(res.length == rows);
    for(int r = 0; r<rows; r++) {
      res[r] = 0;
      if( row_bits != null && (row_bits[r / 8] & (1 << (r % 8))) == 0) continue;
      float tmp = 0;
      for(int i = 0; i<cols; i++)
        tmp += a[r*cols+i] * x[i];
      res[r] += tmp + y[r];
    }
  }

  /**
   * Optimized Mat-Vec Plus Add (with optional row dropout)
   * Optimization: Partial sums can be evaluated in parallel
   * @param res = a*x+y (pre-allocated, will be overwritten)
   * @param a matrix of size rows x cols
   * @param x vector of length cols
   * @param y vector of length rows
   * @param row_bits if not null, check bits of this byte[] to determine whether a row is used or not
   */
  static void gemv(float[] res, float[] a, float[] x, final float[] y, byte[] row_bits) {
    final int cols = x.length;
    final int rows = y.length;
    assert(res.length == rows);
    final int extra=cols-cols%8;
    final int multiple = (cols/8)*8-1;
    int idx = 0;
    for (int r = 0; r<rows; r++) {
      res[r] = 0;
      if( row_bits == null || (row_bits[r / 8] & (1 << (r % 8))) != 0) {
        float psum0 = 0, psum1 = 0, psum2 = 0, psum3 = 0, psum4 = 0, psum5 = 0, psum6 = 0, psum7 = 0;
        for (int c = 0; c < multiple; c += 8) {
          int off = idx + c;
          psum0 += a[off    ] * x[c    ];
          psum1 += a[off + 1] * x[c + 1];
          psum2 += a[off + 2] * x[c + 2];
          psum3 += a[off + 3] * x[c + 3];
          psum4 += a[off + 4] * x[c + 4];
          psum5 += a[off + 5] * x[c + 5];
          psum6 += a[off + 6] * x[c + 6];
          psum7 += a[off + 7] * x[c + 7];
        }
        for (int j = extra; j < cols; j++)
          res[r] += a[idx + j] * x[j];
        res[r] += psum0 + psum1 + psum2 + psum3;
        res[r] += psum4 + psum5 + psum6 + psum7;
        res[r] += y[r];
      }
      idx += cols;
    }
  }

  // Test mat-vec performance
  static public class MatVecTester {
    @Test
    @Ignore
    public void run() {
      int rows = 2048;
      int cols = 1024;
      int loops = 5000;

      float [] a = new float[rows*cols];
      float [] x = new float[cols];
      float [] y = new float[rows];
      float [] res = new float[rows];
      byte [] bits = new byte[rows];

      for (int i=0;i<rows;++i) {
        y[i] = 0;
        res[i] = 0;
        bits[i] = (byte)(new String("abcdefghijklmnopqrstuvwxyz").toCharArray()[i%26]);
      }
      for (int i=0;i<cols;++i) {
        x[i] = ((float)i)/cols;
      }
      for (int i=0;i<rows;++i) {
        int off = i*cols;
        for (int j=0;j<cols;++j) {
          a[off+j] = ((float)(i+j))/cols;
        }
      }

      /**
       * naive version
       */
      System.out.println("warming up.");
      float sum = 0;
      //warmup
      for (int l=0;l<11000;++l) {
        gemv_naive(res, a, x, y, bits);
        sum += res[rows/2];
      }
      //warmup
      for (int l=0;l<11000;++l) {
        gemv(res, a, x, y, bits);
        sum += res[rows/2];
      }
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }

      /**
       * naive version
       */
      System.out.println("starting naive.");
      sum = 0;
      long start = System.currentTimeMillis();
      for (int l=0;l<loops;++l) {
        gemv_naive(res, a, x, y, bits);
        sum += res[rows/2]; //do something useful
      }
      System.out.println("result: " + sum + " and " + Utils.sum(res));
      System.out.println("Naive time: " + PrettyPrint.msecs(System.currentTimeMillis()-start, true));

      /**
       * optimized version
       */
      System.out.println("starting optimized.");
      sum = 0;
      start = System.currentTimeMillis();
      for (int l=0;l<loops;++l) {
        gemv(res, a, x, y, bits);
        sum += res[rows/2]; //do something useful
      }
      System.out.println("result: " + sum + " and " + Utils.sum(res));
      System.out.println("Optimized time: " + PrettyPrint.msecs(System.currentTimeMillis()-start, true));
    }
  }

}
