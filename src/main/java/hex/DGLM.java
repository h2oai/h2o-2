package hex;

import hex.ConfusionMatrix.ErrMetric;
import hex.DGLM.GLMModel.Status;
import hex.DLSM.ADMMSolver.NonSPDMatrixException;
import hex.DLSM.LSMSolver;
import hex.NewRowVecTask.DataFrame;
import hex.NewRowVecTask.RowFunc;
import hex.RowVecTask.Sampling;

import java.text.DecimalFormat;
import java.util.*;

import jsr166y.CountedCompleter;
import jsr166y.ForkJoinTask;
import jsr166y.RecursiveAction;
import water.*;
import water.H2O.H2OCountedCompleter;
import water.Job.ChunkProgressJob;
import water.Job.JobCancelledException;
import water.ValueArray.Column;
import water.api.Constants;
import water.util.*;
import Jama.CholeskyDecomposition;
import Jama.Matrix;

import com.google.common.base.Objects;
import com.google.gson.*;

public abstract class DGLM {
  public static final int DEFAULT_MAX_ITER = 50;
  public static final double DEFAULT_BETA_EPS = 1e-4;

  public static class GLMException extends RuntimeException {
    public GLMException(String msg) {
      super(msg);
    }
  }

  public static class LambdaMax extends Iced {
    private final double[] _z;
    private final double _var;
    private final double _nobs;

    public LambdaMax(int sz, double var, long nobs) {
      _z = MemoryManager.malloc8d(sz);
      _var = var;
      _nobs = nobs;
    }

    public void add(LambdaMax lm) { Utils.add(_z,lm._z); }

    public double value() {
      double res = Math.abs(_z[0]);
      for( int i = 1; i < _z.length; ++i )
        res = Math.max(res, Math.abs(_z[i]));
      return _var * res / _nobs;
    }

    @Override public String toString() {
      return "LambdaMax(z:" + Arrays.toString(_z) + ", val: " + value() + ")";
    }
  }

  public static class LambdaMaxFunc extends RowFunc<LambdaMax> {
    final double _mu;
    final double _var;
    final double _gPrimeMu;
    final int _sz;
    final long _nobs;

    public LambdaMaxFunc(DataFrame data, double ymu, Link l, Family f) {
      _mu = ymu;
      _gPrimeMu = l.linkDeriv(ymu);
      _var = f.variance(ymu);
      _sz = data.expandedSz();
      _nobs = data._nobs;
    }

    @Override public LambdaMax newResult() {
      return new LambdaMax(_sz, _var, _nobs);
    }

    @Override public void processRow(LambdaMax res, double[] x, int[] indexes) {
      double w = (x[x.length - 1] - _mu) * _gPrimeMu;
      for( int i = 0; i < x.length - 1; ++i )
        res._z[indexes[i]] += w * x[i];
    }

    @Override public LambdaMax reduce(LambdaMax x, LambdaMax y) {
      x.add(y);
      return x;
    }
  }

  public static class GLMJob extends ChunkProgressJob {
    public GLMJob(ValueArray data, Key dest, int xval, GLMParams params) {
      // approximate the total number of computed chunks as 25 per normal model computation + 10 iterations per xval model)
      super((params._family._family == Family.gaussian) ? data.chunks() * (xval + 1) : data.chunks() * (20 + 4 * xval),dest);
      description = "GLM(" + data._key.toString() + ")";
    }

    public boolean isDone() {
      return DKV.get(self()) == null;
    }

    @Override public float progress() {
      ChunkProgress progress = UKV.get(progressKey());
      return (progress != null ? progress.progress() : 0);
    }
  }

  public static class GLMParams extends Iced {
//    public Family _family = Family.gaussian;
    public FamilyIced _family = new FamilyIced(Family.gaussian);
    public LinkIced _link;
    public double _betaEps = 1e-4;
    public int _maxIter = 50;
    public double _caseVal;
    public double _caseWeight = 1.0;
    public CaseMode _caseMode = CaseMode.none;
    public boolean _reweightGram = true;

    public GLMParams(Family family) {
      this(family, family.defaultLink);
    }

    public GLMParams(Family family, Link link) {
      _family = new FamilyIced( family );
      _link = new LinkIced( link );
    }

    public GLMParams(Family family, Link link, double tweedieVariancePower, double tweedieLinkPower){
      _family = new FamilyIced( family, tweedieVariancePower );
      _link = new LinkIced( _family._family.defaultLink, tweedieLinkPower );
    }

    public void checkResponseCol(Column ycol, ArrayList<String> warnings) {
      switch( _family._family ) {
        case poisson:
          if( ycol._min < 0 ) throw new GLMException("Invalid response variable " + ycol._name
              + ", Poisson family requires response to be >= 0. ");
          if( ycol._domain != null && ycol._domain.length > 0 ) throw new GLMException("Invalid response variable "
              + ycol._name + ", Poisson family requires response to be integer number >= 0. Got categorical.");
          if( ycol.isFloat() ) warnings
              .add("Running family=Poisson on non-integer response column. Poisson is dicrete distribution, consider using gamma or gaussian instead.");
          break;
        case gamma:
          if( ycol._min <= 0 ) throw new GLMException("Invalid response variable " + ycol._name
              + ", Gamma family requires response to be > 0. ");
          if( ycol._domain != null && ycol._domain.length > 0 ) throw new GLMException("Invalid response variable "
              + ycol._name + ", Poisson family requires response to be integer number >= 0. Got categorical.");
          break;
        case binomial:
          if( _caseMode == CaseMode.none && (ycol._min < 0 || ycol._max > 1) ) if( ycol._min <= 0 ) throw new GLMException(
              "Invalid response variable " + ycol._name
                  + ", Binomial family requires response to be from [0,1] or have Case predicate. ");
          break;
        case tweedie:
          if( ycol._min < 0 ) throw new GLMException("Invalid response variable " + ycol._name
              + ", Tweedie family requires response to be >= 0. ");
          if( ycol._domain != null && ycol._domain.length > 0 ) throw new GLMException("Invalid response variable "
              + ycol._name + ", Tweedie family requires response to be a number >= 0. Got categorical.");
          break;

        default:
          //pass
      }
    }

    public JsonObject toJson() {
      JsonObject res = new JsonObject();
      res.addProperty("family", _family._family.toString());
      if(_family._family == Family.tweedie){
        res.addProperty("variance_power", _family._tweedieVariancePower);
        res.addProperty("link_power", _link._tweedieLinkPower);
      }
      res.addProperty("link", _link.toString());
      res.addProperty("betaEps", _betaEps);
      res.addProperty("maxIter", _maxIter);
      if( _caseMode != null && _caseMode != CaseMode.none ) {
        res.addProperty("caseVal", _caseMode.exp(_caseVal));
        res.addProperty("weight", _caseWeight);
      }
      return res;
    }

    public String toString2(){
      return String.format("GLMParams: Family(%s) glmparams.Link(%s) _betaEps(%f) _maxIter(%d), _caseVal(%f), _prior(%f), _caseMode(%s), _reweightGram(%s)",
          _family, _link, _betaEps, _maxIter, _caseVal, _caseWeight, _caseMode, _reweightGram);
    }
  }

  public enum CaseMode {
    none("n/a"), lt("<"), gt(">"), lte("<="), gte(">="), eq("="), neq("!="), ;
    final String _str;

    CaseMode(String str) {
      _str = str;
    }

    public String toString() {
      return _str;
    }

    public String exp(double v) {
      switch( this ) {
        case none:
          return "n/a";
        default:
          return "x" + _str + v;
      }
    }

    public final boolean isCase(double x, double y) {
      switch( this ) {
        case lt:
          return x < y;
        case gt:
          return x > y;
        case lte:
          return x <= y;
        case gte:
          return x >= y;
        case eq:
          return x == y;
        case neq:
          return x != y;
        default:
          assert false;
          return false;
      }
    }
  }

  /**
   * passthrough class around Link that supports Icing
   */
  public static class LinkIced extends Iced {
    public final Link _link;
    public final double _tweedieLinkPower;

    public LinkIced( Link link ){
      _link = link;
      _tweedieLinkPower = Double.NaN;
    }
    public LinkIced( Link link, Double tweedieLinkPower ){
      _link = link;
      _tweedieLinkPower = tweedieLinkPower;
    }

    public final double link(double x) {
      switch( _link ) {
        default:
          return _link.link( x );
        case tweedie:
          return Math.pow(x, _tweedieLinkPower);
      }
    }

    public final double linkDeriv(double x) {
      switch( _link ) {
        default:
          return _link.linkDeriv( x );
        case tweedie:
          return _tweedieLinkPower * Math.pow(x, _tweedieLinkPower - 1.);
      }
    }

    public final double linkInv(double x) {
      switch( _link ) {
        default:
          return _link.linkInv( x );
        case tweedie:
          return Math.pow(x, 1./_tweedieLinkPower);
      }
    }

    public final double linkInvDeriv(double x) {
      switch( _link ) {
        default:
          return _link.linkInvDeriv( x );
        case tweedie:
          double vp = (1. - _tweedieLinkPower) / _tweedieLinkPower;
          return (1./_tweedieLinkPower) * Math.pow(x, vp);
      }
    }

    public String toString2(){
      return String.format("LinkIced link(%s; _tweedieLinkPower %f)", _link.toString2(), _tweedieLinkPower);
    }
  }

  public static enum Link {
    familyDefault(0), identity(0), logit(0), log(0.1),
    //    probit(0),
    //    cauchit(0),
    //    cloglog(0),
    //    sqrt(0),
    inverse(0),
    //    oneOverMu2(0);
    tweedie(0, Double.NaN /* default: 1. - 1.5 */)
    ;
    public final double defaultBeta;
    public double tweedieLinkPower;

    Link(double b) {
      defaultBeta = b;
    }
    Link(double b, double tweedieLinkPower){
      defaultBeta = b;
      this.tweedieLinkPower = tweedieLinkPower;
    }

    public final double link(double x) {
      switch( this ) {
        case identity:
          return x;
        case logit:
          assert 0 <= x && x <= 1;
          return Math.log(x / (1 - x));
        case log:
          return Math.log(x);
        case inverse:
          double xx = (x < 0) ? Math.min(-1e-5, x) : Math.max(1e-5, x);
          return 1.0 / xx;
        case tweedie:
          return Math.pow(x, tweedieLinkPower);
        default:
          throw new RuntimeException("unsupported link function id  " + this);
      }
    }

    public final double linkDeriv(double x) {
      switch( this ) {
        case logit:
          return 1 / (x * (1 - x));
        case identity:
          return 1;
        case log:
          return 1.0 / x;
        case inverse:
          return -1.0 / (x * x);
        case tweedie:
          return tweedieLinkPower * Math.pow(x, tweedieLinkPower - 1.);
        default:
          throw H2O.unimpl();
      }
    }

    public final double linkInv(double x) {
      switch( this ) {
        case identity:
          return x;
        case logit:
          return 1.0 / (Math.exp(-x) + 1.0);
        case log:
          return Math.exp(x);
        case inverse:
          double xx = (x < 0) ? Math.min(-1e-5, x) : Math.max(1e-5, x);
          return 1.0 / xx;
        case tweedie:
          return Math.pow(x, 1./tweedieLinkPower);
        default:
          throw new RuntimeException("unexpected link function id  " + this);
      }
    }

    public final double linkInvDeriv(double x) {
      switch( this ) {
        case identity:
          return 1;
        case logit:
          double g = Math.exp(-x);
          double gg = (g + 1) * (g + 1);
          return g / gg;
        case log:
          //return (x == 0)?MAX_SQRT:1/x;
          return Math.max(Math.exp(x), Double.MIN_NORMAL);
        case inverse:
          double xx = (x < 0) ? Math.min(-1e-5, x) : Math.max(1e-5, x);
          return -1 / (xx * xx);
        case tweedie:
          double vp = (1. - tweedieLinkPower) / tweedieLinkPower;
          return (1./tweedieLinkPower) * Math.pow(x, vp);
        default:
          throw new RuntimeException("unexpected link function id  " + this);
      }
    }

    public String toString2(){
      String s = "link(";
      switch(this){
        case identity:
          s += "identity: "; break;
        case logit:
          s += "logit: "; break;
        case log:
          s += "log: "; break;
        case inverse:
          s += "inverse: "; break;
        case tweedie:
          s += "tweedie: "; break;
        case familyDefault:
          s += "familyDefault: "; break;
        default:
          s+= " BAD DEFAULT: "; break;
      }
      s += String.format("defaultBeta: %f", defaultBeta);

      switch(this){
        case tweedie: s += String.format("; tweedieLinkPower: %2.2f", tweedieLinkPower); break;
        default: break;
      }

      s += ")";
      return s;
    }
  }

  // helper function
  static final double y_log_y(double y, double mu) {
    mu = Math.max(Double.MIN_NORMAL, mu);
    return (y != 0) ? (y * Math.log(y / mu)) : 0;
  }


  /**
   * passthrough class around family that properly supports icing
   */
  public static class FamilyIced extends Iced {
    public final double _tweedieVariancePower;
    public final Family _family;

    public FamilyIced( Family family ){
      _family = family;
      _tweedieVariancePower = Double.NaN;
    }
    public FamilyIced( Family family, double tweedieVariancePower ){
      _family = family;
      _tweedieVariancePower = tweedieVariancePower;
    }

    public double mustart(double y) {
      return _family.mustart(y);
    }
    public double variance(double mu) {
      switch( _family ){
        default:
          return _family.variance( mu );
        case tweedie:
          return Math.pow(mu, _tweedieVariancePower);
      }

    }
    public double deviance(double yr, double ym){
      switch( _family ){
        case gaussian:
        case binomial:
        case poisson:
        case gamma:
          return _family.deviance(yr,  ym);
        case tweedie:
          double one_minus_p = 1. - _tweedieVariancePower;
          double two_minus_p = 2. - _tweedieVariancePower;
          return Math.pow(yr, two_minus_p) / (one_minus_p * two_minus_p) - (yr * (Math.pow(ym, one_minus_p)))/one_minus_p + Math.pow(ym, two_minus_p)/two_minus_p;
        default:
          throw new RuntimeException("FamilyIced.deviance unknown family");
      }
    }

    public String toString2(){
      return String.format("FamilyIced(_family %s; tweedievp %f)", _family.toString2(), _tweedieVariancePower);
    }
  }

  // supported families
  public enum Family {
    gaussian(Link.identity, null), binomial(Link.logit, new double[] { Double.NaN, 1.0, 0.5 }), poisson(Link.log, null),
    gamma(Link.inverse, null), tweedie(Link.tweedie, null, Double.NaN);
    public Link defaultLink;
    public final double[] defaultArgs;
    public double tweedieVariancePower = Double.NaN;

    Family(Link l, double[] d) {
      defaultLink = l;
      defaultArgs = d;
    }

    Family(Link link, double[] d, double tweedieVariancePower){
      defaultLink = link;
      defaultArgs = d;
      this.tweedieVariancePower = tweedieVariancePower;
    }

    public double mustart(double y) {
      switch( this ) {
        case gaussian:
          return y;
        case binomial:
          return 0.5;
        case poisson:
          return y + 0.1;
        case gamma:
          return y;
        case tweedie:
          return y + (y==0. ? 0.1 : 0.);
        default:
          throw new RuntimeException("unimplemented");
      }
    }

    public double variance(double mu) {
      switch( this ) {
        case gaussian:
          return 1;
        case binomial:
          assert (0 <= mu && mu <= 1) : "mu out of bounds<0,1>:" + mu;
          return mu * (1 - mu);
        case poisson:
          return mu;
        case gamma:
          return mu * mu;
        case tweedie:
          return Math.pow(mu, tweedieVariancePower);
        default:
          throw new RuntimeException("unknown family Id " + this);
      }
    }

    /**
     * Per family deviance computation.
     *
     * @param yr
     * @param ym
     * @return
     */
    public double deviance(double yr, double ym) {
      switch( this ) {
        case gaussian:
          return (yr - ym) * (yr - ym);
        case binomial:
          return 2 * ((y_log_y(yr, ym)) + y_log_y(1 - yr, 1 - ym));
        case poisson:
          if( yr == 0 ) return 2 * ym;
          return 2 * ((yr * Math.log(yr / ym)) - (yr - ym));
        case gamma:
          if( yr == 0 ) return -2;
          return -2 * (Math.log(yr / ym) - (yr - ym) / ym);
        case tweedie:
          // Theory of Dispersion Models: Jorgensen
          // pg49: $$ d(y;\mu) = 2 [ y \cdot \left(\tau^{-1}(y) - \tau^{-1}(\mu) \right) - \kappa \{ \tau^{-1}(y)\} + \kappa \{ \tau^{-1}(\mu)\} ] $$
          // pg133: $$ \frac{ y^{2 - p} }{ (1 - p) (2-p) }  - \frac{y \cdot \mu^{1-p}}{ 1-p} + \frac{ \mu^{2-p} }{ 2 - p }$$
          double one_minus_p = 1. - tweedieVariancePower;
          double two_minus_p = 2. - tweedieVariancePower;
          return Math.pow(yr, two_minus_p) / (one_minus_p * two_minus_p) - (yr * (Math.pow(ym, one_minus_p)))/one_minus_p + Math.pow(ym, two_minus_p)/two_minus_p;

        default:
          throw new RuntimeException("unknown family Id " + this);
      }
    }

    public String toString2(){
      String s = "family(";
      switch(this){
        case gaussian: s += "gaussian: "; break;
        case binomial: s += "binomial: "; break;
        case gamma: s += "gamma: "; break;
        case poisson: s += "poisson: "; break;
        case tweedie: s += String.format("tweedie: variancePower %2.2f", this.tweedieVariancePower); break;
        default: s += "BAD UNKNOWN"; break;
      }
      s += String.format(", link: %s)", defaultLink);
      return s;
    }
  }

  public abstract GLMModel solve(GLMModel model, ValueArray ary);

  public static final DecimalFormat dformat = new DecimalFormat("0.#####");

  static final class Cholesky {
    protected final double[][] _xx;
    protected final double[] _diag;
    protected boolean _isSPD;

    Cholesky(double[][] xx, double[] diag) {
      _xx = xx;
      _diag = diag;
    }

    public Cholesky(Gram gram) {
      _xx = gram._xx.clone();
      for( int i = 0; i < _xx.length; ++i )
        _xx[i] = gram._xx[i].clone();
      _diag = gram._diag.clone();

    }

    public String toString() {
      return "";
    }

    /**
     * Find solution to A*x = y.
     *
     * If the gram is not in Cholesky decomposed form, it will perform Cholesky decomposition first.
     * Result is stored in the y input vector. May throw NonSPDMatrix exception in case Gram is not
     * positive definite.
     *
     * @param y
     */
    public final void solve(double[] y) {
      if( !_isSPD ) throw new NonSPDMatrixException();
      assert _xx.length + _diag.length == y.length;
      // diagonal
      for( int k = 0; k < _diag.length; ++k )
        y[k] /= _diag[k];
      // rest
      final int n = y.length;
      // Solve L*Y = B;
      for( int k = _diag.length; k < n; ++k ) {
        for( int i = 0; i < k; i++ )
          y[k] -= y[i] * _xx[k - _diag.length][i];
        y[k] /= _xx[k - _diag.length][k];
      }
      // Solve L'*X = Y;
      for( int k = n - 1; k >= _diag.length; --k ) {
        for( int i = k + 1; i < n; ++i )
          y[k] -= y[i] * _xx[i - _diag.length][k];
        y[k] /= _xx[k - _diag.length][k];
      }
      // diagonal
      for( int k = _diag.length - 1; k >= 0; --k ) {
        for( int i = _diag.length; i < n; ++i )
          y[k] -= y[i] * _xx[i - _diag.length][k];
        y[k] /= _diag[k];
      }
    }
  }

  static final class Gram extends Iced {
    double[][] _xx;
    double[] _diag;
    double[] _xy;
    double _yy;
    long _nobs;

    public Gram() {}

    public Gram(int N, int diag) {
      _xy = MemoryManager.malloc8d(N);
      _xx = new double[N - diag][];
      _diag = MemoryManager.malloc8d(diag);
      int dense = N - _diag.length;
      for( int i = 0; i < dense; ++i )
        _xx[i] = MemoryManager.malloc8d(diag + i + 1);
    }

    public String pprint(double[][] arr) {
      int colDim = 0;
      for( double[] line : arr )
        colDim = Math.max(colDim, line.length);
      StringBuilder sb = new StringBuilder();
      int max_width = 0;
      int[] ilengths = new int[colDim];
      Arrays.fill(ilengths, -1);
      for( double[] line : arr ) {
        for( int c = 0; c < line.length; ++c ) {
          double d = line[c];
          String dStr = dformat.format(d);
          if( dStr.indexOf('.') == -1 ) dStr += ".0";
          ilengths[c] = Math.max(ilengths[c], dStr.indexOf('.'));
          int prefix = (d >= 0 ? 1 : 2);
          max_width = Math.max(dStr.length() + prefix, max_width);
        }
      }
      for( double[] line : arr ) {
        for( int c = 0; c < line.length; ++c ) {
          double d = line[c];
          String dStr = dformat.format(d);
          if( dStr.indexOf('.') == -1 ) dStr += ".0";
          for( int x = dStr.indexOf('.'); x < ilengths[c] + 1; ++x )
            sb.append(' ');
          sb.append(dStr);
          if( dStr.indexOf('.') == -1 ) sb.append('.');
          for( int i = dStr.length() - Math.max(0, dStr.indexOf('.')); i <= 5; ++i )
            sb.append('0');
        }
        sb.append("\n");
      }
      return sb.toString();
    }

    public void addDiag(double d) {
      for( int i = 0; i < _diag.length; ++i )
        _diag[i] += d;
      for( int i = 0; i < _xx.length - 1; ++i )
        _xx[i][_xx[i].length - 1] += d;
    }

    /**
     * Compute the cholesky decomposition.
     *
     * In case our gram starts with diagonal submatrix of dimension N, we exploit this fact to reduce the complexity of the problem.
     * We use the standard decompostion of the cholesky factorization into submatrices.
     *
     * We split the Gram into 3 regions (4 but we only consider lower diagonal, sparse means diagonal region in this context):
     *     diagonal
     *     diagonal*dense
     *     dense*dense
     * Then we can solve the cholesky in 3 steps:
     *  1. We solve the diagnonal part right away (just do the sqrt of the elements).
     *  2. The diagonal*dense part is simply divided by the sqrt of diagonal.
     *  3. Compute Cholesky of dense*dense - outer product of cholesky of diagonal*dense computed in previous step
     *
     * @param chol
     * @return
     */
    public Cholesky cholesky(Cholesky chol, final Key jobKey) {
      long t = System.currentTimeMillis();
      if( chol == null ) {
        double[][] xx = _xx.clone();
        for( int i = 0; i < xx.length; ++i )
          xx[i] = xx[i].clone();
        chol = new Cholesky(xx, _diag.clone());
      }
      final Cholesky fchol = chol;
      final int sparseN = _diag.length;
      final int denseN = _xy.length - sparseN;
      // compute the cholesky of the diagonal and diagonal*dense parts
      if( _diag != null ) for( int i = 0; i < sparseN; ++i ) {
        double d = 1.0 / (chol._diag[i] = Math.sqrt(_diag[i]));
        for( int j = 0; j < denseN; ++j )
          chol._xx[j][i] = d*_xx[j][i];
      }
      RecursiveAction [] ras = new RecursiveAction[denseN];
      // compute the outer product of diagonal*dense
      for( int i = 0; i < denseN; ++i ) {
        final int fi = i;
        ras[i] = new RecursiveAction() {
          @Override protected void compute() {
            for( int j = 0; j <= fi; ++j ) {
              if(((j & 15) == 0) && jobKey != null && !Job.isRunning(jobKey))return;
              double s = 0;
              for( int k = 0; k < sparseN; ++k )
                s += fchol._xx[fi][k] * fchol._xx[j][k];
              fchol._xx[fi][j + sparseN] = _xx[fi][j + sparseN] - s;
            }
          }
        };
      }
      ForkJoinTask.invokeAll(ras);
      Log.info("GLM(" + jobKey + "): CHOL PRECOMPUTE TOOK " + (System.currentTimeMillis() - t) + "ms");
      if(jobKey != null && !Job.isRunning(jobKey))
        throw new JobCancelledException();
      // compute the choesky of dense*dense-outer_product(diagonal*dense)
      // TODO we still use Jama, which requires (among other things) copy and expansion of the matrix. Do it here without copy and faster.
      double[][] arr = new double[denseN][];
      for( int i = 0; i < arr.length; ++i )
        arr[i] = Arrays.copyOfRange(fchol._xx[i], sparseN, sparseN + denseN);
      // make it symmetric
      for( int i = 0; i < arr.length; ++i )
        for( int j = 0; j < i; ++j )
          arr[j][i] = arr[i][j];
      CholeskyDecomposition c = new Matrix(arr).chol();
      fchol._isSPD = c.isSPD();
      arr = c.getL().getArray();
      for( int i = 0; i < arr.length; ++i )
        System.arraycopy(arr[i], 0, fchol._xx[i], sparseN, i + 1);
      return chol;
    }

    public double[][] getXX() {
      final int N = _xy.length;
      double[][] xx = new double[N][];
      for( int i = 0; i < N; ++i )
        xx[i] = MemoryManager.malloc8d(N);
      for( int i = 0; i < _diag.length; ++i )
        xx[i][i] = _diag[i];
      for( int i = 0; i < _xx.length; ++i ) {
        for( int j = 0; j < _xx[i].length; ++j ) {
          xx[i + _diag.length][j] = _xx[i][j];
          xx[j][i + _diag.length] = _xx[i][j];
        }
      }
      return xx;
    }

    public void add(Gram grm) {
      final int N = _xy.length;
      assert N > 0;
      _yy += grm._yy;
      _nobs += grm._nobs;
      Utils.add(_xx,grm._xx);
      Utils.add(_xy,grm._xy);
      // add the diagonals
      Utils.add(_diag,grm._diag);
    }

    public final boolean hasNaNsOrInfs() {
      for( int i = 0; i < _xy.length; ++i )
        if( Double.isInfinite(_xy[i]) || Double.isNaN(_xy[i]) ) return true;
      for( int i = 0; i < _xx.length; ++i )
        for( int j = 0; j < _xx[i].length; ++j )
          if( Double.isInfinite(_xx[i][j]) || Double.isNaN(_xx[i][j]) ) return true;
      for( double d : _diag )
        if( Double.isInfinite(d) || Double.isNaN(d) ) return true;
      return false;
    }
  }

  private static class GLMXvalSetup extends Iced {
    final int _id;

    public GLMXvalSetup(int i) {
      _id = i;
    }
  }

  private static class GLMXValTask extends MRTask {
    transient ValueArray _ary;
    Key _aryKey;
    Job _job;
    boolean _standardize;
    LSMSolver _lsm;
    GLMParams _glmp;
    double[] _betaStart;
    int[] _cols;
    double[] _thresholds;
    final int _folds;
    Key[] _models;
    boolean _parallel;
    final double _prior;

    public GLMXValTask(Job job, int folds, ValueArray ary, int[] cols, boolean standardize, LSMSolver lsm,
        GLMParams glmp, double[] betaStart, double prior, double[] thresholds, boolean parallel) {
      _job = job;
      _folds = folds;
      _ary = ary;
      _aryKey = ary._key;
      _cols = cols;
      _standardize = standardize;
      _lsm = lsm;
      _glmp = glmp;
      _betaStart = betaStart;
      _thresholds = thresholds;
      _parallel = parallel;
      _prior = prior;
    }

    @Override public void init() {
      super.init();
      _ary = DKV.get(_aryKey).get();
      _models = new Key[_folds];
    }

    @Override public void map(Key key) {
      GLMXvalSetup setup = DKV.get(key).get();
      Sampling s = new Sampling(setup._id, _folds, false);
      assert _models[setup._id] == null;
      Key mkey = _models[setup._id] = GLMModel.makeKey(false);
      DataFrame data = getData(_ary, _cols, s, _standardize);
      try {
        DGLM.buildModel(_job, mkey, data, _lsm, _glmp,_betaStart.clone(), _prior, 0, _parallel);
      } catch( JobCancelledException e ) {
        Lockable.delete(_models[setup._id]);
      }
      // cleanup before sending back
      UKV.remove(key);
      _betaStart = null;
      _lsm = null;
      _glmp = null;
      _cols = null;
      _aryKey = null;
    }

    @Override public void reduce(DRemoteTask drt) {
      GLMXValTask other = (GLMXValTask) drt;
      if( _models == null ) _models = other._models;
      if( other._models != _models ) {
        for( int i = 0; i < _models.length; ++i )
          if( _models[i] == null ) _models[i] = other._models[i];
          else assert other._models[i] == null;
      }
    }
  }

  public static class GLMModel extends water.OldModel {
    public enum Status {
      NotStarted, ComputingModel, ComputingValidation, Done, Cancelled, Error
    };

    String _error;
    final Sampling _s;
    final int[] _colCatMap;
    public final boolean _converged;
    public final boolean _standardized;

    public final int _iterations;     // Iterations used to solve
    public final long _time;          // Total solve time in millis
    public long _lsmSolveTime;
    public final LSMSolver _solver;   // Which solver is used
    public final GLMParams _glmParams;
    public final long _dof;
    public final long _nCols;
    public final long _nLines;
    final int _response;

    public double[] _beta;            // The output coefficients!  Main model result.
    public double[] _normBeta;        // normalized coefficients

    final double _prior;

    public String[] _warnings;
    public GLMValidation[] _vals;
    // Empty constructor for deseriaization

    Status _status;

    public Status status() {
      return _status == null ? Status.NotStarted : _status;
    }

    public String error() {
      return _error;
    }

    public int rank() {
      int res = 0;
      if( _beta == null ) return res;
      for( int i = 0; i < _beta.length-1; ++i)
        if(_beta[i] != 0 ) ++res;
      return res;
    }

    public boolean isSolved() {
      return _beta != null;
    }

    public static final String NAME = GLMModel.class.getSimpleName();
    public static final String KEY_PREFIX = "__GLMModel_";

    // Hand out the coffients.  Must be treated as a read-only array.
    public double[] beta() {
      return _beta;
    }

    // Warm-start setup; clone the incoming array since we will be mutating it

    public static final Key makeKey(boolean visible) {
      return visible ? Key.make(KEY_PREFIX + Key.make()) : Key.make(Key.make()._kb, (byte) 0, Key.DFJ_INTERNAL_USER,
          H2O.SELF);
    }

    /**
     * Ids of selected columns (the last idx is the response variable) of the original dataset, if
     * it still exists in H2O, or null.
     *
     * @return array of column ids, the last is the response var.
     */
    public int[] selectedColumns() {
      if( DKV.get(_dataKey) == null ) return null;
      ValueArray ary = DKV.get(_dataKey).get();
      HashSet<String> colNames = new HashSet<String>();
      for( int i = 0; i < _va._cols.length - 1; ++i )
        colNames.add(_va._cols[i]._name);
      String responseCol = _va._cols[_va._cols.length - 1]._name;
      int[] res = new int[colNames.size() + 1];
      int j = 0;
      for( int i = 0; i < ary._cols.length; ++i )
        if( colNames.contains(ary._cols[i]._name) ) res[j++] = i;
        else if( ary._cols[i]._name.equals(responseCol) ) res[res.length - 1] = i;
      return res;
    }

    /**
     * Expanded (categoricals expanded to vector of levels) ordered list of column names.
     *
     * @return
     */
    public String xcolNames() {
      StringBuilder sb = new StringBuilder();
      for( ValueArray.Column C : _va._cols ) {
        if( C._domain != null ) for( int i = 1; i < C._domain.length; ++i )
          sb.append(C._name).append('.').append(C._domain[i]).append(',');
        else sb.append(C._name).append(',');
      }
      sb.setLength(sb.length() - 1); // Remove trailing extra comma
      return sb.toString();
    }


    public GLMModel(Status status, float progress, Key k, DataFrame data, double prior,  double[] beta, double[] normBeta,
        GLMParams glmp, LSMSolver solver, long nLines, long nCols, boolean converged, int iters, long time,
        String[] warnings) {
      this(status, progress, k, data._ary, data._modelDataMap, data._colCatMap, data._response, data._standardized,
          prior,data.getSampling(), beta, normBeta, glmp, solver, nLines, nCols, converged, iters, time, warnings);
    }

    public GLMModel(Status status, float progress, Key k, ValueArray ary, int[] colIds, int[] colCatMap, int response,
        boolean standardized, double prior, Sampling s, double[] beta, double[] normBeta, GLMParams glmp, LSMSolver solver,
        long nLines, long nCols, boolean converged, int iters, long time, String[] warnings) {
      super(k, colIds, ary._key);
      _prior = prior;
      _status = status;
      _colCatMap = colCatMap;
      assert _va._cols.length == _colCatMap.length-1;
      _beta = beta;
      _normBeta = normBeta;
      _glmParams = glmp;
      _s = s;
      _standardized = standardized;
      _converged = converged;
      _iterations = iters;
      _time = time;
      _solver = solver;
      _warnings = warnings;
      _dof = nLines - 1 - rank();
      _nLines = nLines;
      _nCols = nCols;
      _response = response;
    }

    public boolean converged() {
      return _converged;
    }

    public void store(Key job_key) {
      clone().update(job_key);
    }

    @Override
    public GLMModel clone(){
      GLMModel res = (GLMModel)super.clone();
      if( res._beta != null ) res._beta = res._beta.clone();
      if( res._normBeta != null ) res._normBeta  = res._normBeta.clone();
      res._vals = res._vals == null?null:res._vals.clone();
      return res;
    }

    @Override public Futures delete_impl(Futures fs) { 
      if( _vals != null ) 
        for( GLMValidation val : _vals )
          if( val._modelKeys != null ) 
            for( Key k : val._modelKeys )
              ((GLMModel)DKV.get(k).get()).delete();
      return fs;
    }

    public static class GLMValidationTask extends MRTask<GLMValidationTask>{
      final GLMModel _m;
      final OldModel _adaptedModel;
      final int _response;
      final double [] _thresholds;
      final double _ymu;
      GLMValidation _res;
      final Sampling _sampling;

      public GLMValidationTask(GLMModel m, ValueArray ary, double prior, Sampling s,double [] thresholds){
        _m = m;
        _sampling = s;
        _adaptedModel = m.adapt(ary);
        _thresholds = thresholds;
        int response = -1;
        final String responseName = m.responseName();
        for(int i = 0; i < ary._cols.length && response == -1; ++i)
          if(ary._cols[i]._name.equalsIgnoreCase(responseName))
            response = i;
        if(response == -1)throw new RuntimeException("Incompatible dataset, missing response '" + responseName + "' in '" + ary._key + "'");
        _response = response;
//
//        if(Double.isNaN(ymu) || m._glmParams._caseMode != CaseMode.none){
//          final CaseMode caseMode = m._glmParams._caseMode;
//          final double caseVal = m._glmParams._caseVal;
//          ymu = new RowFunc<YMUVal>(){
//            @Override public YMUVal newResult() {return new YMUVal();}
//            @Override public void processRow(YMUVal res, double[] x, int[] indexes) {
//              double y = x[0];
//              if(caseMode != CaseMode.none) y = caseMode.isCase(y,caseVal)?1:0;
//              res.add(y);
//            }
//            @Override public YMUVal reduce(YMUVal x, YMUVal y) {return x.add(y);}
//          }.apply(null, new DataFrame(ary,new int[]{response},null, false, false)).val();
//        }
        _ymu = prior;
        System.out.println("validation with y = " + _ymu);
      }

      @Override public void map(Key key) {
        GLMValidation res = new GLMValidation();
        final OldModel adaptedModel = (OldModel)_adaptedModel.clone();
        if( _m._glmParams._family._family == Family.binomial ) {
          res._cm = new ConfusionMatrix[_thresholds.length];
          for( int i = 0; i < _thresholds.length; ++i )
            res._cm[i] = new ConfusionMatrix(2);
        }
        ValueArray ary = DKV.get(ValueArray.getArrayKey(key)).get();
        ValueArray.Column response = ary._cols[_response];
        AutoBuffer bits = ary.getChunk(key);
        int nrows = bits.remaining()/ary.rowSize();
        Sampling s = _sampling == null?null:_sampling.clone();
        for(int rid = 0; rid < nrows; ++rid){
          if( s != null && s.skip(rid) ) continue;
          if(ary.isNA(bits, rid, response))continue;
          double yr = ary.datad(bits, rid, response);
          double ym = adaptedModel.score(ary, bits, rid);
          if(Double.isNaN(ym))continue;
          ++res._n;
          if(_m._glmParams._caseMode != CaseMode.none)
            yr = _m._glmParams._caseMode.isCase(yr, _m._glmParams._caseVal)?1:0;
          res._deviance += _m._glmParams._family.deviance(yr, ym);
          res._nullDeviance += _m._glmParams._family.deviance(yr, _ymu);
          if(_m._glmParams._family._family == Family.poisson ) { // aic for poisson
            res._err += (ym - yr) * (ym - yr);
            long y = Math.round(yr);
            double logfactorial = 0;
            for( long i = 2; i <= y; ++i )
              logfactorial += Math.log(i);
            res._aic += (yr * Math.log(ym) - logfactorial - ym);
          } else if( _m._glmParams._family._family == Family.binomial ) { // cm computation for binomial
            if( yr < 0 || yr > 1 ) throw new RuntimeException("response variable value out of range: " + yr);
            int i = 0;
            for( double t : _thresholds ) {
              int p = ym >= t ? 1 : 0;
              res._cm[i++].add((int) yr, p);
            }
          } else res._err += (ym - yr) * (ym - yr);
        }
        _res = res;
      }
      @Override public void reduce(GLMValidationTask drt) {
        if(_res == null)_res = drt._res;
        else {
          _res._n += drt._res._n;
          _res._nullDeviance += drt._res._nullDeviance;
          _res._deviance += drt._res._deviance;
          _res._aic += drt._res._aic;
          _res._err += drt._res._err;
          _res._caseCount += drt._res._caseCount;
          if( _res._cm != null ) {
            for( int i = 0; i < _res._cm.length; ++i )
              _res._cm[i].add(drt._res._cm[i]);
          } else _res._cm = drt._res._cm;
        }
      }
    }

//    // Validate on a dataset.  Columns must match, including the response column.
//    public GLMValidation validateOn(Job job, ValueArray ary, Sampling s, double[] thresholds)
//        throws JobCancelledException {
//      int[] modelDataMap = ary.getColumnIds(_va.colNames());//columnMapping(ary.colNames());
//      if( !isCompatible(modelDataMap) ) // This dataset is compatible or not?
//        throw new GLMException("incompatible dataset");
//      DataFrame data = new DataFrame(ary, modelDataMap, s, false, true);
//      double ymu = ary._cols[modelDataMap[modelDataMap.length - 1]]._mean;
//      if(_glmParams._caseMode != CaseMode.none || Double.isNaN(ymu)){ // we need to compute the mean of the response...
//        final CaseMode caseMode = _glmParams._caseMode;
//        final double caseVal = _glmParams._caseVal;
//        ymu = new RowFunc<YMUVal>(){
//          @Override public YMUVal newResult() {return new YMUVal();}
//          @Override public void processRow(YMUVal res, double[] x, int[] indexes) {
//            for(double d:x)if(Double.isNaN(d))return;
//            double y = x[x.length-1];
//            if(caseMode != CaseMode.none) y = caseMode.isCase(y,caseVal)?1:0;
//            res.add(y);
//          }
//          @Override public YMUVal reduce(YMUVal x, YMUVal y) {return x.add(y);}
//        }.apply(null, data).val();
//      }
//      GLMValidationFunc f = new GLMValidationFunc(this, _glmParams, _beta, thresholds,ymu);
//      GLMValidation val = f.apply(job, data);
//      val._modelKey = _selfKey;
//      if( _vals == null ) _vals = new GLMValidation[] { val };
//      else {
//        int n = _vals.length;
//        _vals = Arrays.copyOf(_vals, n + 1);
//        _vals[n] = val;
//      }
//      return val;
//    }
    // Validate on a dataset.  Columns must match, including the response column.

    public GLMValidation validateOn(Job job, ValueArray ary, Sampling s, double[] thresholds)
        throws JobCancelledException {
      long t1 = System.currentTimeMillis();
      GLMValidationTask valtsk = new GLMValidationTask(this, ary, _prior, s,thresholds);
      valtsk.invoke(ary._key);
      GLMValidation res = valtsk._res;
      res._dataKey = ary._key;
      res._modelKey = this._key;
      res._time = System.currentTimeMillis() - t1;
      if( _glmParams._family._family != Family.binomial ) res._err = Math.sqrt(res._err / res._n);
      res._dataKey = ary._key;
      res._thresholds = thresholds;
      res._s = s;
      res.computeBestThreshold(ErrMetric.SUMC);
      res.computeAUC();
      switch( _glmParams._family._family ) {
        case gaussian:
          res._aic = res._n * (Math.log(res._deviance / res._n * 2 * Math.PI) + 1) + 2;
          break;
        case binomial:
          res._aic = res._deviance;
          break;
        case poisson:
          res._aic *= -2;
          break; // aic is set during the validation task
        case gamma:
          res._aic = Double.NaN;
          break; // aic for gamma is not computed
        case tweedie:
          res._aic = Double.NaN;
          break;
        default:
          assert false : "missing implementation for family " + _glmParams._family;
      }
      res._aic += 2 * (rank()+1);//_glmp._family.aic(res._deviance, res._n, _beta.length);
      if( _vals == null ) _vals = new GLMValidation[] { res };
      else {
        _vals = Arrays.copyOf(_vals, _vals.length + 1);
        _vals[_vals.length - 1] = res;
      }
      return res;
    }
    public GLMValidation xvalidate(Job job, ValueArray ary, int folds, double[] thresholds, boolean parallel)
        throws JobCancelledException {
      int[] modelDataMap = ary.getColumnIds(_va.colNames());//columnMapping(ary.colNames());
      if( !isCompatible(modelDataMap) )  // This dataset is compatible or not?
        throw new GLMException("incompatible dataset");
      final int myNodeId = H2O.SELF.index();
      final int cloudsize = H2O.CLOUD.size();
      Key[] keys = new Key[folds];
      for( int i = 0; i < folds; ++i )
        DKV.put(
            keys[i] = Key.make(Key.make()._kb, (byte) 0, Key.DFJ_INTERNAL_USER, H2O.CLOUD._memary[(myNodeId + i)
                % cloudsize]), new GLMXvalSetup(i));
      DKV.write_barrier();
      GLMXValTask tsk = new GLMXValTask(job, folds, ary, modelDataMap, _standardized, _solver, _glmParams, _normBeta,_prior,
          thresholds, parallel);
      long t1 = System.currentTimeMillis();
      if( parallel ) tsk.invoke(keys);       // Needs a CPS-style transform here
      else {
        tsk.keys(keys);
        tsk.init();
        for( int i = 0; i < keys.length; i++ ) {
          GLMXValTask child = new GLMXValTask(job, folds, ary, modelDataMap, _standardized, _solver, _glmParams,
              _normBeta, _prior, thresholds, parallel);
          child.keys(keys);
          child.init();
          child.map(keys[i]);
          tsk.reduce(child);
        }
      }
      if( !Job.isRunning(job.self()) ) throw new JobCancelledException();
      GLMValidation res = new GLMValidation(_key, tsk._models, ErrMetric.SUMC, thresholds, System.currentTimeMillis() - t1);
      if( _vals == null ) _vals = new GLMValidation[] { res };
      else {
        _vals = Arrays.copyOf(_vals, _vals.length + 1);
        _vals[_vals.length - 1] = res;
      }
      return res;
    }
    @Override public JsonObject toJson() {
      JsonObject res = new JsonObject();
      res.addProperty(Constants.VERSION, H2O.VERSION);
      res.addProperty(Constants.TYPE, GLMModel.class.getName());
      res.addProperty("model_time", _time);
      res.addProperty("model_iterations", _iterations);
      res.addProperty("lsm_time", _lsmSolveTime);
      res.addProperty("dof", _dof);
      res.addProperty("nLines", _nLines);
      res.addProperty("nCols", _nCols);
      res.addProperty(Constants.MODEL_KEY, _key.toString());
      if( _warnings != null ) {
        JsonArray warnings = new JsonArray();
        for( String w : _warnings )
          warnings.add(new JsonPrimitive(w));
        res.add("warnings", warnings);
      }
      if( _beta == null ) return res; // Not solved!

      // Get the coefficents out in a pretty format
      JsonObject coefs = new JsonObject();
      JsonObject normalizedCoefs = new JsonObject();
      int idx = 0;
      JsonArray colNames = new JsonArray();
      for( int i = 0; i < _va._cols.length - 1; i++ ) {
        ValueArray.Column C = _va._cols[i];
        if( C._domain != null ) for( int j = 1; j < C._domain.length; ++j ) {
          String d = C._domain[j];
          String cname = C._name + "." + d;
          colNames.add(new JsonPrimitive(cname));
          if( _standardized ) normalizedCoefs.addProperty(cname, _normBeta[idx]);
          coefs.addProperty(cname, _beta[idx++]);
        }
        else {
          colNames.add(new JsonPrimitive(C._name));
          if( _standardized ) normalizedCoefs.addProperty(C._name, _normBeta[idx]);
          double b = _beta[idx];//*_normMul[idx];
          coefs.addProperty(C._name, b);
          //norm += b*_normSub[idx]; // Also accumulate the intercept adjustment
          idx++;
        }
      }
      res.add("column_names", colNames);
      if( _standardized ) normalizedCoefs.addProperty("Intercept", _normBeta[_normBeta.length - 1]);
      coefs.addProperty("Intercept", _beta[_beta.length - 1]);
      res.add("coefficients", coefs);
      if( _standardized ) res.add("normalized_coefficients", normalizedCoefs);
      res.add("LSMParams", _solver.toJson());
      res.add("GLMParams", _glmParams.toJson());
      res.addProperty("iterations", _iterations);
      if( _vals != null ) {
        JsonArray vals = new JsonArray();
        for( GLMValidation v : _vals )
          vals.add(v.toJson());
        res.add("validations", vals);
      }
      return res;
    }

    /**
     * Single row scoring, on properly ordered data. Will return NaN if any data element contains a
     * NaN.
     */
    protected double score0(double[] data) {
      double p = 0;             // Prediction; scored value
      for( int i = 0; i < data.length; i++ ) {
        int idx = _colCatMap[i];
        if( idx + 1 == _colCatMap[i + 1] ) { // No room for categories ==> numerical
                                             // No normalization???  These betas came from the JSON, not the
                                             // original model build.  The JSON has the beta's AFTER being
                                             // denormalized, so that the user-visible equation is to simply apply
                                             // the predictors directly (instead of normalizing them).
          double d = data[i];// - _normSub[idx]) * _normMul[idx];
          p += _beta[idx] * d;
        } else {
          int d = (int) data[i]; // Enum value d can be -1 if we got enum values not seen in training
          if(d == 0) continue; // level 0 of factor is skipped (coef==0).
          if( d > 0 && (idx += d) <= _colCatMap[i + 1] ) p += _beta[idx-1]/* *1.0 */;
          else             // Enum out of range?
          p = Double.NaN;// Can use a zero, or a NaN
        }
      }
      p += _beta[_beta.length - 1]; // And the intercept as the last beta
      double pp = _glmParams._link.linkInv(p);
      //if( _glmParams._family._family == Family.binomial ) return pp >= _vals[0].bestThreshold() ? 1.0 : 0.0;
      return pp;
    }
    @Override public double getThreshold() {
      if( _glmParams._family._family == Family.binomial )
        return _vals[0].bestThreshold();
      return Float.NaN;
    }

    /** Single row scoring, on a compatible ValueArray (when pushed throw the mapping) */
    protected double score0(ValueArray data, int row) {
      throw H2O.unimpl();
    }

    /** Bulk scoring API, on a compatible ValueArray (when pushed throw the mapping) */
    protected double score0(ValueArray data, AutoBuffer ab, int row_in_chunk) {
      throw H2O.unimpl();
    }
  }

  public static class GLMValidation extends Iced {
    public final Key[] _modelKeys; // Multiple models for n-fold cross-validation
    public static final String KEY_PREFIX = "__GLMValidation_";
    Key _key;
    Key _dataKey;
    Key _modelKey;
    Sampling _s;
    public long _n;
    public int _xvalIterations;
    public long _caseCount;
    public double _aic;
    public double _deviance;
    public double _nullDeviance;
    public double _err;
    ErrMetric _errMetric = ErrMetric.SUMC;
    double _auc = Double.NaN;
    public ConfusionMatrix[] _cm;
    int _tid;
    double[] _thresholds;
    long _time;
    public double[] _tprs;
    public double[] _fprs;

    public final long computationTime() {
      return _time;
    }

    public GLMValidation() {
      _modelKeys = null;
    }

    public GLMValidation(Key modelKey, Key[] modelKeys, ErrMetric m, double[] thresholds, long time) {
      _time = time;
      _errMetric = m;
      _modelKey = modelKey;
      _modelKeys = modelKeys;
      GLMModel[] models = new GLMModel[modelKeys.length];
      for( int i = 0; i < models.length; ++i )
        models[i] = DKV.get(modelKeys[i]).get();
      _dataKey = models[0]._dataKey;
      int i = 0;
      boolean solved = true;
      _xvalIterations = 0;
      for( GLMModel xm : models ) {
        if( !xm.isSolved() ) solved = false;
        _xvalIterations += xm._iterations;
      }
      if( !solved ) {
        _aic = Double.NaN;
        _auc = Double.NaN;
        _deviance = Double.NaN;
        _nullDeviance = Double.NaN;
        _err = Double.NaN;
        _n = -1;
        return;
      }
      long n = 0;
      double nDev = 0;
      double dev = 0;
      double aic = 0;
      double err = 0;
      GLMModel mainModel = DKV.get(modelKey).get();
      int rank = mainModel.rank();
      if( models[0]._vals[0]._cm != null ) {
        int nthresholds = models[0]._vals[0]._cm.length;
        _cm = new ConfusionMatrix[nthresholds];
        for( int t = 0; t < nthresholds; ++t )
          _cm[t] = models[0]._vals[0]._cm[t].clone();
        n += models[0]._vals[0]._n;
        dev = models[0]._vals[0]._deviance;
        rank = models[0].rank();
        aic = models[0]._vals[0]._aic - 2 * models[0].rank();
        _auc = models[0]._vals[0]._auc;
        nDev = models[0]._vals[0]._nullDeviance;
        for( i = 1; i < models.length; ++i ) {
          n += models[i]._vals[0]._n;
          dev += models[i]._vals[0]._deviance;
          aic += models[i]._vals[0]._aic - 2 * models[i].rank();
          nDev += models[i]._vals[0]._nullDeviance;
          _auc += models[i]._vals[0]._auc;
          for( int t = 0; t < nthresholds; ++t )
            _cm[t].add(models[i]._vals[0]._cm[t]);
        }
        _thresholds = thresholds;
        computeBestThreshold(m);
        _auc /= models.length;
      } else {
        for( GLMModel xm : models ) {
          n += xm._vals[0]._n;
          dev += xm._vals[0]._deviance;
          nDev += xm._vals[0]._nullDeviance;
          err += xm._vals[0]._err;
          aic += (xm._vals[0]._aic - 2 * xm.rank());
        }
      }
      _err = err / models.length;
      _deviance = dev;
      _nullDeviance = nDev;
      _n = n;
      _aic = aic + 2 * rank;
    }

    public Key dataKey() {
      return _dataKey;
    }

    public Key modelKey() {
      return _modelKey;
    }

    public Iterable<GLMModel> models() {
      final Key[] keys = _modelKeys;
      return new Iterable<GLMModel>() {
        int idx;

        @Override public Iterator<GLMModel> iterator() {
          return new Iterator<GLMModel>() {
            @Override public void remove() {
              throw new UnsupportedOperationException();
            }

            @Override public GLMModel next() {
              if( idx == keys.length ) throw new NoSuchElementException();
              return DKV.get(keys[idx++]).get();
            }

            @Override public boolean hasNext() {
              return idx < keys.length;
            }
          };
        }
      };
    }

    public int fold() {
      return (_modelKeys == null) ? 1 : _modelKeys.length;
    }

    public ConfusionMatrix bestCM() {
      if( _cm == null ) return null;
      return bestCM(ErrMetric.SUMC);
    }

    public double err() {
      if( _cm != null ) return bestCM().err();
      return _err;
    }

    public ConfusionMatrix bestCM(ErrMetric errM) {
      computeBestThreshold(errM);
      return _cm[_tid];
    }

    public double bestThreshold() {
      return (_thresholds != null) ? _thresholds[_tid] : 0;
    }

    public void computeBestThreshold(ErrMetric errM) {
      if( _cm == null ) return;
      double e = errM.computeErr(_cm[0]);
      _tid = 0;
      for( int i = 1; i < _cm.length; ++i ) {
        double r = errM.computeErr(_cm[i]);
        if( r < e ) {
          e = r;
          _tid = i;
        }
      }
    }

    double[] err(int c) {
      double[] res = new double[_cm.length];
      for( int i = 0; i < res.length; ++i )
        res[i] = _cm[i].classErr(c);
      return res;
    }

    double err(int c, int threshold) {
      return _cm[threshold].classErr(c);
    }

    public double[] classError() {
      return _cm[_tid].classErr();
    }

    private double trapeziod_area(double x1, double x2, double y1, double y2) {
      double base = Math.abs(x1 - x2);
      double havg = 0.5 * (y1 + y2);
      return base * havg;
    }

    public double AUC() {
      return _auc;
    }

    /**
     * Computes area under the ROC curve. The ROC curve is computed from the confusion matrices
     * (there is one for each computed threshold). Area under this curve is then computed as a sum
     * of areas of trapezoids formed by each neighboring points.
     */
    protected void computeAUC() {
      if( _cm == null ) return;
      _tprs = new double[_cm.length];
      _fprs = new double[_cm.length];
      double auc = 0;           // Area-under-ROC
      double TPR_pre = 1;
      double FPR_pre = 1;
      for( int t = 0; t < _cm.length; ++t ) {
        double TPR = 1 - _cm[t].classErr(1); // =TP/(TP+FN) = true -positive-rate
        double FPR = _cm[t].classErr(0); // =FP/(FP+TN) = false-positive-rate
        auc += trapeziod_area(FPR_pre, FPR, TPR_pre, TPR);
        TPR_pre = TPR;
        FPR_pre = FPR;
        _tprs[t] = TPR;
        _fprs[t] = FPR;
      }
      auc += trapeziod_area(FPR_pre, 0, TPR_pre, 0);
      _auc = auc;
    }

    public JsonObject toJson() {
      JsonObject res = new JsonObject();
      if( _dataKey != null ) res.addProperty("dataset", _dataKey.toString());
      else res.addProperty("dataset", "");
      if( _s != null ) res.addProperty("sampling", _s.toString());
      res.addProperty("nrows", _n);
      res.addProperty("val_time", _time);
      res.addProperty("val_iterations", _xvalIterations);
      res.addProperty("resDev", _deviance);
      res.addProperty("nullDev", _nullDeviance);
      if( !Double.isNaN(_auc) ) res.addProperty("auc", _auc);
      if( !Double.isNaN(_aic) ) res.addProperty("aic", _aic);

      if( _cm != null ) {
        double[] err = _cm[_tid].classErr();
        JsonArray arr = new JsonArray();
        for( int i = 0; i < err.length; ++i )
          arr.add(new JsonPrimitive(err[i]));
        res.add("classErr", arr);
        res.addProperty("err", err());
        res.addProperty("threshold", _thresholds[_tid]);
        res.add("cm", _cm[_tid].toJson());
      } else res.addProperty("err", _err);
      if( _modelKeys != null ) {
        JsonArray arr = new JsonArray();
        for( Key k : _modelKeys )
          arr.add(new JsonPrimitive(k.toString()));
        res.add("xval_models", arr);
      }
      return res;
    }

    public double AIC() {
      return _aic;
    }
  }

  public static class GramMatrixFunc extends RowFunc<Gram> {
    final int _dense;
    final int _response;
    final int _diag;
    final int _N;
    final long _nobs; // number of observations in the dataset
    boolean _computeXX = true;
    final boolean _weighted;
    final FamilyIced _family;
    final LinkIced _link;
    double[] _beta;
    final CaseMode _cMode;
    final double _cVal;

    public GramMatrixFunc(DataFrame data, GLMParams glmp, double[] beta) {
      _nobs = data._nobs;
      _beta = beta;
      _dense = data.dense();
      _weighted = glmp._family._family != Family.gaussian;
      _family = glmp._family;
      _link = glmp._link;
      _cMode = glmp._caseMode;
      _cVal = glmp._caseVal;
      _N = data.expandedSz();
      int d = data.largestCatSz();
      _diag = d > 50 ? d : 0;
      _response = data._response;
    }

    @Override public Gram newResult() {
      return new Gram(_N, _diag);
    }

    @Override public long memReq() {
      return ValueArray.CHUNK_SZ + ((((_N * _N) >> 1) + (_N << 1)) << 3);
    }

    public final double computeEta(double[] x, int[] indexes) {
      double mu = 0;
      for( int i = 0; i < indexes.length; ++i )
        mu += x[i] * _beta[indexes[i]];
      return mu;
    }

    @Override public final void processRow(Gram gram, double[] x, int[] indexes) {
      double y = 0;
      if(_response != -1){
        y = x[_response];
        assert ((_family._family != Family.gamma) || y > 0) : "illegal response column, y must be > 0  for family=Gamma.";
        x[_response] = 1; // put intercept in place of y
      }
      if( _cMode != CaseMode.none ) y = (_cMode.isCase(y, _cVal)) ? 1 : 0;
      double w = 1;
      if( _weighted ) {
        double eta, mu, var;
        if( _beta == null ) {
          mu = _family.mustart(y);
          eta = _link.link(mu);
        } else {
          eta = computeEta(x, indexes);
          mu = _link.linkInv(eta);
        }
        var = Math.max(1e-5, _family.variance(mu)); // avoid numerical problems with 0 variance
        if( _family._family == Family.binomial || _family._family == Family.poisson ) {
          w = var;
          y = eta + (y - mu) / var;
        } else {
          double dp = _link.linkInvDeriv(eta);
          w = dp * dp / var;
          y = eta + (y - mu) / dp;
        }
      }

      assert w >= 0 : "invalid weight " + w;
      gram._yy += 0.5 * w * y * y;
      double wy = w * y;
      ++gram._nobs;
      final int N = gram._xy.length;
      final int n = x.length;
      int denseStart = n - _dense;
      final int ii = N - _dense - _diag;
      final int jj = N - _dense;
      // DENSE
      for( int i = 0; i < _dense; ++i ) {
        final int iii = ii + i;
        gram._xy[iii + _diag] += wy * x[i + denseStart];
        for( int j = 0; j <= i; ++j )
          // DENSE*DENSE
          gram._xx[iii][jj + j] += w * x[i + denseStart] * x[j + denseStart];
        for( int j = 0; j < denseStart; ++j )
          // DENSE*SPARSE
          gram._xx[iii][indexes[j]] += w * x[i + denseStart] * x[j];
      }
      // SPARSE
      for( int i = (_diag > 0) ? 1 : 0; i < denseStart; ++i ) {
        final int idx = indexes[i];
        assert idx < N - _dense;
        final int iii = idx - _diag;
        gram._xy[idx] += wy * x[i];
        for( int j = 0; j <= i; ++j )
          // SPARSE*SPARSE
          gram._xx[iii][indexes[j]] += w * x[i] * x[j];
      }
      // DIAG
      if( _diag > 0 && x[0] != 0 ) {
        assert x[0] == 1;
        int diagId = indexes[0];
        assert diagId < N - _dense;
        gram._diag[diagId] += w; // we know x[0] == 1
        gram._xy[diagId] += wy;
      }
    }

    @Override public Gram reduce(Gram x, Gram y) {
      assert x != y;
      x.add(y);
      return x;
    }

    @Override public Gram result(Gram g) {
      double nobsInv = 1.0 / _nobs;
      if( g._xx != null ) for( int i = 0; i < g._xx.length; ++i ) {
        for( int j = 0; j < g._xx[i].length; ++j )
          g._xx[i][j] *= nobsInv;
      }
      if( g._diag != null ) for( int i = 0; i < _diag; ++i ) {
        g._diag[i] *= nobsInv;
      }
      for( int i = 0; i < g._xy.length; ++i )
        g._xy[i] *= nobsInv;
      g._yy *= nobsInv;
      return g;
    }
  }

  public static class GLMValidationFunc extends RowFunc<GLMValidation> {
    OldModel _adaptedModel;
    final GLMModel _m;
    final GLMParams _glmp;
    final double[] _beta;
    final double[] _thresholds;
    final double _ymu;
    int _response;

    public GLMValidationFunc(GLMModel m, GLMParams params, double[] beta, double[] thresholds, double ymu) {
      _m = m;
      _glmp = params;
      _beta = beta;
      _thresholds = Objects.firstNonNull(thresholds, DEFAULT_THRESHOLDS);
      _ymu = ymu;
      _response = m._response;
    }

    @Override public GLMValidation apply(Job job, DataFrame data) throws JobCancelledException {
      long t1 = System.currentTimeMillis();
      if(!data._ary._key.equals(_m._dataKey)){
        _adaptedModel = _m.adapt(data._ary);
       _response = data._response;
      }
      NewRowVecTask<GLMValidation> tsk = new NewRowVecTask<GLMValidation>(job, this, data);
      tsk.invoke(data._ary._key);
      if( job != null && !Job.isRunning(job.self()) ) throw new JobCancelledException();
      GLMValidation res = tsk._result;
      res._time = System.currentTimeMillis() - t1;
      if( _glmp._family._family != Family.binomial ) res._err = Math.sqrt(res._err / res._n);
      res._dataKey = data._ary._key;
      res._thresholds = _thresholds;
      res._s = data.getSampling();
      res.computeBestThreshold(ErrMetric.SUMC);
      res.computeAUC();
      switch( _glmp._family._family ) {
        case gaussian:
          res._aic = res._n * (Math.log(res._deviance / res._n * 2 * Math.PI) + 1) + 2;
          break;
        case binomial:
          res._aic = res._deviance;
          break;
        case poisson:
          res._aic *= -2;
          break; // aic is set during the validation task
        case gamma:
          res._aic = Double.NaN;
          break; // aic for gamma is not computed
        case tweedie:
          res._aic = Double.NaN;
          break;
        default:
          assert false : "missing implementation for family " + _glmp._family;
      }
      res._aic += 2 * _m.rank();//_glmp._family.aic(res._deviance, res._n, _beta.length);
      return res;
    }

    @Override public GLMValidation newResult() {
      GLMValidation res = new GLMValidation();
      if( _glmp._family._family == Family.binomial ) {
        res._cm = new ConfusionMatrix[_thresholds.length];
        for( int i = 0; i < _thresholds.length; ++i )
          res._cm[i] = new ConfusionMatrix(2);
      }
      return res;
    }

    @Override public void processRow(GLMValidation res, double[] x, int[] indexes) {
      ++res._n;
      double yr = x[_response];
      x[_response] = 1.0;
      if( _glmp._caseMode != CaseMode.none ) yr = (_glmp._caseMode.isCase(yr, _glmp._caseVal)) ? 1 : 0;
      if( yr == 1 ) ++res._caseCount;
      double ym = 0;
      if(_adaptedModel != null){
        ym = _adaptedModel.score(x);
      } else {
        for( int i = 0; i < x.length; ++i )
          ym += _beta[indexes[i]] * x[i];
        ym = _glmp._link.linkInv(ym);
      }
      res._deviance += _glmp._family.deviance(yr, ym);
      res._nullDeviance += _glmp._family.deviance(yr, _ymu);
      if( _glmp._family._family == Family.poisson ) { // aic for poisson
        res._err += (ym - yr) * (ym - yr);
        long y = Math.round(yr);
        double logfactorial = 0;
        for( long i = 2; i <= y; ++i )
          logfactorial += Math.log(i);
        res._aic += (yr * Math.log(ym) - logfactorial - ym);
      } else if( _glmp._family._family == Family.binomial ) { // cm computation for binomial
        if( yr < 0 || yr > 1 ) throw new RuntimeException("response variable value out of range: " + yr);
        int i = 0;
        for( double t : _thresholds ) {
          int p = ym >= t ? 1 : 0;
          res._cm[i++].add((int) yr, p);
        }
      } else res._err += (ym - yr) * (ym - yr);
    }

    @Override public GLMValidation reduce(GLMValidation x, GLMValidation y) {
      x._n += y._n;
      x._nullDeviance += y._nullDeviance;
      x._deviance += y._deviance;
      x._aic += y._aic;
      x._err += y._err;
      x._caseCount += y._caseCount;
      if( x._cm != null ) {
        for( int i = 0; i < _thresholds.length; ++i )
          x._cm[i].add(y._cm[i]);
      } else x._cm = y._cm;
      return x;
    }
  }

  private static double betaDiff(double[] b1, double[] b2) {
    double res = Math.abs(b1[0] - b2[0]);
    for( int i = 1; i < b1.length; ++i )
      res = Math.max(res, Math.abs(b1[i] - b2[i]));
    return res;
  }

  public static DataFrame getData(ValueArray ary, int[] xs, int y, Sampling s, boolean standardize) {
    int[] colIds = Arrays.copyOf(xs, xs.length + 1);
    colIds[xs.length] = y;
    return getData(ary, colIds, s, standardize);
  }

  public static DataFrame getData(ValueArray ary, int[] colIds, Sampling s, boolean standardize) {
    int [] cols = new int[colIds.length];
    int j = 0;
    for(int i = 0; i < colIds.length-1; ++i) if(ary._cols[colIds[i]]._min < ary._cols[colIds[i]]._max)
      cols[j++] = colIds[i];
    cols[j++] = colIds[colIds.length-1];
    return new DataFrame(ary, Arrays.copyOf(cols,j), s, standardize, true);
  }

  public static GLMJob startGLMJob(final DataFrame data, final LSMSolver lsm, final GLMParams params,
      final double[] betaStart, double caseWeight, final int xval, final boolean parallel) {
    return startGLMJob(null, data, lsm, params, betaStart, caseWeight, xval, parallel);
  }

  public static GLMJob startGLMJob(Key dest, final DataFrame data, final LSMSolver lsm, final GLMParams params,
      final double[] betaStart, final double caseWeight, final int xval, final boolean parallel) {
    if( dest == null ) dest = GLMModel.makeKey(true);
    final GLMJob job = new GLMJob(data._ary, dest, xval, params);
    lsm._jobKey = job.self();
    final double[] beta;
    final double[] denormalizedBeta;
    if( betaStart != null ) {
      beta = betaStart.clone();
      denormalizedBeta = data.denormalizeBeta(beta);
    } else {
      beta = denormalizedBeta = null;
    }
    data._ary.read_lock(job.self()); // Read-lock the input dataset
    final H2OCountedCompleter fjtask = new H2OCountedCompleter() {
      @Override public void compute2() {
        try {
          buildModel(job, job.dest(), data, lsm, params, beta, caseWeight, xval, parallel);
          assert Job.isRunning(job.self());
          job.remove();
        } catch( JobCancelledException e ) {
          Lockable.delete(job.dest());
        } finally {
          data._ary.unlock(job.self()); // Read-lock the input dataset
        }
        tryComplete();
      }

      @Override public boolean onExceptionalCompletion(Throwable ex, CountedCompleter caller) {
        ex.printStackTrace();
        if( job != null ) job.onException(ex);
        return super.onExceptionalCompletion(ex, caller);
      }
    };
    job.start(fjtask);
    H2O.submitTask(fjtask);
    return job;
  }

  private static boolean solve(LSMSolver solver, Gram gram, double [] beta, ArrayList<String> warns){
    try {
      return solver.solve(gram, beta);
    } catch(NonSPDMatrixException ex){
      DLSM.ADMMSolver admm = (DLSM.ADMMSolver)solver;
      if(admm._autoHandleNonSPDMatrix)throw ex;
      admm._autoHandleNonSPDMatrix = true;
      warns.add("Got Non-SPD Matrix, adding L2-regularization to get solution.");
      return solver.solve(gram, beta);
    }
  }
  public static GLMModel buildModel(Job job, Key resKey, ValueArray ary, int [] cols, boolean standardize, LSMSolver lsm, GLMParams params,
      double[] oldBeta, double caseWeight, int xval, boolean parallel) throws JobCancelledException {
    return buildModel(job, resKey, getData(ary, cols, null, standardize), lsm, params, oldBeta, caseWeight,  xval, parallel);
  }

  private static class YMUVal extends Iced {
    private double _val;
    private long _nobs;
    public void add(double d){_val += d; ++_nobs;}
    public double val(){return _nobs == 0?0:_val/_nobs;}
    public YMUVal add(YMUVal val){
      _val += val._val;
      _nobs += val._nobs;
      return this;
    }
  }

  private static GLMModel buildModel(Job job, Key resKey, DataFrame data, LSMSolver lsm, GLMParams params,
                                    double[] oldBeta, double prior, int xval, boolean parallel) throws JobCancelledException {
    Log.info("running GLM on " + data._ary._key + " with " + data.expandedSz() + " predictors in total, " + (data.expandedSz() - data._dense) + " of which are categoricals. Largest categorical has " + data.largestCatSz() + " levels, prior = " + prior);

    ArrayList<String> warns = new ArrayList<String>();
    long t1 = System.currentTimeMillis();
    // make sure we have a valid response variable for the current family
    int ycolId = data._modelDataMap[data._response];
    Column ycol = data._ary._cols[ycolId];
    params.checkResponseCol(ycol, warns);
    final double ymu;
    if(params._caseMode != CaseMode.none){
      final CaseMode caseMode = params._caseMode;
      final double caseVal = params._caseVal;
      ymu = new RowFunc<YMUVal>(){
        @Override public YMUVal newResult() {return new YMUVal();}
        @Override public void processRow(YMUVal res, double[] x, int[] indexes) {
          double y = x[0];
          if(caseMode != CaseMode.none) y = caseMode.isCase(y,caseVal)?1:0;
          res.add(y);
        }
        @Override public YMUVal reduce(YMUVal x, YMUVal y) {return x.add(y);}
      }.apply(null, new DataFrame(data._ary,new int[]{ycolId},null, false, false)).val();
    } else
      ymu = ycol._mean;
    final double iceptFix;
    if(!Double.isNaN(prior) && prior != ymu){
      double ratio = prior/ymu;
      double pi0 = 1,pi1 = 1;
      if(ratio > 1){
        pi1 = 1.0/ratio;
      } else if(ratio < 1) {
        pi0 = ratio;
      }
      iceptFix = Math.log(pi0/pi1);
    } else {
      iceptFix = 0;
      prior = ycol._mean;
    }
    // filter out constant columns...
//        double ymu = ycol._mean;
//        if(params._family == Family.binomial && params._caseMode != CaseMode.none){ // wee need to compute the mean of the case predicate applied to the ycol
//          GetResponseMeanTask tsk = new GetResponseMeanTask(ycolId, params._caseMode, params._caseVal);
//          tsk.invoke(data._ary._key);
//          ymu = tsk.value();
//        }
//        LambdaMaxFunc lmax = new LambdaMaxFunc(data, ymu, params._link,params._family);
//        LambdaMax lm = lmax.apply(job, data);
//        lsm._lambda *= lm.value();

    GramMatrixFunc gramF = new GramMatrixFunc(data, params, oldBeta);
    double[] newBeta = MemoryManager.malloc8d(data.expandedSz());
    boolean converged = true;
    Gram gram = gramF.apply(job, data);
    final long nobs = gram._nobs;
    int iter = 1;
    long lsmSolveTime = 0;
    long t = System.currentTimeMillis();
    solve(lsm,gram, newBeta,warns);
    lsmSolveTime += System.currentTimeMillis() - t;
    GLMModel currentModel = new GLMModel(Status.ComputingValidation, 0.0f, resKey, data, prior, data.denormalizeBeta(newBeta), newBeta,
        params, lsm, gram._nobs, newBeta.length, converged, iter, System.currentTimeMillis() - t1, null);
    currentModel.delete_and_lock(job.self()); // Lock the new model
    if( params._family._family != Family.gaussian ) do { // IRLSM
      if( oldBeta == null ) oldBeta = MemoryManager.malloc8d(data.expandedSz());
      if( !Job.isRunning(job.self()) ) throw new JobCancelledException();
      double[] b = oldBeta;
      oldBeta = (gramF._beta = newBeta);
      newBeta = b;
      gram = gramF.apply(job, data);
      if( gram.hasNaNsOrInfs() ) // we can't solve this problem any further, user should increase regularization and try again
        break;
      t = System.currentTimeMillis();
      solve(lsm,gram, newBeta,warns);
      lsmSolveTime += System.currentTimeMillis() - t;
      String[] warnings = new String[warns.size()];
      warns.toArray(warnings);
      double betaDiff = betaDiff(oldBeta, newBeta);
      converged = (betaDiff < params._betaEps);
      float progress = Math.max((float) iter / params._maxIter, Math.min((float) (params._betaEps / betaDiff), 1.0f));
      double [] adjustedBeta = newBeta.clone(); // beta djusted with the prior
      adjustedBeta[adjustedBeta.length-1] += iceptFix;
      currentModel = new GLMModel(Status.ComputingModel, progress, resKey, data, prior,  data.denormalizeBeta(adjustedBeta),
          adjustedBeta, params, lsm, gram._nobs, newBeta.length, converged, iter, System.currentTimeMillis() - t1, warnings);
      currentModel._lsmSolveTime = lsmSolveTime;
      currentModel.store(job.self());
    } while( ++iter < params._maxIter && !converged );
    currentModel._lsmSolveTime = lsmSolveTime;
    currentModel._status = Status.ComputingValidation;
    currentModel.store(job.self());
    if( xval > 1 ) // ... and x-validate
      currentModel.xvalidate(job, data._ary, xval, DEFAULT_THRESHOLDS, parallel);
    else currentModel.validateOn(job, data._ary, data.getSamplingComplement(), DEFAULT_THRESHOLDS); // Full scoring on original dataset
    currentModel._status = Status.Done;
    if(currentModel.rank() > nobs)
      warns.add("Not enough data to compute the model (got more predictors than data points), try limit the number of columns (e.g. increase L1 regularization or run PCA first).");
    String[] warnings = new String[warns.size()];
    warns.toArray(warnings);
    currentModel._warnings = warnings;
    currentModel.unlock(job.self());
    DKV.write_barrier();
    return currentModel;
  }

  static double[] DEFAULT_THRESHOLDS = new double[] { 0.00, 0.01, 0.02, 0.03, 0.04, 0.05, 0.06, 0.07, 0.08, 0.09, 0.10,
      0.11, 0.12, 0.13, 0.14, 0.15, 0.16, 0.17, 0.18, 0.19, 0.20, 0.21, 0.22, 0.23, 0.24, 0.25, 0.26, 0.27, 0.28, 0.29,
      0.30, 0.31, 0.32, 0.33, 0.34, 0.35, 0.36, 0.37, 0.38, 0.39, 0.40, 0.41, 0.42, 0.43, 0.44, 0.45, 0.46, 0.47, 0.48,
      0.49, 0.50, 0.51, 0.52, 0.53, 0.54, 0.55, 0.56, 0.57, 0.58, 0.59, 0.60, 0.61, 0.62, 0.63, 0.64, 0.65, 0.66, 0.67,
      0.68, 0.69, 0.70, 0.71, 0.72, 0.73, 0.74, 0.75, 0.76, 0.77, 0.78, 0.79, 0.80, 0.81, 0.82, 0.83, 0.84, 0.85, 0.86,
      0.87, 0.88, 0.89, 0.90, 0.91, 0.92, 0.93, 0.94, 0.95, 0.96, 0.97, 0.98, 0.99, 1.00 };
}
