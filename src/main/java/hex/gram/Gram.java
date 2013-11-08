package hex.gram;

import hex.DLSM.ADMMSolver.NonSPDMatrixException;
import hex.FrameTask;
import java.lang.Thread;
import java.text.DecimalFormat;
import java.util.Arrays;
import jsr166y.RecursiveAction;
import jsr166y.CountedCompleter;
import water.*;

import water.util.Log;
import water.util.Utils;
import Jama.CholeskyDecomposition;
import Jama.Matrix;

public final class Gram extends Iced {
  final boolean _hasIntercept;
  double[][] _xx;
  double[] _diag;
  final int _diagN;
  final int _denseN;
  final int _fullN;
  final int STEP;
  final static int MIN_TSKSZ=10000;
//  double[] _xy;
//  double _yy;
//  long _nobs;

  public Gram() {_diagN = _denseN = _fullN = 0; _hasIntercept = false; STEP=10;}

  public Gram(int N, int diag, int dense, int sparse, boolean hasIntercept) {
    _hasIntercept = hasIntercept;
    _fullN = N + (_hasIntercept?1:0);
    _xx = new double[_fullN - diag][];
    _diag = MemoryManager.malloc8d(_diagN = diag);
    _denseN = dense;
    STEP = 10;
    for( int i = 0; i < (_fullN - _diagN); ++i )
      _xx[i] = MemoryManager.malloc8d(diag + i + 1);
  }

  public Gram(int N, int diag, int dense, int sparse, int blk, boolean hasIntercept) {
    _hasIntercept = hasIntercept;
    _fullN = N + (_hasIntercept?1:0);
    _xx = new double[_fullN - diag][];
    _diag = MemoryManager.malloc8d(_diagN = diag);
    _denseN = dense;
    STEP = blk;
    for( int i = 0; i < (_fullN - _diagN); ++i )
      _xx[i] = MemoryManager.malloc8d(diag + i + 1);
  }

  public void addDiag(double d) {
    for( int i = 0; i < _diag.length; ++i )
      _diag[i] += d;
    for( int i = 0; i < _xx.length - 1; ++i )
      _xx[i][_xx[i].length - 1] += d;
  }

  private class StripTask extends CountedCompleter {
    final double[][] _xx;
    final int _i0, _i1, _j0, _j1;
    public StripTask(StripTask cc, double xx[][], int ifr, int ito, int jfr, int jto) {
      super(cc);
      _xx = xx;
      _i0 = ifr; _i1 = ito; _j0 = jfr; _j1 = jto;
    }
    @Override public void compute() {
      final int sparseN = _diag.length;
      if ((_i1 - _i0)*(_j1 - _j0) > Gram.MIN_TSKSZ) {
        int mid = (_i0 + _i1)>>>1;
        setPendingCount(1);
        new StripTask(this, _xx, mid, _i1, _j0, _j1).fork();
        new StripTask(this, _xx, _i0, mid, _j0, _j1).compute();
      } else {
        for (int j = _j0; j < _j1; j++) {
          int k = j - sparseN;
          for (int i = _i0; i < _i1; i++)
            for (int s = _j0; s < j; s++) _xx[i][j] -= _xx[k][s]*_xx[i][s];
        }
        tryComplete();
      }      
    }    
  }

  static private class StripTask2 extends CountedCompleter {
    final double[][] _xx;
    final int _i0, _i1, _j0, _j1;
    public StripTask2(CountedCompleter cc, double xx[][], int ifr, int ito, int jfr, int jto) {
      super(cc);
      _xx = xx;
      _i0 = ifr; _i1 = ito; _j0 = jfr; _j1 = jto;
    }
    @Override public void compute() {
      if ((_i1 - _i0)*(_j1 - _j0) > Gram.MIN_TSKSZ) {
        int mid = (_i0 + _i1)>>>1;
        setPendingCount(0);
        new StripTask2(this, _xx, mid, _i1, _j0, _j1).compute();
        new StripTask2(this, _xx, _i0, mid, _j0, _j1).compute();
      } else {
        for (int j = _j0; j < _j1; j++) {
          double d = 1.0/_xx[j][j];
          for (int i = _i0; i < _i1; i++) {
            double s = 0.0;
            for (int k = _j0; k < j; k++) s += _xx[j][k]*_xx[i][k];
            _xx[i][j] = (_xx[i][j]-s) / d;
          }
        }
        tryComplete();
      }      
    }    
  }

  static private class BlockTask extends RecursiveAction {
    final double[][] _xx;
    final int _i0, _i1, _j0, _j1;
    public BlockTask(double xx[][], int ifr, int ito, int jfr, int jto) {
      _xx = xx;
      _i0 = ifr; _i1 = ito; _j0 = jfr; _j1 = jto;
    }
    @Override public void compute() {
      //Log.info("TASK THREAD ID -- " + Thread.currentThread().getName());
      //Log.info("  BLOCK SIZE " + (_i1 - _i0));
      for (int i=_i0; i < _i1; i++) {
        double rowi[] = _xx[i];
        for (int k=_j0; k < _j1; k++) {
          double rowk[] = _xx[k];
          double s = 0.0;
          for (int jj = 0; jj < k; jj++) s += rowk[jj]*rowi[jj];
          rowi[k] = (rowi[k] - s) / rowk[k];
        }
      }
    }
  }

  private class TriangleTask extends CountedCompleter {
    final double[][] _xx;
    final int _s0,_s1;          // the column range of the strip whose outer product is to be subtracted from the triangle.
    final int _j0,_j1;          // the column range in the lower right triangle covered by this task
    public TriangleTask(TriangleTask cc, double[][] xx, int s0, int s1, int j0, int j1) {
      super(cc);
      _xx = xx; _s0 = s0; _s1 = s1; _j0 = j0; _j1 = j1;
    }
    @Override public void compute() {
      final int sparseN = _diag.length;
      if ((_j1 - _j0)*(_fullN<<2 - _j0 - _j1)>>>2 > Gram.MIN_TSKSZ) {
        int mid = (_j0 + _j1) >>>1;
        setPendingCount(1);
        new TriangleTask(this,_xx,_s0,_s1,mid,_j1).fork();
        new TriangleTask(this,_xx,_s0,_s1,_j0,mid).compute();
      } else {
        for (int j = _j0; j < _j1; j++) {
          int k = j-sparseN;
          for (int i = k; i < _denseN; i++)
            for (int s = _s0; s < _s1; s++) _xx[i][j] -= _xx[k][s]*_xx[i][s];
        }
        tryComplete();
      }
    }
  }

  static private class TriangleTask2 extends CountedCompleter {
    final double[][] _xx;
    final int _s0,_s1;          // the column range of the strip whose outer product is to be subtracted from the triangle.
    final int _j0,_j1;          // the column range in the lower right triangle covered by this task
    public TriangleTask2(CountedCompleter cc, double[][] xx, int s0, int s1, int j0, int j1) {
      super(cc);
      _xx = xx; _s0 = s0; _s1 = s1; _j0 = j0; _j1 = j1;
    }
    @Override public void compute() {
//       Log.err("CHOL TO UPDATE LOWER RIGHT TRIANGLE J[" + _j0 + ":" + _j1 + "]");
      int N = _xx.length;
      if (_j1 > (_j0 + 1) && ((_j1 - _j0)*(N - (_j0 + _j1)>>>1) > Gram.MIN_TSKSZ)) {
        int mid = (_j0 + _j1) >>>1;
        setPendingCount(0);
        new TriangleTask2(this,_xx,_s0,_s1,mid,_j1).compute();
        new TriangleTask2(this,_xx,_s0,_s1,_j0,mid).compute();
      } else {
        for (int j = _j0; j < _j1; j++) {
          for (int i = j; i < N; i++)
            for (int s = _s0; s < _s1; s++) _xx[i][j] -= _xx[j][s]*_xx[i][s];
        }
        tryComplete();
      }
    }
  }

  static public class InPlaceCholesky {
    final double _xx[][];             // Lower triagle of the symmetric matrix. 
    private boolean _isSPD;
    private InPlaceCholesky(double xx[][], boolean isspd) { _xx = xx; _isSPD = isspd; }
    public static InPlaceCholesky decompose_1(double xx[][], int STEP) {
      boolean isspd = true;
      final int N = xx.length;
      for (int j=0; j < N; j+=STEP) {
        // update the upper left triangle.
        int tjR = Math.min(j+STEP, N);
        for (int tj=j; tj < tjR; tj++) {
          if (xx[tj][tj] <= 0) { xx[tj][tj] = 0.0; isspd = false; }
          xx[tj][tj] = Math.sqrt(xx[tj][tj]);
          double d=1.0/xx[tj][tj];
          // update column of tj
          for (int ti = tj+1; ti < tjR; ti++) {
            xx[ti][tj] *= d;
          }
          // subtract outerproduct of column tj from its right part
          for (int ui = tj+1; ui < tjR; ui++)
            for (int uj = tj+1; uj <= ui; uj++)
              xx[ui][uj] -= xx[ui][tj]*xx[uj][tj];
        }
        if (tjR == N) break;
        // update the lower left strip
        new StripTask2(null,xx,j+STEP,N,j,j+STEP).invoke();
        // update the lower right triangle
        new TriangleTask2(null,xx,j,j+STEP,j+STEP,N).invoke();
      }
      return new InPlaceCholesky(xx, isspd);
    }
    public static InPlaceCholesky decompose_2(double xx[][], int STEP, int P) {
      boolean isspd = true;
      final int N = xx.length;
      P = Math.max(1, P);
      for (int j=0; j < N; j+=STEP) {
        // update the upper left triangle.
        final int tjR = Math.min(j+STEP, N);
        for (int i=j; i < tjR; i++) {
          double rowi[] = xx[i];
          double d = 0.0;
          for (int k=j; k < i; k++) {
            double rowk[] = xx[k];
            double s = 0.0;
            for (int jj = 0; jj < k; jj++) s += rowk[jj]*rowi[jj];
            rowi[k] = s = (rowi[k] - s) / rowk[k];
            d += s*s;
          }
          for (int jj = 0; jj < j; jj++) { double s = rowi[jj]; d += s*s; }
          d = rowi[i] - d;
          isspd = isspd && (d > 0.0);
          rowi[i] = Math.sqrt(Math.max(0.0, d));
        }
        if (tjR == N) break;
        // update the lower strip
        int i = tjR;
        Futures fs = new Futures();
        int rpb = 0;                // rows per block
        int p = P;                  // concurrency 
        while ( tjR*(rpb=(N - tjR)/p)<Gram.MIN_TSKSZ && p>1) --p;
        while (p-- > 1) {
          fs.add(new BlockTask(xx,i,i+rpb,j,tjR).fork());
          i += rpb;
        }
        new BlockTask(xx,i,N,j,tjR).compute();
        fs.blockForPending();
      }
      return new InPlaceCholesky(xx, isspd);
    }
    public double[][] getL() { return _xx; }
    public boolean isSPD() { return _isSPD; }
  }

  public Cholesky cholesky(Cholesky chol) {
    return cholesky(chol,1);
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
  public Cholesky cholesky(Cholesky chol, int parallelize) {
    long start = System.currentTimeMillis();
    if( chol == null ) {
      double[][] xx = _xx.clone();
      for( int i = 0; i < xx.length; ++i )
        xx[i] = xx[i].clone();
      chol = new Cholesky(xx, _diag.clone());
    }
    final Cholesky fchol = chol;
    final int sparseN = _diag.length;
    final int denseN = _fullN - sparseN;
    boolean spd=true;
    // compute the cholesky of the diagonal and diagonal*dense parts
    if( _diag != null ) for( int i = 0; i < sparseN; ++i ) {
      double d = 1.0 / (chol._diag[i] = Math.sqrt(_diag[i]));
      for( int j = 0; j < denseN; ++j )
        chol._xx[j][i] = d*_xx[j][i];
    }
    Futures fs = new Futures();
    // compute the outer product of diagonal*dense
    for( int i = 0; i < denseN; ++i ) {
      final int fi = i;
      fs.add(new RecursiveAction() {
        @Override protected void compute() {
          for( int j = 0; j <= fi; ++j ) {
            double s = 0;
            for( int k = 0; k < sparseN; ++k )
              s += fchol._xx[fi][k] * fchol._xx[j][k];
            fchol._xx[fi][j + sparseN] = _xx[fi][j + sparseN] - s;
          }
        }
      }.fork());
    }
    fs.blockForPending();
        
    // compute the cholesky of dense*dense-outer_product(diagonal*dense)
    // TODO we still use Jama, which requires (among other things) copy and expansion of the matrix. Do it here without copy and faster.
    double[][] arr = new double[denseN][];
    for( int i = 0; i < arr.length; ++i )
      arr[i] = Arrays.copyOfRange(fchol._xx[i], sparseN, sparseN + denseN);

    Log.info ("CHOLESKY PRECOMPUTE TIME " + (System.currentTimeMillis()-start));
    start = System.currentTimeMillis();
    // parallelize cholesky
    if (parallelize == 1) {
      int p = Runtime.getRuntime().availableProcessors();
      InPlaceCholesky d = InPlaceCholesky.decompose_2(arr, 10, p);
      fchol.setSPD(d.isSPD());
      arr = d.getL();
      Log.info ("H2O CHOLESKY DECOMPOSE TAKES: " + (System.currentTimeMillis()-start));
    } else {
      // make it symmetric
      for( int i = 0; i < arr.length; ++i )
        for( int j = 0; j < i; ++j )
          arr[j][i] = arr[i][j];
      CholeskyDecomposition c = new Matrix(arr).chol();
      fchol.setSPD(c.isSPD());
      arr = c.getL().getArray();
      Log.info ("JAMA CHOLESKY DECOMPOSE TAKES: " + (System.currentTimeMillis()-start));
    }
    for( int i = 0; i < arr.length; ++i )
      System.arraycopy(arr[i], 0, fchol._xx[i], sparseN, i + 1);
    return chol;
  }

  public double[][] getXX() {
    final int N = _fullN;
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
    Utils.add(_xx,grm._xx);
    Utils.add(_diag,grm._diag);
  }

  public final boolean hasNaNsOrInfs() {
    for( int i = 0; i < _xx.length; ++i )
      for( int j = 0; j < _xx[i].length; ++j )
        if( Double.isInfinite(_xx[i][j]) || Double.isNaN(_xx[i][j]) ) return true;
    for( double d : _diag )
      if( Double.isInfinite(d) || Double.isNaN(d) ) return true;
    return false;
  }

  public static final class Cholesky {
    protected final double[][] _xx;
    protected final double[] _diag;
    private boolean _isSPD;

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

    @Override
    public String toString() {
      return "";
    }

    /**
     * Find solution to A*x = y.
     *
     * Result is stored in the y input vector. May throw NonSPDMatrix exception in case Gram is not
     * positive definite.
     *
     * @param y
     */
    public final void solve(double[] y) {
      if( !isSPD() ) throw new NonSPDMatrixException();
      assert _xx.length + _diag.length == y.length:"" + _xx.length + " + " + _diag.length + " != " + y.length;
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
    public final boolean isSPD() {return _isSPD;}
    public final void setSPD(boolean b) {_isSPD = b;}
  }

  public final void addRow(final double[] x, final int catN, final int [] catIndexes, final double w) {
    final int intercept = _hasIntercept?1:0;
    final int denseRowStart = _fullN - _denseN - _diagN - intercept; // we keep dense numbers at the right bottom of the matrix, -1 is for intercept
    final int denseColStart = _fullN - _denseN - intercept;

    assert _denseN + denseRowStart == _xx.length-intercept;
    final double [] interceptRow = _hasIntercept?_xx[_denseN + denseRowStart]:null;
    // nums
    for(int i = 0; i < _denseN; ++i) if(x[i] != 0) {
      final double [] mrow = _xx[i+denseRowStart];
      final double d = w*x[i];
      for(int j = 0; j <= i; ++j)if(x[j] != 0)
        mrow[j+denseColStart] += d*x[j];
      if(_hasIntercept)
        interceptRow[i+denseColStart] += d; // intercept*x[i]
      // nums * cats
      for(int j = 0; j < catN; ++j)
        mrow[catIndexes[j]] += d;
    }
    if(_hasIntercept){
      // intercept*intercept
      interceptRow[_denseN+denseColStart] += w;
      // intercept X cat
      for(int j = 0; j < catN; ++j)
        interceptRow[catIndexes[j]] += w;
    }
    final boolean hasDiag = (_diagN > 0 && catN > 0 && catIndexes[0] < _diagN);
    // cat X cat
    for(int i = hasDiag?1:0; i < catN; ++i){
      final double [] mrow = _xx[catIndexes[i] - _diagN];
      for(int j = 0; j <= i; ++j)
        mrow[catIndexes[j]] += w;
    }
    // DIAG
    if(hasDiag)_diag[catIndexes[0]] += w;
  }
  public void mul(double x){
    if(_diag != null)for(int i = 0; i < _diag.length; ++i)
      _diag[i] *= x;
    for(int i = 0; i < _xx.length; ++i)
      for(int j = 0; j < _xx[i].length; ++j)
        _xx[i][j] *= x;
  }

  /**
   * Task to compute gram matrix normalized by the number of observations (not counting rows with NAs).
   * in R's notation g = t(X)%*%X/nobs, nobs = number of rows of X with no NA.
   * @author tomasnykodym
   */
  public static class GramTask extends FrameTask<GramTask> {
    final boolean _hasIntercept;
    public Gram _gram;
    public long _nobs;

    public GramTask(Job job, boolean standardize, boolean hasIntercept){
      super(job,standardize,false);
      _hasIntercept = hasIntercept;
    }
    @Override protected void chunkInit(){
      _gram = new Gram(fullN(), largestCat(), _nums, _cats,_hasIntercept);
    }
    @Override protected void processRow(double[] nums, int ncats, int[] cats) {
      _gram.addRow(nums, ncats, cats, 1.0);
      ++_nobs;
    }
    @Override protected void chunkDone(){_gram.mul(1.0/_nobs);}
    @Override public void reduce(GramTask gt){
      double r = (double)_nobs/(_nobs+gt._nobs);
      _gram.mul(r);
      double r2 = (double)gt._nobs/(_nobs+gt._nobs);
      gt._gram.mul(r2);
      _gram.add(gt._gram);
      _nobs += gt._nobs;
    }
  }
}

