package hex.gram;

import hex.DLSM.ADMMSolver.NonSPDMatrixException;

import java.text.DecimalFormat;
import java.util.Arrays;

import jsr166y.RecursiveAction;
import water.*;
import water.util.Utils;
import Jama.CholeskyDecomposition;
import Jama.Matrix;

public final class Gram extends Iced {
  public static final DecimalFormat dformat = new DecimalFormat("0.#####");

  double[][] _xx;
  double[] _diag;
  final int _diagN;
  final int _denseN;
  final int _fullN;
//  double[] _xy;
//  double _yy;
//  long _nobs;

  public Gram() {_diagN = _denseN = _fullN = 0;}

  public Gram(int N, int diag, int dense, int sparse) {
//    _xy = MemoryManager.malloc8d(N);
    _xx = new double[N - diag][];
    _diag = MemoryManager.malloc8d(_diagN = diag);
    _denseN = dense;
    _fullN = N;
    for( int i = 0; i < (_fullN - _diagN); ++i )
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
  public Cholesky cholesky(Cholesky chol) {
    if( chol == null ) {
      double[][] xx = _xx.clone();
      for( int i = 0; i < xx.length; ++i )
        xx[i] = xx[i].clone();
      chol = new Cholesky(xx, _diag.clone());
    }
    final Cholesky fchol = chol;
    final int sparseN = _diag.length;
    final int denseN = _fullN - sparseN;
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
    // make it symmetric
    for( int i = 0; i < arr.length; ++i )
      for( int j = 0; j < i; ++j )
        arr[j][i] = arr[i][j];
    CholeskyDecomposition c = new Matrix(arr).chol();
    fchol.setSPD(c.isSPD());
    arr = c.getL().getArray();
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
    public final boolean isSPD() {return _isSPD;}
    public final void setSPD(boolean b) {_isSPD = b;}
  }

  public final void addRow(final double[] x, final int catN, final int [] catIndexes, final double w) {
    final int denseRowStart = _fullN - _denseN - _diagN - 1; // we keep dense numbers at the right bottom of the matrix, -1 is for intercept
    final int denseColStart = _fullN - _denseN - 1;

    assert _denseN + denseRowStart == _xx.length-1;
    final double [] interceptRow = _xx[_denseN + denseRowStart];
    // nums
    for(int i = 0; i < _denseN; ++i) if(x[i] != 0) {
      final double [] mrow = _xx[i+denseRowStart];
      final double d = w*x[i];
      for(int j = 0; j <= i; ++j)if(x[j] != 0)
        mrow[j+denseColStart] += d*x[j];
      interceptRow[i+denseColStart] += d; // intercept*x[i]
      // nums * cats
      for(int j = 0; j < catN; ++j)
        mrow[catIndexes[j]] += d;
    }
    // intercept*intercept
    interceptRow[_denseN+denseColStart] += w;
    // intercept X cat
    for(int j = 0; j < catN; ++j)
      interceptRow[catIndexes[j]] += w;
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
}

