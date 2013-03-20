package hex;

import java.util.Arrays;
import water.Iced;
import water.MemoryManager;
import Jama.CholeskyDecomposition;
import Jama.Matrix;

import com.google.gson.JsonObject;

/**
 * Distributed least squares solvers
 * @author tomasnykodym
 *
 */
public class DLSM {

  public enum LSMSolverType {
    AUTO, // AUTO: (len(beta) < 1000)?ADMM:GenGradient
    ADMM,
    GenGradient
  }

  public static abstract class LSMSolver extends Iced {
    public double _lambda;
    public final double _alpha;

    public LSMSolver(double lambda, double alpha){
      _lambda = lambda;
      _alpha  = alpha;
    }
    /**
     *  @param xx - gram matrix. gaussian: X'X, binomial:(1/4)X'X
     *  @param xy - guassian: -X'y binomial: -(1/4)X'(XB + (y-p)/(p*1-p))
     *  @param yy - <y,y>/2
     *  @param beta - previous vector of coefficients, will be modified/destroyed
     *  @param newBeta - resulting vector of coefficients
     *  @return true if converged
     *
     */
    public abstract boolean solve(double [][] xx, double [] xy, double yy,  double [] newBeta);
    public abstract JsonObject toJson();

    protected boolean _converged;
    public final boolean converged(){return _converged;}
    public static class LSMSolverException extends RuntimeException {
      public LSMSolverException(String msg){super(msg);}
    }
    public abstract String name();
  }


  private static double shrinkage(double x, double kappa) {
    return Math.max(0, x - kappa) - Math.max(0, -x - kappa);
  }

  static final double[] mul(double[][] X, double[] y, double[] z) {
    final int M = X.length;
    final int N = y.length;
    for( int i = 0; i < M; ++i ) {
      z[i] = X[i][0] * y[0];
      for( int j = 1; j < N; ++j )
        z[i] += X[i][j] * y[j];
    }
    return z;
  }

  static final double[] mul(double[] x, double a, double[] z) {
    for( int i = 0; i < x.length; ++i )
      z[i] = a * x[i];
    return z;
  }

  static final double[] plus(double[] x, double[] y, double[] z) {
    for( int i = 0; i < x.length; ++i )
      z[i] = x[i] + y[i];
    return z;
  }

  static final double[] minus(double[] x, double[] y, double[] z) {
    for( int i = 0; i < x.length; ++i )
      z[i] = x[i] - y[i];
    return z;
  }

  static final double[] shrink(double[] x, double[] z, double kappa) {
    for( int i = 0; i < x.length - 1; ++i )
      z[i] = shrinkage(x[i], kappa);
    z[x.length - 1] = x[x.length - 1]; // do not penalize intercept!
    return z;
  }



  public static final class ADMMSolver extends LSMSolver {

    public static final double DEFAULT_LAMBDA = 1e-5;
    public static final double DEFAULT_ALPHA = 0.5;
    public double _orlx = 1;//1.4; // over relaxation param
    public double _rho = 1e-3;

    public boolean normalize() {
      return _lambda != 0;
    }

    public ADMMSolver (double lambda, double alpha) {
      super(lambda,alpha);
    }

    public JsonObject toJson(){
      JsonObject res = new JsonObject();
      res.addProperty("lambda",_lambda);
      res.addProperty("alpha",_alpha);
      return res;
    }

    public static class NonSPDMatrixException extends LSMSolverException {
      public NonSPDMatrixException(){
        super("Matrix is not SPD, can't solve without regularization");
      }
    }

    public double [] solve(Matrix xx, Matrix xy) {
      final int N = xx.getRowDimension();
      double lambda =  _lambda*(1-_alpha)*0.5 + _rho;
      if(_lambda != 0) for(int i = 0; i < N-1; ++i)
          xx.set(i, i, xx.get(i,i)+lambda);
      CholeskyDecomposition lu = new CholeskyDecomposition(xx);
      if(_alpha == 0 || _lambda == 0) // no l1 penalty
        try {
          double[] res = lu.solve(xy).getColumnPackedCopy();
          _converged = true;
          return res;
        }catch(Exception e){
          if( !e.getMessage().equals("Matrix is not symmetric positive definite.") )
            throw new Error(e);
          throw new NonSPDMatrixException();
        }

      final double ABSTOL = Math.sqrt(N) * 1e-4;
      final double RELTOL = 1e-2;
      double[] z = new double[N];
      double[] u = new double[N-1];
      Matrix xm = null;
      Matrix xyPrime = (Matrix)xy.clone();
      OUTER:
      for(int a = 0; a < 5; ++a){
        double kappa = _lambda*_alpha / _rho;
        for( int i = 0; i < 1000; ++i ) {
          // first compute the x update
          // add rho*(z-u) to A'*y
          for( int j = 0; j < N-1; ++j ) {
            xyPrime.set(j, 0, xy.get(j, 0) + _rho * (z[j] - u[j]));
          }
          // updated x
          try{
            xm = lu.solve(xyPrime);
          }catch(Exception e){
            if( !e.getMessage().equals("Matrix is not symmetric positive definite.") )
              throw new Error(e);
            // bump the rho and try again
            _rho *= 10;
            lambda = (_lambda*(1-_alpha) + _rho) - lambda;
            for(int j = 0; j < N-1; ++j)
              xx.set(j, j, xx.get(j,j)+lambda);
            Arrays.fill(z, 0);
            Arrays.fill(u, 0);
            continue OUTER;
          }
          // vars to be used for stopping criteria
          double x_norm = 0;
          double z_norm = 0;
          double u_norm = 0;
          double r_norm = 0;
          double s_norm = 0;
          double eps_pri = 0; // epsilon primal
          double eps_dual = 0;
          // compute u and z update
          for( int j = 0; j < N-1; ++j ) {
            double x_hat = xm.get(j, 0);
            x_norm += x_hat * x_hat;
            x_hat = x_hat * _orlx + (1 - _orlx) * z[j];
            double zold = z[j];
            z[j] = shrinkage(x_hat + u[j], kappa);
            z_norm += z[j] * z[j];
            s_norm += (z[j] - zold) * (z[j] - zold);
            r_norm += (xm.get(j, 0) - z[j]) * (xm.get(j, 0) - z[j]);
            u[j] += x_hat - z[j];
            u_norm += u[j] * u[j];
          }
          z[N-1] = xm.get(N-1, 0);
          // compute variables used for stopping criterium
          r_norm = Math.sqrt(r_norm);
          s_norm = _rho * Math.sqrt(s_norm);
          eps_pri = ABSTOL + RELTOL * Math.sqrt(Math.max(x_norm, z_norm));
          eps_dual = ABSTOL + _rho * RELTOL * Math.sqrt(u_norm);
          if( r_norm < eps_pri && s_norm < eps_dual ){
            _converged = true;
            break;
          }
        }
        return z;
      }
      throw new NonSPDMatrixException();
    }

    @Override
    public boolean solve(double[][] xx, double[] xy, double yy, double[] newBeta) {
      double [] beta = solve(new Matrix(xx), new Matrix(xy,xy.length));
      // not really nice solution, but we get new Vector from Jama
      // and I want to keep option of using user-supplied beta (to avoid allocation)
      System.arraycopy(beta, 0, newBeta, 0, beta.length);
      return true;
    }

    @Override
    public String name() {
      return "ADMM";
    }
  }




  /**
   * Generalized gradient solver for solving LSM problem with combination of L1 and L2 penalty.
   *
   * @author tomasnykodym
   *
   */
  public static final class GeneralizedGradientSolver extends LSMSolver {
    public final double        _kappa;             // _lambda*_alpha
    public final double        _betaEps;
    private double[]           _beta;
    private double[]           _betaGradient;
    double                     _objVal;
    double                     _t          = 1.0;
    int                        _iterations = 0;
    public static final int    MAX_ITER    = 1000;
    public static final double EPS         = 1e-5;

    public GeneralizedGradientSolver(double lambda, double alpha) {
      this(lambda,alpha,1e-5);
    }
    public GeneralizedGradientSolver(double lambda, double alpha, double betaEps) {
      super(lambda,alpha);
      _kappa = _lambda * _alpha;
      _betaEps = betaEps;
    }

    /**
     * Compute least squares objective function value: g(beta) = 0.5*(y - X*b)'*(y
     * - X*b) = 0.5*y'y - (X'y)'*b + 0.5*b'*X'X*b)
     * @param xx: X'X
     * @param xy: -X'y
     * @param yy: 0.5*y'y
     * @param beta: b (vector of coefficients)
     * @return 0.5*(y - X*b)'*(y - X*b)
     */
    private double g_beta(double[][] xx, double[] xy, double yy, double[] beta) {
      final int n = xy.length;
      double res = yy;
      for( int i = 0; i < n; ++i ) {
        double x = 0;
        for( int j = 0; j < n; ++j ){
          x += xx[i][j] * beta[j];
        }
        res += (0.5*x + xy[i]) * beta[i];
      }
      if(!(res >= 0))
        throw new LSMSolverException("Generalized Gradient: Can not solved this problem.");
      return res;
    }

    /*
     * Compute beta gradient.
     */
    private void g_beta_gradient(double[][] xx, double[] xy, double t) {
      mul(xx, _beta, _betaGradient);
      plus(xy, _betaGradient, _betaGradient);
      mul(_betaGradient, -t, _betaGradient);
    }

    /**
     * Compute new beta according to:
     *    B = B -t*G_t(B), where G(t) = (B - S(B-t*gradient(B),lambda,t)
     * @param newBeta: vector to be filled with the new value of beta
     * @param t: step size
     * @return newBeta
     */
    private double[] beta_update(double[] newBeta, double t) {
      // S(b - t*g_beta_gradient(b),_kappa*t)/(1+ (1 - alpha)*0.5*t)
      double div = 1 / (1 + ((1 - _alpha) * 0.5 * _lambda * t));
      shrink(plus(_beta, _betaGradient, newBeta), newBeta, _kappa*t);
      for(int i = 0; i < newBeta.length-1; ++i)
        newBeta[i] *= div;
      return newBeta;
    }

    /**
     * Compute right hand side of Armijo rule updated for generalized gradient.
     * Used as a threshold for backtracking (finding optimal step t).
     *
     * @param gbeta
     * @param newBeta
     * @param t
     * @return
     */
    private double backtrack_cond_rs(double gbeta, double[] newBeta, double t) {
      double norm = 0;
      double zg = 0;
      double t_inv = 1.0 / t;
      for( int i = 0; i < _beta.length; ++i ) {
        double diff = _beta[i] - newBeta[i];
        norm += diff * diff;
        zg += _betaGradient[i] * diff;
      }
      return gbeta + (norm * 0.5 + zg) * t_inv;
    }

    double l1norm(double[] v) {
      double res = Math.abs(v[0]);
      for( int i = 1; i < v.length - 1; ++i )
        res += Math.abs(v[i]);
      return res;
    }


    /**
     * @param xx: gram matrix. gaussian: X'X, binomial:(1/4)X'X
     * @param xy: -X'y (LSM) l or -(1/4)X'(XB + (y-p)/(p*1-p))(IRLSM
     * @param yy: 0.5*y'*y gaussian, 0.25*z'*z IRLSM
     * @param beta: previous vector of coefficients, will be modified/destroyed
     * @param newBeta: resulting vector of coefficients
     * @return true if converged
     */
    @Override
    public boolean solve(double[][] xx, double[] xy, double yy, double[] newBeta) {
      if(_beta == null || _beta.length != newBeta.length){
        _beta = MemoryManager.malloc8d(newBeta.length);
        _betaGradient = MemoryManager.malloc8d(newBeta.length);
      }
      int i = 0;
      _converged = false;
      _iterations = 0;
      _objVal = Double.MAX_VALUE;
      mul(xy, -1, xy);
      while( !_converged && _iterations != MAX_ITER ) {
        double gbeta = g_beta(xx, xy, yy, _beta);
        // use backtracking to find proper step size t
        double t = _t;
        for( int k = 0; k < 100; ++k ) {
          g_beta_gradient(xx, xy, t);
          newBeta = beta_update(newBeta, t);
          if( g_beta(xx, xy, yy, newBeta) <= backtrack_cond_rs(gbeta, newBeta, t) ) {
            if( _t > t ) {
              _t = t;
              break;
            }
            t = 1.25 * t;
          } else {
            _t = t;
            t = 0.8 * t;
          }
        }
        // compare objective function values between the runs
        double newObjVal = g_beta(xx, xy, yy, newBeta) + _kappa * l1norm(newBeta);
        System.arraycopy(newBeta, 0, _beta, 0, _beta.length);
        _converged = (1 - newObjVal / _objVal) <= EPS;
        _objVal = newObjVal;
        _iterations = ++i;
      }
      // return xy back to its original state
      mul(xy, -1, xy);
      return _converged;
    }
    @Override
    public JsonObject toJson() {
      JsonObject json = new JsonObject();
      return json;
    }
    @Override
    public String name() {
      return "GeneralizedGradient";
    }
  }
}
