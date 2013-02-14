package hex;

import java.util.Arrays;

import water.Iced;
import Jama.CholeskyDecomposition;
import Jama.Matrix;

import com.google.gson.JsonObject;

public final class LSMSolver extends Iced {

  public static final double DEFAULT_LAMBDA = 1e-5;
  public static final double DEFAULT_ALPHA = 0.5;
  public double _orlx = 1;//1.4; // over relaxation param
  public double _lambda = 1e-5;
  public double _alpha = 0.5;
  public double _rho = 1e-3;

  public boolean normalize() {
    return _lambda != 0;
  }

  public LSMSolver () {}

  public static LSMSolver makeSolver(){
    LSMSolver res =  new LSMSolver();
    res._alpha = 0;
    res._lambda = 0;
    res._rho = 0;
    return res;
  }

  public static LSMSolver makeL2Solver(double lambda){
    LSMSolver res = new LSMSolver();
    res._lambda = lambda;
    res._alpha = 0.0;
    res._rho = 0.0;
    return res;
  }

  public static LSMSolver makeL1Solver(double lambda){
    LSMSolver res = new LSMSolver();
    res._lambda = lambda;
    res._alpha = 1.0;
    return res;
  }

  public static LSMSolver makeElasticNetSolver(double lambda){
    LSMSolver res = new LSMSolver();
    res._lambda = lambda;
    res._alpha = 0.5;
    return res;
  }

  public static LSMSolver makeSolver(double lambda, double alpha){
    LSMSolver res = new LSMSolver();
    res._lambda = lambda;
    res._alpha = alpha;
    return res;
  }


  public JsonObject toJson(){
    JsonObject res = new JsonObject();
    res.addProperty("lambda",_lambda);
    res.addProperty("alpha",_alpha);
    return res;
  }

  private static double shrinkage(double x, double kappa) {
    return Math.max(0, x - kappa) - Math.max(0, -x - kappa);
  }

  public static class LSMSolverException extends RuntimeException {
    public LSMSolverException(String msg){super(msg);}
  }

  public static class NonSPDMatrixException extends LSMSolverException {
    public NonSPDMatrixException(){
      super("Matrix is not SPD, can't solve without regularization");
    }
  }

  public double [] solve(Matrix xx, Matrix xy) {
    double lambda = _lambda*(1-_alpha) + _rho;
    final int N = xx.getRowDimension();
    for(int i = 0; i < N-1; ++i)
      xx.set(i, i, xx.get(i,i)+lambda);
    CholeskyDecomposition lu = new CholeskyDecomposition(xx);
    if(_alpha == 0 || _lambda == 0) // no l1 penalty
      try {
        return lu.solve(xy).getColumnPackedCopy();
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
    for(int a = 0; a < 40; ++a){
      double kappa = _lambda*_alpha / _rho;
      for( int i = 0; i < 10000; ++i ) {
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
        if( r_norm < eps_pri && s_norm < eps_dual ) break;
      }
      return z;
    }
    throw new LSMSolverException("Unexpected error when solving LSM. Can't solve this problem.");
  }
}
