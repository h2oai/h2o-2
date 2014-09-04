package hex.glm;

import water.fvec.Vec;

import java.util.List;

/**
 * Temporary Util class for matrix operation
 * Created by bersakain on 9/3/14.
 */
public class MatrixUtils {

  public static double[] Hk0q(int k, double[] q, List<double[]> s, List<double[]> y) {
    double niu = 1;
    if (k>0) {
      int m = LBFGS.m;
      int limitedIdx = k-1 > m-1 ? m-1 : k-1; // check if we have used all the storage.
      niu = innerProduct(s.get(limitedIdx), y.get(limitedIdx)) / innerProduct(y.get(limitedIdx), y.get(limitedIdx));
    }
    return scalarProduct(niu, q);
  }

  public static double innerProduct(double[] x, double[] y) {
    assert x.length==y.length: "Vector must have same length to produce inner product.";
    double result = 0;
    for (int i = 0; i < x.length; i++) {
      result += x[i] * y[i];
    }
    return result;
  }

  public static double[] scalarProduct(double scalar, double[] x) {
    double[] result = new double[x.length];
    for (int i = 0; i < x.length; i++) {
      result[i] = scalar * x[i];
    }
    return result;
  }

  public static double[] add(double[] x, double[] y) {
    assert x.length==y.length: "Vector must have same length to produce add.";
    double[] result = new double[x.length];
    for (int i = 0; i < x.length; i++) {
      result[i] = x[i] + y[i];
    }
    return result;
  }

  public static double[] minus(double[] x, double[] y) {
    assert x.length==y.length: "Vector must have same length to produce minus.";
    double[] result = new double[x.length];
    for (int i = 0; i < x.length; i++) {
      result[i] = x[i] - y[i];
    }
    return result;
  }

}
