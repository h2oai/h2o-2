package hex;

import static org.junit.Assert.assertEquals;
import hex.gram.Gram.InPlaceCholesky;
import jsr166y.ForkJoinPool;
import jsr166y.RecursiveAction;

import org.junit.Test;

import water.H2O;
import water.TestUtil;
import water.util.Log;
import Jama.CholeskyDecomposition;
import Jama.Matrix;

public class CholTest extends TestUtil{

  static int[] STEPS = {5, 10, 50, 100, 200};
  @Test public void test_null() {
  }
  public void test () {
    Log.info("CholTest::test enter");
    for (int sz = 6000; sz < 10000; sz+=2000) {
      Log.info("CholTest::test sz is " + sz);
      DataSetup data = new DataSetup(sz, 12345);
      long start = System.currentTimeMillis();
      CholeskyDecomposition jamaChol = new Matrix(data.xx).chol();
      Log.info("JAMA CHOLESKY [N = " + sz + "] TAKES " + (System.currentTimeMillis() - start) + " MILLISECONDS.");
      if (!jamaChol.isSPD()) continue;
      ForkJoinPool fjp = new ForkJoinPool(32);
      for (int t = 2; t <= 32; t += 2) {
        for (int step : STEPS)
          fjp.invoke(new TestSetup(new DataSetup(data.xx),jamaChol.getL().getArray(),step,t));
      }
    }
    Log.info("CholTest::test exit");
  }

  private final static class DataSetup implements Cloneable {
    public double xx[][];
    public DataSetup(double xx[][]) {
      this.xx = xx.clone();
      for (int i = 0; i < xx.length; i++) this.xx[i] = xx[i].clone();
    }
    public DataSetup(int N, int rseed) {
      xx = new double[N][];
      for (int i = 0; i < N; i++) {
        xx[i] = new double[N];
        for (int j = 0; j <= i; j++) xx[i][j] = j + 1;
        for (int j = i+1; j < N; j++) xx[i][j] = i + 1;
      }
    }
  }

  private static  void print_matrix(double matrix[][]) {
    for (int i = 0; i < matrix.length; i++) {
      for (int j = 0; j <= i; j++)
        System.out.print(String.format("%.2f ",matrix[i][j]));
      System.out.println();
    }
  }

  private final static class TestSetup extends RecursiveAction {
    DataSetup data;
    double jama[][];
    int p;
    int step;
    public TestSetup(DataSetup data, double jama[][],int step, int p) {
      this.data = data; this.jama = jama; this.step = step; this.p = p;
    }
    public void compute() {
      long start = System.currentTimeMillis();
      double[][] chol = InPlaceCholesky.decompose_2(data.xx, step, p).getL();
      Log.info("H2O CHOLESKY [N = "+data.xx.length+"  P = "+p+"  STEP = "+step+"] TAKES " + (System.currentTimeMillis() - start) + " MILLISECONDS.");
      assertEquals(jama.length, chol.length);
      for (int i = 0; i < chol.length; i++)
        for( int j = 0; j <= i; j++)
          assertEquals(jama[i][j], chol[i][j], 0.0001);
    }
  }

  public static void main(String[] args) throws Exception {
    water.Boot.main(CholTest.class, args);
  }

  public static void userMain(String[] args) throws Exception {
    H2O.main(args);
    new CholTest().test();
    System.exit(0);
  }
}

