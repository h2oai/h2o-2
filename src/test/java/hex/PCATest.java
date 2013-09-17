package hex;

import org.junit.Assert;
import org.junit.Test;

import water.*;
import water.deploy.Node;
import water.deploy.NodeVM;
import water.fvec.Frame;
import hex.DPCA.*;
import hex.NewRowVecTask.DataFrame;
import hex.NewRowVecTask.JobCancelledException;

public class PCATest extends TestUtil {
  public final double threshold = 0.000001;

  public void checkSdev(double[] expected, double[] actual) {
    for(int i = 0; i < actual.length; i++)
      Assert.assertEquals(expected[i], actual[i], threshold);
  }

  public void checkEigvec(double[][] expected, double[][] actual) {
    int nfeat = actual.length;
    int ncomp = actual[0].length;
    for(int j = 0; j < ncomp; j++) {
      boolean flipped = Math.abs(expected[0][j] - actual[0][j]) > threshold;
      for(int i = 0; i < nfeat; i++) {
        if(flipped)
          Assert.assertEquals(expected[i][j], -actual[i][j], threshold);
        else
          Assert.assertEquals(expected[i][j], actual[i][j], threshold);
      }
    }
  }

  @Test public void testBasic() {
    Key kdata = Key.make("basicdata.hex");
    ValueArray va = va_maker(kdata,
        new byte []{0, 1, 2, 3, 4, 5, 6, 7},
       new double[]{1.0, 0.5, 0.3333333, 0.25, 0.20, 0.1666667, 0.1428571, 0.1250000},
        new float[]{-120.4f, 89.3f, 291.0f, -2.5f, 412.9f, -123.4f, -0.1f, 18.3f});
    int[] cols = {0, 1};
    boolean standardize = true;
    DataFrame df = DataFrame.makePCAData(va, cols, standardize);

    Key kpca = Key.make("basicdata.pca");
    PCAParams params = new PCAParams(0.0, standardize);
    DPCA.startPCAJob(kpca, df, params).get();

    UKV.remove(kdata);
    UKV.remove(kpca);
  }

  @Test public void testLinDep() {
    Key kdata = Key.make("depdata.hex");
    ValueArray va = va_maker(kdata, new double []{0, 1, 2, 3, 4,  5},
                                    new double []{0, 2, 4, 6, 8, 10});
    int[] cols = {0, 1};
    DataFrame df = DataFrame.makePCAData(va, cols, true);

    Key kpca = Key.make("depdata.pca");
    PCAParams params = new PCAParams(0.0, true);
    PCAModel model = DPCA.startPCAJob(kpca, df, params).get();

    double[] sdev_R = {1.414214, 0};
    for(int i = 0; i < model._sdev.length; i++)
      Assert.assertEquals(sdev_R[i], model._sdev[i], threshold);

    UKV.remove(kdata);
    UKV.remove(kpca);
  }

  @Test public void testArrests() throws JobCancelledException {
    Key ksrc = loadAndParseFile("arrests.hex", "smalldata/pca_test/USArrests.csv");
    ValueArray va = DKV.get(ksrc).get();

    double tol = 0.25;
    boolean standardize = true;
    int[] cols = new int[va._cols.length];
    for( int i = 0; i < cols.length; i++ ) cols[i] = i;
    DataFrame df = DataFrame.makePCAData(va, cols, standardize);

    // Build PCA model on all columns
    Key kdst = Key.make("arrests.pca");
    PCAParams params = new PCAParams(tol, standardize);
    PCAModel model = DPCA.startPCAJob(kdst, df, params).get();

    // Compare standard deviation and eigenvectors to R results
    double[] sdev_R = {1.5748783, 0.9948694, 0.5971291, 0.4164494};
    double[][] eigv_R = {{-0.5358995, 0.4181809, -0.3412327, 0.64922780},
                         {-0.5831836, 0.1879856, -0.2681484, -0.74340748},
                         {-0.2781909, -0.8728062, -0.3780158, 0.13387773},
                         {-0.5434321, -0.1673186, 0.8177779, 0.08902432}};
    checkSdev(sdev_R, model._sdev);
    checkEigvec(eigv_R, model._eigVec);

    // Score original data set using PCA model
    // Key kscore = Key.make("arrests.score");
    // Frame score = PCAScoreTask.score(df, model._eigVec, kscore);

    UKV.remove(ksrc);
    UKV.remove(kdst);
    // DKV.remove(kscore);
    System.out.println("DONE!");
  }

  void runTests() throws JobCancelledException {
    System.out.println("testBasic");
    testBasic();
    System.out.println("testLinDep");
    testLinDep();
    System.out.println("testArrests");
    testArrests();
    checkLeakedKeys();
    System.out.println("DONE!!!");
}

  public void main(String [] args) throws Exception{
    System.out.println("Running PCATest");
    final int nnodes = 1;
    for( int i = 1; i < nnodes; i++ ) {
      Node n = new NodeVM(args);
      n.inheritIO();
      n.start();
    }
    H2O.waitForCloudSize(nnodes);
    System.out.println("Cloud formed");
    runTests();
  }
}
