package hex;

import java.io.File;
import java.util.concurrent.ExecutionException;

import org.junit.*;

import water.*;
import water.deploy.Node;
import water.deploy.NodeVM;
import water.fvec.*;
import hex.pca.*;

public class PCATest extends TestUtil {
  public final double threshold = 0.000001;

  private static Frame getFrameForFile(Key outputKey, String path, String [] ignores) {
    File f = TestUtil.find_test_file(path);
    Key k = NFSFileVec.make(f);
    try{
      Frame fr = ParseDataset2.parse(outputKey, new Key[]{k});
      if(ignores != null)
        for(String s:ignores) UKV.remove(fr.remove(s)._key);
      return fr;
    } finally {
      UKV.remove(k);
    }
  }

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

  @Test public void testBasic() throws InterruptedException, ExecutionException{
    boolean standardize = true;
    Key kraw = Key.make("basicdata.raw");
    Key kdata = Key.make("basicdata.hex");
    Key kpca = Key.make("basicdata.pca");
    PCAModel model = null;

    try {
    // ValueArray va = va_maker(kdata,
    //    new byte []{0, 1, 2, 3, 4, 5, 6, 7},
    //   new double[]{1.0, 0.5, 0.3333333, 0.25, 0.20, 0.1666667, 0.1428571, 0.1250000},
    //    new float[]{-120.4f, 89.3f, 291.0f, -2.5f, 412.9f, -123.4f, -0.1f, 18.3f});
    FVecTest.makeByteVec(kraw, "x1,x2,x3\n0,1.0,-120.4\n1,0.5,89.3\n2,0.3333333,291.0\n3,0.25,-2.5\n4,0.20,-2.5\n5,0.1666667,-123.4\n6,0.1428571,-0.1\n7,0.1250000,18.3");
    Frame fr = ParseDataset2.parse(kdata, new Key[]{kraw});
    // DataFrame df = DataFrame.makePCAData(va, cols, standardize);

    new PCA("PCA on basic small dataset", kpca, fr, 0.0, standardize).invoke();
    model = DKV.get(kpca).get();
    // PCAParams params = new PCAParams(0.0, standardize);
    // DPCA.startPCAJob(kpca, df, params).get();
    } finally {
      UKV.remove(kdata);
      UKV.remove(kpca);
      if(model != null) model.delete();
    }
  }

  @Test public void testLinDep() throws InterruptedException, ExecutionException {
    Key kraw = Key.make("depdata.raw");
    Key kdata = Key.make("depdata.hex");
    Key kpca = Key.make("depdata.pca");
    PCAModel model = null;
    double[] sdev_R = {1.414214, 0};

    try {
      // ValueArray va = va_maker(kdata, new double []{0, 1, 2, 3, 4,  5},
      //                                new double []{0, 2, 4, 6, 8, 10});
      FVecTest.makeByteVec(kraw, "x1,x2\n0,0\n1,2\n2,4\n3,6\n4,8\n5,10");
      Frame fr = ParseDataset2.parse(kdata, new Key[]{kraw});

      new PCA("PCA on data with dependent cols", kpca, fr, 0.0, true).invoke();
      model = DKV.get(kpca).get();

      for(int i = 0; i < model.sdev().length; i++)
        Assert.assertEquals(sdev_R[i], model.sdev()[i], threshold);
    } finally {
      UKV.remove(kdata);
      UKV.remove(kpca);
      if(model != null) model.delete();
    }
  }

  @Test public void testArrests() throws InterruptedException, ExecutionException {
    double tol = 0.25;
    boolean standardize = true;
    Key ksrc = Key.make("arrests.hex");
    Key kdst = Key.make("arrests.pca");
    PCAModel model = null;
    double[] sdev_R = {1.5748783, 0.9948694, 0.5971291, 0.4164494};
    double[][] eigv_R = {{-0.5358995, 0.4181809, -0.3412327, 0.64922780},
                         {-0.5831836, 0.1879856, -0.2681484, -0.74340748},
                         {-0.2781909, -0.8728062, -0.3780158, 0.13387773},
                         {-0.5434321, -0.1673186, 0.8177779, 0.08902432}};

    try {
      Frame fr = getFrameForFile(ksrc, "smalldata/pca_test/USArrests.csv", null);

      // Build PCA model on all columns
      new PCA("PCA test on USArrests", kdst, fr, tol, standardize).invoke();
      model = DKV.get(kdst).get();

      // Compare standard deviation and eigenvectors to R results
      checkSdev(sdev_R, model.sdev());
      checkEigvec(eigv_R, model.eigVec());

      // Score original data set using PCA model
      // Key kscore = Key.make("arrests.score");
      // Frame score = PCAScoreTask.score(df, model._eigVec, kscore);
    } finally {
      UKV.remove(ksrc);
      UKV.remove(kdst);
      if(model != null) model.delete();
      // DKV.remove(kscore);
    }
  }

  public static void main(String [] args) throws Exception {
    System.out.println("Running PCATest");
    final int nnodes = 1;
    for( int i = 1; i < nnodes; i++ ) {
      Node n = new NodeVM(args);
      n.inheritIO();
      n.start();
    }
    H2O.waitForCloudSize(nnodes);
    System.out.println("Cloud formed");
    System.out.println("Running testBasic...");
    new PCATest().testBasic();
    System.out.println("Running testLinDep...");
    new PCATest().testLinDep();
    System.out.println("Running testArrests...");
    new PCATest().testArrests();
    System.out.println("DONE!!!");
  }
}
