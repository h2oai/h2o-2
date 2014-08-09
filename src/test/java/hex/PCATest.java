package hex;

import hex.pca.PCA;
import hex.pca.PCAModel;
import hex.pca.PCAModelView;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import water.*;
import water.deploy.Node;
import water.deploy.NodeVM;
import water.fvec.FVecTest;
import water.fvec.Frame;
import water.fvec.NFSFileVec;
import water.fvec.ParseDataset2;

import java.io.File;
import java.util.concurrent.ExecutionException;

public class PCATest extends TestUtil {
  public final double threshold = 0.000001;

  private final void testHTML(PCAModel m) {
    StringBuilder sb = new StringBuilder();
    PCAModelView pcav = new PCAModelView();
    pcav.pca_model = m;
    pcav.toHTML(sb);
    assert(sb.length() > 0);
  }

  @BeforeClass public static void stall() { stall_till_cloudsize(3); }

  private static Frame getFrameForFile(Key outputKey, String path, String [] ignores) {
    File f = TestUtil.find_test_file(path);
    Key k = NFSFileVec.make(f);
    Frame fr = ParseDataset2.parse(outputKey, new Key[]{k});
    if(ignores != null)
      for(String s:ignores) UKV.remove(fr.remove(s)._key);
    return fr;
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
    PCAModel model = null;
    Frame fr = null;

    try {
    Key kraw = Key.make("basicdata.raw");
    FVecTest.makeByteVec(kraw, "x1,x2,x3\n0,1.0,-120.4\n1,0.5,89.3\n2,0.3333333,291.0\n3,0.25,-2.5\n4,0.20,-2.5\n5,0.1666667,-123.4\n6,0.1428571,-0.1\n7,0.1250000,18.3");
    fr = ParseDataset2.parse(Key.make("basicdata.hex"), new Key[]{kraw});

    Key kpca = Key.make("basicdata.pca");
    new PCA("PCA on basic small dataset", kpca, fr, 0.0, standardize).invoke();
    model = DKV.get(kpca).get();
    Job.JobState jstate = model.get_params().state;
    Assert.assertTrue(jstate == Job.JobState.DONE); //HEX-1817
    testHTML(model);
    } finally {
      if( fr    != null ) fr   .delete();
      if( model != null ) model.delete();
    }
  }

  @Test public void testLinDep() throws InterruptedException, ExecutionException {
    Key kdata = Key.make("depdata.hex");
    PCAModel model = null;
    Frame fr = null;
    double[] sdev_R = {1.414214, 0};

    try {
      Key kraw = Key.make("depdata.raw");
      FVecTest.makeByteVec(kraw, "x1,x2\n0,0\n1,2\n2,4\n3,6\n4,8\n5,10");
      fr = ParseDataset2.parse(kdata, new Key[]{kraw});

      Key kpca = Key.make("depdata.pca");
      new PCA("PCA on data with dependent cols", kpca, fr, 0.0, true).invoke();
      model = DKV.get(kpca).get();
      testHTML(model);

      for(int i = 0; i < model.sdev().length; i++)
        Assert.assertEquals(sdev_R[i], model.sdev()[i], threshold);
    } finally {
      if( fr    != null ) fr   .delete();
      if( model != null ) model.delete();
    }
  }

  @Test public void testArrests() throws InterruptedException, ExecutionException {
    double tol = 0.25;
    boolean standardize = true;
    PCAModel model = null;
    Frame fr = null;
    double[] sdev_R = {1.5748783, 0.9948694, 0.5971291, 0.4164494};
    double[][] eigv_R = {{-0.5358995, 0.4181809, -0.3412327, 0.64922780},
                         {-0.5831836, 0.1879856, -0.2681484, -0.74340748},
                         {-0.2781909, -0.8728062, -0.3780158, 0.13387773},
                         {-0.5434321, -0.1673186, 0.8177779, 0.08902432}};

    try {
      Key ksrc = Key.make("arrests.hex");
      fr = getFrameForFile(ksrc, "smalldata/pca_test/USArrests.csv", null);

      // Build PCA model on all columns
      Key kdst = Key.make("arrests.pca");
      new PCA("PCA test on USArrests", kdst, fr, tol, standardize).invoke();
      model = DKV.get(kdst).get();
      testHTML(model);

      // Compare standard deviation and eigenvectors to R results
      checkSdev(sdev_R, model.sdev());
      checkEigvec(eigv_R, model.eigVec());

      // Score original data set using PCA model
      // Key kscore = Key.make("arrests.score");
      // Frame score = PCAScoreTask.score(df, model._eigVec, kscore);
    } finally {
      if( fr    != null ) fr   .delete();
      if( model != null ) model.delete();
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
