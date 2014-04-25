package hex;

import static org.junit.Assert.assertEquals;
import hex.FrameTask.DataInfo;
import hex.glm.*;
import hex.glm.GLMParams.Family;
import org.junit.Test;
import water.*;
import water.deploy.Node;
import water.deploy.NodeVM;
import water.fvec.FVecTest;
import water.fvec.Frame;
import water.fvec.NFSFileVec;
import water.fvec.ParseDataset2;

import java.io.File;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;


public class GLMTest2  extends TestUtil {

  final private void testHTML(GLMModel m) {
    StringBuilder sb = new StringBuilder();
    new GLMModelView(m).toHTML(sb);
    assert(sb.length() > 0);
  }

  //------------------- simple tests on synthetic data------------------------------------

 @Test public void testGaussianRegression() throws InterruptedException, ExecutionException{
   Key raw = Key.make("gaussian_test_data_raw");
   Key parsed = Key.make("gaussian_test_data_parsed");
   Key modelKey = Key.make("gaussian_test");
   GLMModel model = null;
   Frame fr = null;
   try {
     // make data so that the expected coefficients is icept = col[0] = 1.0
     FVecTest.makeByteVec(raw, "x,y\n0,0\n1,0.1\n2,0.2\n3,0.3\n4,0.4\n5,0.5\n6,0.6\n7,0.7\n8,0.8\n9,0.9");
     fr = ParseDataset2.parse(parsed, new Key[]{raw});
     DataInfo dinfo = new DataInfo(fr, 1, false);
     GLMParams glm = new GLMParams(Family.gaussian);
     new GLM2("GLM test of gaussian(linear) regression.",Key.make(),modelKey,dinfo,glm,new double[]{0},0).fork().get();
     model = DKV.get(modelKey).get();
     testHTML(model);
     HashMap<String, Double> coefs = model.coefficients();
     assertEquals(0.0,coefs.get("Intercept"),1e-4);
     assertEquals(0.1,coefs.get("x"),1e-4);
   }finally{
     if( fr != null ) fr.delete();
     if(model != null)model.delete();
   }
 }

 /**
  * Test Poisson regression on simple and small synthetic dataset.
  * Equation is: y = exp(x+1);
  */
 @Test public void testPoissonRegression() throws InterruptedException, ExecutionException {
   Key raw = Key.make("poisson_test_data_raw");
   Key parsed = Key.make("poisson_test_data_parsed");
   Key modelKey = Key.make("poisson_test");
   GLMModel model = null;
   Frame fr = null;
   try {
     // make data so that the expected coefficients is icept = col[0] = 1.0
     FVecTest.makeByteVec(raw, "x,y\n0,2\n1,4\n2,8\n3,16\n4,32\n5,64\n6,128\n7,256");
     fr = ParseDataset2.parse(parsed, new Key[]{raw});
     DataInfo dinfo = new DataInfo(fr, 1, false);
     GLMParams glm = new GLMParams(Family.poisson);
     new GLM2("GLM test of poisson regression.",Key.make(),modelKey,dinfo,glm,new double[]{0},0).fork().get();
     model = DKV.get(modelKey).get();
     for(double c:model.beta())assertEquals(Math.log(2),c,1e-4);
     // Test 2, example from http://www.biostat.umn.edu/~dipankar/bmtry711.11/lecture_13.pdf
     //new byte []{1,2,3,4,5,6,7,8, 9, 10,11,12,13,14},
//     new byte []{0,1,2,3,1,4,9,18,23,31,20,25,37,45});
     model.delete();
     fr.delete();
     FVecTest.makeByteVec(raw, "x,y\n1,0\n2,1\n3,2\n4,3\n5,1\n6,4\n7,9\n8,18\n9,23\n10,31\n11,20\n12,25\n13,37\n14,45\n");
     fr = ParseDataset2.parse(parsed, new Key[]{raw});
     dinfo = new DataInfo(fr, 1, false);
     new GLM2("GLM test of poisson regression(2).",Key.make(),modelKey,dinfo,glm,new double[]{0},0).fork().get();
     model = DKV.get(modelKey).get();
     testHTML(model);
     assertEquals(0.3396,model.beta()[1],1e-4);
     assertEquals(0.2565,model.beta()[0],1e-4);
   }finally{
     if( fr != null ) fr.delete();
     if(model != null)model.delete();
   }
 }


  /**
   * Test Gamma regression on simple and small synthetic dataset.
   * Equation is: y = 1/(x+1);
   * @throws ExecutionException
   * @throws InterruptedException
   */
  @Test public void testGammaRegression() throws InterruptedException, ExecutionException {
    GLMModel model = null;
    Frame fr = null;
    try {
      // make data so that the expected coefficients is icept = col[0] = 1.0
      Key raw = Key.make("gamma_test_data_raw");
      Key parsed = Key.make("gamma_test_data_parsed");
      FVecTest.makeByteVec(raw, "x,y\n0,1\n1,0.5\n2,0.3333333\n3,0.25\n4,0.2\n5,0.1666667\n6,0.1428571\n7,0.125");
      fr = ParseDataset2.parse(parsed, new Key[]{raw});
//      /public GLM2(String desc, Key dest, Frame src, Family family, Link link, double alpha, double lambda) {
      double [] vals = new double[] {1.0,1.0};
      //public GLM2(String desc, Key dest, Frame src, Family family, Link link, double alpha, double lambda) {
      DataInfo dinfo = new DataInfo(fr, 1, false);
      GLMParams glm = new GLMParams(Family.gamma);
      Key modelKey = Key.make("gamma_test");
      new GLM2("GLM test of gamma regression.",Key.make(),modelKey,dinfo,glm,new double[]{0},0).fork().get();
      model = DKV.get(modelKey).get();
      testHTML(model);
      for(double c:model.beta())assertEquals(1.0, c,1e-4);
    }finally{
      if( fr != null ) fr.delete();
      if(model != null)model.delete();
    }
  }

  //simple tweedie test
  @Test public void testTweedieRegression() throws InterruptedException, ExecutionException{
    Key raw = Key.make("gaussian_test_data_raw");
    Key parsed = Key.make("gaussian_test_data_parsed");
    Key modelKey = Key.make("gaussian_test");
    Frame fr = null;
    GLMModel model = null;
    try {
      // make data so that the expected coefficients is icept = col[0] = 1.0
      FVecTest.makeByteVec(raw, "x,y\n0,0\n1,0.1\n2,0.2\n3,0.3\n4,0.4\n5,0.5\n6,0.6\n7,0.7\n8,0.8\n9,0.9\n0,0\n1,0\n2,0\n3,0\n4,0\n5,0\n6,0\n7,0\n8,0\n9,0");
      fr = ParseDataset2.parse(parsed, new Key[]{raw});
      double [] powers = new double [] {1.5,1.1,1.9};
      double [] intercepts = new double []{3.643,1.318,9.154};
      double [] xs = new double []{-0.260,-0.0284,-0.853};
      for(int i = 0; i < powers.length; ++i){
        DataInfo dinfo = new DataInfo(fr, 1, false);
        GLMParams glm = new GLMParams(Family.tweedie,powers[i]);

        new GLM2("GLM test of gaussian(linear) regression.",Key.make(),modelKey,dinfo,glm,new double[]{0},0).fork().get();
        model = DKV.get(modelKey).get();
        testHTML(model);
        HashMap<String, Double> coefs = model.coefficients();
        assertEquals(intercepts[i],coefs.get("Intercept"),1e-3);
        assertEquals(xs[i],coefs.get("x"),1e-3);
      }
    }finally{
      if( fr != null ) fr.delete();
      if(model != null)model.delete();
    }
  }

  //------------ TEST on selected files form small data and compare to R results ------------------------------------
  /**
   * Simple test for poisson, gamma and gaussian families (no regularization, test both lsm solvers).
   * Basically tries to predict horse power based on other parameters of the cars in the dataset.
   * Compare against the results from standard R glm implementation.
   * @throws ExecutionException
   * @throws InterruptedException
   */
  @Test public void testCars() throws InterruptedException, ExecutionException{
    Key parsed = Key.make("cars_parsed");
    Key modelKey = Key.make("cars_model");
    Frame fr = null;
    GLMModel model = null;
    try{
      String [] ignores = new String[]{"name"};
      String response = "power (hp)";
      fr = getFrameForFile(parsed, "smalldata/cars.csv", ignores, response);
      DataInfo dinfo = new DataInfo(fr, 1, true);
      GLMParams glm = new GLMParams(Family.poisson,0,Family.poisson.defaultLink,0);
      new GLM2("GLM test on cars.",Key.make(),modelKey,dinfo,glm,new double[]{0},0).fork().get();
      model = DKV.get(modelKey).get();
      testHTML(model);
      HashMap<String,Double> coefs = model.coefficients();
      String [] cfs1 = new String[]{"Intercept","economy (mpg)", "cylinders", "displacement (cc)", "weight (lb)", "0-60 mph (s)", "year"};
      double [] vls1 = new double []{4.9504805,-0.0095859,-0.0063046,0.0004392,0.0001762,-0.0469810,0.0002891};
      for(int i = 0; i < cfs1.length; ++i)
        assertEquals(vls1[i], coefs.get(cfs1[i]),1e-4);
      // test gamma
      double [] vls2 = new double []{8.992e-03,1.818e-04,-1.125e-04,1.505e-06,-1.284e-06,4.510e-04,-7.254e-05};
      model.delete();
      dinfo = new DataInfo(fr, 1, true);
      glm = new GLMParams(Family.gamma,0,Family.gamma.defaultLink,0);
      new GLM2("GLM test on cars.",Key.make(),modelKey,dinfo,glm,new double[]{0},0).fork().get();
      model = DKV.get(modelKey).get();
      testHTML(model);
      coefs = model.coefficients();
      for(int i = 0; i < cfs1.length; ++i)
        assertEquals(vls2[i], coefs.get(cfs1[i]),1e-4);
      model.delete();
      // test gaussian
      double [] vls3 = new double []{166.95862,-0.00531,-2.46690,0.12635,0.02159,-4.66995,-0.85724};
      glm = new GLMParams(Family.gaussian);
      dinfo = new DataInfo(fr, 1, true);
      new GLM2("GLM test on cars.",Key.make(),modelKey,dinfo,glm,new double[]{0},0).fork().get();
      model = DKV.get(modelKey).get();
      testHTML(model);
      coefs = model.coefficients();
      for(int i = 0; i < cfs1.length; ++i)
        assertEquals(vls3[i], coefs.get(cfs1[i]),1e-4);
    } finally {
      if( fr != null ) fr.delete();
      if(model != null)model.delete();
    }
  }

  /**
   * Simple test for binomial family (no regularization, test both lsm solvers).
   * Runs the classical prostate, using dataset with race replaced by categoricals (probably as it's supposed to be?), in any case,
   * it gets to test correct processing of categoricals.
   *
   * Compare against the results from standard R glm implementation.
   * @throws ExecutionException
   * @throws InterruptedException
   */
  @Test public void testProstate() throws InterruptedException, ExecutionException{
    Key parsed = Key.make("prostate_parsed");
    Key modelKey = Key.make("prostate_model");
    GLMModel model = null;
    File f = TestUtil.find_test_file("smalldata/glm_test/prostate_cat_replaced.csv");
    Frame fr = getFrameForFile(parsed, "smalldata/glm_test/prostate_cat_replaced.csv", new String[]{"ID"}, "CAPSULE");
    try{
      // R results
//      Coefficients:
//        (Intercept)           ID          AGE       RACER2       RACER3        DPROS        DCAPS          PSA          VOL      GLEASON
//          -8.894088     0.001588    -0.009589     0.231777    -0.459937     0.556231     0.556395     0.027854    -0.011355     1.010179
      String [] cfs1 = new String [] {"Intercept","AGE", "RACE.R2","RACE.R3", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON"};
      double [] vals = new double [] {-8.14867, -0.01368, 0.32337, -0.38028, 0.55964, 0.49548, 0.02794, -0.01104, 0.97704};
      DataInfo dinfo = new DataInfo(fr, 1, false);
      GLMParams glm = new GLMParams(Family.binomial);

      new GLM2("GLM test on prostate.",Key.make(),modelKey,dinfo,glm,new double[]{0},0).fork().get();

      model = DKV.get(modelKey).get();
      testHTML(model);
      HashMap<String, Double> coefs = model.coefficients();
      for(int i = 0; i < cfs1.length; ++i)
        assertEquals(vals[i], coefs.get(cfs1[i]),1e-4);
      GLMValidation val = model.validation();
      assertEquals(512.3, val.nullDeviance(),1e-1);
      assertEquals(378.3, val.residualDeviance(),1e-1);
      assertEquals(396.3, val.aic(),1e-1);
    } finally {
      fr.delete();
      if(model != null)model.delete();
    }
  }

  private static Frame getFrameForFile(Key outputKey, String path,String [] ignores, String response){
    File f = TestUtil.find_test_file(path);
    Key k = NFSFileVec.make(f);
    Frame fr = ParseDataset2.parse(outputKey, new Key[]{k});
    if(ignores != null)
      for(String s:ignores) UKV.remove(fr.remove(s)._key);
    // put the response to the end
    fr.add(response, fr.remove(response));
    return fr;
  }


  public static void main(String [] args) throws Exception{
    System.out.println("Running ParserTest2");
    final int nnodes = 1;
    for( int i = 1; i < nnodes; i++ ) {
      Node n = new NodeVM(args);
      n.inheritIO();
      n.start();
    }
    H2O.waitForCloudSize(nnodes);
    System.out.println("Running...");
    new GLMTest2().testGaussianRegression();
    new GLMTest2().testPoissonRegression();
    new GLMTest2().testGammaRegression();
    new GLMTest2().testTweedieRegression();
    new GLMTest2().testProstate();
    new GLMTest2().testCars();
    System.out.println("DONE!");
  }
}
