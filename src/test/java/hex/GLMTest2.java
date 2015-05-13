package hex;

import hex.FrameTask.DataInfo.TransformType;
import hex.glm.GLM2.Source;
import hex.glm.GLMParams.Link;
import hex.glm.GLMTask.GLMIterationTask;
import org.junit.Assert;

import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import hex.FrameTask.DataInfo;
import hex.glm.*;
import hex.glm.GLMParams.Family;
import org.junit.Test;
import water.*;
import water.deploy.Node;
import water.deploy.NodeVM;
import water.fvec.*;
import water.util.ModelUtils;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.ExecutionException;


public class GLMTest2  extends TestUtil {

  final static public void testHTML(GLMModel m) {
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
     GLMParams glm = new GLMParams(Family.gaussian);
     new GLM2("GLM test of gaussian(linear) regression.",Key.make(),modelKey,new Source(fr,fr.vec("y"),false),Family.gaussian).doInit().fork().get();
     model = DKV.get(modelKey).get();
     testHTML(model);
     Assert.assertTrue(model.get_params().state == Job.JobState.DONE); //HEX-1817
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
     new GLM2("GLM test of poisson regression.",Key.make(),modelKey,new Source(fr,fr.lastVec(),false),Family.poisson).setRegularization(new double []{0},new double[]{0}).doInit().fork().get();
     model = DKV.get(modelKey).get();
     for(double c:model.beta())assertEquals(Math.log(2),c,1e-4);
     // Test 2, example from http://www.biostat.umn.edu/~dipankar/bmtry711.11/lecture_13.pdf
     //new byte []{1,2,3,4,5,6,7,8, 9, 10,11,12,13,14},
//     new byte []{0,1,2,3,1,4,9,18,23,31,20,25,37,45});
     model.delete();
     fr.delete();
     FVecTest.makeByteVec(raw, "x,y\n1,0\n2,1\n3,2\n4,3\n5,1\n6,4\n7,9\n8,18\n9,23\n10,31\n11,20\n12,25\n13,37\n14,45\n");
     fr = ParseDataset2.parse(parsed, new Key[]{raw});
     new GLM2("GLM test of poisson regression(2).",Key.make(),modelKey,new Source(fr,fr.lastVec(),false),Family.poisson).setRegularization(new double []{0},new double[]{0}).doInit().fork().get();
     model = DKV.get(modelKey).get();
     testHTML(model);
     assertEquals(0.3396,model.beta()[1],5e-3);
     assertEquals(0.2565,model.beta()[0],5e-3);
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
      Key modelKey = Key.make("gamma_test");
      new GLM2("GLM test of gamma regression.",Key.make(),modelKey,new Source(fr,fr.lastVec(),false),Family.gamma).setRegularization(new double []{0},new double[]{0}).doInit().fork().get();
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
    Key raw = Key.make("tweedie_test_data_raw");
    Key parsed = Key.make("tweedie_test_data_parsed");
    Key modelKey = Key.make("tweedie_test");
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
        GLM2 glm = new GLM2("GLM test of tweedie regression.",Key.make(),modelKey,new Source(fr,fr.lastVec(),false),Family.tweedie, Link.family_default,0,true);//.doInit().fork().get();
        glm.setTweediePower(powers[i]);
        glm.setLambda(0);
        glm.max_iter = 1000;
        glm.doInit().fork().get();
        model = DKV.get(modelKey).get();
        testHTML(model);
        HashMap<String, Double> coefs = model.coefficients();
        assertEquals(intercepts[i], coefs.get("Intercept"), 1e-3);
        assertEquals(xs[i],coefs.get("x"),1e-3);
      }
    }finally{
      if( fr != null ) fr.delete();
      if(model != null)model.delete();
    }
  }

  @Test public void testOffset()throws InterruptedException, ExecutionException{
    Key parsed = Key.make("prostate_parsed");
    Key modelKey = Key.make("prostate_model");
    GLMModel model = null;
    File f = TestUtil.find_test_file("smalldata/glm_test/prostate_cat_replaced.csv");
    Frame fr = getFrameForFile(parsed, "smalldata/glm_test/prostate_cat_replaced.csv", new String[]{"ID"}, "CAPSULE");
    Key k = Key.make("rebalanced");
    H2O.submitTask(new RebalanceDataSet(fr,k,64)).join();
    fr.delete();
    fr = DKV.get(k).get();
    try {
//      R results:
//      Call:  glm(formula = CAPSULE ~ . - ID - AGE, family = binomial, data = D,
//        offset = D$AGE)
//
//      Coefficients:
//      (Intercept)       RACER2       RACER3        DPROS        DCAPS          PSA          VOL      GLEASON
//      -95.16718     -0.67663     -2.11848      2.31296      3.47783      0.10842     -0.08657      2.90452
//
//      Degrees of Freedom: 379 Total (i.e. Null);  372 Residual
//      Null Deviance:	    2015
//      Residual Deviance: 1516 	AIC: 1532
      // H2O differs on intercept and race, same residual deviance though
      String[] cfs1 = new String[]{/*"Intercept","RACE.R2","RACE.R3",*/ "AGE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON"};
      double[] vals = new double[]{/*-95.16718, -0.67663, -2.11848,*/1, 2.31296, 3.47783, 0.10842, -0.08657, 2.90452};
      new GLM2("GLM offset test on prostate.", Key.make(), modelKey, new GLM2.Source(fr, fr.vec("CAPSULE"), false, true, fr.vec("AGE")), Family.binomial).setRegularization(new double[]{0}, new double[]{0}).doInit().fork().get();
      model = DKV.get(modelKey).get();
      Assert.assertTrue(model.get_params().state == Job.JobState.DONE); //HEX-1817
      testHTML(model);
      HashMap<String, Double> coefs = model.coefficients();
      for (int i = 0; i < cfs1.length; ++i)
        assertEquals(vals[i], coefs.get(cfs1[i]), 1e-4);
      GLMValidation val = model.validation();
      assertEquals(2015, model.null_validation.residualDeviance(), 1e-1);
      assertEquals(1516, val.residualDeviance(), 1e-1);
      assertEquals(1532, val.aic(), 1e-1);
      fr.delete();
      // test constant offset (had issues with constant-column filtering)
      fr = getFrameForFile(parsed, "smalldata/glm_test/abcd.csv", new String[0], "D");
      new GLM2("GLM testing constant offset on a toy dataset.", Key.make(), modelKey, new GLM2.Source(fr, fr.vec("D"), false, false, fr.vec("E")), Family.gaussian).setRegularization(new double[]{0}, new double[]{0}).doInit().fork().get();
      // just test it does not blow up and the model is sane
      model = DKV.get(modelKey).get();
      assertEquals(model.coefficients().get("E"), 1, 0); // should be exactly 1
    } finally {
      fr.delete();
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
    try {
      String[] ignores = new String[]{"name"};
      String response = "power (hp)";
      fr = getFrameForFile(parsed, "smalldata/cars.csv", ignores, response);
      new GLM2("GLM test on cars.", Key.make(), modelKey, new Source(fr,fr.lastVec(),true),Family.poisson).setRegularization(new double []{0},new double[]{0}).doInit().fork().get();
      model = DKV.get(modelKey).get();
      testHTML(model);
      HashMap<String, Double> coefs = model.coefficients();
      String[] cfs1 = new String[]{"Intercept", "economy (mpg)", "cylinders", "displacement (cc)", "weight (lb)", "0-60 mph (s)", "year"};
      double[] vls1 = new double[]{4.9504805, -0.0095859, -0.0063046, 0.0004392, 0.0001762, -0.0469810, 0.0002891};
      for (int i = 0; i < cfs1.length; ++i)
        assertEquals(vls1[i], coefs.get(cfs1[i]), 1e-4);
      // test gamma
      double[] vls2 = new double[]{8.992e-03, 1.818e-04, -1.125e-04, 1.505e-06, -1.284e-06, 4.510e-04, -7.254e-05};
      model.delete();
      new GLM2("GLM test on cars.", Key.make(), modelKey, new Source(fr,fr.lastVec(),true), Family.gamma).setRegularization(new double []{0},new double[]{0}).doInit().fork().get();
      model = DKV.get(modelKey).get();
      testHTML(model);
      coefs = model.coefficients();
      for (int i = 0; i < cfs1.length; ++i)
        assertEquals(vls2[i], coefs.get(cfs1[i]), 1e-4);
      model.delete();
      // test gaussian
      double[] vls3 = new double[]{166.95862, -0.00531, -2.46690, 0.12635, 0.02159, -4.66995, -0.85724};
      new GLM2("GLM test on cars.", Key.make(), modelKey, new Source(fr,fr.lastVec(),true), Family.gaussian).setRegularization(new double []{0},new double[]{0}).doInit().fork().get();
      model = DKV.get(modelKey).get();
      testHTML(model);
      coefs = model.coefficients();
      for (int i = 0; i < cfs1.length; ++i)
        assertEquals(vls3[i], coefs.get(cfs1[i]), 1e-4);
    } catch(Throwable t){
      t.printStackTrace();
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
      new GLM2("GLM test on prostate.",Key.make(),modelKey,new Source(fr,fr.lastVec(),false),Family.binomial).setRegularization(new double []{0},new double[]{0}).doInit().fork().get();
      model = DKV.get(modelKey).get();
      Assert.assertTrue(model.get_params().state == Job.JobState.DONE); //HEX-1817
      testHTML(model);
      HashMap<String, Double> coefs = model.coefficients();
      for(int i = 0; i < cfs1.length; ++i)
        assertEquals(vals[i], coefs.get(cfs1[i]),1e-4);
      GLMValidation val = model.validation();
      assertEquals(512.3, model.null_validation.residualDeviance(), 1e-1);
      assertEquals(378.3, val.residualDeviance(),1e-1);
      assertEquals(396.3, val.aic(), 1e-1);
      double prior = 1e-5;
      // test the same data and model with prior, should get the same model except for the intercept
      new GLM2("GLM test on prostate.",Key.make(),modelKey,new Source(fr,fr.lastVec(),false),Family.binomial).setRegularization(new double []{0},new double[]{0}).setPrior(prior).doInit().fork().get();
      GLMModel model2 = DKV.get(modelKey).get();
      for(int i = 0; i < model2.beta().length-1; ++i)
        assertEquals(model.beta()[i], model2.beta()[i], 1e-8);
      assertEquals(model.beta()[model.beta().length-1] -Math.log(model.ymu * (1-prior)/(prior * (1-model.ymu))),model2.beta()[model.beta().length-1],1e-10);
    } finally {
      fr.delete();
      if(model != null)model.delete();
    }
  }


  @Test public void testNoNNegative() {
//    glmnet's result:
//    res2 <- glmnet(x=M,y=D$CAPSULE,lower.limits=0,family='binomial')
//    res2$beta[,100]
//    AGE       RACE      DPROS      DCAPS        PSA        VOL    GLEASON
//    0.00000000 0.00000000 0.54788332 0.53816534 0.02380097 0.00000000 0.98115670
//    res2$a0[100]
//    s99
//      -8.945984
    Key parsed = Key.make("prostate_parsed");
    Key modelKey = Key.make("prostate_model");
    GLMModel model = null;
    Frame fr = getFrameForFile(parsed, "smalldata/logreg/prostate.csv", new String[]{"ID"}, "CAPSULE");
    Key k = Key.make("rebalanced");
    H2O.submitTask(new RebalanceDataSet(fr, k, 64)).join();
    fr.delete();
    fr = DKV.get(k).get();
    try {
      // H2O differs on intercept and race, same residual deviance though
      String[] cfs1 = new String[]{"RACE", "AGE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON","Intercept"};
      double[] vals = new double[]{0, 0, 0.54788332,0.53816534, 0.02380097, 0, 0.98115670,-8.945984};
      new GLM2("GLM offset test on prostate.", Key.make(), modelKey, new GLM2.Source((Frame)fr.clone(), fr.vec("CAPSULE"), true, true), Family.binomial).setNonNegative(true).setRegularization(new double[]{1},new double[]{2.22E-5}).doInit().fork().get(); //.setHighAccuracy().doInit().fork().get();
      model = DKV.get(modelKey).get();
      Assert.assertTrue(model.get_params().state == Job.JobState.DONE);
      testHTML(model);
      HashMap<String, Double> coefs = model.coefficients();
      for (int i = 0; i < cfs1.length; ++i)
        assertEquals(vals[i], coefs.get(cfs1[i]), 1e-2);
      GLMValidation val = model.validation();
      assertEquals(512.2888, model.null_validation.residualDeviance(), 1e-1);
      assertEquals(383.8068, val.residualDeviance(), 1e-1);
    } finally {
      fr.delete();
      if(model != null)model.delete();
    }
  }

  @Test public void testBounds() {
//    glmnet's result:
//    res2 <- glmnet(x=M,y=D$CAPSULE,lower.limits=-.5,upper.limits=.5,family='binomial')
//    res2$beta[,58]
//    AGE        RACE          DPROS       PSA         VOL         GLEASON
//    -0.00616326 -0.50000000  0.50000000  0.03628192 -0.01249324  0.50000000 //    res2$a0[100]
//    res2$a0[58]
//    s57
//    -4.155864
//    lambda = 0.001108, null dev =  512.2888, res dev = 379.7597
    Key parsed = Key.make("prostate_parsed");
    Key modelKey = Key.make("prostate_model");
    GLMModel model = null;
    Frame fr = getFrameForFile(parsed, "smalldata/logreg/prostate.csv", new String[]{"ID"}, "CAPSULE");
    Key k = Key.make("rebalanced");
    H2O.submitTask(new RebalanceDataSet(fr, k, 64)).join();
    fr.delete();
    fr = DKV.get(k).get();
    Key betaConsKey = Key.make("beta_constraints");
    FVecTest.makeByteVec(betaConsKey, "names, lower_bounds, upper_bounds\n RACE, -.5, .5\n DCAPS, -.4, .4\n DPROS, -.5, .5 \nPSA, -.5, .5\n VOL, -.5, .5\nGLEASON, -.5, .5\nAGE, -.5, .5");
    Frame betaConstraints = ParseDataset2.parse(parsed, new Key[]{betaConsKey});
    try {
      // H2O differs on intercept and race, same residual deviance though
      String[] cfs1 = new String[]{"AGE", "RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON","Intercept"};
      double[] vals = new double[]{-0.006502588, -0.500000000,  0.500000000,  0.400000000,  0.034826559, -0.011661747,  0.500000000, -4.564024 };
      new GLM2("GLM offset test on prostate.", Key.make(), modelKey, new GLM2.Source((Frame)fr.clone(), fr.vec("CAPSULE"), true, true), Family.binomial).setNonNegative(false).setRegularization(new double[]{1},new double[]{0.001607}).setBetaConstraints(betaConstraints).doInit().fork().get(); //.setHighAccuracy().doInit().fork().get();
      model = DKV.get(modelKey).get();
      Assert.assertTrue(model.get_params().state == Job.JobState.DONE);
      testHTML(model);
      System.out.println(Arrays.toString(model.norm_beta(model.lambda())));
      System.out.println(model.coefficients().toString());
      System.out.println(model.validation().toString());
      HashMap<String, Double> coefs = model.coefficients();
      for (int i = 0; i < cfs1.length; ++i)
        assertEquals(vals[i], coefs.get(cfs1[i]), 1e-2);
      GLMValidation val = model.validation();
      assertEquals(512.2888, model.null_validation.residualDeviance(), 1e-1);
      assertEquals(388.4686, val.residualDeviance(),1e-1);
      model.delete();
      betaConstraints.delete();
      // fix AGE at .5
      FVecTest.makeByteVec(betaConsKey, "names, lower_bounds, upper_bounds\n RACE, -.5, .5\n DCAPS, -.4, .4\n DPROS, -.5, .5 \nPSA, -.5, .5\n VOL, -.5, .5\nGLEASON, -.5, .5\nAGE, .5, .5");
      betaConstraints = ParseDataset2.parse(parsed, new Key[]{betaConsKey});
      new GLM2("GLM offset test on prostate.", Key.make(), modelKey, new GLM2.Source((Frame)fr.clone(), fr.vec("CAPSULE"), true, true), Family.binomial).setNonNegative(false).setRegularization(new double[]{1},new double[]{0.001607}).setBetaConstraints(betaConstraints).doInit().fork().get(); //.setHighAccuracy().doInit().fork().get();
      model = DKV.get(modelKey).get();
      System.out.println(model.coefficients());
      assertEquals(.5,model.coefficients().get("AGE"), 1e-16);

      // negative tests
      // a) upper bound > lower bound
      FVecTest.makeByteVec(betaConsKey, "names, lower_bounds, upper_bounds\n AGE, .51, .5\n RACE, -.5, .5\n DCAPS, -.4, .4\n DPROS, -.5, .5 \nPSA, -.5, .5\n VOL, -.5, .5\nGLEASON, -.5, .5");
      model.delete();
      betaConstraints = ParseDataset2.parse(parsed, new Key[]{betaConsKey});
      try {
        new GLM2("GLM offset test on prostate.", Key.make(), modelKey, new GLM2.Source((Frame) fr.clone(), fr.vec("CAPSULE"), true, true), Family.binomial).setNonNegative(false).setRegularization(new double[]{1}, new double[]{0.001607}).setBetaConstraints(betaConstraints).doInit().fork().get(); //.setHighAccuracy().doInit().fork().get();
        assertTrue("should've thrown",false);
      } catch(IllegalArgumentException t) {
        assertTrue(t.getMessage().contains("Invalid upper/lower bounds"));
      }
      // b) duplicate coordinate
      FVecTest.makeByteVec(betaConsKey, "names, lower_bounds, upper_bounds\n AGE, .5, .5\n AGE, -.5, .5\n DCAPS, -.4, .4\n DPROS, -.5, .5 \nPSA, -.5, .5\n VOL, -.5, .5\nGLEASON, -.5, .5");
      model.delete();
      betaConstraints = ParseDataset2.parse(parsed, new Key[]{betaConsKey});
      try {
        new GLM2("GLM offset test on prostate.", Key.make(), modelKey, new GLM2.Source((Frame) fr.clone(), fr.vec("CAPSULE"), true, true), Family.binomial).setNonNegative(false).setRegularization(new double[]{1}, new double[]{0.001607}).setBetaConstraints(betaConstraints).doInit().fork().get(); //.setHighAccuracy().doInit().fork().get();
        assertTrue("should've thrown",false);
      } catch(IllegalArgumentException t) {
        assertTrue(t.getMessage().contains("duplicate constraints for 'AGE'"));
      }
      FVecTest.makeByteVec(betaConsKey, "names, lower_bounds, upper_bounds\n AGE, .5, .5\n XXX, -.5, .5\n DCAPS, -.4, .4\n DPROS, -.5, .5 \nPSA, -.5, .5\n VOL, -.5, .5\nGLEASON, -.5, .5");
      betaConstraints = ParseDataset2.parse(parsed, new Key[]{betaConsKey});
      try {
        new GLM2("GLM offset test on prostate.", Key.make(), modelKey, new GLM2.Source((Frame) fr.clone(), fr.vec("CAPSULE"), true, true), Family.binomial).setNonNegative(false).setRegularization(new double[]{1}, new double[]{0.001607}).setBetaConstraints(betaConstraints).doInit().fork().get(); //.setHighAccuracy().doInit().fork().get();
        assertTrue("should've thrown",false);
      } catch(IllegalArgumentException t) {
        assertTrue(t.getMessage().contains("unknown predictor name 'XXX'"));
      }
      betaConstraints.delete();
      FVecTest.makeByteVec(betaConsKey, "nms, lower_bounds, upper_bounds\n AGE, .5, .5\n XXX, -.5, .5\n DCAPS, -.4, .4\n DPROS, -.5, .5 \nPSA, -.5, .5\n VOL, -.5, .5\nGLEASON, -.5, .5");
      betaConstraints = ParseDataset2.parse(parsed, new Key[]{betaConsKey});
      try {
        new GLM2("GLM offset test on prostate.", Key.make(), modelKey, new GLM2.Source((Frame) fr.clone(), fr.vec("CAPSULE"), true, true), Family.binomial).setNonNegative(false).setRegularization(new double[]{1}, new double[]{0.001607}).setBetaConstraints(betaConstraints).doInit().fork().get(); //.setHighAccuracy().doInit().fork().get();
        assertTrue("should've thrown",false);
      } catch(IllegalArgumentException t) {
        assertTrue(t.getMessage().contains("missing column with predictor names"));
      }
    } finally {
      if(betaConstraints != null)
        betaConstraints.delete();
      fr.delete();
      if(model != null)model.delete();
    }
  }

  @Test public void testProximal() {
//    glmnet's result:
//    res2 <- glmnet(x=M,y=D$CAPSULE,lower.limits=-.5,upper.limits=.5,family='binomial')
//    res2$beta[,58]
//    AGE        RACE          DPROS       PSA         VOL         GLEASON
//    -0.00616326 -0.50000000  0.50000000  0.03628192 -0.01249324  0.50000000 //    res2$a0[100]
//    res2$a0[58]
//    s57
//    -4.155864
//    lambda = 0.001108, null dev =  512.2888, res dev = 379.7597
    Key parsed = Key.make("prostate_parsed");
    Key modelKey = Key.make("prostate_model");
    GLMModel model = null;
    Frame fr = getFrameForFile(parsed, "smalldata/logreg/prostate.csv", new String[]{"ID"}, "CAPSULE");
    Key k = Key.make("rebalanced");
    H2O.submitTask(new RebalanceDataSet(fr, k, 64)).join();
    fr.delete();
    fr = DKV.get(k).get();
    fr.remove("ID");
    Key betaConsKey = Key.make("beta_constraints");
    //String[] cfs1 = new String[]{"RACE", "AGE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON","Intercept"};
    //double[] vals = new double[]{0, 0, 0.54788332,0.53816534, 0.02380097, 0, 0.98115670,-8.945984};
    // [AGE, RACE, DPROS, DCAPS, PSA, VOL, GLEASON, Intercept]
    FVecTest.makeByteVec(betaConsKey, "names, beta_given, rho\n AGE, 0.1, 1\nRACE, -0.1, 1 \n DPROS, 10, 1 \n DCAPS, -10, 1 \n PSA, 0, 1\n VOL, 0, 1\nGLEASON, 0, 1\n Intercept, 0, 0 \n");
    Frame betaConstraints = ParseDataset2.parse(parsed, new Key[]{betaConsKey});
    try {
      // H2O differs on intercept and race, same residual deviance though
      GLM2.Source src = new GLM2.Source((Frame)fr.clone(), fr.vec("CAPSULE"), false, true);
      new GLM2("GLM offset test on prostate.", Key.make(), modelKey, src, Family.binomial).setNonNegative(false).setRegularization(new double[]{0},new double[]{0.000}).setBetaConstraints(betaConstraints).setHighAccuracy().doInit().fork().get(); //.setHighAccuracy().doInit().fork().get();
      model = DKV.get(modelKey).get();
      fr.add("CAPSULE", fr.remove("CAPSULE"));
      DataInfo dinfo = new DataInfo(fr, 1, true, false, TransformType.NONE, DataInfo.TransformType.NONE);
      GLMIterationTask glmt = new GLMTask.GLMIterationTask(0,null, dinfo, new GLMParams(Family.binomial),false, true, true, model.beta(), 0, 1.0/380, ModelUtils.DEFAULT_THRESHOLDS, null).doAll(dinfo._adaptedFrame);
      double [] beta = model.beta();
      double [] grad = glmt.gradient(0,0);
      for(int i = 0; i < beta.length; ++i)
        Assert.assertEquals(0,grad[i] + betaConstraints.vec("rho").at(i) * (beta[i] - betaConstraints.vec("beta_given").at(i)),1e-8);
      // now standardized
      src = new GLM2.Source((Frame)fr.clone(), fr.vec("CAPSULE"), true, true);
      new GLM2("GLM offset test on prostate.", Key.make(), modelKey, src, Family.binomial).setNonNegative(false).setRegularization(new double[]{0},new double[]{0.000}).setBetaConstraints(betaConstraints).setHighAccuracy().doInit().fork().get(); //.setHighAccuracy().doInit().fork().get();
      model = DKV.get(modelKey).get();
      fr.add("CAPSULE", fr.remove("CAPSULE"));
      dinfo = new DataInfo(fr, 1, true, false, TransformType.STANDARDIZE, DataInfo.TransformType.NONE);
      glmt = new GLMTask.GLMIterationTask(0,null, dinfo, new GLMParams(Family.binomial),false, true, true, model.norm_beta(0), 0, 1.0/380, ModelUtils.DEFAULT_THRESHOLDS, null).doAll(dinfo._adaptedFrame);
      double [] beta2 = model.norm_beta(0);
      double [] grad2 = glmt.gradient(0,0);
      for(int i = 0; i < beta.length-1; ++i)
        Assert.assertEquals("grad[" + i + "] != 0",0,grad2[i] + betaConstraints.vec("rho").at(i) * (beta2[i] - betaConstraints.vec("beta_given").at(i) * dinfo._adaptedFrame.vec(i).sigma()),1e-8);
      Assert.assertEquals("grad[intercept] != 0",0,grad2[grad2.length-1],1e-8);
    } finally {
      fr.delete();
      if(model != null)model.delete();
    }
  }


    @Test public void testNoIntercept(){
//    Call:  glm(formula = CAPSULE ~ . - ID - 1, family = binomial, data = D)
//
//    Coefficients:
//    AGE      RACE     DPROS     DCAPS       PSA       VOL   GLEASON
//      -0.07205  -1.23262   0.47899   0.13934   0.03626  -0.01155   0.63645
//
//    Degrees of Freedom: 380 Total (i.e. Null);  373 Residual
//    Null Deviance:	    526.8
//    Residual Deviance: 399 	AIC: 413
    Key parsed = Key.make("prostate_parsed");
    Key modelKey = Key.make("prostate_model");
    GLMModel model = null;
    Frame fr = getFrameForFile(parsed, "smalldata/logreg/prostate.csv", new String[]{"ID"}, "CAPSULE");
    Key k = Key.make("rebalanced");
    H2O.submitTask(new RebalanceDataSet(fr,k,64)).join();
    fr.delete();
    fr = DKV.get(k).get();
    Frame score = null;
    try{
      // H2O differs on intercept and race, same residual deviance though
      // VOL=0.07800102000216368, AGE=0.4763358013317742, Intercept=-33.31091128704016, DCAPS=1.3862187988985282, PSA=-0.14941426719071213, DPROS=-0.47448094905875854, RACE=0.7167868302661414, GLEASON=0.013676100109077757}
      String [] cfs1 = new String [] {"RACE", "AGE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON"};
      double [] vals = new double [] { -1.23262,-0.07205, 0.47899, 0.13934, 0.03626, -0.01155, 0.63645};
      new GLM2("GLM offset test on prostate.",Key.make(),modelKey,new GLM2.Source((Frame)fr.clone(),fr.vec("CAPSULE"),false,false),Family.binomial).setRegularization(new double[]{0},new double[]{0}).doInit().fork().get(); //.setHighAccuracy().doInit().fork().get();
      model = DKV.get(modelKey).get();
      Assert.assertTrue(model.get_params().state == Job.JobState.DONE);
      testHTML(model);
      HashMap<String, Double> coefs = model.coefficients();
      for(int i = 0; i < cfs1.length; ++i)
        assertEquals(vals[i], coefs.get(cfs1[i]),1e-4);
      GLMValidation val = model.validation();
      assertEquals(526.8, model.null_validation.residualDeviance(),1e-1);
      assertEquals(399, val.residualDeviance(),1e-1);
      assertEquals(413, val.aic(),1e-1);
      // test scoring
      score = model.score((Frame)fr.clone());
      Vec mu = score.vec("1");
      final double [] exp_preds =
        new double []{ // R predictions using R model
          0.2790619,0.4983728,0.1791504,0.3179892,0.1227505,0.6407688,0.6086971,0.8692052,0.2198773,0.08973103,
          0.3612737,0.5100686,0.9109716,0.8954879,0.07149991,0.1073158,0.01838251,0.1152114,0.3904417,0.2489027,
          0.5629947,0.9801603,0.4037248,0.179598,0.459759,0.06532427,0.3314463,0.1428987,0.2182262,0.5992186,
          0.5902301,0.2907103,0.6824957,0.723047,0.3096409,0.3182108,0.4573366,0.4957492,0.6979306,0.3537596,
          0.5224244,0.1372099,0.2386254,0.3372425,0.9438167,0.9186791,0.04887559,0.5780143,0.101601,0.2495288,
          0.1152114,0.992729,0.3241788,0.9455764,0.527273,0.6789504,0.4396949,0.6118608,0.4396932,0.4433259,
          0.09217928,0.1718421,0.2733261,0.534392,0.8947366,0.5070448,0.543244,0.1760429,0.1587279,0.120139,
          0.230559,0.1838054,0.6437882,0.2357325,0.3408042,0.7405974,0.225001,0.3285307,0.2709872,0.698206,
          0.2430985,0.54366,0.5325359,0.2517555,0.20072,0.2483879,0.957223,0.9493145,0.866129,0.5205794,
          0.1206937,0.1304155,0.5742516,0.9235101,0.2142854,0.2317031,0.5402695,0.3272389,0.4129856,0.5158623,
          0.3303411,0.3651679,0.1585129,0.1237278,0.4078402,0.4843822,0.2863726,0.8078961,0.4044774,0.5935165,
          0.2365318,0.2232613,0.5775281,0.4272229,0.97787,0.9394984,0.5734764,0.5001313,0.1140847,0.7091469,
          0.2474317,0.07108103,0.4702847,0.7315436,0.5285277,0.3130729,0.3107732,0.2458944,0.1584744,0.1261198,
          0.06565271,0.3980803,0.1742766,0.6937854,0.2508427,0.3177764,0.2621678,0.9889184,0.9792494,0.3773912,
          0.1606691,0.7699755,0.3038182,0.9349492,0.222803,0.07258553,0.9597009,0.3351248,0.6378875,0.3786587,
          0.06284628,0.1737639,0.1482272,0.6689168,0.4699873,0.04251894,0.6456895,0.3105649,0.4429625,0.595572,
          0.3196979,0.5035891,0.7084547,0.6600298,0.2110469,0.5676662,0.2077393,0.2516736,0.5292617,0.777053,
          0.2858721,0.3028988,0.7719771,0.6168979,0.1803735,0.3461169,0.7885772,0.1189895,0.2998581,0.6705114,
          0.7083223,0.7471706,0.2958453,0.5998061,0.6174054,0.8464897,0.8724295,0.0529646,0.323008,0.5425115,
          0.4691805,0.9033616,0.1397801,0.1515056,0.2604321,0.5680744,0.1702089,0.2599474,0.2410981,0.4224218,
          0.3699072,0.7741795,0.352852,0.202532,0.3876063,0.5091125,0.1403465,0.3263904,0.4990924,0.3713234,
          0.2126325,0.5911457,0.9437311,0.4720828,0.387815,0.2707227,0.8353962,0.896327,0.2910632,0.1353718,
          0.5688478,0.6956094,0.09815098,0.675314,0.2265392,0.4702665,0.321468,0.5911756,0.350539,0.5475017,
          0.3069707,0.5467453,0.6713496,0.9915501,0.421299,0.2042643,0.1522847,0.2505383,0.3841292,0.0665612,
          0.1617935,0.251719,0.8010179,0.1755443,0.2864689,0.3067574,0.1087108,0.4872522,0.1974353,0.8422357,
          0.4334588,0.8472403,0.4085235,0.1092982,0.4357049,0.8977747,0.7387849,0.2449383,0.4908928,0.1334274,
          0.2282918,0.3815987,0.3493979,0.3307988,0.5747723,0.3146818,0.5184166,0.1786566,0.6330598,0.3373586,
          0.2120764,0.134929,0.9091373,0.3451438,0.142635,0.1559291,0.3735968,0.1252362,0.4867681,0.305977,
          0.7427962,0.006477887,0.06593239,0.07762176,0.5986354,0.3879587,0.4083299,0.7713339,0.2778816,0.07709849,
          0.2372032,0.1341624,0.3215959,0.814327,0.4853451,0.8217658,0.7465689,0.1396363,0.3774837,0.09754716,
          0.1782466,0.2008813,0.9958686,0.5042077,0.6177981,0.2189784,0.2797684,0.5289506,0.03569642,0.7797529,
          0.03918494,0.2265129,0.6268007,0.2234737,0.3341935,0.6285033,0.3302472,0.2205676,0.8441454,0.2983196,
          0.5755281,0.5844469,0.2310026,0.7117795,0.04170531,0.1020103,0.1554328,0.4709666,0.3739278,0.07840264,
          0.634026,0.592427,0.06120752,0.692224,0.1963099,0.5465022,0.3068802,0.868874,0.1502109,0.8650777,0.5293211,
          0.3454249,0.07389645,0.3731161,0.9075499,0.0944298,0.2188017,0.06919131,0.5516276,0.3083056,0.4818407,
          0.2932327,0.8026013,0.6212048,0.01829989,0.2865116,0.005850647,0.1678272,0.3456439,0.260818,0.2883414,
          0.2521343,0.5790858,0.6529569,0.1452642,0.2745046,0.1087368,0.546329,0.2560442,0.06902664,0.1696336,
          0.3607475,0.1879519,0.9986248,0.2345369,0.4297052,0.028796,0.2803801,0.03908738,0.1887357};
      assertEquals(exp_preds.length,mu.length());
      for(int i = 0; i < mu.length(); ++i)
        assertEquals(exp_preds[i],mu.at(i),1e-4);
      // test that it throws with categoricals
      fr.delete();
      fr = getFrameForFile(parsed, "smalldata/glm_test/prostate_cat_replaced.csv", new String[]{"ID"}, "CAPSULE");
      try {
        new GLM2("GLM offset test on prostate.", Key.make(), modelKey, new GLM2.Source(fr, fr.vec("CAPSULE"), false,false), Family.binomial).doInit().fork().get();
        assertTrue("should've thrown",false);
      } catch(IllegalArgumentException iae){}
    } finally {
      if(score != null)
        score.delete();
      fr.delete();
      if(model != null)model.delete();
    }
  }

  @Test public void testNoInterceptAndOffset(){
//    r <- glm(data=D,CAPSULE ~ . - ID - 1 - DCAPS,offset=as.numeric(DCAPS),family=binomial)
//    > r
//
//    Call:  glm(formula = CAPSULE ~ . - ID - 1 - DCAPS, family = binomial,
//      data = D, offset = as.numeric(DCAPS))
//
//    Coefficients:
//    AGE      RACE     DPROS       PSA       VOL   GLEASON
//    -0.07731  -1.34515   0.44422   0.03559  -0.01001   0.57414
//
//    Degrees of Freedom: 380 Total (i.e. Null);  374 Residual
//    Null Deviance:	    696.8
//    Residual Deviance:  402.9 	AIC: 414.9
    Key parsed = Key.make("prostate_parsed");
    Key modelKey = Key.make("prostate_model");
    GLMModel model = null;
    Frame fr = getFrameForFile(parsed, "smalldata/logreg/prostate.csv", new String[]{"ID"}, "CAPSULE");
    Key k = Key.make("rebalanced");
    H2O.submitTask(new RebalanceDataSet(fr,k,64)).join();
    fr.delete();
    fr = DKV.get(k).get();
    Frame score = null;
    try{
      // H2O differs on intercept and race, same residual deviance though
      String [] cfs1 = new String [] {"RACE", "AGE", "DPROS", "PSA", "VOL", "GLEASON"};
      double [] vals = new double [] { -1.34515,-0.07731, 0.44422, 0.03559, -0.01001, 0.57414};
      new GLM2("GLM offset test on prostate.",Key.make(),modelKey,new GLM2.Source(fr,fr.vec("CAPSULE"),false,false,fr.vec("DCAPS")),Family.binomial).setRegularization(new double[]{0},new double[]{0}).doInit().fork().get(); //.setHighAccuracy().doInit().fork().get();
      model = DKV.get(modelKey).get();
      Assert.assertTrue(model.get_params().state == Job.JobState.DONE);
      testHTML(model);
      HashMap<String, Double> coefs = model.coefficients();
      for(int i = 0; i < cfs1.length; ++i)
        assertEquals(vals[i], coefs.get(cfs1[i]),1e-4);
      GLMValidation val = model.validation();
      assertEquals(696.8, model.null_validation.residualDeviance(),1e-1);
      assertEquals(402.9, val.residualDeviance(),1e-1);
      assertEquals(414.9, val.aic(),1e-1);
      // test scoring
      score = model.score(fr);
      Vec mu = score.vec("1");
      final double [] exp_preds =
        new double []{ // R predictions using R model
          0.2714328, 0.6633844, 0.3332337, 0.261754, 0.1286857, 0.7700994, 0.7278019, 0.9234909,
          0.2111474, 0.1685586, 0.4976614, 0.6792206, 0.9448146, 0.9433877, 0.07163949, 0.1055238,
          0.01654711, 0.1167567, 0.6150695, 0.3851188, 0.5469755, 0.9775073, 0.3686457, 0.1734122,
          0.4399385, 0.0647587, 0.3155403, 0.139005, 0.2258441, 0.5898738, 0.5922247, 0.2692957,
          0.6137802, 0.8462912, 0.3058918, 0.2957983, 0.462281, 0.6524339, 0.6799241, 0.3226737,
          0.4941844, 0.1330671, 0.2254533, 0.3066927, 0.9200088, 0.958889, 0.04513577, 0.5383411,
          0.1038777, 0.2440525, 0.1092492, 0.9958668, 0.3193829, 0.9367929, 0.4897762, 0.663691,
          0.4023, 0.5698951, 0.4341724, 0.4453501, 0.1106898, 0.1682721, 0.2460871, 0.4990961,
          0.8788688, 0.4803311, 0.5465907, 0.1569607, 0.1633997, 0.1044413, 0.2212412, 0.1808158,
          0.6181282, 0.2396049, 0.3213105, 0.7081065, 0.2098653, 0.3235215, 0.2801671, 0.6652517,
          0.2194231, 0.5277527, 0.4905273, 0.2323389, 0.1850839, 0.2254083, 0.9757958, 0.9727239,
          0.8319945, 0.482306, 0.1248545, 0.1360881, 0.5320567, 0.9574291, 0.2080324, 0.2144612,
          0.4982939, 0.333785, 0.4088947, 0.4901476, 0.3223926, 0.3371016, 0.1693325, 0.1297989,
          0.3793889, 0.6960325, 0.2683522, 0.7797531, 0.3924964, 0.5530082, 0.2158664, 0.1981704,
          0.5450428, 0.4019126, 0.9884714, 0.9327079, 0.5102081, 0.495269, 0.112279, 0.6908168,
          0.2224574, 0.08042856, 0.4025428, 0.6777602, 0.4874214, 0.3087576, 0.3105473, 0.2562402,
          0.146729, 0.1274273, 0.06670393, 0.3613933, 0.1757637, 0.6709157, 0.2491046, 0.3167014,
          0.2513937, 0.9851177, 0.9885838, 0.3577745, 0.1546274, 0.72168, 0.2868293, 0.9631664,
          0.2343213, 0.06899739, 0.9769368, 0.3338782, 0.6347512, 0.3485507, 0.05771538, 0.175731,
          0.1481765, 0.6632638, 0.4730665, 0.04656982, 0.6374328, 0.2778723, 0.4285074, 0.5850365,
          0.2805779, 0.4880345, 0.6923684, 0.6431633, 0.2055755, 0.7452229, 0.2049388, 0.2320392,
          0.5034929, 0.7341303, 0.2612126, 0.2810201, 0.7449264, 0.5941076, 0.1742119, 0.3320475,
          0.7572676, 0.12218, 0.2932756, 0.6242029, 0.8294807, 0.7162603, 0.2853046, 0.5877545,
          0.6005982, 0.8300135, 0.8581161, 0.04492492, 0.5175156, 0.5203193, 0.4543528, 0.8947635,
          0.1580703, 0.149249, 0.2448207, 0.5182805, 0.1475552, 0.2579333, 0.2301171, 0.3908543,
          0.3775577, 0.73236, 0.348705, 0.191458, 0.3810784, 0.6695089, 0.1496464, 0.3539768,
          0.4774868, 0.3786629, 0.2268355, 0.5461673, 0.9277121, 0.4923828, 0.3681328,
          0.2882241, 0.8115473, 0.9391989, 0.2793372, 0.1293679, 0.5765253, 0.6672549,
          0.1085629, 0.6530963, 0.2099487, 0.4395374, 0.3355906, 0.5774188, 0.3320534,
          0.553845, 0.2571366, 0.555651, 0.8082447, 0.9878579, 0.3889303, 0.2042754,
          0.1462106, 0.2517127, 0.3565349, 0.05953082, 0.1621768, 0.2344363, 0.8734925,
          0.1734602, 0.2671484, 0.2510726, 0.1139504, 0.4709787, 0.2081558, 0.8334942,
          0.6389909, 0.8736708, 0.3707539, 0.1036151, 0.4302368, 0.9448559, 0.8462579,
          0.2411054, 0.4935917, 0.13429, 0.241161, 0.3752208, 0.3194919, 0.3351406,
          0.5469151, 0.3131954, 0.4897492, 0.1936143, 0.6055124, 0.324853, 0.2217794,
          0.1513135, 0.889535, 0.3500166, 0.1428834, 0.1542479, 0.3720363, 0.1281707, 0.4973429,
          0.3057025, 0.7167489, 0.009317823, 0.06013731, 0.07956679, 0.5774665, 0.3988433, 0.3558458,
          0.7577223, 0.2729823, 0.08260389, 0.2412571, 0.1479702, 0.3244259, 0.7868388, 0.4718041,
          0.7904578, 0.8405282, 0.1406044, 0.3987826, 0.1000184, 0.1835483, 0.1950392, 0.9977054,
          0.4791991, 0.6242925, 0.2214897, 0.273065, 0.4966417, 0.04303004, 0.7397745, 0.04042993, 0.2130047,
          0.7908355, 0.2323056, 0.3072782, 0.5756885, 0.2978859, 0.2074281, 0.8241888, 0.2913779, 0.5488127,
          0.56141, 0.2361691, 0.6870129, 0.04781581, 0.1052127, 0.1451822, 0.482763, 0.3677431, 0.08324137,
          0.7907079, 0.5541721, 0.05215705, 0.691283, 0.1813831, 0.5188524, 0.3090686, 0.9383484, 0.1454368,
          0.8514089, 0.5071669, 0.2971994, 0.08048813, 0.3643893, 0.8936522, 0.09901629, 0.2188425, 0.07477413,
          0.537254, 0.3051141, 0.4667297, 0.2651349, 0.7941239, 0.782783, 0.01870947, 0.2772949, 0.008545014,
          0.1708219, 0.3550333, 0.239455, 0.3003783, 0.2502883, 0.7356557, 0.6204092, 0.1454039, 0.2910956,
          0.09259536, 0.5196652, 0.2838468, 0.06632939, 0.1611899, 0.3349858, 0.1828278, 0.998065, 0.2499865,
          0.3866357, 0.02725202, 0.252886, 0.03562421, 0.1845115
         };
      assertEquals(exp_preds.length,mu.length());
      for(int i = 0; i < mu.length(); ++i)
        assertEquals(exp_preds[i],mu.at(i),1e-4);
      // test that it throws with categoricals
      fr.delete();
      fr = getFrameForFile(parsed, "smalldata/glm_test/prostate_cat_replaced.csv", new String[]{"ID"}, "CAPSULE");
      try {
        new GLM2("GLM offset test on prostate.", Key.make(), modelKey, new GLM2.Source(fr, fr.vec("CAPSULE"), false,false), Family.binomial).doInit().fork().get();
        assertTrue("should've thrown",false);
      } catch(IllegalArgumentException iae){}
    } finally {
      fr.delete();
      if(model != null)model.delete();
      if(score != null) score.delete();
    }
  }

  private static Frame getFrameForFile(Key outputKey, String path,String [] ignores, String response){
    File f = TestUtil.find_test_file(path);
    Key k = NFSFileVec.make(f);
    Frame fr = ParseDataset2.parse(outputKey, new Key[]{k});
    if(ignores != null)
      for(String s:ignores) UKV.remove(fr.remove(s)._key);
    // put the response to the end
    if(response != null)
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
