//package hex;
//
//import static org.junit.Assert.*;
//import hex.DGLM.GLMParams;
//import hex.DLSM.*;
//import hex.DLSM.LSMSolver;
//import hex.NewRowVecTask.DataFrame;
//import hex.glm.*;
//import hex.glm.GLMParams.Family;
//import hex.glm.GLMParams.Link;
//
//import java.io.File;
//import java.util.Arrays;
//import java.util.concurrent.ExecutionException;
//
//import org.junit.Test;
//
//import com.google.gson.JsonObject;
//
//import water.*;
//import water.TestUtil.DataExpr;
//import water.api.Constants;
//import water.deploy.Node;
//import water.deploy.NodeVM;
//import water.fvec.*;
//
//
//public class GLMTest2  extends TestUtil {
//  /**
//   * Test Gamma regression on simple and small synthetic dataset.
//   * Equation is: y = 1/(x+1);
//   * @throws ExecutionException
//   * @throws InterruptedException
//   */
//  @Test public void testGammaRegression() throws InterruptedException, ExecutionException {
//    Key raw = Key.make("gamma_test_data_raw");
//    Key parsed = Key.make("gamma_test_data_parsed");
//    Key model = Key.make("gamma_test");
//    try {
//      // make data so that the expected coefficients is icept = col[0] = 1.0
//      Key k = FVecTest.makeByteVec(raw, "x,y\n0,1\n1,0.5\n2,0.3333333\n3,0.25\n4,0.2\n5,0.1666667\n6,0.1428571\n7,0.125");
//      Frame fr = ParseDataset2.parse(parsed, new Key[]{k});
////      /public GLM2(String desc, Key dest, Frame src, Family family, Link link, double alpha, double lambda) {
//      double [] vals = new double[] {1.0,1.0};
//      //public GLM2(String desc, Key dest, Frame src, Family family, Link link, double alpha, double lambda) {
//      new GLM2("GLM test of gamma regression.",model,fr,false,Family.gamma, Family.gamma.defaultLink,0,0).fork().get();
//      GLMModel m = DKV.get(model).get();
//      for(double c:m.beta())assertEquals(1.0, c,1e-4);
//    }finally{
//      UKV.remove(raw);
//      UKV.remove(parsed);
//      UKV.remove(model);
//    }
//  }
//
//  /**
//   * Test Poisson regression on simple and small synthetic dataset.
//   * Equation is: y = exp(x+1);
//   */
//  @Test public void testPoissonRegression() throws InterruptedException, ExecutionException {
//    Key raw = Key.make("poisson_test_data_raw");
//    Key parsed = Key.make("poisson_test_data_parsed");
//    Key model = Key.make("poisson_test");
//    try {
//      // make data so that the expected coefficients is icept = col[0] = 1.0
//      Key k = FVecTest.makeByteVec(raw, "x,y\n0,2\n1,4\n2,8\n3,16\n4,32\n5,64\n6,128\n7,256");
//      Frame fr = ParseDataset2.parse(parsed, new Key[]{k});
//      new GLM2("GLM test of poisson regression.",model,fr,false,Family.poisson, Family.poisson.defaultLink,0,0).fork().get();
//      GLMModel m = DKV.get(model).get();
//      for(double c:m.beta())assertEquals(Math.log(2),c,1e-4);
//      // Test 2, example from http://www.biostat.umn.edu/~dipankar/bmtry711.11/lecture_13.pdf
//      k = FVecTest.makeByteVec(raw, "x,y\n1,0\n2,1\n3,2\n4,3\n5,1\n6,4\n7,9\n8,18\n9,23\n10,31\n11,20\n,12,25\n13,37\n14,45\n");
//      fr = ParseDataset2.parse(parsed, new Key[]{k});
//      new GLM2("GLM test of poisson regression(2).",model,fr,false,Family.poisson, Family.poisson.defaultLink,0,0).fork().get();
//      m = DKV.get(model).get();
//      assertEquals(0.3396,m.beta()[0],1e-4);
//      assertEquals(0.2565,m.beta()[1],1e-4);
//    }finally{
//      UKV.remove(raw);
//      UKV.remove(parsed);
//      UKV.remove(model);
//    }
//  }
//
//  /**
//   * Simple test for poisson, gamma and gaussian families (no regularization, test both lsm solvers).
//   * Basically tries to predict horse power based on other parameters of the cars in the dataset.
//   * Compare against the results from standard R glm implementation.
//   */
//  @Test public void testCars(){
//    Key k = loadAndParseFile("h.hex","smalldata/cars.csv");
//    try{
//      File f = new File("smalldata/cars.csv");
//      assertTrue(f.exists());
//      String [] ignores = new String[]{"name"};
//      String response = "power (hp)";
//      Key outputKey = Key.make("cars_test_model");
//      runModel(f, outputKey, response, ignores, true);
//      GLMModel m = DKV.get(outputKey).get();
//      final double [] beta = m.beta();
//      double [] vls1 = new double []{4.9504805,-0.0095859,-0.0063046,0.0004392,0.0001762,-0.0469810,0.0002891};
//      for(int i = 0; i < beta.length; ++i)
//        assertEquals(vls1[i], beta[i],1e-4);
//
//      runGLMTest(data, new ADMMSolver(0,0), new GLMParams(Family.poisson), 1, cfs1, vls1, /*5138*/ Double.NaN, 427.4, Double.NaN, 2961, Double.NaN, 1e-4,1e-1);
//      runGLMTest(data,new GeneralizedGradientSolver(0,0), new GLMParams(Family.poisson), 1,  cfs1, vls1, /*5138*/ Double.NaN, 427.4, Double.NaN, 2961, Double.NaN, 1e-2,1e-1);
//      // test gamma
//      double [] vls2 = new double []{8.992e-03,1.818e-04,-1.125e-04,1.505e-06,-1.284e-06,4.510e-04,-7.254e-05};
//      runGLMTest(data, new ADMMSolver(0,0), new GLMParams(Family.gamma), 1, cfs1, vls2, 47.79, 4.618, Double.NaN, Double.NaN, Double.NaN, 1e-4,1e-1);
//      runGLMTest(data, new GeneralizedGradientSolver(0,0), new GLMParams(Family.gamma), 1, cfs1, vls2, 47.79, 4.618, Double.NaN, Double.NaN, Double.NaN, 1e-4,1e-1);
//      // test gaussian
//      double [] vls3 = new double []{166.95862,-0.00531,-2.46690,0.12635,0.02159,-4.66995,-0.85724};
//      runGLMTest(data, new ADMMSolver(0,0), new GLMParams(Family.gaussian), 1, cfs1, vls3, /*579300*/Double.NaN, 61640, Double.NaN, 3111,Double.NaN,1e-3,5e-1);
//      // TODO: GG is producing really low-precision results here...
//      runGLMTest(data, new GeneralizedGradientSolver(0,0), new GLMParams(Family.gaussian), 1, cfs1, vls3, Double.NaN, Double.NaN, Double.NaN, 3111, Double.NaN,5e-1,5e-1);
//    } finally {
//      UKV.remove(k);
//    }
//  }
//
//  /**
//   * Simple test for binomial family (no regularization, test both lsm solvers).
//   * Runs the classical prostate, using dataset with race replaced by categoricals (probably as it's supposed to be?), in any case,
//   * it gets to test correct processing of categoricals.
//   *
//   * Compare against the results from standard R glm implementation.
//   */
//  @Test public void testProstate(){
//    Key k = loadAndParseFile("h.hex","smalldata/glm_test/prostate_cat_replaced.csv");
//    try{
//      ValueArray ary = DKV.get(k).get();
//      // R results
//      //(Intercept)       AGE       RACER2       RACER3        DPROS        DCAPS          PSA          VOL      GLEASON
//      // -8.14867     -0.01368      0.32337     -0.38028      0.55964      0.49548      0.02794     -0.01104      0.97704
//      String [] cfs1 = new String [] {"Intercept","AGE", "RACE.R2","RACE.R3", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON"};
//      double [] vals = new double [] {-8.14867, -0.01368, 0.32337, -0.38028, 0.55964, 0.49548, 0.02794, -0.01104, 0.97704};
//      int [] cols = ary.getColumnIds(new String[]{"AGE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON","RACE","CAPSULE"});
//      DataFrame data = DGLM.getData(ary, cols, null, true);
//      runGLMTest(data, new ADMMSolver(0,0), new GLMParams(Family.binomial), 1,  cfs1, vals, 512.3, 378.3, Double.NaN, 396.3 , Double.NaN,1e-3,5e-1);
//      runGLMTest(data, new GeneralizedGradientSolver(0,0), new GLMParams(Family.binomial), 1,  cfs1, vals, 512.3, 378.3, Double.NaN, 396.3 , Double.NaN,1e-1,5e-1);
//    } finally {
//      UKV.remove(k);
//    }
//  }
//
//  // ---
//  // Test GLM on a simple dataset that has an easy Linear Regression.
//  @Test public void testLinearRegression() {
//    Key datakey = Key.make("datakey");
//    try {
//      // Make some data to test with.
//      // Equation is: y = 0.1*x+0
//      ValueArray va =
//        va_maker(datakey,
//                 new byte []{  0 ,  1 ,  2 ,  3 ,  4 ,  5 ,  6 ,  7 ,  8 ,  9 },
//                 new float[]{0.0f,0.1f,0.2f,0.3f,0.4f,0.5f,0.6f,0.7f,0.8f,0.9f});
//      // Compute LinearRegression between columns 0 & 1
//      JsonObject lr = LinearRegression.run(va,0,1);
//      assertEquals( 0.0, lr.get("Beta0"   ).getAsDouble(), 0.000001);
//      assertEquals( 0.1, lr.get("Beta1"   ).getAsDouble(), 0.000001);
//      assertEquals( 1.0, lr.get("RSquared").getAsDouble(), 0.000001);
//      LSMSolver lsms = new ADMMSolver(0,0);
//      JsonObject glm = computeGLM(Family.gaussian,lsms,va,null); // Solve it!
//      JsonObject coefs = glm.get("coefficients").getAsJsonObject();
//      assertEquals( 0.0, coefs.get("Intercept").getAsDouble(), 0.000001);
//      assertEquals( 0.1, coefs.get("0")        .getAsDouble(), 0.000001);
//      UKV.remove(Key.make(glm.get(Constants.MODEL_KEY).getAsString()));
//    } finally {
//      UKV.remove(datakey);
//    }
//  }
//
//
//  // simple tweedie test
//  @Test public void testTweedieRegression() {
//    Key datakey = Key.make("datakey");
//    try {
//      // Make some data to test with.
//      // Equation is: y = 0.1*x+0
//      ValueArray va =
//        va_maker(datakey,
//                 new byte []{  0 ,  1 ,  2 ,  3 ,  4 ,  5 ,  6 ,  7 ,  8 ,  9,  0 ,  1 ,  2 ,  3 ,  4 ,  5 ,  6 ,  7 ,  8 ,  9 },
//                 new float[]{0.0f,0.1f,0.2f,0.3f,0.4f,0.5f,0.6f,0.7f,0.8f,0.9f, 0f, 0f, 0f, 0f, 0f, 0f, 0f, 0f, 0f, 0f});
//      LSMSolver lsms = new ADMMSolver(0,0);
//      double[] var_powers = new double[]{ 1.5,    1.1,     1.9, };
//      double[] beta0s = new double[]{     3.643,  1.318,   9.154,};
//      double[] beta1s = new double[]{    -0.260, -0.0284, -0.853,};
//      for(int test=0; test < var_powers.length; test++){
//        Family family = Family.tweedie;
//        family.tweedieVariancePower = var_powers[ test ];
//        family.defaultLink = Link.tweedie;
//        family.defaultLink.tweedieLinkPower = 1. - var_powers[ test ];
//        JsonObject glm = computeGLM(family, lsms, va, null); // Solve it!
//        JsonObject coefs = glm.get("coefficients").getAsJsonObject();
//        assertEquals( "tweedie test variance power = " + var_powers[ test ], beta0s[ test ], coefs.get("Intercept").getAsDouble(), 0.001);
//        assertEquals( "tweedie test variance power = " + var_powers[ test ], beta1s[ test ], coefs.get("0")        .getAsDouble(), 0.001);
//        UKV.remove(Key.make(glm.get(Constants.MODEL_KEY).getAsString()));
//      }
//    } finally {
//      UKV.remove(datakey);
//    }
//  }
//
//
//  // Now try with a more complex binomial regression
//  @Test public void testLogReg_Basic() {
//    Key datakey = Key.make("datakey");
//    try {
//      // Make some data to test with.  2 columns, all numbers from 0-9
//      ValueArray va = va_maker(datakey,2,10, new DataExpr() {
//         public double expr( byte[] x ) { return 1.0/(1.0+Math.exp(-(0.1*x[0]+0.3*x[1]-2.5))); } } );
//
//      LSMSolver lsms = new ADMMSolver(0,0); // Default normalization of NONE
//      JsonObject glm = computeGLMlog(lsms,va); // Solve it!
//      JsonObject coefs = glm.get("coefficients").getAsJsonObject();
//      assertEquals(-2.5, coefs.get("Intercept").getAsDouble(), 0.000001);
//      assertEquals( 0.1, coefs.get("0")        .getAsDouble(), 0.000001);
//      assertEquals( 0.3, coefs.get("1")        .getAsDouble(), 0.000001);
//      UKV.remove(Key.make(glm.get(Constants.MODEL_KEY).getAsString()));
//
//    } finally {
//      UKV.remove(datakey);
//    }
//  }
//
//
//
//  private static void runModel(File f, Key outputKey, String response, Family fam,String [] ignores, boolean standardize){
//    Key k = NFSFileVec.make(f);
//    Key fk = Key.make(outputKey + "_data");
//    Frame fr = ParseDataset2.parse(fk, new Key[]{k});
////    if(outputKey.equals("airlines")){
////      String [] names2keep = new String[]{"DepTime","Distance","Origin","Dest","IsArrDelayed"};
////      Vec [] vecs = new Vec[names2keep.length];
////      for(int i = 0; i < names2keep.length; ++i)
////        vecs[i] = fr.remove(names2keep[i]);
////      DKV.put(Key.make("airlines_data_reduced"), new Frame(names2keep,vecs));
////    }
//    for(String c:ignores)fr.remove(c);
//    fr.add(response, fr.remove(response));
//    new GLM2("glm", outputKey, fr, standardize, Family.binomial, Link.logit,0.5,1e-3).run();
//  }
//  public static void main(String [] args) throws Exception{
//    System.out.println("Running ParserTest2");
//    final int nnodes = 1;
//    for( int i = 1; i < nnodes; i++ ) {
//      Node n = new NodeVM(args);
//      n.inheritIO();
//      n.start();
//    }
//    H2O.waitForCloudSize(nnodes);
//    System.out.println("Running...");
////    File f = new File("/Users/tomasnykodym/h2o/smalldata/logreg/prostate.csv");
////    File f = new File("smalldata/airlines/allyears2k_headers.csv");
////    String response = "IsArrDelayed";
////    String [] ignores = new String []{"ArrTime","ActualElapsedTime","ArrDelay","DepDelay","TailNum","AirTime","TaxiIn","TaxiOut","CancellationCode","CarrierDelay","WeatherDelay","NASDelay","SecurityDelay","LateAircraftDelay","IsDepDelayed"};
////    runModel(f, Key.make("airlines"), response, ignores,true);
////    f = new File("smalldata/logreg/prostate.csv");
////    ignores = new String[]{};
////    response = "CAPSULE";
////    runModel(f, Key.make("prostate"), response, ignores,true);
////    f = new File("/Users/tomasnykodym/Downloads/140k_train_anonymised.csv");
////    ignores = new String[]{"Choicepoint_Cx","Choicepoint_pass","Has_bnk_AC"};
////    response = "Converted";
////    runModel(f, Key.make("rushcard"), response, ignores,true);
//    System.out.println("DONE!");
//  }
//}
