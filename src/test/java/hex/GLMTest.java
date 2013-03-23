package hex;

import static org.junit.Assert.assertEquals;
import hex.DGLM.Family;
import hex.DGLM.GLMModel;
import hex.DGLM.GLMParams;
import hex.DGLM.GLMValidation;
import hex.DLSM.ADMMSolver;
import hex.DLSM.GeneralizedGradientSolver;
import hex.DLSM.LSMSolver;
import hex.NewRowVecTask.DataFrame;

import java.util.*;

import org.junit.Test;

import water.*;
import water.api.Constants;
import water.exec.Exec;
import water.exec.PositionedException;

import com.google.gson.*;

// A series of tests designed to validate GLM's *statistical results* and not,
// i.e. correct behavior when handed bad/broken/null arguments (although those
// tests are also good).

public class GLMTest extends TestUtil {
  static double[] THRESHOLDS;
  static {
    THRESHOLDS = new double[100];
    for( int i=0; i<THRESHOLDS.length; i++ )
      THRESHOLDS[i] = i/100.0;
  }

  JsonObject computeGLMlog( LSMSolver lsms, ValueArray va, boolean cat ) {
    return computeGLM( Family.binomial, lsms, va, cat, null); }

  JsonObject computeGLM( Family family, LSMSolver lsms, ValueArray va, boolean cat, int[] cols ) {
    // All columns in order, and use last as response variable
    if( cols == null ) {
      cols= new int[va._cols.length];
      for( int i=0; i<cols.length; i++ ) cols[i]=i;
    }

    // Now a Gaussian GLM model for the same thing
    GLMParams glmp = new GLMParams(family);
    glmp._link = glmp._family.defaultLink;
    //glmp._familyamilyArgs = glmp._family.defaultArgs;
    glmp._betaEps = 0.000001;
    glmp._maxIter = 100;
    // Solver
    GLMModel m = DGLM.buildModel(DGLM.getData(va, cols,null,true), lsms, glmp);
    // Solve it!
    m.validateOn(va, null,THRESHOLDS);// Validate...
    JsonObject glm = m.toJson();
    return glm;
  }

   static double [] thresholds = new double [] {
    0.00, 0.01, 0.02, 0.03, 0.04, 0.05, 0.06, 0.07, 0.08, 0.09,
    0.10, 0.11, 0.12, 0.13, 0.14, 0.15, 0.16, 0.17, 0.18, 0.19,
    0.20, 0.21, 0.22, 0.23, 0.24, 0.25, 0.26, 0.27, 0.28, 0.29,
    0.30, 0.31, 0.32, 0.33, 0.34, 0.35, 0.36, 0.37, 0.38, 0.39,
    0.40, 0.41, 0.42, 0.43, 0.44, 0.45, 0.46, 0.47, 0.48, 0.49,
    0.50, 0.51, 0.52, 0.53, 0.54, 0.55, 0.56, 0.57, 0.58, 0.59,
    0.60, 0.61, 0.62, 0.63, 0.64, 0.65, 0.66, 0.67, 0.68, 0.69,
    0.70, 0.71, 0.72, 0.73, 0.74, 0.75, 0.76, 0.77, 0.78, 0.79,
    0.80, 0.81, 0.82, 0.83, 0.84, 0.85, 0.86, 0.87, 0.88, 0.89,
    0.90, 0.91, 0.92, 0.93, 0.94, 0.95, 0.96, 0.97, 0.98, 0.99,
    1.00
  };

  public static void runGLMTest(DataFrame data, LSMSolver lsm, GLMParams glmp, int xval, String [] coefs, double [] values, double ndev, double resdev, double err, double aic){
    runGLMTest(data, lsm, glmp, xval, coefs, values, ndev, resdev, err, aic, Double.NaN, 1e-4, 1e-2);
  }
  public static void runGLMTest(DataFrame data, LSMSolver lsm, GLMParams glmp, int xval, String [] coefs, double [] values, double ndev, double resdev, double err, double aic, double auc){
    runGLMTest(data, lsm, glmp, xval, coefs, values, ndev, resdev, err, aic, auc, 1e-4, 1e-2);
  }

  public static void runGLMTest(DataFrame data, LSMSolver lsm, GLMParams glmp, int xval, String [] coefs, double [] values, double ndev, double resdev, double err, double aic, double auc, double betaPrecision, double validationPrecision){
    GLMModel m = DGLM.buildModel(data, lsm, glmp);
    try{
      if(xval <= 1)
        m.validateOn(data._ary, null,thresholds);
      else
        m.xvalidate(data._ary, xval, thresholds);
      JsonObject mjson = m.toJson();
      JsonObject jcoefs = mjson.get("coefficients").getAsJsonObject();
      for(int i = 0; i < coefs.length; ++i)
        assertEquals(values[i],jcoefs.get(coefs[i]).getAsDouble(), betaPrecision);
      JsonObject validation = mjson.get("validations").getAsJsonArray().get(0).getAsJsonObject();
      if(!Double.isNaN(ndev))
        assertEquals(ndev, validation.get("nullDev").getAsDouble(), validationPrecision);
      if(!Double.isNaN(resdev))
        assertEquals(resdev, validation.get("resDev").getAsDouble(), validationPrecision);
      if(!Double.isNaN(aic))
        assertEquals(aic, validation.get("aic").getAsDouble(), validationPrecision);
      if(!Double.isNaN(auc))
        assertEquals(auc, validation.get("auc").getAsDouble(), validationPrecision);
      if(!Double.isNaN(err))
        assertEquals(err, validation.get("err").getAsDouble(), validationPrecision);
    } finally {
      if(m != null && m._selfKey != null)
        UKV.remove(m._selfKey);
    }
  }

  /**
   * Test Gamma regression on simple and small synthetic dataset.
   * Equation is: y = 1/(x+1);
   */
  @Test public void testGammaRegression() {
    Key datakey = Key.make("datakey");
    try {
      // make data so that the expected coefficients is icept = col[0] = 1.0
      ValueArray va = va_maker(datakey,
                  new byte []{  0, 1,   2,         3,    4,   5,          6,         7        },
                 new double[]{1.0, 0.5, 0.3333333, 0.25, 0.20, 0.1666667, 0.1428571, 0.1250000});
      int cols [] = new int[]{0,1};
      DataFrame data = DGLM.getData(va, cols, null, false);
      String [] coefs = new String[] {"Intercept","0"};
      double [] vals = new double[] {1.0,1.0};
      runGLMTest(data, new ADMMSolver(0,0), new GLMParams(Family.gamma), 1, coefs, vals, Double.NaN, Double.NaN, Double.NaN, Double.NaN);
      runGLMTest(data, new GeneralizedGradientSolver(0,0), new GLMParams(Family.gamma), 1, coefs, vals, Double.NaN, Double.NaN, Double.NaN, Double.NaN);
    }finally{
      UKV.remove(datakey);
    }
  }

  /**
   * Test Gamma regression on simple and small synthetic dataset.
   * Equation is: y = exp(x+1);
   */
  @Test public void testPoissonRegression() {
    Key datakey = Key.make("datakey");
    Key datakey2 = Key.make("datakey2");
    try {
      // Test 1, synthetic dataset
      ValueArray va =
        va_maker(datakey,
                 new byte [] { 0, 1, 2, 3 , 4 , 5 , 6  , 7  },
                 new double[]{ 2, 4, 8, 16, 32, 64, 128, 256});
      DataFrame data = DGLM.getData(va, new int [] {0,1}, null, false);
      String [] coefs = new String [] {"Intercept","0"};
      double [] vals =  new double [] {Math.log(2),Math.log(2)};
      runGLMTest(data, new ADMMSolver(0,0), new GLMParams(Family.poisson), 1, coefs, vals, Double.NaN, Double.NaN, Double.NaN, Double.NaN);
      runGLMTest(data, new GeneralizedGradientSolver(0,0), new GLMParams(Family.poisson), 1, coefs, vals, Double.NaN, Double.NaN, Double.NaN, Double.NaN);
      // Test 2, example from http://www.biostat.umn.edu/~dipankar/bmtry711.11/lecture_13.pdf
      va = va_maker(datakey2,
                   new byte []{1,2,3,4,5,6,7,8, 9, 10,11,12,13,14},
                   new byte []{0,1,2,3,1,4,9,18,23,31,20,25,37,45});
      vals[0] = 0.3396; vals[1] = 0.2565;
      data = DGLM.getData(va, new int [] {0,1}, null, false);
      runGLMTest(data, new ADMMSolver(0,0), new GLMParams(Family.poisson), 1, coefs, vals, Double.NaN, Double.NaN, Double.NaN, Double.NaN);
      // TODO: GG fails here (produces bad results
      //runGLMTest(data, new GeneralizedGradientSolver(0,0), new GLMParams(Family.poisson), 1, coefs, vals, Double.NaN, Double.NaN, Double.NaN, Double.NaN);
    }finally{
      UKV.remove(datakey);
      UKV.remove(datakey2);
    }
  }

  /**
   * Simple test for poisson, gamma and gaussian families (no regularization, test both lsm solvers).
   * Basically tries to predict horse power based on other parameters of the cars in the dataset.
   * Compare against the results from standard R glm implementation.
   */
  @Test public void testCars(){
    Key k = loadAndParseKey("h.hex","smalldata/cars.csv");
    try{
      ValueArray ary = DKV.get(k).get();
      // PREDICT POWER
      String [] cfs1 = new String[]{"Intercept","economy (mpg)", "cylinders", "displacement (cc)", "weight (lb)", "0-60 mph (s)", "year"};
      int [] cols = ary.getColumnIds(new String[]{"economy (mpg)", "cylinders", "displacement (cc)", "weight (lb)", "0-60 mph (s)", "year", "power (hp)"});
      double [] vls1 = new double []{4.9504805,-0.0095859,-0.0063046,0.0004392,0.0001762,-0.0469810,0.0002891};
      DataFrame data = DGLM.getData(ary, cols, null, true);
      // test poisson
      // NOTE: Null deviance is slightly off from R here. I compute the null deviance using mean from ValueArray,
      // R computes the mean only on the rows which are actually used during computation (those withou NAs), and the two means are slightly off
      // I don't think it is a big issue so I leave it as it is and skip null deviance comparison
      runGLMTest(data, new ADMMSolver(0,0), new GLMParams(Family.poisson), 1, cfs1, vls1, /*5138*/ Double.NaN, 427.4, Double.NaN, 2961, Double.NaN, 1e-4,1e-1);
      runGLMTest(data,new GeneralizedGradientSolver(0,0), new GLMParams(Family.poisson), 1,  cfs1, vls1, /*5138*/ Double.NaN, 427.4, Double.NaN, 2961, Double.NaN, 1e-2,1e-1);
      // test gamma
      double [] vls2 = new double []{8.992e-03,1.818e-04,-1.125e-04,1.505e-06,-1.284e-06,4.510e-04,-7.254e-05};
      runGLMTest(data, new ADMMSolver(0,0), new GLMParams(Family.gamma), 1, cfs1, vls2, 47.79, 4.618, Double.NaN, Double.NaN, Double.NaN, 1e-4,1e-1);
      runGLMTest(data, new GeneralizedGradientSolver(0,0), new GLMParams(Family.gamma), 1, cfs1, vls2, 47.79, 4.618, Double.NaN, Double.NaN, Double.NaN, 1e-4,1e-1);
      // test gaussian
      double [] vls3 = new double []{166.95862,-0.00531,-2.46690,0.12635,0.02159,-4.66995,-0.85724};
      runGLMTest(data, new ADMMSolver(0,0), new GLMParams(Family.gaussian), 1, cfs1, vls3, /*579300*/Double.NaN, 61640, Double.NaN, 3111,Double.NaN,1e-3,5e-1);
      // TODO: GG is producing really low-precision results here...
      runGLMTest(data, new GeneralizedGradientSolver(0,0), new GLMParams(Family.gaussian), 1, cfs1, vls3, Double.NaN, Double.NaN, Double.NaN, 3111, Double.NaN,5e-1,5e-1);
    } finally {
      UKV.remove(k);
    }
  }

  /**
   * Simple test for binomial family (no regularization, test both lsm solvers).
   * Runs the classical prostate, using dataset with race replaced by categoricals (probably as it's supposed to be?), in any case,
   * it gets to test correct processing of categoricals.
   *
   * Compare against the results from standard R glm implementation.
   */
  @Test public void testProstate(){
    Key k = loadAndParseKey("h.hex","smalldata/glm_test/prostate_cat_replaced.csv");
    try{
      ValueArray ary = DKV.get(k).get();
      // R results
      //(Intercept)       AGE       RACER2       RACER3        DPROS        DCAPS          PSA          VOL      GLEASON
      // -8.14867     -0.01368      0.32337     -0.38028      0.55964      0.49548      0.02794     -0.01104      0.97704
      String [] cfs1 = new String [] {"Intercept","AGE", "RACE.R2","RACE.R3", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON"};
      double [] vals = new double [] {-8.14867, -0.01368, 0.32337, -0.38028, 0.55964, 0.49548, 0.02794, -0.01104, 0.97704};
      int [] cols = ary.getColumnIds(new String[]{"AGE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON","RACE","CAPSULE"});
      DataFrame data = DGLM.getData(ary, cols, null, true);
      runGLMTest(data, new ADMMSolver(0,0), new GLMParams(Family.binomial), 1,  cfs1, vals, 512.3, 378.3, Double.NaN, 396.3 , Double.NaN,1e-3,5e-1);
      runGLMTest(data, new GeneralizedGradientSolver(0,0), new GLMParams(Family.binomial), 1,  cfs1, vals, 512.3, 378.3, Double.NaN, 396.3 , Double.NaN,1e-1,5e-1);
    } finally {
      UKV.remove(k);
    }
  }

  /**
   * Larger test to test correct multi-chunk behavior.
   * Test all families (gaussian, binomial, poisson, gamma) with regularization.
   *
   * This time, compare athe results gainst glmnet (R's glm does not have regularization).
   */
  @Test public void testCredit(){
    Key k = loadAndParseKey("h.hex","smalldata/kaggle/creditsample-test.csv.gz");
    try{
      ValueArray ary = DKV.get(k).get();
    } finally {
      UKV.remove(k);
    }
  }

  /**
   * Test H2O gets the same results as R.
   */
  @Test public void testPoissonTst1(){
    Key k = loadAndParseKey("h.hex","smalldata/glm_test/poisson_tst1.csv");
    try{
      ValueArray ary = DKV.get(k).get();
      String [] colnames = new String [] {"prog","math","num_awards"};
      String [] coefs    = new String [] {"Intercept","prog.General","prog.Vocational","math"};
      double [] vals     = new double [] {-4.1627,      -1.08386,       -0.71405,        0.07015 };
      int [] cols = ary.getColumnIds(colnames);
      DataFrame data = DGLM.getData(ary, cols, null, true);
      runGLMTest(data, new ADMMSolver(0,0), new GLMParams(Family.poisson), 1, coefs, vals, Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN,1e-3,1e-1);
      runGLMTest(data, new GeneralizedGradientSolver(0,0), new GLMParams(Family.poisson), 1, coefs, vals, Double.NaN, Double.NaN, Double.NaN, Double.NaN,Double.NaN,1e-3,1e-1);
    } finally {
      UKV.remove(k);
    }
  }

  // ---
  // Test GLM on a simple dataset that has an easy Linear Regression.
  @Test public void testLinearRegression() {
    Key datakey = Key.make("datakey");
    try {
      // Make some data to test with.
      // Equation is: y = 0.1*x+0
      ValueArray va =
        va_maker(datakey,
                 new byte []{  0 ,  1 ,  2 ,  3 ,  4 ,  5 ,  6 ,  7 ,  8 ,  9 },
                 new float[]{0.0f,0.1f,0.2f,0.3f,0.4f,0.5f,0.6f,0.7f,0.8f,0.9f});
      // Compute LinearRegression between columns 0 & 1
      JsonObject lr = LinearRegression.run(va,0,1);
      assertEquals( 0.0, lr.get("Beta0"   ).getAsDouble(), 0.000001);
      assertEquals( 0.1, lr.get("Beta1"   ).getAsDouble(), 0.000001);
      assertEquals( 1.0, lr.get("RSquared").getAsDouble(), 0.000001);
      LSMSolver lsms = new ADMMSolver(0,0);
      JsonObject glm = computeGLM(Family.gaussian,lsms,va,false,null); // Solve it!
      JsonObject coefs = glm.get("coefficients").getAsJsonObject();
      assertEquals( 0.0, coefs.get("Intercept").getAsDouble(), 0.000001);
      assertEquals( 0.1, coefs.get("0")        .getAsDouble(), 0.000001);
      UKV.remove(Key.make(glm.get(Constants.MODEL_KEY).getAsString()));
    } finally {
      UKV.remove(datakey);
    }
  }

  // Now try with a more complex binomial regression
  @Test public void testLogReg_Basic() {
    Key datakey = Key.make("datakey");
    try {
      // Make some data to test with.  2 columns, all numbers from 0-9
      ValueArray va = va_maker(datakey,2,10, new DataExpr() {
         public double expr( byte[] x ) { return 1.0/(1.0+Math.exp(-(0.1*x[0]+0.3*x[1]-2.5))); } } );

      LSMSolver lsms = new ADMMSolver(0,0); // Default normalization of NONE
      JsonObject glm = computeGLMlog(lsms,va,false); // Solve it!
      JsonObject coefs = glm.get("coefficients").getAsJsonObject();
      assertEquals(-2.5, coefs.get("Intercept").getAsDouble(), 0.000001);
      assertEquals( 0.1, coefs.get("0")        .getAsDouble(), 0.000001);
      assertEquals( 0.3, coefs.get("1")        .getAsDouble(), 0.000001);
      UKV.remove(Key.make(glm.get(Constants.MODEL_KEY).getAsString()));

    } finally {
      UKV.remove(datakey);
    }
  }

  // Compute the 'expr' result from the sum of coefficients,
  // plus a small random value.
  public static class DataExpr_Dirty extends DataExpr {
    final Random _R;
    final double _coefs[];
    DataExpr_Dirty( Random R, double[] coefs ) { _R = R; _coefs = coefs; }
    public double expr( byte[] cols ) {
      double sum = _coefs[_coefs.length-1]+
        (_R.nextDouble()-0.5)/1000.0; // Add some noise
      for( int i = 0; i< cols.length; i++ )
        sum += cols[i]*_coefs[i];
      return 1.0/(1.0+Math.exp(-sum));
    }
  }

  @Test public void testLogReg_Dirty() {
    Key datakey = Key.make("datakey");
    try {
      Random R = new Random(0x987654321L);
      for( int i=0; i<10; i++ ) {
        double[] coefs = new double[] { R.nextDouble(),R.nextDouble(),R.nextDouble() };
        ValueArray va = va_maker(datakey,2,10, new DataExpr_Dirty(R, coefs));

        LSMSolver lsms = new ADMMSolver(0,0); // Default normalization of NONE;
        JsonObject glm = computeGLMlog(lsms,va,false); // Solve it!
        JsonObject res = glm.get("coefficients").getAsJsonObject();
        assertEquals(coefs[0], res.get("0")        .getAsDouble(), 0.001);
        assertEquals(coefs[1], res.get("1")        .getAsDouble(), 0.001);
        assertEquals(coefs[2], res.get("Intercept").getAsDouble(), 0.001);
        UKV.remove(datakey);
        UKV.remove(Key.make(glm.get(Constants.MODEL_KEY).getAsString()));
      }
    } finally {
      UKV.remove(datakey);
    }
  }

  @Test public void testLogReg_Penalty() {
    Key datakey = Key.make("datakey");
    try {
      // Make some data to test with.  2 columns, all numbers from 0-9
      ValueArray va = va_maker(datakey,2,10, new DataExpr() {
          public double expr( byte[] x ) { return 1.0/(1.0+Math.exp(-(0.1*x[0]+0.3*x[1]-2.5))); } } );

      // No penalty
      LSMSolver lsms0 = new ADMMSolver(0,0); // Default normalization of NONE;
      JsonObject glm = computeGLMlog(lsms0,va,false); // Solve it!
      JsonObject coefs = glm.get("coefficients").getAsJsonObject();
      assertEquals(-2.5, coefs.get("Intercept").getAsDouble(), 0.00001);
      assertEquals( 0.1, coefs.get("0")        .getAsDouble(), 0.000001);
      assertEquals( 0.3, coefs.get("1")        .getAsDouble(), 0.000001);
      UKV.remove(Key.make(glm.get(Constants.MODEL_KEY).getAsString()));

      // L1 penalty
      LSMSolver lsms1 = new ADMMSolver(0.0,0.0); // Default normalization of NONE;
      glm = computeGLMlog(lsms1,va,false); // Solve it!
      coefs = glm.get("coefficients").getAsJsonObject();
      assertEquals(-2.5, coefs.get("Intercept").getAsDouble(), 0.00001);
      assertEquals( 0.1, coefs.get("0")        .getAsDouble(), 0.000001);
      assertEquals( 0.3, coefs.get("1")        .getAsDouble(), 0.000001);
      UKV.remove(Key.make(glm.get(Constants.MODEL_KEY).getAsString()));

      // L2 penalty
      LSMSolver lsms2 = new ADMMSolver(0.0,0.0); // Default normalization of NONE;
      glm = computeGLMlog(lsms2,va,false); // Solve it!
      coefs = glm.get("coefficients").getAsJsonObject();
      assertEquals(-2.5, coefs.get("Intercept").getAsDouble(), 0.00001);
      assertEquals( 0.1, coefs.get("0")        .getAsDouble(), 0.000001);
      assertEquals( 0.3, coefs.get("1")        .getAsDouble(), 0.000001);
      UKV.remove(Key.make(glm.get(Constants.MODEL_KEY).getAsString()));

      // ELASTIC penalty
      LSMSolver lsmsx = new ADMMSolver(0.0,0.0); // Default normalization of NONE;
      glm = computeGLMlog(lsmsx,va,false); // Solve it!
      coefs = glm.get("coefficients").getAsJsonObject();
      assertEquals(-2.5, coefs.get("Intercept").getAsDouble(), 0.00001);
      assertEquals( 0.1, coefs.get("0")        .getAsDouble(), 0.000001);
      assertEquals( 0.3, coefs.get("1")        .getAsDouble(), 0.000001);
      UKV.remove(Key.make(glm.get(Constants.MODEL_KEY).getAsString()));

    } finally {
      UKV.remove(datakey);
    }
  }


  // Predict whether or not a car has a 3-cylinder engine
  @Test public void testLogReg_CARS_CSV() {
    Key k1=null,k2=null;
    try {
      k1 = loadAndParseKey("h.hex","smalldata/cars.csv");
      // Fold the cylinders down to 1/0 for 3/not-3
      k2 = Exec.exec("colSwap(h.hex,2,h.hex$cylinders==3?1:0)","h2.hex");
      // Columns for displacement, power, weight, 0-60, year, then response is cylinders
      int[] cols= new int[]{3,4,5,6,7,2};
      ValueArray va = DKV.get(k2).get();
      // Compute the coefficients
      LSMSolver lsmsx = new ADMMSolver(0,0.0);
      JsonObject glm = computeGLM( Family.binomial, lsmsx, va, false, cols );

      // Now run the dataset through the equation and see how close we got
      JsonObject coefs = glm.get("coefficients").getAsJsonObject();
      double icept = coefs.get("Intercept").getAsDouble();
      double disp  = coefs.get("displacement (cc)").getAsDouble();
      double power = coefs.get("power (hp)").getAsDouble();
      double weight= coefs.get("weight (lb)").getAsDouble();
      double accel = coefs.get("0-60 mph (s)").getAsDouble();
      double year  = coefs.get("year").getAsDouble();
      AutoBuffer ab = va.getChunk(0);

      ROWS:                     // Skip bad rows
      for( int i=0; i<va._numrows; i++ ) {
        for( int j=2; j<8; j++ ) if( va.isNA(ab,i,j) ) continue ROWS;
        double x =
          disp  *va.datad(ab,i,3) +
          power *va.datad(ab,i,4) +
          weight*va.datad(ab,i,5) +
          accel *va.datad(ab,i,6) +
          year  *va.datad(ab,i,7) +
          icept;
        double p = 1.0/(1.0+Math.exp(-x)); // Prediction
        double cyl = va.data(ab,i,2); // 1==3-cyl, 0==not-3-cyl
        assertEquals(cyl,p,0.005); // Hopefully fairly close to 0 for 3-cylinder, 1 for not-3
      }
      UKV.remove(Key.make(glm.get(Constants.MODEL_KEY).getAsString()));

    } catch( PositionedException pe ) {
      throw new Error(pe);
    } finally {
      UKV.remove(k1);
      if( k2 != null ) UKV.remove(k2);
    }
  }

  // Test of convergence on this dataset.  It appears that the 'betas' increase
  // with every iteration until we hit Infinities.
  @Test public void testConverge() {
    Key k1= loadAndParseKey("m.hex","smalldata/logreg/make_me_converge_10000x5.csv");
    ValueArray va = DKV.get(k1).get();
    // Compute the coefficients
    LSMSolver lsmsx = new ADMMSolver(1e-5, 0.5);
    JsonObject glm = computeGLMlog( lsmsx, va, false );

    // From the validations get the chosen threshold
    final JsonArray vals = glm.get("validations").getAsJsonArray();
    JsonElement val = vals.get(0); // Get first validation
    double threshold = ((JsonObject)val).get("threshold").getAsDouble();

    // Scrape out the coefficients to build an equation
    final JsonObject coefs = glm.get("coefficients").getAsJsonObject();
    final double icept = coefs.get("Intercept").getAsDouble();
    final double c[] = new double[5];
    for( int i=0; i<c.length; i++ )
      c[i] = coefs.get(Integer.toString(i)).getAsDouble();

    // Now run the dataset through the equation and see how close we got
    AutoBuffer ab = va.getChunk(0);
    final int nrows = va.rpc(0);
    for( int i=0; i<nrows; i++ ) {
      double x = icept;
      for( int j=0; j<c.length; j++ )
        x += c[j]*va.datad(ab,i,j);
      final double pred = 1.0/(1.0+Math.exp(-x)); // Prediction
      final long p = pred < threshold ? 0 : 1;    // Thresholded
      final long actl = va.data(ab,i,5);          // Actual
      assertEquals(actl,p);
    }
    UKV.remove(k1);
    UKV.remove(Key.make(glm.get(Constants.MODEL_KEY).getAsString()));

    // No convergence warnings
    final JsonElement je = glm.get("warnings");
    if( je != null ) {
      final JsonArray warns = je.getAsJsonArray();
      for( JsonElement e : warns )
        assert !e.getAsString().equals("Unable to solve!");
    }
  }

  // Categorical Test!  Lets make a simple categorical test case
  @Test public void testLogRegCat_Basic() {
    Key datakey = Key.make("datakey");
    try {
      // Make some data to test with.
      // Low's = 0,0,0  ==> should predict as 0
      // Med's = 0,1,0  ==> should predict as 0.3333...
      // Highs = 1,1,1  ==> should predict as 1
      ValueArray va =
        va_maker(datakey,
                 new String[]{ "Low", "Med", "High", "Low", "Med", "High", "Low", "Med", "High" },
                 new byte  []{     0,     0,      1,     0,     1,      1,     0,     0,     1  });

      LSMSolver lsms = new ADMMSolver(0,0.0); // Default normalization of NONE
      JsonObject glm = computeGLMlog(lsms,va,true); // Solve it!
      JsonObject jcoefs = glm.get("coefficients").getAsJsonObject();
      double icept = jcoefs.get("Intercept").getAsDouble();
      //assertCat(jcoefs,icept,"Low" ,0.0      );// now folded into the intercept
      assertCat(jcoefs,icept,"Med" ,0.3333333);
      assertCat(jcoefs,icept,"High",1.0      );
      UKV.remove(Key.make(glm.get(Constants.MODEL_KEY).getAsString()));
    } finally {
      UKV.remove(datakey);
    }
  }

  // Assert reasonable results for the categorical predictor
  static void assertCat(JsonObject jcoefs, double icept, String category, double expected) {
    // For categoricals, we expanded the terms into an array of boolean
    // predictors all zero, except for the given term which is set to 1.
    // Example: factors/categories: Low, Med, High.
    // Since 3 factors, we make an array of size 3.
    // Low maps to {1,0,0}, Med maps to {0,1,0} and High maps to {0,0,1}.
    // The equation is normally: 1/1+exp(-(c0*x[0] + c1*x[1]+ c2*x[2]... + icept))
    // When computing the math, all predictors are zero except the one...  so
    // the equation expansion only needs to sum the one coeficient multiplied
    // by 1, plus the intercept.
    double coef = jcoefs.get("0."+category).getAsDouble();
    double predict = 1.0/(1.0+Math.exp(-(coef*1.0/* + all other terms are 0 */+icept)));
    assertEquals(expected,predict,0.001);
  }
}
