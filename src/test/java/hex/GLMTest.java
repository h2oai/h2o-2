package hex;

import static org.junit.Assert.assertEquals;
import hex.DGLM.Family;
import hex.DGLM.GLMModel;
import hex.DGLM.GLMParams;
import hex.DLSM.ADMMSolver;
import hex.DLSM.GeneralizedGradientSolver;
import hex.DLSM.LSMSolver;
import hex.NewRowVecTask.DataFrame;

import java.util.Random;

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

  @Test public void testGammaRegression() {
    Key datakey = Key.make("datakey");
//    Key datakey2 = Key.make("datakey2");
    try {
      ///////////////////////////////////////////
      // Test 1.
      // Make some synthetic data to test with.
      // Equation is: y = 1/(x+1);
      ///////////////////////////////////////////
      ValueArray va =
        va_maker(datakey,
                 new byte []{  0, 1, 2, 3 , 4 , 5 , 6  , 7  },
                 //  e^1, e^2, ..., e^8
                 new double[]{1.0, 0.5, 0.3333333, 0.25, 0.20,  0.1666667, 0.1428571, 0.1250000});
      JsonObject glm = computeGLM(Family.gamma,new ADMMSolver(0,0),va,false,null); // Solve it!
      JsonObject coefs = glm.get("coefficients").getAsJsonObject();
      assertEquals( 1.0, coefs.get("Intercept").getAsDouble(), 0.000001);
      assertEquals( 1.0, coefs.get("0")        .getAsDouble(), 0.000001);
      UKV.remove(Key.make(glm.get(Constants.MODEL_KEY).getAsString()));
      // recompute with GG solver
      glm = computeGLM(Family.gamma,new GeneralizedGradientSolver(0,0),va,false,null); // Solve it!
      coefs = glm.get("coefficients").getAsJsonObject();
      assertEquals( 1.0, coefs.get("Intercept").getAsDouble(), 0.0001);
      assertEquals( 1.0, coefs.get("0")        .getAsDouble(), 0.0001);
      UKV.remove(Key.make(glm.get(Constants.MODEL_KEY).getAsString()));
    }finally{
      UKV.remove(datakey);
//      UKV.remove(datakey2);
    }
  }


  /**
   * Test H2O gets the same results as R.
   */
  @Test public void testOnData(){
    Key k = loadAndParseKey("h.hex","smalldata/glm_test/poisson_tst1.csv");
    ValueArray ary = ValueArray.value(k);
    // Test poisson
    DataFrame data = DGLM.getData(ary, new int[]{2, 3},1, null, true);
    GLMModel m1 = DGLM.buildModel(data, new ADMMSolver(0,0), new GLMParams(Family.poisson));
    GLMModel m2 = DGLM.buildModel(data, new GeneralizedGradientSolver(0,0), new GLMParams(Family.poisson));
    JsonObject j1 = m1.toJson();
    JsonObject j2 = m2.toJson();
    JsonObject coefs1 = j1.get("coefficients").getAsJsonObject();
    JsonObject coefs2 = j2.get("coefficients").getAsJsonObject();
    assertEquals( -4.1627, coefs1.get("Intercept").getAsDouble(), 0.001);
    assertEquals( -4.1627, coefs2.get("Intercept").getAsDouble(), 0.001);
    assertEquals( -1.08386, coefs1.get("prog.General").getAsDouble(), 0.001);
    assertEquals( -1.08386, coefs2.get("prog.General").getAsDouble(), 0.001);
    assertEquals( -0.71405 , coefs1.get("prog.Vocational").getAsDouble(), 0.001);
    assertEquals( -0.71405 , coefs2.get("prog.Vocational").getAsDouble(), 0.001);
    assertEquals( 0.07015 , coefs1.get("math").getAsDouble(), 0.001);
    assertEquals( 0.07015 , coefs2.get("math").getAsDouble(), 0.001);
    UKV.remove(Key.make(j1.get(Constants.MODEL_KEY).getAsString()));
    UKV.remove(Key.make(j2.get(Constants.MODEL_KEY).getAsString()));
    // Test Gamma
    data = DGLM.getData(ary, new int[]{1,2},3, null, true);
    m1 = DGLM.buildModel(data, new ADMMSolver(0,0), new GLMParams(Family.gamma));
    m2 = DGLM.buildModel(data, new GeneralizedGradientSolver(0,0), new GLMParams(Family.gamma));
    j1 = m1.toJson();
    j2 = m2.toJson();
    coefs1 = j1.get("coefficients").getAsJsonObject();
    coefs2 = j2.get("coefficients").getAsJsonObject();
    assertEquals( 0.01869, coefs1.get("Intercept").getAsDouble(), 0.001);
    assertEquals( 0.01869, coefs2.get("Intercept").getAsDouble(), 0.001);
    assertEquals( 0.0015022, coefs1.get("prog.General").getAsDouble(), 0.001);
    assertEquals( 0.0015022, coefs2.get("prog.General").getAsDouble(), 0.001);
    assertEquals( 0.0030964, coefs1.get("prog.Vocational").getAsDouble(), 0.001);
    assertEquals( 0.0030964, coefs2.get("prog.Vocational").getAsDouble(), 0.001);
    assertEquals( -0.0009666, coefs1.get("num_awards").getAsDouble(), 0.001);
    assertEquals( -0.0009666, coefs2.get("num_awards").getAsDouble(), 0.001);
    UKV.remove(Key.make(j1.get(Constants.MODEL_KEY).getAsString()));
    UKV.remove(Key.make(j2.get(Constants.MODEL_KEY).getAsString()));
    UKV.remove(k);
  }

  @Test public void testPoissonRegression() {
    Key datakey = Key.make("datakey");
    Key datakey2 = Key.make("datakey2");
    try {
      ///////////////////////////////////////////
      // Test 1.
      // Make some synthetic data to test with.
      // Equation is: y = exp(x+1);
      ///////////////////////////////////////////
      ValueArray va =
        va_maker(datakey,
                 new byte []{  0, 1, 2, 3 , 4 , 5 , 6  , 7  },
                 //  e^1, e^2, ..., e^8
                 new double[]{2.718282,7.389056, 20.085537, 54.598150, 148.413159, 403.428793, 1096.633158, 2980.957987});
      JsonObject glm = computeGLM(Family.poisson,new ADMMSolver(0,0),va,false,null); // Solve it!
      JsonObject coefs = glm.get("coefficients").getAsJsonObject();
      assertEquals( 1.0, coefs.get("Intercept").getAsDouble(), 0.000001);
      assertEquals( 1.0, coefs.get("0")        .getAsDouble(), 0.000001);
      UKV.remove(Key.make(glm.get(Constants.MODEL_KEY).getAsString()));
      // recompute with GG solver
      glm = computeGLM(Family.poisson,new GeneralizedGradientSolver(0,0),va,false,null); // Solve it!
      coefs = glm.get("coefficients").getAsJsonObject();
      assertEquals( 1.0, coefs.get("Intercept").getAsDouble(), 0.0001);
      assertEquals( 1.0, coefs.get("0")        .getAsDouble(), 0.0001);
      UKV.remove(Key.make(glm.get(Constants.MODEL_KEY).getAsString()));
      ////////////////////////////////////////////////////////////////////////////////////
      // Test 2.
      // example from http://www.biostat.umn.edu/~dipankar/bmtry711.11/lecture_13.pdf
      // Equation is: y = exp(0.2565+0.3396);
      ////////////////////////////////////////////////////////////////////////////////////
      //    Month Period Deaths Month Period Deaths
      //    1 0 | 8  18
      //    2 1 | 9  23
      //    3 2 | 10 31
      //    4 3 | 11 20
      //    5 1 | 12 25
      //    6 4 | 13 37
      //    7 9 | 14 45
      va = va_maker(datakey2,
                   new byte []{1,2,3,4,5,6,7,8, 9, 10,11,12,13,14},
                   new byte []{0,1,2,3,1,4,9,18,23,31,20,25,37,45});
      glm = computeGLM(Family.poisson,new ADMMSolver(0,0),va,false,null); // Solve it!
      coefs = glm.get("coefficients").getAsJsonObject();
      assertEquals( 0.3396, coefs.get("Intercept").getAsDouble(), 0.0001);
      assertEquals( 0.2565, coefs.get("0")        .getAsDouble(), 0.0001);
      UKV.remove(Key.make(glm.get(Constants.MODEL_KEY).getAsString()));
      // recompute with GG solver
      glm = computeGLM(Family.poisson,new GeneralizedGradientSolver(0,0),va,false,null); // Solve it!
      coefs = glm.get("coefficients").getAsJsonObject();
      assertEquals( 0.3396, coefs.get("Intercept").getAsDouble(), 0.0001);
      assertEquals( 0.2565, coefs.get("0")        .getAsDouble(), 0.0001);
      UKV.remove(Key.make(glm.get(Constants.MODEL_KEY).getAsString()));
    }finally{
      UKV.remove(datakey);
      UKV.remove(datakey2);
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
      ValueArray va = ValueArray.value(DKV.get(k2));
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
    ValueArray va = ValueArray.value(DKV.get(k1));
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
//      assertCat(jcoefs,icept,"Low" ,0.0      );
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
