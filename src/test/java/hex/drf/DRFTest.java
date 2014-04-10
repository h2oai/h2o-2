package hex.drf;

import hex.drf.DRF.DRFModel;

import java.util.Arrays;

import org.junit.*;

import water.*;
import water.fvec.Frame;
import water.fvec.Vec;

public class DRFTest extends TestUtil {

  @BeforeClass public static void stall() { stall_till_cloudsize(1); }

  abstract static class PrepData { abstract int prep(Frame fr); }

  static final String[] s(String...arr)  { return arr; }
  static final long[]   a(long ...arr)   { return arr; }
  static final long[][] a(long[] ...arr) { return arr; }

  //  @Ignore
  @Test public void testClassIris1() throws Throwable {

    // iris ntree=1
    // the DRF should  use only subset of rows since it is using oob validation
    basicDRFTestOOBE(
          "./smalldata/iris/iris_train.csv","iris_train.hex",
          new PrepData() { @Override int prep(Frame fr) { return fr.numCols()-1; } },
          1,
          a( a(6, 0,  0),
             a(0, 7,  0),
             a(0, 3, 10)),
          s("Iris-setosa","Iris-versicolor","Iris-virginica") );
  }

  //  @Ignore
  @Test public void testClassIris50() throws Throwable {
    // iris ntree=50
    basicDRFTestOOBE(
          "./smalldata/iris/iris_train.csv","iris_train.hex",
          new PrepData() { @Override int prep(Frame fr) { return fr.numCols()-1; } },
          50,
          a( a(30, 0,  0),
             a(0, 31,  3),
             a(0,  4, 32)),
          s("Iris-setosa","Iris-versicolor","Iris-virginica") );
  }

  //  @Ignore
  @Test public void testClassCars1() throws Throwable {
    // cars ntree=1
    basicDRFTestOOBE(
        "./smalldata/cars.csv","cars.hex",
        new PrepData() { @Override int prep(Frame fr) { UKV.remove(fr.remove("name")._key); return fr.find("cylinders"); } },
        1,
        a( a(2,  3, 0, 3, 1),
           a(1, 51, 0, 3, 1),
           a(0,  0, 0, 0, 0),
           a(0,  2, 0,16, 2),
           a(0,  0, 0, 0,33)),
        s("3", "4", "5", "6", "8"));
  }

  //  @Ignore
  @Test public void testClassCars50() throws Throwable {
    basicDRFTestOOBE(
        "./smalldata/cars.csv","cars.hex",
        new PrepData() { @Override int prep(Frame fr) { UKV.remove(fr.remove("name")._key); return fr.find("cylinders"); } },
        50,
        a( a(0,   4, 0,  0,   0),
           a(0, 207, 0,  0,   0),
           a(0,   2, 0,  1,   0),
           a(0,   4, 0, 80,   0),
           a(0,   0, 1,  3, 104)),
        s("3", "4", "5", "6", "8"));
  }

  @Test
  public void testConstantCols() throws Throwable {
    try { 
      basicDRFTestOOBE(
        "./smalldata/poker/poker100","poker.hex",
        new PrepData() { @Override int prep(Frame fr) {
          for (int i=0; i<7;i++) UKV.remove(fr.remove(3)._key);
          return 3;
        } },
        1,
        null,
        null);
    Assert.fail();
    } catch( IllegalArgumentException iae ) { /*pass*/}
  }

  //@Test
  public void testCreditSample1() throws Throwable {
    basicDRFTestOOBE(
        "./smalldata/kaggle/creditsample-training.csv.gz","credit.hex",
        new PrepData() { @Override int prep(Frame fr) {
          UKV.remove(fr.remove("MonthlyIncome")._key); return fr.find("SeriousDlqin2yrs");
          } },
        1,
        a( a(46294, 202),
           a( 3187, 107)),
        s("0", "1"));

  }

  //@Ignore("We need to have proper regression test.")
  //@Test
  public void testCreditProstate1() throws Throwable {
    basicDRFTestOOBE(
        "./smalldata/logreg/prostate.csv","prostate.hex",
        new PrepData() { @Override int prep(Frame fr) {
          UKV.remove(fr.remove("ID")._key); return fr.find("CAPSULE");
          } },
        1,
        a( a(46294, 202),
           a( 3187, 107)),
        s("0", "1"));

  }


  /*@Test*/ public void testAirlines() throws Throwable {
    basicDRFTestOOBE(
        "./smalldata/airlines/allyears2k_headers.zip","airlines.hex",
        new PrepData() {
          @Override int prep(Frame fr) {
            UKV.remove(fr.remove("DepTime")._key);
            UKV.remove(fr.remove("ArrTime")._key);
            UKV.remove(fr.remove("ActualElapsedTime")._key);
            UKV.remove(fr.remove("AirTime")._key);
            UKV.remove(fr.remove("ArrDelay")._key);
            UKV.remove(fr.remove("DepDelay")._key);
            UKV.remove(fr.remove("Cancelled")._key);
            UKV.remove(fr.remove("CancellationCode")._key);
            UKV.remove(fr.remove("CarrierDelay")._key);
            UKV.remove(fr.remove("WeatherDelay")._key);
            UKV.remove(fr.remove("NASDelay")._key);
            UKV.remove(fr.remove("SecurityDelay")._key);
            UKV.remove(fr.remove("LateAircraftDelay")._key);
            UKV.remove(fr.remove("IsArrDelayed")._key);
            return fr.find("IsDepDelayed"); }
        },
        50,
        a( a(14890, 5997),
           a( 6705,16386)),
        s("NO", "YES"));
  }



  // Put response as the last vector in the frame and return it.
  // Also fill DRF.
  static Vec unifyFrame(DRF drf, Frame fr, PrepData prep) {
    int idx = prep.prep(fr);
    if( idx < 0 ) { drf.classification = false; idx = ~idx; }
    String rname = fr._names[idx];
    drf.response = fr.vecs()[idx];
    fr.remove(idx);           // Move response to the end
    fr.add(rname,drf.response);
    return drf.response;
  }

  public void basicDRFTestOOBE(String fnametrain, String hexnametrain, PrepData prep, int ntree, long[][] expCM, String[] expRespDom) throws Throwable { basicDRF(fnametrain, hexnametrain, null, null, prep, ntree, expCM, expRespDom, 10/*max_depth*/, 20/*nbins*/, 0/*optflag*/); }
  public void basicDRF(String fnametrain, String hexnametrain, String fnametest, String hexnametest, PrepData prep, int ntree, long[][] expCM, String[] expRespDom, int max_depth, int nbins, int optflags) throws Throwable {
    DRF drf = new DRF();
    Key destTrain = Key.make(hexnametrain);
    Key destTest  = hexnametest!=null?Key.make(hexnametest):null;
    Frame frTest = null, pred = null;
    DRFModel model = null;
    try {
      Frame frTrain = drf.source = parseFrame(destTrain, fnametrain);
      unifyFrame(drf, frTrain, prep);
      // Configure DRF
      drf.classification = true;
      drf.ntrees = ntree;
      drf.max_depth = max_depth;
      drf.min_rows = 1; // = nodesize
      drf.nbins = nbins;
      drf.mtries = -1;
      drf.sample_rate = 0.66667f;   // Simulated sampling with replacement
      drf.seed = (1L<<32)|2;
      drf.destination_key = Key.make("DRF_model_4_" + hexnametrain);
      // Invoke DRF and block till the end
      drf.invoke();
      // Get the model
      model = UKV.get(drf.dest());
      // And compare CMs
      assertCM(expCM, model.cms[model.cms.length-1]._arr);
      Assert.assertEquals("Number of trees differs!", ntree, model.errs.length-1);
      String[] cmDom = model._domains[model._domains.length-1];
      Assert.assertArrayEquals("CM domain differs!", expRespDom, cmDom);

      frTest = fnametest!=null ? parseFrame(destTest, fnametest) : null;
      pred = drf.score(frTest!=null?frTest:drf.source);

    } catch (Throwable t) {
      t.printStackTrace();
      throw t;
    } finally {
      drf.source.delete();
      UKV.remove(drf.response._key);
      drf.remove();
      if (frTest!=null) frTest.delete();
      if( model != null ) model.delete(); // Remove the model
      if( pred != null ) pred.delete();
    }
  }

  void assertCM(long[][] expectedCM, long[][] givenCM) {
    Assert.assertEquals("Confusion matrix dimension does not match", expectedCM.length, givenCM.length);
    String m = "Expected: " + Arrays.deepToString(expectedCM) + ", but was: " + Arrays.deepToString(givenCM);
    for (int i=0; i<expectedCM.length; i++) Assert.assertArrayEquals(m, expectedCM[i], givenCM[i]);
  }
}
