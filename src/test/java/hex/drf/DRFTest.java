package hex.drf;

import hex.drf.DRF.DRFModel;

import java.util.Arrays;

import org.junit.*;

import water.*;
import water.fvec.Frame;
import water.fvec.Vec;

public class DRFTest extends TestUtil {

  @BeforeClass public static void stall() { stall_till_cloudsize(1); }

  private abstract class PrepData { abstract int prep(Frame fr); }

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
             a(0, 2, 11)),
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
             a(0,  2, 34)),
          s("Iris-setosa","Iris-versicolor","Iris-virginica") );
  }

//  @Ignore
  @Test public void testClassCars1() throws Throwable {
    // cars ntree=1
    basicDRFTestOOBE(
        "./smalldata/cars.csv","cars.hex",
        new PrepData() { @Override int prep(Frame fr) { UKV.remove(fr.remove("name")._key); return fr.find("cylinders"); } },
        1,
        a( a(0,  1, 0, 0, 0),
           a(1, 55, 0, 3, 0),
           a(0,  0, 0, 0, 0),
           a(0,  0, 0,16, 0),
           a(0,  0, 0, 0,34)),
        s("3", "4", "5", "6", "8"));
  }

//  @Ignore
  @Test public void testClassCars50() throws Throwable {
    basicDRFTestOOBE(
        "./smalldata/cars.csv","cars.hex",
        new PrepData() { @Override int prep(Frame fr) { UKV.remove(fr.remove("name")._key); return fr.find("cylinders"); } },
        50,
        a( a(2,   2, 0,  0,   0),
           a(1, 205, 0,  1,   0),
           a(0,   2, 0,  1,   0),
           a(0,   4, 0, 79,   1),
           a(0,   0, 0,  0, 108)),
        s("3", "4", "5", "6", "8"));
  }

  @Test public void testCreditSample1() throws Throwable {
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

  @Ignore("We need to have proper regression test.")
  @Test public void testCreditProstate1() throws Throwable {
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

  public void basicDRFTestOOBE(String fnametrain, String hexnametrain, PrepData prep, int ntree, long[][] expCM, String[] expRespDom) throws Throwable { basicDRF(fnametrain, hexnametrain, null, null, prep, ntree, expCM, expRespDom); }
  public void basicDRF(String fnametrain, String hexnametrain, String fnametest, String hexnametest, PrepData prep, int ntree, long[][] expCM, String[] expRespDom) throws Throwable {
    DRF drf = null;
    Frame frTrain = null, frTest = null;
    Key destTrain = Key.make(hexnametrain);
    Key destTest  = hexnametest!=null?Key.make(hexnametest):null;
    Frame pred = null;
    try {
      drf = new DRF();
      frTrain = drf.source = parseFrame(destTrain, fnametrain);
      unifyFrame(drf, frTrain, prep);
      // Configure DRF
      drf.classification = true;
      drf.ntrees = ntree;
      drf.max_depth = 10;
      drf.min_rows = 1; // = nodesize
      drf.nbins = 1024;
      drf.mtries = -1;
      drf.sample_rate = 0.66667f;   // Simulated sampling with replacement
      drf.seed = (1L<<32)|2;
      // Invoke DRF and block till the end
      drf.invoke();
      // Get the model
      DRFModel model = UKV.get(drf.dest());
      // And compare CMs
      assertCM(expCM, model.cm);
      Assert.assertEquals("Number of trees differs!", ntree, model.errs.length-1);
      String[] cmDom = model._domains[model._domains.length-1];
      Assert.assertArrayEquals("CM domain differs!", expRespDom, cmDom);

      frTest = fnametest!=null ? parseFrame(destTest, fnametest) : null;
      pred = drf.score(frTest!=null?frTest:drf.source);
    } catch (Throwable t) {
      t.printStackTrace();
      throw t;
    } finally {
      frTrain.remove();
      UKV.remove(destTrain);
      if (frTest!=null) { frTest.remove(); UKV.remove(destTest); }
      if( drf != null ) {
        UKV.remove(drf.dest()); // Remove the model
        UKV.remove(drf.response._key);
        drf.remove();
        if( pred != null ) pred.remove();
      }
    }
  }

  void assertCM(long[][] expectedCM, long[][] givenCM) {
    Assert.assertEquals("Confusion matrix dimension does not match", expectedCM.length, givenCM.length);
    String m = "Expected: " + Arrays.deepToString(expectedCM) + ", but was: " + Arrays.deepToString(givenCM);
    for (int i=0; i<expectedCM.length; i++) Assert.assertArrayEquals(m, expectedCM[i], givenCM[i]);
  }
}
