package hex.drf;

import hex.drf.DRF.DRFModel;

import java.io.File;
import java.util.Arrays;

import org.junit.*;

import water.*;
import water.fvec.*;

public class DRFTest extends TestUtil {

  @BeforeClass public static void stall() { stall_till_cloudsize(1); }

  private abstract class PrepData { abstract int prep(Frame fr); }

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
             a(0, 2, 11)));
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
             a(0,  2, 34)));
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
           a(0,  0, 0, 0,34)));
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
           a(0,   0, 0,  0, 108)));
  }

    //basicDRF("./smalldata/logreg/prostate.csv","prostate.hex",
    //         new PrepData() {
    //           int prep(Frame fr) {
    //             assertEquals(380,fr.numRows());
    //             // Remove patient ID vector
    //             UKV.remove(fr.remove("ID")._key);
    //             // Prostate: predict on CAPSULE
    //             return fr.remove("CAPSULE");
    //           }
    //         });
    //basicDRF("../datasets/UCI/UCI-large/covtype/covtype.data","covtype.hex",
    //         //basicDRF("./smalldata/covtype/covtype.20k.data","covtype.hex",
    //         new PrepData() {
    //           int prep(Frame fr) {
    //             for( int ign : IGNS )
    //               UKV.remove(fr.remove(Integer.toString(ign))._key);
    //             // Covtype: predict on last column
    //             return fr.remove(fr.numCols()-1);
    //           }
    //         });

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

  public void basicDRFTestOOBE(String fnametrain, String hexnametrain, PrepData prep, int ntree, long[][] expCM) throws Throwable { basicDRF(fnametrain, hexnametrain, null, null, prep, ntree, expCM); }
  public void basicDRF(String fnametrain, String hexnametrain, String fnametest, String hexnametest, PrepData prep, int ntree, long[][] expCM) throws Throwable {
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
      drf.max_depth = 50;
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
    Assert.assertEquals(expectedCM.length, givenCM.length);
    String m = "Expected: " + Arrays.deepToString(expectedCM) + ", but was: " + Arrays.deepToString(givenCM);
    for (int i=0; i<expectedCM.length; i++) Assert.assertArrayEquals(m, expectedCM[i], givenCM[i]);
  }
}
