package hex.gbm;

import java.io.File;

import org.junit.BeforeClass;
import org.junit.Test;

import water.*;
import water.fvec.*;

public class DRFTest extends TestUtil {

  @BeforeClass public static void stall() { stall_till_cloudsize(1); }

  private abstract class PrepData { abstract int prep(Frame fr); }

  @Test public void testBasicDRF() {
    // Disabled Regression tests
    //basicDRF("./smalldata/cars.csv","cars.hex",
    //         new PrepData() { int prep(Frame fr) { UKV.remove(fr.remove("name")._key); return fr.remove("economy (mpg)"); }
    //         });

    // Classification tests
    basicDRF(
            "./smalldata/iris/iris_train.csv","iris_train.hex",
            //"./smalldata/iris/iris_test.csv" ,"iris_test.hex",
             //"./smalldata/test/test_tree.csv","tree.hex",
             new PrepData() { @Override int prep(Frame fr) { return fr.numCols()-1; }
             });

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
    //basicDRF("./smalldata/iris/iris_wheader.csv","iris.hex",
    //         new PrepData() { int prep(Frame fr) { return fr.numCols()-1; }
    //         });
    //basicDRF("./smalldata/airlines/allyears2k_headers.zip","airlines.hex",
    //         new PrepData() { int prep(Frame fr) {
    //           UKV.remove(fr.remove("IsArrDelayed")._key);
    //           return fr.remove("IsDepDelayed");
    //         }
    //         });
    //basicDRF("./smalldata/cars.csv","cars.hex",
    //         new PrepData() { int prep(Frame fr) { UKV.remove(fr.remove("name")._key); return fr.remove("cylinders"); }
    //         });
    //basicDRF("./smalldata/airlines/allyears2k_headers.zip","air.hex",
    //         new PrepData() { int prep(Frame fr) { return fr.remove("IsDepDelayed"); }
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
  }

  // Parse given file and returns a frame representing the file.
  // The caller is responsible for frame remove.
  static Frame parseDs(String fname, Key destKey) {
    File file = TestUtil.find_test_file(fname);
    Key fkey = NFSFileVec.make(file);
    Frame fr = ParseDataset2.parse(destKey,new Key[]{fkey});
    UKV.remove(fkey);
    return fr;
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

  public void basicDRF(String fnametrain, String hexnametrain, PrepData prep) { basicDRF(fnametrain, hexnametrain, null, null, prep); }
  public void basicDRF(String fnametrain, String hexnametrain, String fnametest, String hexnametest, PrepData prep) {
    DRF drf = null;
    Frame frTrain = null, frTest = null;
    Key destTrain = Key.make(hexnametrain);
    Key destTest  = hexnametest!=null?Key.make(hexnametest):null;
    Frame pred = null;
    try {
      drf = new DRF();
      drf.classification = true;
      frTrain = drf.source = parseDs(fnametrain, destTrain);
      unifyFrame(drf, frTrain, prep);
      // Configure DRF
      drf.ntrees = 1;
      drf.max_depth = 50;
      drf.min_rows = 1;
      drf.nbins = 100;
      drf.mtries = -1;
      drf.sample_rate = 0.66667f;   // Simulated sampling with replacement
      drf.seed = (1L<<32)|2;
      drf.invoke();

      frTest = fnametest!=null ? parseDs(fnametest, destTest) : null;
      pred = drf.score(frTest!=null?frTest:drf.source);
    } catch (Throwable t) {
      t.printStackTrace();
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
}
