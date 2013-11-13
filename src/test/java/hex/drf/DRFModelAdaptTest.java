package hex.drf;


import hex.drf.DRF.DRFModel;

import org.junit.*;

import water.*;
import water.fvec.Frame;
import water.fvec.Vec;

public class DRFModelAdaptTest extends TestUtil {

  private abstract class PrepData { abstract Vec prep(Frame fr); }

  @BeforeClass public static void stall() { stall_till_cloudsize(1); }

  /**
   * The scenario:
   *  - test data contains an input column which contains less enum values than the same column in train data.
   *  In this case we should provide correct values mapping:
   *  A - 0
   *  B - 1    B - 0                                   B - 1
   *  C - 2    D - 1    mapping should remap it into:  D - 3
   *  D - 3
   */
  @Test public void testModelAdapt() {
    testModelAdaptation(
        "./smalldata/test/classifier/coldom_train.csv",
        "./smalldata/test/classifier/coldom_test.csv",
        new PrepData() { @Override Vec prep(Frame fr) { return fr.vecs()[fr.numCols()-1]; } });
  }

  /**
   * The scenario:
   *  - test data contains an input column which contains more enum values than the same column in train data.
   *  A - 0
   *  B - 1    B - 0                                   B - 1
   *  C - 2    X - 1    mapping should remap it into:  X - NA
   *  D - 3
   */
  @Test public void testModelAdapt2() {
    testModelAdaptation(
        "./smalldata/test/classifier/coldom_train.csv",
        "./smalldata/test/classifier/coldom_test2.csv",
        new PrepData() { @Override Vec prep(Frame fr) { return fr.vecs()[fr.numCols()-1]; } });
  }

  void testModelAdaptation(String train, String test, PrepData dprep) {
    DRFModel model = null;
    Frame frTest = null;
    Frame frTrain = null;
    Key trainKey = Key.make("train.hex");
    Key testKey = Key.make("test.hex");
    Frame[] frAdapted = null;
    try {
      // Prepare a simple model
      frTrain = parseFrame(trainKey, train);
      model = runDRF(frTrain,dprep);
      // Load test dataset - test data contains input columns matching train data,
      // BUT each input requires adaptation. Moreover, test data contains additional columns
      // containing correct value mapping.
      frTest = parseFrame(testKey, test);
      Assert.assertEquals("TEST CONF ERROR: The test dataset should contain 2*<number of input columns>+1!", 2*(frTrain.numCols()-1)+1, frTest.numCols());
      // Adapt test dataset
      frAdapted = model.adapt(frTest, true);
      Assert.assertEquals("Adapt method should return two frames", 2, frAdapted.length);
      Assert.assertEquals("Test expects that all columns in  test dataset has to be adapted", frAdapted[0].numCols(), frAdapted[1].numCols());

      // Compare vectors
      Frame adaptedFrame = frAdapted[0];
      for (int av=0; av<frTrain.numCols()-1; av++) {
        int ev = av + frTrain.numCols();
        Vec actV = adaptedFrame.vecs()[av];
        Vec expV = frTest.vecs()[ev];
        Assert.assertEquals("Different number of rows in test vectors", expV.length(), actV.length());
        for (long r=0; r<expV.length(); r++) {
          if (expV.isNA(r)) Assert.assertTrue("Badly adapted vector - expected NA! Col: " + av + ", row: " + r, actV.isNA(r));
          else {
            Assert.assertTrue("Badly adapted vector - expected value but get NA! Col: " + av + ", row: " + r, !actV.isNA(r));
            Assert.assertEquals("Badly adapted vector - wrong values! Col: " + av + ", row: " + r, expV.at8(r), actV.at8(r));
          }
        }
      }

    } finally {
      // Test cleanup
      if (model!=null) UKV.remove(model._selfKey);
      if (frTrain!=null) frTrain.remove();
      UKV.remove(trainKey);
      if (frTest!=null) frTest.remove();
      UKV.remove(testKey);
      // Remove adapted vectors which were saved into KV-store, rest of vectors are remove by frTest.remove()
      if (frAdapted!=null) frAdapted[1].remove();
    }
  }

  private DRFModel runDRF(Frame data, PrepData dprep) {
    DRF drf = new DRF();
    drf.source = data;
    drf.response = dprep.prep(data);
    drf.ntrees = 1;
    drf.invoke();
    return UKV.get(drf.dest());
  }
}
