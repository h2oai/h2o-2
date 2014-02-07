package hex.drf;


import hex.drf.DRF.DRFModel;

import org.junit.*;

import water.*;
import water.fvec.Frame;
import water.fvec.Vec;
import water.util.Utils;

public class DRFModelAdaptTest extends TestUtil {

  private abstract class PrepData { abstract Vec prep(Frame fr); int needAdaptation(Frame fr) { return fr.numCols(); };}

  @BeforeClass public static void stall() { stall_till_cloudsize(3); }

  /**
   * The scenario:
   *  - test data contains an input column which contains less enum values than the same column in train data.
   *  In this case we should provide correct values mapping:
   *  A - 0
   *  B - 1    B - 0                                   B - 1
   *  C - 2    D - 1    mapping should remap it into:  D - 3
   *  D - 3
   */
  //@Ignore
  @Test public void testModelAdapt1() {
    testModelAdaptation(
        "./smalldata/test/classifier/coldom_train_1.csv",
        "./smalldata/test/classifier/coldom_test_1.csv",
        new PrepData() { @Override Vec prep(Frame fr) { return fr.vecs()[fr.numCols()-1]; } },
        true);
  }

  /**
   * The scenario:
   *  - test data contains an input column which contains more enum values than the same column in train data.
   *  A - 0
   *  B - 1    B - 0                                   B - 1
   *  C - 2    X - 1    mapping should remap it into:  X - NA
   *  D - 3
   */
  //@Ignore
  @Test public void testModelAdapt1_2() {
    testModelAdaptation(
        "./smalldata/test/classifier/coldom_train_1.csv",
        "./smalldata/test/classifier/coldom_test_1_2.csv",
        new PrepData() { @Override Vec prep(Frame fr) { return fr.vecs()[fr.numCols()-1]; } },
        true);
  }

  //@Ignore
  @Test public void testModelAdapt2() {
    testModelAdaptation(
        "./smalldata/test/classifier/coldom_train_2.csv",
        "./smalldata/test/classifier/coldom_test_2.csv",
        new PrepData() { @Override Vec prep(Frame fr) { return fr.vecs()[fr.find("R")]; }; @Override int needAdaptation(Frame fr) { return 0;} },
        true);
  }

  /** Test adaptation of numeric values in response column. */
  //@Ignore
  @Test public void testModelAdapt3() {
    testModelAdaptation(
        "./smalldata/test/classifier/coldom_train_3.csv",
        "./smalldata/test/classifier/coldom_test_3.csv",
        new PrepData() { @Override Vec prep(Frame fr) { return fr.vecs()[fr.numCols()-1]; } },
        false);
  }

  static final int[]   a(int ...arr)   { return arr; }

  @Test public void testBasics_1() {
    // Simple domain mapping
    Assert.assertArrayEquals( a(0, 1, 2, 3),      Utils.mapping(a( 0, 1, 2, 3)));
    Assert.assertArrayEquals( a(0, 1, 2, -1, 3),  Utils.mapping(a( 0, 1, 2, 4)));
    Assert.assertArrayEquals( a(0, -1, 1),        Utils.mapping(a(-1, 1)));
    Assert.assertArrayEquals( a(0, -1, 1, -1, 2), Utils.mapping(a(-1, 1, 3)));
  }

  @Test public void testBasics_2() {
    Assert.assertArrayEquals( a(2, 30, 400, 5000),      Utils.compose(Utils.mapping(a( 0, 1, 2, 3)), a(2,30,400,5000) ));
    Assert.assertArrayEquals( a(2, 30, 400, -1, 5000),  Utils.compose(Utils.mapping(a( 0, 1, 2, 4)), a(2,30,400,5000) ));
    Assert.assertArrayEquals( a(2, -1, 30),             Utils.compose(Utils.mapping(a(-1, 1)),       a(2,30,400,5000) ));
    Assert.assertArrayEquals( a(2, -1, 30, -1, 400),    Utils.compose(Utils.mapping(a(-1, 1, 3)),    a(2,30,400,5000) ));
  }

  void testModelAdaptation(String train, String test, PrepData dprep, boolean exactAdaptation) {
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
      frAdapted = model.adapt(frTest, exactAdaptation); // do/do not perform translation to enums
      Assert.assertEquals("Adapt method should return two frames", 2, frAdapted.length);
      Assert.assertEquals("Test expects that all columns in  test dataset has to be adapted", dprep.needAdaptation(frTrain), frAdapted[1].numCols());

      // Compare vectors
      Frame adaptedFrame = frAdapted[0];
      //System.err.println(frTest.toStringAll());
      //System.err.println(adaptedFrame.toStringAll());

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
      if( model  !=null ) model  .delete();
      if( frTrain!=null ) frTrain.delete();
      if( frTest !=null ) frTest .delete();
      // Remove adapted vectors which were saved into KV-store, rest of vectors are remove by frTest.remove()
      if (frAdapted!=null) frAdapted[1].delete();
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
