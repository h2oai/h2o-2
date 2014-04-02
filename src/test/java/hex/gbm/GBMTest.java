package hex.gbm;

import hex.gbm.GBM.GBMModel;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import water.*;
import water.api.ConfusionMatrix;
import water.fvec.Frame;
import water.fvec.NFSFileVec;
import water.fvec.ParseDataset2;

import java.io.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class GBMTest extends TestUtil {

  @BeforeClass public static void stall() { stall_till_cloudsize(1); }

  private abstract class PrepData { abstract int prep(Frame fr); }

  static final String ignored_aircols[] = new String[] { "DepTime", "ArrTime", "AirTime", "ArrDelay", "DepDelay", "TaxiIn", "TaxiOut", "Cancelled", "CancellationCode", "Diverted", "CarrierDelay", "WeatherDelay", "NASDelay", "SecurityDelay", "LateAircraftDelay", "IsDepDelayed"};

  @Test public void testBasicGBM() {
    // Regression tests
    basicGBM("./smalldata/cars.csv","cars.hex",
             new PrepData() { int prep(Frame fr ) { UKV.remove(fr.remove("name")._key); return ~fr.find("economy (mpg)"); }});

    // Classification tests
    basicGBM("./smalldata/test/test_tree.csv","tree.hex",
             new PrepData() { int prep(Frame fr) { return 1; }
             });
    basicGBM("./smalldata/test/test_tree_minmax.csv","tree_minmax.hex",
             new PrepData() { int prep(Frame fr) { return fr.find("response"); }
             });
    basicGBM("./smalldata/logreg/prostate.csv","prostate.hex",
             new PrepData() {
               int prep(Frame fr) {
                 assertEquals(380,fr.numRows());
                 // Remove patient ID vector
                 UKV.remove(fr.remove("ID")._key);
                 // Prostate: predict on CAPSULE
                 return fr.find("CAPSULE");
               }
             });
    basicGBM("./smalldata/cars.csv","cars.hex",
             new PrepData() { int prep(Frame fr) { UKV.remove(fr.remove("name")._key); return fr.find("cylinders"); }
             });
    basicGBM("./smalldata/airlines/allyears2k_headers.zip","air.hex",
             new PrepData() { int prep(Frame fr) {
               for( String s : ignored_aircols ) UKV.remove(fr.remove(s)._key);
               return fr.find("IsArrDelayed"); }
             });
    //basicGBM("../datasets/UCI/UCI-large/covtype/covtype.data","covtype.hex",
    //         new PrepData() {
    //           int prep(Frame fr) {
    //             assertEquals(581012,fr.numRows());
    //             for( int ign : IGNS )
    //               UKV.remove(fr.remove("C"+Integer.toString(ign))._key);
    //             // Covtype: predict on last column
    //             return fr.numCols()-1;
    //           }
    //         });
  }

  // covtype ignorable columns
  static final int IGNS[] = new int[] { 6, 7, 10,11 };

  // ==========================================================================
  public GBMModel basicGBM(String fname, String hexname, PrepData prep) {
    return basicGBM(fname, hexname, prep, false);
  }
  public GBMModel basicGBM(String fname, String hexname, PrepData prep, boolean validation) {
    File file = TestUtil.find_test_file(fname);
    if( file == null ) return null;  // Silently abort test if the file is missing
    Key fkey = NFSFileVec.make(file);
    Key dest = Key.make(hexname);
    GBM gbm = null;               // The Builder
    GBM.GBMModel gbmmodel = null; // The Model
    try {
      gbm = new GBM();
      Frame fr = gbm.source = ParseDataset2.parse(dest,new Key[]{fkey});
      UKV.remove(fkey);
      int idx = prep.prep(fr);
      if( idx < 0 ) { gbm.classification = false; idx = ~idx; }
      gbm.response = fr.vecs()[idx];
      gbm.ntrees = 4;
      gbm.max_depth = 4;
      gbm.min_rows = 1;
      gbm.nbins = 50;
      gbm.cols = new int[fr.numCols()];
      for( int i=0; i<gbm.cols.length; i++ ) gbm.cols[i]=i;
      gbm.validation = validation ? new Frame(gbm.source) : null;
      gbm.learn_rate = .2f;
      gbm.score_each_iteration=true;
      gbm.invoke();
      gbmmodel = UKV.get(gbm.dest());
      //System.out.println(gbmmodel.toJava());

      Frame preds = gbm.score(gbm.source);
      preds.delete();

      return gbmmodel;
    } finally {
      gbm.source.delete();              // Remove original hex frame key
      if (gbm.validation != null) gbm.validation.delete(); // Remove validation dataset if specified
      if( gbmmodel != null ) gbmmodel.delete(); // Remove the model
      gbm.remove();             // Remove GBM Job
    }
  }

  // Test-on-Train.  Slow test, needed to build a good model.
  @Test public void testGBMTrainTest() {
    File file1 = TestUtil.find_test_file("smalldata/gbm_test/ecology_model.csv");
    if( file1 == null ) return; // Silently ignore if file not found
    Key fkey1 = NFSFileVec.make(file1);
    Key dest1 = Key.make("train.hex");
    File file2 = TestUtil.find_test_file("smalldata/gbm_test/ecology_eval.csv");
    Key fkey2 = NFSFileVec.make(file2);
    Key dest2 = Key.make("test.hex");
    GBM gbm = null;               // The Builder
    GBM.GBMModel gbmmodel = null; // The Model
    Frame ftest = null, fpreds = null;
    try {
      gbm = new GBM();
      Frame fr = ParseDataset2.parse(dest1,new Key[]{fkey1});
      UKV.remove(fr.remove("Site")._key); // Remove unique ID; too predictive
      gbm.response = fr.vecs()[fr.find("Angaus")];   // Train on the outcome
      gbm.source = fr;
      gbm.ntrees = 5;
      gbm.max_depth = 10;
      gbm.learn_rate = 0.2f;
      gbm.min_rows = 10;
      gbm.nbins = 100;
      gbm.invoke();
      gbmmodel = UKV.get(gbm.dest());

      // Test on the train data
      ftest = ParseDataset2.parse(dest2,new Key[]{fkey2});
      fpreds = gbm.score(ftest);

      // Build a confusion matrix
      ConfusionMatrix CM = new ConfusionMatrix();
      CM.actual = ftest;
      CM.vactual = ftest.vecs()[ftest.find("Angaus")];
      CM.predict = fpreds;
      CM.vpredict = fpreds.vecs()[fpreds.find("predict")];
      CM.invoke();               // Start it, do it

      StringBuilder sb = new StringBuilder();
      CM.toASCII(sb);
      System.out.println(sb);

    } finally {
      gbm.source.delete(); // Remove the original hex frame key
      if( ftest  != null ) ftest .delete();
      if( fpreds != null ) fpreds.delete();
      if( gbmmodel != null ) gbmmodel.delete(); // Remove the model
      UKV.remove(gbm.response._key);
      gbm.remove();           // Remove GBM Job
    }
  }

  // Adapt a trained model to a test dataset with different enums
  @Test public void testModelAdapt() {
    File file1 = TestUtil.find_test_file("./smalldata/kaggle/KDDTrain.arff.gz");
    Key fkey1 = NFSFileVec.make(file1);
    Key dest1 = Key.make("KDDTrain.hex");
    File file2 = TestUtil.find_test_file("./smalldata/kaggle/KDDTest.arff.gz");
    Key fkey2 = NFSFileVec.make(file2);
    Key dest2 = Key.make("KDDTest.hex");
    GBM gbm = null;
    GBM.GBMModel gbmmodel = null; // The Model
    try {
      gbm = new GBM();
      gbm.source = ParseDataset2.parse(dest1,new Key[]{fkey1});
      gbm.response = gbm.source.vecs()[41]; // Response is col 41
      gbm.ntrees = 2;
      gbm.max_depth = 8;
      gbm.learn_rate = 0.2f;
      gbm.min_rows = 10;
      gbm.nbins = 50;
      gbm.invoke();
      gbmmodel = UKV.get(gbm.dest());

      // The test data set has a few more enums than the train
      Frame ftest = ParseDataset2.parse(dest2,new Key[]{fkey2});
      Frame preds = gbm.score(ftest);
      ftest.delete();
      preds.delete();

    } finally {
      if( gbmmodel != null ) gbmmodel.delete(); // Remove the model
      gbm.source.delete();      // Remove original hex frame key
      UKV.remove(gbm.response._key);
      gbm.remove();             // Remove GBM Job
    }
  }

  // A test of locking the input dataset during model building.
  @Test public void testModelLock() {
    GBM gbm = new GBM();
    try {
      Frame fr = gbm.source = parseFrame(Key.make("air.hex"),"./smalldata/airlines/allyears2k_headers.zip");
      for( String s : ignored_aircols ) UKV.remove(fr.remove(s)._key);
      int idx =  fr.find("IsArrDelayed");
      gbm.response = fr.vecs()[idx];
      gbm.ntrees = 10;
      gbm.max_depth = 5;
      gbm.min_rows = 1;
      gbm.nbins = 20;
      gbm.cols = new int[fr.numCols()];
      for( int i=0; i<gbm.cols.length; i++ ) gbm.cols[i]=i;
      gbm.learn_rate = .2f;
      gbm.fork();
      try { Thread.sleep(100); } catch( Exception _ ) { }

      try {
        fr.delete();            // Attempted delete while model-build is active
        H2O.fail();             // Should toss IAE instead of reaching here
      } catch( IllegalArgumentException _ ) {
      } catch( DException.DistributedException de ) {
        assertTrue( de.getMessage().indexOf("java.lang.IllegalArgumentException") != -1 );
      }

      GBM.GBMModel model = gbm.get();
      if( model != null ) model.delete();

    } finally {
      if( gbm.source != null ) gbm.source.delete(gbm.self(),0.0f); // Remove original hex frame key
      gbm.remove();             // Remove GBM Job
    }
  }

  //  MSE generated by GBM with/without validation dataset should be same
  @Test public void testModelMSEEqualityOnProstate() {
    final PrepData prostatePrep =
            new PrepData() {
              @Override int prep(Frame fr) {
                assertEquals(380,fr.numRows());
                // Remove patient ID vector
                UKV.remove(fr.remove("ID")._key);
                // Prostate: predict on CAPSULE
                return fr.find("CAPSULE");
              }
    };
    double[] mseWithoutVal = basicGBM("./smalldata/logreg/prostate.csv","prostate.hex", prostatePrep, false).errs;
    double[] mseWithVal    = basicGBM("./smalldata/logreg/prostate.csv","prostate.hex", prostatePrep, true ).errs;
    Assert.assertArrayEquals("GBM has to report same list of MSEs for run without/with validation dataset (which is equal to training data)", mseWithoutVal, mseWithVal, 0.0001);
  }

  @Test public void testModelMSEEqualityOnTitanic() {
    final PrepData titanicPrep =
            new PrepData() {
              @Override int prep(Frame fr) {
                assertEquals(1309,fr.numRows());
                // Airlines: predict on CAPSULE
                return fr.find("survived");
              }
    };
    double[] mseWithoutVal = basicGBM("./smalldata/titanicalt.csv","titanic.hex", titanicPrep, false).errs;
    double[] mseWithVal    = basicGBM("./smalldata/titanicalt.csv","titanic.hex", titanicPrep, true ).errs;
    Assert.assertArrayEquals("GBM has to report same list of MSEs for run without/with validation dataset (which is equal to training data)", mseWithoutVal, mseWithVal, 0.0001);
  }

}
