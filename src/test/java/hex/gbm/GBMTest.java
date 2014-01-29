package hex.gbm;

import org.junit.BeforeClass;
import org.junit.Test;
import water.Key;
import water.TestUtil;
import water.UKV;
import water.api.ConfusionMatrix;
import water.fvec.Frame;
import water.fvec.NFSFileVec;
import water.fvec.ParseDataset2;

import java.io.File;

import static org.junit.Assert.assertEquals;

public class GBMTest extends TestUtil {

  @BeforeClass public static void stall() { stall_till_cloudsize(1); }

  private abstract class PrepData { abstract int prep(Frame fr); }

  static final String ignored_aircols[] = new String[] { "DepTime", "ArrTime", "AirTime", "ArrDelay", "DepDelay", "TaxiIn", "TaxiOut", "Cancelled", "CancellationCode", "Diverted", "CarrierDelay", "WeatherDelay", "NASDelay", "SecurityDelay", "LateAircraftDelay", "IsDepDelayed"};

  @Test
  public void testBasicGBM() {
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
  public void basicGBM(String fname, String hexname, PrepData prep) {
    File file = TestUtil.find_test_file(fname);
    if( file == null ) return;  // Silently abort test if the file is missing
    Key fkey = NFSFileVec.make(file);
    Key dest = Key.make(hexname);
    GBM gbm = null;
    Frame fr = null;
    try {
      gbm = new GBM();
      gbm.source = fr = ParseDataset2.parse(dest,new Key[]{fkey});
      UKV.remove(fkey);
      int idx = prep.prep(fr);
      if( idx < 0 ) { gbm.classification = false; idx = ~idx; }
      String rname = fr._names[idx];
      gbm.response = fr.vecs()[idx];
      fr.remove(idx);           // Move response to the end
      fr.add(rname,gbm.response);
      gbm.ntrees = 4;
      gbm.max_depth = 4;
      gbm.min_rows = 1;
      gbm.nbins = 50;
      gbm.cols = new int[fr.numCols()];
      for( int i=0; i<gbm.cols.length; i++ ) gbm.cols[i]=i;
      gbm.learn_rate = .2f;
      gbm.invoke();

      fr = gbm.score(gbm.source);

      GBM.GBMModel gbmmodel = UKV.get(gbm.dest());
      //System.out.println(gbmmodel.toJava());

    } finally {
      UKV.remove(dest);         // Remove original hex frame key
      if( gbm != null ) {
        UKV.remove(gbm.dest()); // Remove the model
        UKV.remove(gbm.response._key);
        gbm.remove();           // Remove GBM Job
        if( fr != null ) fr.remove();
      }
    }
  }

  // Test-on-Train.  Slow test, needed to build a good model.
  @Test public void testGBMTrainTest() {
    File file1 = TestUtil.find_test_file("..//classifcation1Train.txt");
    if( file1 == null ) return; // Silently ignore if file not found
    Key fkey1 = NFSFileVec.make(file1);
    Key dest1 = Key.make("train.hex");
    File file2 = TestUtil.find_test_file("..//classification1Test.txt");
    Key fkey2 = NFSFileVec.make(file2);
    Key dest2 = Key.make("test.hex");
    GBM gbm = null;
    Frame fr = null, fpreds = null;
    try {
      gbm = new GBM();
      fr = ParseDataset2.parse(dest1,new Key[]{fkey1});
      UKV.remove(fkey1);
      UKV.remove(fr.remove("agentId")._key); // Remove unique ID; too predictive
      gbm.response = fr.remove("outcome");  // Train on the outcome
      gbm.source = fr;
      gbm.ntrees = 5;
      gbm.max_depth = 10;
      gbm.learn_rate = 0.2f;
      gbm.min_rows = 10;
      gbm.nbins = 100;
      gbm.invoke();

      // Test on the train data
      Frame ftest = ParseDataset2.parse(dest2,new Key[]{fkey2});
      UKV.remove(fkey2);
      fpreds = gbm.score(ftest);

      // Build a confusion matrix
      ConfusionMatrix CM = new ConfusionMatrix();
      CM.actual = ftest;
      CM.vactual = ftest.vecs()[ftest.find("outcome")];
      CM.predict = fpreds;
      CM.vpredict = fpreds.vecs()[fpreds.find("predict")];
      CM.serve();               // Start it, do it

      System.out.println(CM.toASCII(new StringBuilder()));

    } finally {
      UKV.remove(dest1);        // Remove original hex frame key
      UKV.remove(fkey2);
      UKV.remove(dest2);
      if( gbm != null ) {
        UKV.remove(gbm.dest()); // Remove the model
        UKV.remove(gbm.response._key);
        gbm.remove();           // Remove GBM Job
      }
      if( fr != null ) fr.remove();
      if( fpreds != null ) fpreds.remove();
    }
  }

  // Adapt a trained model to a test dataset with different enums
  /*@Test*/ public void testModelAdapt() {
    File file1 = TestUtil.find_test_file("./smalldata/kaggle/KDDTrain.arff.gz");
    Key fkey1 = NFSFileVec.make(file1);
    Key dest1 = Key.make("KDDTrain.hex");
    File file2 = TestUtil.find_test_file("./smalldata/kaggle/KDDTest.arff.gz");
    Key fkey2 = NFSFileVec.make(file2);
    Key dest2 = Key.make("KDDTest.hex");
    GBM gbm = null;
    Frame fr = null;
    try {
      gbm = new GBM();
      gbm.source = ParseDataset2.parse(dest1,new Key[]{fkey1});
      UKV.remove(fkey1);
      gbm.response = gbm.source.remove(41); // Response is col 41
      gbm.ntrees = 2;
      gbm.max_depth = 8;
      gbm.learn_rate = 0.2f;
      gbm.min_rows = 10;
      gbm.nbins = 50;
      gbm.invoke();

      // The test data set has a few more enums than the train
      Frame ftest = ParseDataset2.parse(dest2,new Key[]{fkey2});
      Frame preds = gbm.score(ftest);

    } finally {
      UKV.remove(dest1);        // Remove original hex frame key
      if( gbm != null ) {
        UKV.remove(gbm.dest()); // Remove the model
        UKV.remove(gbm.response._key);
        gbm.remove();           // Remove GBM Job
        if( fr != null ) fr.remove();
      }
    }
  }
}
