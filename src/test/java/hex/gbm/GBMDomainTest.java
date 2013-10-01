package hex.gbm;


import java.io.File;

import org.junit.BeforeClass;
import org.junit.Test;

import water.*;
import water.fvec.*;

public class GBMDomainTest extends TestUtil {

  private abstract class PrepData { abstract Vec prep(Frame fr); }

  @BeforeClass public static void stall() { stall_till_cloudsize(1); }

  @Test public void testModelAdapt() {
    runAndScoreGBM(
        "./smalldata/test/classifier/coldom_train.csv",
        "./smalldata/test/classifier/coldom_test.csv",
        new PrepData() { @Override Vec prep(Frame fr) { return fr.vecs()[fr.numCols()-1]; } });
  }

  // Adapt a trained model to a test dataset with different enums
  void runAndScoreGBM(String train, String test, PrepData prepData) {
    File file1 = TestUtil.find_test_file(train);
    Key fkey1 = NFSFileVec.make(file1);
    Key dest1 = Key.make("train.hex");
    File file2 = TestUtil.find_test_file(test);
    Key fkey2 = NFSFileVec.make(file2);
    Key dest2 = Key.make("test.hex");
    GBM gbm = null;
    Frame preds = null;
    try {
      gbm = new GBM();
      gbm.source = ParseDataset2.parse(dest1,new Key[]{fkey1});
      gbm.response = prepData.prep(gbm.source);
      gbm.ntrees = 2;
      gbm.max_depth = 3;
      gbm.learn_rate = 0.2f;
      gbm.min_rows = 10;
      gbm.nbins = 1024;
      gbm.cols =  new int[] {0,1,2};
      gbm.run();

      // The test data set has a few more enums than the train
      Frame ftest = ParseDataset2.parse(dest2,new Key[]{fkey2});
      preds = gbm.score(ftest);
      // Delete test frame
      ftest.remove();

      System.err.println(preds);

    } catch (Throwable t) {
      t.printStackTrace();
    } finally {
      UKV.remove(fkey1);
      UKV.remove(dest1);        // Remove original hex frame key
      UKV.remove(fkey2);
      UKV.remove(dest2);
      if( gbm != null ) {
        UKV.remove(gbm.dest()); // Remove the model
        UKV.remove(gbm.response._key);
        gbm.remove();           // Remove GBM Job
        if( preds != null ) preds.remove();
      }
    }
  }

}
