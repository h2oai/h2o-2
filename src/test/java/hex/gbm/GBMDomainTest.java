package hex.gbm;


import java.io.File;

import org.junit.BeforeClass;
import org.junit.Test;

import water.*;
import water.fvec.*;

public class GBMDomainTest extends TestUtil {

  private abstract class PrepData { abstract Vec prep(Frame fr); }

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
  @Test public void testModelAdapt() {
    runAndScoreGBM(
        "./smalldata/test/classifier/coldom_train_1.csv",
        "./smalldata/test/classifier/coldom_test_1.csv",
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
    runAndScoreGBM(
        "./smalldata/test/classifier/coldom_train_1.csv",
        "./smalldata/test/classifier/coldom_test_1_2.csv",
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
    GBM.GBMModel model = null;
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
      gbm.invoke();
      model = UKV.get(gbm.dest());

      // The test data set has a few more enums than the train
      Frame ftest = ParseDataset2.parse(dest2,new Key[]{fkey2});
      preds = gbm.score(ftest);
      // Delete test frame
      ftest.delete();

    } catch (Throwable t) {
      t.printStackTrace();
    } finally {
      gbm.source.delete();      // Remove original hex frame key
      if( preds != null ) preds.delete();
      if( model != null ) model.delete();
      UKV.remove(gbm.response._key);
      gbm.remove();             // Remove GBM Job
    }
  }

}
