package hex.gbm;

import junit.framework.Assert;
import hex.gbm.GBM.GBMModel;
import hex.trees.TreeTestWithBalanceAndCrossVal;
import org.junit.BeforeClass;
import water.*;
import water.fvec.Frame;
import water.fvec.Vec;

/** Test for advanced GBM workflows including data rebalancing and cross validation. */
public class GBMTest2 extends TreeTestWithBalanceAndCrossVal {
  @BeforeClass public static void stall() { stall_till_cloudsize(1); }

  @Override protected void testBalanceWithCrossValidation(String dataset, int response, int[] ignored_cols, int ntrees, int nfolds) {
    Frame f = parseFrame(dataset);
    GBMModel model = null;
    GBM gbm = new GBM();
    try {
      Vec respVec = f.vec(response);
      // Build a model
      gbm.source = f;
      gbm.response = respVec;
      gbm.ignored_cols = ignored_cols;
      gbm.classification = true;
      gbm.ntrees = ntrees;
      gbm.balance_classes = true;
      gbm.n_folds = nfolds;
      gbm.keep_cross_validation_splits = false;
      gbm.invoke();
      Assert.assertEquals("Number of cross validation model is wrond!", nfolds, gbm.xval_models.length);
      model = UKV.get(gbm.dest());
      Assert.assertTrue(model.get_params().state == Job.JobState.DONE); //HEX-1817
    } finally {
      if (f!=null) f.delete();
      if (model!=null) {
        if (gbm.xval_models!=null) {
          for (Key k : gbm.xval_models) {
            Model m = UKV.get(k);
            m.delete();
          }
        }
        model.delete();
      }
    }
  }
}
