package hex.singlenoderf;

import hex.trees.TreeTestWithBalanceAndCrossVal;
import junit.framework.Assert;
import water.*;
import water.fvec.Frame;
import water.fvec.Vec;

public class SpeeDRFTest2 extends TreeTestWithBalanceAndCrossVal {

//  @Override public void testWeatherDataset() { }
//  @Override public void testCarBalanceAndCrossValidation() { }
//  @Override public void testCovtypeBalanceAndCrossValidation() { }

  @Override protected void testBalanceWithCrossValidation(String dataset, int response, int[] ignored_cols, int ntrees, int nfolds) {
    Frame f = parseFrame(dataset);
    SpeeDRFModel model = null;
    SpeeDRF spdrf = new SpeeDRF();
    try {
      Vec respVec = f.vec(response);
      // Build a model
      spdrf.source = f;
      spdrf.response = respVec;
      spdrf.ignored_cols = ignored_cols;
      spdrf.classification = true;
      spdrf.ntrees = ntrees;
      spdrf.balance_classes = true;
      spdrf.n_folds = nfolds;
      spdrf.keep_cross_validation_splits = false;
      spdrf.invoke();
      Assert.assertEquals("Number of cross validation model is wrong!", nfolds, spdrf.xval_models.length);
      model = UKV.get(spdrf.dest());
    } finally {
      if (f!=null) f.delete();
      if (model!=null) {
        if (spdrf.xval_models!=null) {
          for (Key k : spdrf.xval_models) {
            Model m = UKV.get(k);
            m.delete();
          }
        }
        model.delete();
      }
    }
  }
}
