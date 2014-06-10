package hex.gbm;

import hex.gbm.GBM.GBMModel;
import static org.junit.Assert.*;

import org.junit.Test;

import water.*;
import water.fvec.Frame;
import water.fvec.Vec;

public class GBMCheckpointTest extends TestUtil {

  // Test for multinomial
  @Test public void testCheckpointReconstruction4Multinomial() {
    testCheckPointReconstruction("smalldata/iris/iris.csv", 4, true, 5, 3);
  }

  // Binomial model checkpointing
  @Test public void testCheckpointReconstruction4Binomial() {
    testCheckPointReconstruction("smalldata/logreg/prostate.csv", 1, true, 5, 3);
  }

  // And then test regression
  @Test public void testCheckpointReconstruction4Regression() {
    testCheckPointReconstruction("smalldata/logreg/prostate.csv", 8, false, 5, 3);
  }

  private void testCheckPointReconstruction(String dataset, int response, boolean classification, int ntreesInPriorModel, int ntreesInANewModel) {
    Frame f = parseFrame(dataset);
    GBMModel model = null;
    GBMModel modelFromCheckpoint = null;
    GBMModel modelFinal = null;
    try {
      Vec respVec = f.vec(response);
      // Build a model
      GBMWithHooks gbm = new GBMWithHooks();
      gbm.source = f;
      gbm.response = respVec;
      gbm.classification = classification;
      gbm.ntrees = ntreesInPriorModel;
      gbm.collectPoint = WhereToCollect.AFTER_BUILD;
      gbm.score_each_iteration = true;
      gbm.invoke();
      model = UKV.get(gbm.dest());

      // Build a checkpointed model
      GBMWithHooks gbmFromCheckpoint = new GBMWithHooks();
      gbmFromCheckpoint.source = f;
      gbmFromCheckpoint.response = respVec;
      gbmFromCheckpoint.classification = classification;
      gbmFromCheckpoint.ntrees = ntreesInANewModel;
      gbmFromCheckpoint.collectPoint = WhereToCollect.AFTER_RECONSTRUCTION;
      gbmFromCheckpoint.checkpoint = gbm.dest();
      gbmFromCheckpoint.score_each_iteration = true;
      gbmFromCheckpoint.invoke();
      modelFromCheckpoint = UKV.get(gbmFromCheckpoint.dest());

      // Check if reconstructed frame computation data are same
      assertArrayEquals("Tree data produced by drf run and reconstructed from a model do not match!",
                              gbm.treesCols, gbmFromCheckpoint.treesCols);
      // Build a model which contains old+new trees and compare prediction results
      GBM gbmFinal = new GBM();
      gbmFinal.source = f;
      gbmFinal.response = respVec;
      gbmFinal.classification = classification;
      gbmFinal.ntrees = ntreesInANewModel + ntreesInPriorModel;
      gbmFinal.score_each_iteration = true;
      gbmFinal.invoke();
      modelFinal = UKV.get(gbmFinal.dest());

      assertTreeModelEquals(modelFinal, modelFromCheckpoint);

    } finally {
      if (f!=null) f.delete();
      if (model!=null) model.delete();
      if (modelFromCheckpoint!=null) modelFromCheckpoint.delete();
      if (modelFinal!=null) modelFinal.delete();
    }
  }

  private enum WhereToCollect { NONE, AFTER_BUILD, AFTER_RECONSTRUCTION }

  static class GBMWithHooks extends GBM {
    WhereToCollect collectPoint;
    public float[][] treesCols;

    @Override protected void initWorkFrame(GBMModel initialModel, Frame fr) {
      super.initWorkFrame(initialModel, fr);
      if (collectPoint==WhereToCollect.AFTER_RECONSTRUCTION) {
        //debugPrintTreeColumns(fr);
        treesCols = collectTreeCols(fr);
      }
    }
    // Collect ntrees temporary results in expensive way
    @Override protected void cleanUp(Frame fr, Timer t_build) {
      if (collectPoint==WhereToCollect.AFTER_BUILD) {
        //debugPrintTreeColumns(fr);
        treesCols = collectTreeCols(fr);
      }
      super.cleanUp(fr, t_build);
    }
    private float[][] collectTreeCols(Frame fr) {
      float[][] r = new float[(int) _nrows][_nclass];
      for (int c=0; c<_nclass; c++) {
        Vec ctree = vec_tree(fr, c);
        for (int row=0; row<_nrows; row++) {
          r[row][c] = (float) ctree.at(row);
        }
      }
      return r;
    }
  }
}
