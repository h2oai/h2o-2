package hex.drf;

import java.util.Arrays;

import hex.drf.DRF.DRFModel;

import org.junit.Assert;
import org.junit.Test;

import water.*;
import water.fvec.*;

public class DRFCheckpointTest extends TestUtil {

  /** Test if reconstructed initial frame match the last iteration
   * of DRF model builder.
   *
   * <p>This test verify multinominal model.</p>
   */
  @Test public void testCheckpointReconstruction4Multinomial() {
    testCheckPointReconstruction("smalldata/iris/iris.csv", 4, true, 5);
  }

  /** Test if reconstructed initial frame match the last iteration
   * of DRF model builder.
   *
   * <p>This test verify binominal model.</p>
   */
  @Test public void testCheckpointReconstruction4Binomial() {
    testCheckPointReconstruction("smalldata/logreg/prostate.csv", 1, true, 5);
  }


  /** Test if reconstructed initial frame match the last iteration
   * of DRF model builder.
   *
   * <p>This test verify regression model.</p>
   */
  @Test public void testCheckpointReconstruction4Regression() {
    testCheckPointReconstruction("smalldata/logreg/prostate.csv", 8, false, 5);
  }

  private void testCheckPointReconstruction(String dataset, int response, boolean classification, int ntreesInPriorModel) {
    Frame f = parseFrame(dataset);
    DRFModel model = null;
    DRFModel modelFromCheckpoint = null;
    try {
      Vec respVec = f.vec(response);
      // Build a model
      DRFWithHooks drf = new DRFWithHooks();
      drf.source = f;
      drf.response = respVec;
      drf.classification = classification;
      drf.ntrees = ntreesInPriorModel;
      drf.collectPoint = WhereToCollect.AFTER_BUILD;
      drf.invoke();
      model = UKV.get(drf.dest());

      DRFWithHooks drfFromCheckpoint = new DRFWithHooks();
      drfFromCheckpoint.source = f;
      drfFromCheckpoint.response = respVec;
      drfFromCheckpoint.classification = classification;
      drfFromCheckpoint.ntrees = 1;
      drfFromCheckpoint.collectPoint = WhereToCollect.AFTER_RECONSTRUCTION;
      drfFromCheckpoint.checkpoint = drf.dest();
      drfFromCheckpoint.invoke();
      modelFromCheckpoint = UKV.get(drf.dest());

      System.err.println(Arrays.deepToString(drf.treesCols));
      System.err.println(Arrays.deepToString(drfFromCheckpoint.treesCols));

      Assert.assertArrayEquals("Tree data produced by drf run and reconstructed from a model do not match!",
                              drf.treesCols, drfFromCheckpoint.treesCols);

    } finally {
      if (f!=null) f.delete();
      if (model!=null) model.delete();
      if (modelFromCheckpoint!=null) modelFromCheckpoint.delete();
    }
  }

  private enum WhereToCollect { AFTER_BUILD, AFTER_RECONSTRUCTION }
  // Helper class with a hook to collect tree cols
  static class DRFWithHooks extends DRF {
    WhereToCollect collectPoint;
    public float[][] treesCols;

    @Override protected void initWorkFrame(DRFModel initialModel, Frame fr) {
      super.initWorkFrame(initialModel, fr);
      if (collectPoint==WhereToCollect.AFTER_RECONSTRUCTION) treesCols = collectTreeCols(fr);
    }
    // Collect ntrees temporary results in expensive way
    @Override protected void cleanUp(Frame fr, Timer t_build) {
      if (collectPoint==WhereToCollect.AFTER_BUILD) treesCols = collectTreeCols(fr);
      super.cleanUp(fr, t_build);
    }
    private float[][] collectTreeCols(Frame fr) {
      float[][] r = new float[(int) _nrows][_nclass];
      for (int c=0; c<_nclass; c++) {
        Vec ctree = vec_tree(fr, c);
        for (int row=0; row<_nrows; row++) {
          r[row][c] = ctree.at8(row);
        }
      }
      return r;
    }
  }
}
