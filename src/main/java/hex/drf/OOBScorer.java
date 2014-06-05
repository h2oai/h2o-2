package hex.drf;

import java.util.Arrays;
import java.util.Random;

import hex.gbm.DTree.TreeModel.CompressedTree;
import hex.gbm.*;
import water.*;
import water.fvec.Chunk;

/**
 * Computing oob scores over all trees and rows
 * and reconstructing <code>ntree_id, oobt</code> fields in given frame.
 *
 * <p>It prepares voter per tree and also marks
 * rows which were consider out-of-bag.</p>
 */
/* package */ class OOBScorer extends DTreeScorer<OOBScorer> {

  /* @IN */ final protected float _rate;

  public OOBScorer(int ncols, int nclass, float rate, Key[][] treeKeys) {
    super(ncols,nclass,treeKeys);
    _rate = rate;
  }

  @Override public void map(Chunk[] chks) {
    double[] data = new double[_ncols];
    float [] preds = new float[_nclass+1];
    int ntrees = _trees.length;
    Chunk coobt = chk_oobt(chks);
    Chunk cys   = chk_resp(chks);
    for( int tidx=0; tidx<ntrees; tidx++) { // tree
      // OOB RNG for this tree
      Random rng = rngForTree(_trees[tidx], coobt.cidx());
      for (int row=0; row<coobt._len; row++) {
        if( rng.nextFloat() >= _rate || Double.isNaN(cys.at0(row)) ) {
          // Mark oob row and store number of trees voting for this row (only for regression)
          coobt.set0(row, _nclass>1?1:coobt.at0(row)+1);
          // Make a prediction
          for (int i=0;i<_ncols;i++) data[i] = chks[i].at0(row);
          Arrays.fill(preds, 0);
          score0(data, preds, _trees[tidx]);
          if (_nclass==1) preds[1]=preds[0]; // Only for regression, keep consistency
          // Write tree predictions
          for (int c=0;c<_nclass;c++) { // over all class
            if (preds[1+c] != 0) {
              Chunk ctree = chk_tree(chks, c);
              ctree.set0(row, (float)(ctree.at0(row) + preds[1+c]));
            }
          }
        }
      }
    }
  }

  private Random rngForTree(CompressedTree[] ts, int cidx) {
    return ts[0].rngForChunk(cidx); // k-class set of trees shares the same random number
  }
}
