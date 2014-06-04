package hex.drf;

import java.util.Arrays;
import java.util.Random;

import hex.gbm.DTree.TreeModel.CompressedTree;
import hex.gbm.DTreeUtils;
import water.*;
import water.fvec.Chunk;

/**
 * Computing oob scores over all trees and rows.
 *
 * <p>It prepares voter per tree and also marks
 * rows which were consider out-of-bag.</p>
 */
/* package */ class OOBScorer extends MRTask2<OOBScorer> {
  /* @IN */ final private int _ncols;
  /* @IN */ private final int _nclass;
  /* @IN */ private final float _rate;
  /* @IN */ private final Key[][] _treeKeys;

  private transient CompressedTree[][] _trees;

  public OOBScorer(int ncols, int nclass, float rate, Key[][] treeKeys) {
    _ncols = ncols;
    _nclass = nclass;
    _rate = rate;
    _treeKeys = treeKeys;
  }

  @Override protected void setupLocal() {
    int ntrees = _treeKeys.length;
    _trees = new CompressedTree[ntrees][];
    for (int t=0; t<ntrees; t++) {
      Key[] treek = _treeKeys[t];
      _trees[t] = new CompressedTree[treek.length];
      // FIXME remove get by introducing fetch class for all trees
      for (int i=0; i<treek.length; i++) {
        if (treek[i]!=null)
          _trees[t][i] = DKV.get(treek[i]).get();
      }
    }
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

  private Chunk chk_oobt(Chunk chks[]) { return chks[_ncols+1+_nclass+_nclass+_nclass]; }
  private Chunk chk_tree(Chunk chks[], int c) { return chks[_ncols+1+c]; }
  private Chunk chk_resp( Chunk chks[] ) { return chks[_ncols]; }

  private void score0(double data[], float preds[], CompressedTree[] ts) {
    DTreeUtils.scoreTree(data, preds, ts);
  }
}
