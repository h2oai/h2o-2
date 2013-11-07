package hex.drf;

import java.util.Arrays;
import java.util.Random;

import hex.gbm.DTree.TreeModel;
import hex.gbm.DTree.TreeModel.CompressedTree;
import water.MRTask2;
import water.fvec.Chunk;
import water.fvec.Frame;
import water.util.Utils;

/** Score given tree model and preserve errors per tree.
 *
 * This is different from Model.score() function since the MR task
 * uses inverse loop: first over all trees and over all rows in chunk.
 */
public class TreeModelCM extends MRTask2<TreeModelCM> {
  final private boolean   _oob;
  final private float     _rate;
  final private TreeModel _tmodel;

  private long[]   _treeCVotes; // Number of correct votes per tree
  private long[]   _nrows;     // Number of scored row per tree

  /** Returns number of rows which were used during voting per individual tree. */
  public final long[] nrows()      { return _nrows; }
  /** Returns number of positive votes per tree. */
  public final long[] treeCVotes() { return _treeCVotes; }
  /** Returns accuracy per individual trees. */
  public final double[] accuracy()     {
    assert _nrows.length == _treeCVotes.length;
    double[] r = new double[_nrows.length];
    for (int i=0;i<_nrows.length;i++) r[i] = (double) _treeCVotes[i] / _nrows[i];
    return r;
  }

  TreeModelCM(TreeModel tmodel, float rate) { _tmodel = tmodel; _rate = rate; _oob = true; }

  @Override public void map(Chunk[] chks) {
    int ntrees = _tmodel.numTrees();
    double[] data = new double[chks.length-1];
    float [] preds = new float[_tmodel.nclasses()];
    // prepare output data
    _nrows = new long[ntrees];
    _treeCVotes = new long[ntrees];
    for( int tidx=0; tidx<ntrees; tidx++) { // tree
      Chunk cresp = chk_resp(chks);
      Random rng = rngForTree(_tmodel.treeBits[tidx], cresp.cidx());
      for(int row=0; row<cresp._len; row++) {
        if (rng.nextFloat()>=_rate) { // it is out-of-bag row
          // Do scoring:
          // - prepare a row data
          for (int i=0;i<chks.length-1;i++) data[i] = chks[i].at0(row);
          // - score data
          Arrays.fill(preds, 0);
          _tmodel.score0(data, preds, tidx);
          // - derive a prediction
          int pred = Utils.maxIndex(preds);
          assert preds[pred] > 0 : "There should be a vote for at least one class.";
          // - collect only correct votes
          if (pred==cresp.at80(row)) _treeCVotes[tidx]++;
          // - collect rows which were used for voting
          _nrows[tidx]++;
        }
      }
    }
  }
  @Override public void reduce( TreeModelCM t ) { Utils.add(_treeCVotes,t._treeCVotes); Utils.add(_nrows, t._nrows); }

  private Chunk chk_resp( Chunk chks[] ) { return chks[chks.length-1]; }
  private Random rngForTree(CompressedTree[] ts, int cidx) {
    return _oob ? ts[0].rngForChunk(cidx) : new DummyRandom();
  }

  public static TreeModelCM varimp(TreeModel tmodel, Frame f, float rate) {
    return new TreeModelCM(tmodel, rate).doAll(f);
  }

  public static final class DummyRandom extends Random {
    @Override public float nextFloat() { return 1.0f; }
  }
}
