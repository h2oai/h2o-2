package hex.drf;

import hex.ShuffleTask;
import hex.gbm.DTree.TreeModel;
import hex.gbm.DTree.TreeModel.CompressedTree;

import java.util.Arrays;
import java.util.Random;

import water.MRTask2;
import water.fvec.*;
import water.util.ModelUtils;
import water.util.Utils;

/** Score given tree model and preserve errors per tree.
 *
 * This is different from Model.score() function since the MR task
 * uses inverse loop: first over all trees and over all rows in chunk.
 */
public class TreeVotes extends MRTask2<TreeVotes> {
  /* @IN */ final private float     _rate;
  /* @IN */ final private TreeModel _tmodel; // Pased over wire !!!
  /* @IN */ final private int       _var;
  /* @IN */ final private boolean   _oob;
  /* @IN */ final private int       _ncols;

  /* @OUT */ private long[]   _treeCVotes; // Number of correct votes per tree
  /* @OUT */ private long[]   _nrows;      // Number of scored row per tree

  /** Returns number of rows which were used during voting per individual tree. */
  public final long[] nrows()      { return _nrows; }
  /** Returns number of positive votes per tree. */
  public final long[] treeCVotes() { return _treeCVotes; }
  /* Returns number of voting predictors */
  public final int    npredictors() { return _tmodel.numTrees(); }
  /** Returns accuracy per individual trees. */
  public final double[] accuracy()  {
    assert _nrows.length == _treeCVotes.length;
    double[] r = new double[_nrows.length];
    // Average of all trees
    for (int tidx=0; tidx<_nrows.length; tidx++) r[tidx] = ((double) _treeCVotes[tidx]) / _nrows[tidx];
    return r;
  }

  /** Compute variable importance with respect to given votes.
   * The given {@link TreeVotes} object represents correct votes.
   * This object represents votes over shuffled data.
   *
   * @param right individual tree voters performed over not shuffled data.
   * @return computed importance and standard deviation
   */
  public final double[] imp(TreeVotes right) {
    assert npredictors() == right.npredictors();
    int ntrees = npredictors();
    double imp = 0;
    double sd  = 0;
    // Over all trees
    for (int tidx = 0; tidx < ntrees; tidx++) {
      assert right.nrows()[tidx] == nrows()[tidx];
      double delta = ((double) (right.treeCVotes()[tidx] - treeCVotes()[tidx])) / nrows()[tidx];
      imp += delta;
      sd  += delta * delta;
    }
    double av = imp / ntrees;
    double csd = Math.sqrt( (sd/ntrees - av*av) / ntrees );
    return new double[] { av, csd};
  }

  TreeVotes(TreeModel tmodel, int ncols, float rate, int variable) { _tmodel = tmodel; _ncols = ncols; _rate = rate; _var = variable; _oob = true; }

  @Override public void map(Chunk[] chks) {
    int ntrees = _tmodel.numTrees();
    double[] data = new double[_ncols];
    float [] preds = new float[_tmodel.nclasses()+1];
    Chunk cresp = chk_resp(chks);
    int   nrows = cresp._len;
    int   [] oob = new int[1+(int)((1f-_rate)*nrows*1.2f)];
    int   [] soob = null;

    // prepare output data
    _nrows = new long[ntrees];
    _treeCVotes = new long[ntrees];
    long seedForOob = ShuffleTask.seed(cresp.cidx()); // seed for shuffling oob samples
    for( int tidx=0; tidx<ntrees; tidx++) { // tree
      // OOB RNG for this tree
      Random rng = rngForTree(_tmodel.treeBits[tidx], cresp.cidx());
      // Collect oob rows and permutate them
      oob = ModelUtils.sampleOOBRows(nrows, _rate, rng);
      int oobcnt = oob[0]; // Get number of sample rows
      if (_var>=0) {
        if (soob==null || soob.length < oobcnt) soob = new int[oobcnt];
        Utils.shuffleArray(oob, oobcnt, soob, seedForOob, 1); // Shuffle array and copy results into <code>soob</code>
      }
      for(int j = 1; j < 1+oobcnt; j++) {
        int row = oob[j];
        // Do scoring:
        // - prepare a row data
        for (int i=0;i<_ncols;i++) data[i] = chks[i].at0(row); // 1+i - one free is expected by prediction
        // - permute variable
        if (_var>=0) data[_var] = chks[_var].at0(soob[j-1]);
        else assert soob==null;
        // - score data
        Arrays.fill(preds, 0);
        _tmodel.score0(data, preds, tidx);
        // - derive a prediction
        int pred = ModelUtils.getPrediction(preds, data);
        int actu = (int) cresp.at80(row);
        // assert preds[pred] > 0 : "There should be a vote for at least one class.";
        // - collect only correct votes
        if (pred == actu) _treeCVotes[tidx]++;
        // - collect rows which were used for voting
        _nrows[tidx]++;
        //if (_var<0) System.err.println("VARIMP OOB row: " + (cresp._start+row) + " : " + Arrays.toString(data) + " tree/actu: " + pred + "/" + actu);
      }
    }
  }
  @Override public void reduce( TreeVotes t ) { Utils.add(_treeCVotes,t._treeCVotes); Utils.add(_nrows, t._nrows); }


  @Override public String toString() {
    final int maxLen = 10;
    StringBuilder builder = new StringBuilder();
    builder
        .append("TreeVotes [_rate=").append(_rate).append("\n, _var=")
        .append(_var).append("\n, _oob=").append(_oob)
        .append("\n, _trees=").append(_treeCVotes.length)
        .append("\n, _treeCVotes=")
        .append(
            _treeCVotes != null ? Arrays.toString(Arrays.copyOf(_treeCVotes, Math.min(_treeCVotes.length, maxLen)))
                : null).append("\n, _nrows=")
        .append(_nrows != null ? Arrays.toString(Arrays.copyOf(_nrows, Math.min(_nrows.length, maxLen))) : null)
        .append("]");
    return builder.toString();
  }
  private Chunk chk_resp( Chunk chks[] ) { return chks[_ncols]; }

  private Random rngForTree(CompressedTree[] ts, int cidx) {
    return _oob ? ts[0].rngForChunk(cidx) : new DummyRandom(); // k-class set of trees shares the same random number
  }

  /**
   *
   * @param tmodel
   * @param f     input frame (should be already adapted to a model) - ncols-features + response
   * @param ncols number of features
   * @param rate
   * @param variable
   * @return
   */
  public static TreeVotes varimp(TreeModel tmodel, Frame f, int ncols, float rate, int variable) {
    return new TreeVotes(tmodel, ncols, rate, variable).doAll(f);
  }

  private static final class DummyRandom extends Random {
    @Override public final float nextFloat() { return 1.0f; }
  }
}
