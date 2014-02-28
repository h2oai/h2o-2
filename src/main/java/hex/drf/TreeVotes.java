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

  TreeVotes(TreeModel tmodel, float rate, int variable) { _tmodel = tmodel; _rate = rate; _var = variable; _oob = true; }

  @Override public void map(Chunk[] chks) {
    int ntrees = _tmodel.numTrees();
    double[] data = new double[chks.length-1];
    float [] preds = new float[_tmodel.nclasses()+1];
    Chunk cresp = chk_resp(chks);
    int   nrows = cresp._len;
    int   [] oob = new int[(int)((1f-_rate)*nrows*1.2f)];
    int   [] soob = null;

    // prepare output data
    _nrows = new long[ntrees];
    _treeCVotes = new long[ntrees];
    long seedForOob = ShuffleTask.seed(cresp.cidx()); // seed for shuffling oob samples
    for( int tidx=0; tidx<ntrees; tidx++) { // tree
      // OOB RNG for this tree
      Random rng = rngForTree(_tmodel.treeBits[tidx], cresp.cidx());
      // Collect oob rows and permutate them
      int oobcnt = 0; // Number of oob rows
      Arrays.fill(oob, 0);
      for(int row = 0; row < nrows; row++) {
        if (rng.nextFloat()>=_rate) { // it is out-of-bag row
          oob[oobcnt++] = row;
          if (oobcnt>=oob.length) oob = Arrays.copyOf(oob, (int)(1.2f*oob.length));
        }
      }
      if (soob==null || soob.length < oobcnt) soob = new int[oobcnt];
      Utils.shuffleArray(oob, oobcnt, soob, seedForOob);
      //System.err.println("-> " + cresp.cidx() + " : " + Arrays.toString(soob));
      for(int row = 0; row < oobcnt; row++) {
        // Do scoring:
        // - prepare a row data
        for (int i=0;i<chks.length-1;i++) data[i] = chks[i].at0(oob[row]);
        // - permute variable
        if (_var>=0) data[_var] = chks[_var].at0(soob[row]);
        // - score data
        Arrays.fill(preds, 0);
        _tmodel.score0(data, preds, tidx);
        // - derive a prediction
        int pred = ModelUtils.getPrediction(preds, data);
        // assert preds[pred] > 0 : "There should be a vote for at least one class.";
        // - collect only correct votes
        if (pred==cresp.at80(row)) _treeCVotes[tidx]++;
        // - collect rows which were used for voting
        _nrows[tidx]++;
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
  static Chunk chk_resp( Chunk chks[] ) { return chks[chks.length-1]; }
  static Vec   vec_resp( Frame f      ) { return f.vecs()[f.vecs().length-1]; }

  private Random rngForTree(CompressedTree[] ts, int cidx) {
    return _oob ? ts[0].rngForChunk(cidx) : new DummyRandom(); // k-class set of trees shares the same random number
  }

  public static TreeVotes varimp(TreeModel tmodel, Frame f, float rate, int variable) {
    Frame todelete = new Frame();
    try {
      Frame ff = new Frame(f);
      Vec   fr = ff.remove(ff.numCols()-1);
      Vec  efr = fr.toEnum();
      ff.add("__response__", efr);
      todelete.add("__response__", efr);
      return new TreeVotes(tmodel, rate, variable).doAll(ff);
    } finally {
      todelete.delete();
    }
  }

  private static final class DummyRandom extends Random {
    @Override public final float nextFloat() { return 1.0f; }
  }
}
