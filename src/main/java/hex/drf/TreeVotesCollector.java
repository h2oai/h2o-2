package hex.drf;

import hex.ShuffleTask;
import hex.gbm.DTree.TreeModel;
import hex.gbm.DTree.TreeModel.CompressedTree;

import java.util.Arrays;
import java.util.Random;

import water.Iced;
import water.MRTask2;
import water.fvec.Chunk;
import water.fvec.Frame;
import water.util.ModelUtils;
import water.util.Utils;

/** Score given tree model and preserve errors per tree.
 *
 * This is different from Model.score() function since the MR task
 * uses inverse loop: first over all trees and over all rows in chunk.
 */
public class TreeVotesCollector extends MRTask2<TreeVotesCollector> {
  /* @IN */ final private float     _rate;
  /* @IN */       private CompressedTree[/*N*/][/*nclasses*/] _trees; // FIXME: Pass only tree-keys since serialized trees are passed over wire !!!
  /* @IN */ final private int       _var;
  /* @IN */ final private boolean   _oob;
  /* @IN */ final private int       _ncols;
  /* @IN */ final private int       _nclasses;

  /* @INOUT */ private final int _ntrees;
  /* @OUT */ private long[/*ntrees*/]   _treeCVotes; // Number of correct votes per tree
  /* @OUT */ private long[/*ntrees*/]   _nrows;      // Number of scored row per tree

  private TreeVotesCollector(CompressedTree[/*N*/][/*nclasses*/] trees, int nclasses, int ncols, float rate, int variable) {
    assert trees.length > 0;
    assert nclasses == trees[0].length;
    _trees = trees; _ncols = ncols;
    _rate = rate; _var = variable;
    _oob = true; _ntrees = trees.length;
    _nclasses = nclasses;
  }

  @Override public void map(Chunk[] chks) {
    double[] data = new double[_ncols];
    float [] preds = new float[_nclasses+1];
    Chunk cresp = chk_resp(chks);
    int   nrows = cresp._len;
    int   [] oob = new int[1+(int)((1f-_rate)*nrows*1.2f)];
    int   [] soob = null;

    // prepare output data
    _nrows = new long[_ntrees];
    _treeCVotes = new long[_ntrees];
    long seedForOob = ShuffleTask.seed(cresp.cidx()); // seed for shuffling oob samples
    for( int tidx=0; tidx<_ntrees; tidx++) { // tree
      // OOB RNG for this tree
      Random rng = rngForTree(_trees[tidx], cresp.cidx());
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
        score0(data, preds, _trees[tidx]);
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
    // Clean-up
    _trees = null;
  }
  @Override public void reduce( TreeVotesCollector t ) { Utils.add(_treeCVotes,t._treeCVotes); Utils.add(_nrows, t._nrows); }

  public TreeVotes result() {
    return new TreeVotes(_treeCVotes, _nrows, _ntrees);
  }
  /* This is a copy of score0 method from DTree:615 */
  private void score0(double data[], float preds[], CompressedTree[] ts) {
    for( int c=0; c<ts.length; c++ )
      if( ts[c] != null )
        preds[ts.length==1?0:c+1] += ts[c].score(data);
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
  public static TreeVotes collect(TreeModel tmodel, Frame f, int ncols, float rate, int variable) {
    CompressedTree[][] trees = new CompressedTree[tmodel.ntrees()][];
    for (int tidx = 0; tidx < tmodel.ntrees(); tidx++) trees[tidx] = tmodel.ctree(tidx);
    return new TreeVotesCollector(trees, tmodel.nclasses(), ncols, rate, variable).doAll(f).result();
  }

  public static TreeVotes collect(CompressedTree[/*nclass*/] tree, int nclasses, Frame f, int ncols, float rate, int variable) {
    return new TreeVotesCollector(new CompressedTree[][] {tree}, nclasses, ncols, rate, variable).doAll(f).result();
  }

  private static final class DummyRandom extends Random {
    @Override public final float nextFloat() { return 1.0f; }
  }

  public static class TreeVotes extends Iced {
    private int _ntrees; // Actual number of trees which votes are stored in this object
    private long[/*ntrees*/]   _treeCVotes; // Number of correct votes per tree
    private long[/*ntrees*/]   _nrows;      // Number of scored row per tree

    public TreeVotes(int initialCapacity) {
      _treeCVotes = new long[initialCapacity];
      _nrows = new long[initialCapacity];
    }
    public TreeVotes(long[] treeCVotes, long[] nrows, int ntrees) {
      _treeCVotes = treeCVotes; _nrows = nrows; _ntrees = ntrees;
    }

    /** Returns number of rows which were used during voting per individual tree. */
    public final long[] nrows()      { return _nrows; }
    /** Returns number of positive votes per tree. */
    public final long[] treeCVotes() { return _treeCVotes; }
    /* Returns number of voting predictors */
    public final int    npredictors() { return _ntrees; }

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

    /** Append a tree votes to a list of trees. */
    public TreeVotes append(long rightVotes, long allRows) {
      assert _treeCVotes.length > _ntrees && _treeCVotes.length == _nrows.length : "TreeVotes inconsistency!";
      _treeCVotes[_ntrees] = rightVotes;
      _nrows[_ntrees] = allRows;
      _ntrees++;
      return this;
    }

    public TreeVotes append(final TreeVotes tv) {
      for (int i=0; i<tv.npredictors(); i++) {
        append(tv._treeCVotes[i], tv._nrows[i]);
      }
      return this;
    }

    @Override public String toString() {
      final int maxLen = 10;
      StringBuilder builder = new StringBuilder();
      builder
          .append("TreeVotes [_trees=").append(_ntrees)
          .append("\n, _treeCVotes=")
          .append(
              _treeCVotes != null ? Arrays.toString(Arrays.copyOf(_treeCVotes, Math.min(_ntrees, maxLen)))
                  : null).append("\n, _nrows=")
          .append(_nrows != null ? Arrays.toString(Arrays.copyOf(_nrows, Math.min(_ntrees, maxLen))) : null)
          .append("]");
      return builder.toString();
    }
  }
}
