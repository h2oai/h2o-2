package hex.gbm;

import hex.gbm.DTree.*;
import hex.rng.MersenneTwisterRNG;
import java.util.Arrays;
import java.util.Random;
import water.*;
import water.H2O.H2OCountedCompleter;
import water.fvec.*;
import water.util.Log.Tag.Sys;
import water.util.Log;

// Random Forest Trees
public class DRF extends Job {
  public static final String KEY_PREFIX = "__DRFModel_";

  public static final Key makeKey() { return Key.make(KEY_PREFIX + Key.make());  }
  private DRF(Key dest, Frame fr) { super("DRF "+fr, dest); }
  // Called from a non-FJ thread; makea a DRF and hands it over to FJ threads
  public static DRF start(Key dest, final Frame fr, final int maxDepth, final int ntrees, final int mtrys) {
    final DRF job = new DRF(dest, fr);
    H2O.submitTask(job.start(new H2OCountedCompleter() {
        @Override public void compute2() { job.run(fr,maxDepth,ntrees,mtrys); tryComplete(); }
      })); 
    return job;
  }

  // ==========================================================================

  // Compute a DRF tree.  

  // Start by splitting all the data according to some criteria (minimize
  // variance at the leaves).  Record on each row which split it goes to, and
  // assign a split number to it (for next pass).  On *this* pass, use the
  // split-number to build a per-split histogram, with a per-histogram-bucket
  // variance.

  // Compute a single DRF tree from the Frame.  Last column is the response
  // variable.  Depth is capped at maxDepth.
  private void run(Frame fr, int maxDepth, int ntrees, int mtrys) {
    Timer t_drf = new Timer();
    final String names[] = fr._names;
    Vec vs[] = fr._vecs;
    final int ncols = vs.length-1; // Last column is the response column

    // Response column is the last one in the frame
    Vec vresponse = vs[ncols];
    final long nrows = vresponse.length();
    int ymin = (int)vresponse.min();
    int numClasses = vresponse._isInt ? ((int)vresponse.max()-ymin+1) : 0;
    //if( numClasses == 2 ) numClasses = 0; // Specifically force 2 classes into a regression

    // The RNG used to pick split columns
    Random rand = new MersenneTwisterRNG(new int[]{1,2});

    // Initially setup as-if an empty-split had just happened
    Histogram hs[] = Histogram.initialHist(fr,ncols);
    DRFTree trees[] = new DRFTree[ntrees];
    for( int t=0; t<ntrees; t++ ) {
      trees[t] = new DRFTree(names,mtrys,rand);
      new UndecidedNode(trees[t],-1,hs); // The "root" node 
      // Make a new Vec to hold the split-number for each row (initially all zero).
      fr.add("NIDs"+t,Vec.makeZero(vs[0]));
    }
    int leafs[] = new int[ntrees]; // Define a "working set" of leaf splits, from here to tree._len

    // ----
    // One Big Loop till the tree is of proper depth.
    // Adds a layer to the tree each pass.
    int depth=0;
    for( ; depth<maxDepth; depth++ ) {

      // Fuse 2 conceptual passes into one:
      // Pass 1: Score a prior Histogram, and make new DTree.Node assignments
      // to every row.  This involves pulling out the current assigned Node,
      // "scoring" the row against that Node's decision criteria, and assigning
      // the row to a new child Node (and giving it an improved prediction).
      // Pass 2: Build new summary Histograms on the new child Nodes every row
      // got assigned into.  Collect counts, mean, variance, min, max per bin,
      // per column.
      ScoreBuildHistogram sbh = new ScoreBuildHistogram(trees,leafs,ncols,numClasses,ymin).doAll(fr);

      // Reassign the new Histograms back into the DTrees
      for( int t=0; t<ntrees; t++ ) {
        final int tmax = trees[t]._len; // Number of total splits
        final DTree tree = trees[t];
        for( int i=leafs[t]; i<tmax; i++ )
          tree.undecided(i)._hs = sbh.getFinalHisto(t,i);
      }

      // Build up the next-generation tree splits from the current histograms.
      // Nearly all leaves will split one more level.  This loop nest is
      //           O( #active_splits * #bins * #ncols )
      // but is NOT over all the data.
      boolean still_splitting=false;
      for( int t=0; t<ntrees; t++ ) {
        final DTree tree = trees[t];
        final int tmax = tree._len; // Number of total splits
        int leaf = leafs[t];
        for( ; leaf<tmax; leaf++ ) {
          //System.out.println("Tree#"+t+", "+tree.undecided(leaf));
          // Replace the Undecided with the Split decision
          new DRFDecidedNode(tree.undecided(leaf));
        }
        leafs[t] = leaf;
        // If we did not make any new splits, then the tree is split-to-death
        if( tmax < tree._len ) still_splitting = true;
      }

      // If all trees are done, then so are we
      if( !still_splitting ) break;

      new BulkScore(trees,ncols,numClasses,ymin).doAll(fr).report( Sys.DRF__, nrows, depth );
    }
    Log.info(Sys.DRF__,"DRF done in "+t_drf);

    // One more pass for final prediction error
    Timer t_score = new Timer();
    new BulkScore(trees,ncols,numClasses,ymin).doAll(fr).report( Sys.DRF__, nrows, depth );
    Log.info(Sys.DRF__,"DRF score done in "+t_score);

    // Remove temp vectors; cleanup the Frame
    while( fr.numCols() > ncols+1 )
      UKV.remove(fr.remove(fr.numCols()-1)._key);
  }

  static class DRFTree extends DTree {
    final int _mtrys;           // Number of columns to choose amongst in splits
    final transient Random _rand; // Pseudo-random gen; not locked; only used in the driver thread
    DRFTree( String names[], int mtrys, Random rand ) { super(names); _mtrys = mtrys; _rand = rand; }
  }


  // DRF DTree decision node: same as the normal DecidedNode, but specifies a
  // decision algorithm given complete histograms on all columns.  
  // DRF algo: find the lowest error amongst a random mtry columns.
  static class DRFDecidedNode extends DecidedNode {
    DRFDecidedNode( UndecidedNode n ) { super(n); }
    // Find the column with the best split (lowest score).
    @Override int bestCol( Histogram[] hs ) {
      DRFTree tree = (DRFTree)_tree;
      int[] cols = new int[hs.length];
      int len=0;
      // Gather all active columns to choose from
      for( int i=0; i<hs.length; i++ ) {
        if( hs[i]==null || hs[i]._nbins == 1 ) continue; // Ignore not-tracked cols, or cols with 1 bin (will not split)
        if( hs[i]._mins[0] == hs[i]._maxs[hs[i]._nbins-1] ) continue; // predictor min==max, does not distinguish
        cols[len++] = i;        // Gather active column
      }

      // Draw up to mtry columns at random without replacement.
      // Take the best one.
      double bs = Double.MAX_VALUE; // Best score
      int idx = -1;                 // Column to split on
      for( int i=0; i<tree._mtrys; i++ ) {
        if( len == 0 ) break;       // Out of choices!
        int idx2 = tree._rand.nextInt(len);
        int col = cols[idx2];       // The chosen column
        cols[idx2] = cols[--len];   // Compress out of array; do not choose again
        double s = hs[col].score();
        if( s < bs ) { bs = s; idx = col; }
      }
      return idx;
    }
  }
}
