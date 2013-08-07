package hex.gbm;

import hex.gbm.DTree.*;
import java.util.ArrayList;
import java.util.Arrays;
import water.*;
import water.H2O.H2OCountedCompleter;
import water.fvec.*;
import water.util.Log.Tag.Sys;
import water.util.Log;

// Gradient Boosted Trees
public class GBM extends Job {
  public static final String KEY_PREFIX = "__GBMModel_";

  public static final Key makeKey() { return Key.make(KEY_PREFIX + Key.make());  }
  private GBM(Key dest, Frame fr) { super("GBM "+fr, dest); }
  // Called from a non-FJ thread; makea a GBM and hands it over to FJ threads
  public static GBM start(Key dest, final Frame fr, final int maxDepth) {
    final GBM job = new GBM(dest, fr);
    H2O.submitTask(job.start(new H2OCountedCompleter() {
        @Override public void compute2() { job.run(fr,maxDepth); tryComplete(); }
      })); 
    return job;
  }

  // ==========================================================================

  // Compute a GBM tree.  

  // Start by splitting all the data according to some criteria (minimize
  // variance at the leaves).  Record on each row which split it goes to, and
  // assign a split number to it (for next pass).  On *this* pass, use the
  // split-number to build a per-split histogram, with a per-histogram-bucket
  // variance.

  // Compute a single GBM tree from the Frame.  Last column is the response
  // variable.  Depth is capped at maxDepth.
  private void run(Frame fr, int maxDepth) {
    Timer t_gbm = new Timer();
    final String names[] = fr._names;
    Vec vs[] = fr._vecs;
    final int ncols = vs.length-1; // Last column is the response column

    // Response column is the last one in the frame
    Vec vresponse = vs[ncols];
    final long nrows = vresponse.length();
    int ymin = (int)vresponse.min();
    int numClasses = vresponse._isInt ? ((int)vresponse.max()-ymin+1) : 0;
    //if( numClasses == 2 ) numClasses = 0; // Specifically force 2 classes into a regression

    // Make a new Vec to hold the split-number for each row (initially all zero).
    Vec vnids = Vec.makeZero(vs[0]);
    fr.add("NIDs",vnids);

    // Initially setup as-if an empty-split had just happened
    DTree tree = new DTree(names,ncols);
    new UndecidedNode(tree,-1,DHistogram.initialHist(fr,ncols)); // The "root" node
    int leaf = 0; // Define a "working set" of leaf splits, from here to tree._len

    // ----
    // One Big Loop till the tree is of proper depth.
    // Adds a layer to the tree each pass.
    int depth=0;
    for( ; depth<maxDepth; depth++ ) {

      // Fuse 2 conceptual passes into one:
      // Pass 1: Score a prior DHistogram, and make new DTree.Node
      // assignments to every row.  This involves pulling out the
      // current assigned Node, "scoring" the row against that Node's
      // decision criteria, and assigning the row to a new child Node
      // (and giving it an improved prediction).
      // Pass 2: Build new summary DHistograms on the new child Nodes
      // every row got assigned into.  Collect counts, mean, variance,
      // min, max per bin, per column.
      ScoreBuildHistogram sbh = new ScoreBuildHistogram(new DTree[]{tree},new int[]{leaf},ncols,numClasses,ymin).doAll(fr);

      // Reassign the new DHistogram back into the DTree
      final int tmax = tree._len; // Number of total splits
      for( int i=leaf; i<tmax; i++ )
        tree.undecided(i)._hs = sbh.getFinalHisto(0,i);

      // Build up the next-generation tree splits from the current histograms.
      // Nearly all leaves will split one more level.  This loop nest is
      //           O( #active_splits * #bins * #ncols )
      // but is NOT over all the data.
      for( ; leaf<tmax; leaf++ ) {
        //System.out.println(tree.undecided(leaf));
        // Replace the Undecided with the Split decision
        new GBMDecidedNode(tree.undecided(leaf));
      }

      // If we did not make any new splits, then the tree is split-to-death
      if( tmax == tree._len ) break;
      
      //new BulkScore(new DTree[]{tree},ncols,numClasses,ymin,1.0).doAll(fr).report( Sys.GBM__, nrows, depth );
    }
    Log.info(Sys.GBM__,"GBM done in "+t_gbm);

    // One more pass for final prediction error
    Timer t_score = new Timer();
    new BulkScore(new DTree[]{tree},ncols,numClasses,ymin,1.0).doAll(fr).report( Sys.GBM__, nrows, depth );
    Log.info(Sys.GBM__,"GBM score done in "+t_score);

    // Remove temp vector; cleanup the Frame
    UKV.remove(fr.remove("NIDs")._key);
  }
  
  // GBM DTree decision node: same as the normal DecidedNode, but
  // specifies a decision algorithm given complete histograms on all
  // columns.  GBM algo: find the lowest error amongst *all* columns.
  static class GBMDecidedNode extends DecidedNode {
    GBMDecidedNode( UndecidedNode n ) { super(n); }
    // Find the column with the best split (lowest score).
    @Override int bestCol( DHistogram[] hs ) {
      double bs = Double.MAX_VALUE; // Best score
      int idx = -1;             // Column to split on
      for( int i=0; i<hs.length; i++ ) {
        if( hs[i]==null || hs[i]._bins.length == 1 ) continue;
        double s = hs[i].score();
        if( s < bs ) { bs = s; idx = i; }
      }
      return idx;
    }
  }
}
