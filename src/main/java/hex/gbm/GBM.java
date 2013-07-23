package hex.gbm;

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
    final int ncols = fr._vecs.length-1; // Last column is the response column
    Vec vs[] = fr._vecs;

    // Response column is the last one in the frame
    Vec vresponse = vs[ncols];
    final long nrows = vresponse.length();

    // Make a new Vec to hold the split-number for each row (initially all zero).
    Vec vnids = Vec.makeZero(vs[0]);
    fr.add("NIDs",vnids);

    // Initially setup as-if an empty-split had just happened
    Tree tree = new Tree();
    tree.newNode(null,Histogram.initialHist(fr,ncols)); // The "root" node
    int leaf = 0; // Define a "working set" of leaf splits, from here to tree._len

    // ----
    // One Big Loop till the tree is of proper depth.
    // Adds a layer to the tree each pass.
    for( int depth=0; depth<maxDepth; depth++ ) {

      // Fuse 2 conceptual passes into one:
      // Pass 1: Score a prior Histogram, and make new Tree.Node assignments to
      // every row.  This involves pulling out the current assigned Node,
      // "scoring" the row against that Node's decision criteria, and assigning
      // the row to a new child Node (and giving it an improved prediction).
      // Pass 3: Build new summary Histograms on the new child Nodes every row
      // got assigned into.  Collect counts, mean, variance, min, max per bin,
      // per column.
      ScoreBuildHistogram sbh = new ScoreBuildHistogram(tree,leaf,ncols).doAll(fr);

      // Reassign the new Histogram back into the Tree
      final int tmax = tree._len; // Number of total splits
      for( int i=leaf; i<tmax; i++ )
        tree.n(i)._hs = sbh._hcs[i-leaf];
      
      //new BulkScore(tree).doAll(fr).report( nrows, depth );

      // Build up the next-generation tree splits from the current histograms.
      // Nearly all leaves will split one more level.  This loop nest is
      //           O( #active_splits * #bins * #ncols )
      // but is NOT over all the data.
      for( ; leaf<tmax; leaf++ )
        pickSplits(tree, tree.n(leaf), fr, ncols);
    }
    Log.info(Sys.GBM__,"GBM done in "+t_gbm);

    // One more pass for final prediction error
    Timer t_score = new Timer();
    new BulkScore(tree).doAll(fr).report( nrows, maxDepth );
    Log.info(Sys.GBM__,"GBM score done in "+t_score);

    // Remove temp vector; cleanup the Frame
    UKV.remove(fr.remove("NIDs")._key);
  }

  // --------------------------------------------------------------------------
  // Build up the next-generation tree splits from the current histograms.
  // Nearly all leaves will split one more level.  This loop nest is
  //           O( #active_splits * #bins * #ncols )
  // but is NOT over all the data.
  private void pickSplits( Tree tree, Tree.Node node, Frame fr, int ncols ) {
    // Adjust for observed min/max in the histograms, as some of the bins might
    // already be zero from a prior split (i.e., the maximal element in column
    // 7 went to the "other split"), leaving behind a smaller new maximal element.
    for( int j=0; j<ncols; j++ ) { // For every column in the new split
      Histogram h = node._hs[j];   // Old histogram of column
      if( h != null ) h.tightenMinMax();
    }
        
    // Compute best split
    int col = node.bestSplit(); // Best split-point for this tree

    // Split the best split.  This is a BIN-way split; each bin gets it's own
    // subtree.
    Histogram splitH = node._hs[col];// Histogram of the column being split
    int l = splitH._bins.length;     // Number of split choices
    assert l > 1;               // Should always be some bins to split between
    for( int i=0; i<l; i++ ) {  // For all split-points
      Histogram nhists[] = splitH.split(col,i,node._hs,fr,ncols);
      if( nhists != null )      // Add a new (unsplit) Tree
        node._ns[i]=tree.newNode(node,nhists)._nid;
    }
    node.clean();               // Toss old histogram data
  }

  // --------------------------------------------------------------------------
  // Fuse 2 conceptual passes into one:
  //
  // Pass 1: Score a prior Histogram, and make new Tree.Node assignments to
  //         every row.  This involves pulling out the current assigned Node,
  //         "scoring" the row against that Node's decision criteria, and
  //         assigning the row to a new child Node (and giving it an improved
  //         prediction).
  //
  // Pass 2: Build new summary Histograms on the new child Nodes every row got
  //         assigned into.  Collect counts, mean, variance, min, max per bin,
  //         per column.
  //
  // The result is a set of Histogram arrays; one Histogram array for each
  // unique 'leaf' in the tree being histogramed in parallel.  These have node
  // ID's (nids) from 'leaf' to 'tree._len'.  Each Histogram array is for all
  // the columns in that 'leaf'.
  //
  // The other result is a prediction "score" for the whole dataset, based on
  // the previous passes' Histograms.
  private static class ScoreBuildHistogram extends MRTask2<ScoreBuildHistogram> {
    final Tree _tree;           // Read-only, shared (except at the histograms in the Tree.Nodes)
    final int _ncols;
    final int _leaf;
    Histogram _hcs[][];         // Output: histograms-per-nid-per-column
    ScoreBuildHistogram(Tree tree, int leaf, int ncols) { _tree=tree; _leaf=leaf; _ncols=ncols; }

    @Override public void map( Chunk[] chks ) {
      Chunk nids  = chks[chks.length-1];
      Chunk ys    = chks[chks.length-2];

      // We need private (local) space to gather the histograms.
      // Make local clones of all the histograms that appear in this chunk.
      _hcs = new Histogram[_tree._len-_leaf][]; // A leaf-biased array of all active histograms

      // Pass 1 & 2
      for( int i=0; i<nids._len; i++ ) {
        int nid = (int)nids.at80(i);       // Get Tree.Node to decide from
        if( nid==-1 ) continue;            // row already predicts perfectly

        // Pass 1: Score row against current decisions & assign new split
        if( _leaf > 0 ) {                // Prior pass exists?
          Tree.Node node = _tree.n(nid); // This row is being split in Tree t
          double d = chks[node._col].at0(i); // Value to split on for this row
          int bin = node._hs[node._col].bin(d); // Bin in the histogram in the tree
          nid = node._ns[bin];                  // Tree.Node split
          nids .set80(i,nid);   // Save the new split# for this row
        }

        // Pass 1.9
        if( nid==-1 ) continue;         // row already predicts perfectly
        // We need private (local) space to gather the histograms.
        // Make local clones of all the histograms that appear in this chunk.
        Histogram nhs[] = _hcs[nid-_leaf];
        if( nhs == null ) {     // Lazily manifest this histogram for 'nid'
          nhs = _hcs[nid-_leaf] = new Histogram[_ncols];
          Histogram ohs[] = _tree.n(nid)._hs; // The existing column of Histograms
          for( int j=0; j<_ncols; j++ )       // Make private copies
            if( ohs[j] != null )
              nhs[j] = ohs[j].copy();
        }

        // Pass 2
        // Bump the local histogram counts
        double y = ys.at0(i);
        for( int j=0; j<_ncols; j++) // For all columns
          if( nhs[j] != null ) // Some columns are ignored, since already split to death
            nhs[j].incr(chks[j].at0(i),y);
      }
    }

    @Override public void reduce( ScoreBuildHistogram sbh ) {
      // Merge histograms
      for( int i=0; i<_hcs.length; i++ ) {
        Histogram hs1[] = _hcs[i], hs2[] = sbh._hcs[i];
        if( hs1 == null ) _hcs[i] = hs2;
        else if( hs2 != null )
          for( int j=0; j<hs1.length; j++ )
            if( hs1[j] == null ) hs1[j] = hs2[j];
            else if( hs2[j] != null )
              hs1[j].add(hs2[j]);
      }
    }
  }

  // --------------------------------------------------------------------------
  // Compute sum-squared-error.  Should use the recursive-mean technique.
  private static class BulkScore extends MRTask2<BulkScore> {
    final Tree _tree; // Read-only, shared (except at the histograms in the Tree.Nodes)
    double _sum;
    long _err;
    BulkScore( Tree tree ) { _tree = tree; }
    @Override public void map( Chunk chks[] ) {
      Chunk ys = chks[chks.length-2];
      for( int i=0; i<ys._len; i++ ) {
        double y    = ys.at0(i);
        double pred = score0( chks, i );
        _sum += (y-pred)*(y-pred);
        if( (int)y != (int)(pred+0.5) ) _err++;
      }
    }
    @Override public void reduce( BulkScore t ) { _sum += t._sum; _err += t._err; }

    private double score0( Chunk chks[], int i ) {
      int nid=0;                // Root nid
      double pred=Double.NaN;   // Current prediction
      while( true ) {           // Tree walk
        Tree.Node node = _tree.n(nid);     // This Tree Node
        Histogram h = node._hs[node._col]; // Chosen histogram
        if( h == null ) break;             // Not available yet
        double d = chks[node._col].at0(i); // Value to split on for this row
        int bin = h.bin(d);                // Bin in the histogram in the tree
        pred = h.mean(bin);                // Current prediction
        if( node._ns == null ) break;      // No next decision available
        nid = node._ns[bin];               // Next Tree.Node
        if( nid == -1 ) break;             // Did not split again this bin
      }
      return pred;
    }

    public void report( long nrows, int depth ) {
      Log.info(Sys.GBM__,"============================================================== ");
      Log.info(Sys.GBM__,"Average squared prediction error for tree of depth "+depth+" is "+(_sum/nrows));
      Log.info(Sys.GBM__,"Total of "+_err+" errors on "+nrows+" rows, with "+_tree._len+" nodes");
    }
  }
}
