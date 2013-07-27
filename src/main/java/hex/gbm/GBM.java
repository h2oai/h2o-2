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
    Tree tree = new Tree(names);
    new Tree.UndecidedNode(tree,-1,Histogram.initialHist(fr,ncols)); // The "root" node
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
      ScoreBuildHistogram sbh = new ScoreBuildHistogram(tree,leaf,ncols,numClasses,ymin).doAll(fr);

      // Reassign the new Histogram back into the Tree
      final int tmax = tree._len; // Number of total splits
      for( int i=leaf; i<tmax; i++ )
        tree.undecided(i)._hs = sbh.getFinalHisto(i);

      // Build up the next-generation tree splits from the current histograms.
      // Nearly all leaves will split one more level.  This loop nest is
      //           O( #active_splits * #bins * #ncols )
      // but is NOT over all the data.
      for( ; leaf<tmax; leaf++ ) {
        //System.out.println(tree.undecided(leaf));
        // Replace the Undecided with the Split decision
        new Tree.DecidedNode(tree.undecided(leaf));
      }

      //new BulkScore(tree,numClasses,ymin).doAll(fr).report( nrows, depth );
    }
    Log.info(Sys.GBM__,"GBM done in "+t_gbm);

    // One more pass for final prediction error
    Timer t_score = new Timer();
    new BulkScore(tree,numClasses,ymin).doAll(fr).report( nrows, maxDepth );
    Log.info(Sys.GBM__,"GBM score done in "+t_score);

    // Remove temp vector; cleanup the Frame
    UKV.remove(fr.remove("NIDs")._key);
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
    final int _numClasses;      // Zero for regression, else #classes
    final int _ymin;            // Bias classes to zero
    final int _leaf;
    Histogram _hcs[][];         // Output: histograms-per-nid-per-column
    ScoreBuildHistogram(Tree tree, int leaf, int ncols, int numClasses, int ymin) { 
      _tree=tree; 
      _leaf=leaf; 
      _ncols=ncols; 
      _numClasses = numClasses; 
      _ymin = ymin;
    }

    public Histogram[] getFinalHisto( int nid ) {
      Histogram hs[] = _hcs[nid-_leaf];
      // Having gather min/max/mean/class/etc on all the data, we can now
      // tighten the min & max numbers.
      for( int j=0; j<hs.length; j++ ) {
        Histogram h = hs[j];    // Old histogram of column
        if( h != null ) h.tightenMinMax();
      }
      return hs;
    }

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
        if( _leaf > 0 )         // Prior pass exists?
          nids.set80(i,nid = _tree.decided(nid).ns(chks,i));

        // Pass 1.9
        if( nid==-1 ) continue;         // row already predicts perfectly

        // We need private (local) space to gather the histograms.
        // Make local clones of all the histograms that appear in this chunk.
        Histogram nhs[] = _hcs[nid-_leaf];
        if( nhs == null ) {     // Lazily manifest this histogram for 'nid'
          nhs = _hcs[nid-_leaf] = new Histogram[_ncols];
          Histogram ohs[] = _tree.undecided(nid)._hs; // The existing column of Histograms
          for( int j=0; j<_ncols; j++ )       // Make private copies
            if( ohs[j] != null )
              nhs[j] = ohs[j].copy(_numClasses);
        }

        // Pass 2
        // Bump the local histogram counts
        if( _numClasses == 0 ) { // Regression?
          double y = ys.at0(i);
          for( int j=0; j<_ncols; j++) // For all columns
            if( nhs[j] != null ) // Some columns are ignored, since already split to death
              nhs[j].incr(chks[j].at0(i),y);
        } else {                // Classification
          int ycls = (int)ys.at80(i) - _ymin;
          for( int j=0; j<_ncols; j++) // For all columns
            if( nhs[j] != null ) // Some columns are ignored, since already split to death
              nhs[j].incr(chks[j].at0(i),ycls);
        }
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
    final int _numClasses;
    final int _ymin;
    double _sum;
    long _err;
    BulkScore( Tree tree, int numClasses, int ymin ) { _tree = tree; _numClasses = numClasses; _ymin = ymin; }
    @Override public void map( Chunk chks[] ) {
      Chunk ys = chks[chks.length-2];
      for( int i=0; i<ys._len; i++ ) {
        double err = score0( chks, i, ys.at0(i) );
        _sum += err*err;        // Squared error
      }
    }
    @Override public void reduce( BulkScore t ) { _sum += t._sum; _err += t._err; }

    // Return a relative error.  For regression it's y-mean.  For classification, 
    // it's the %-tage of the response class out of all rows in the leaf, plus
    // a count of absolute errors when we predict the majority class.
    private double score0( Chunk chks[], int i, double y ) {
      Tree.DecidedNode prev = null;
      Tree.Node node = _tree.root();
      while( node instanceof Tree.DecidedNode ) {
        prev = (Tree.DecidedNode)node;
        int nid = prev.ns(chks,i);
        if( nid == -1 ) break;
        node = _tree.node(nid);
      }
      int bin = prev.bin(chks,i); // Which bin did we decide on?
      if( _numClasses == 0 )      // Regression?
        return prev._pred[bin]-y; // Current prediction minus actual

      int ycls = (int)y-_ymin;  // Zero-based response class
      if( prev._ycls[bin] != ycls ) _err++;
      return prev._pred[bin];   // Confidence of our prediction
    }

    public void report( long nrows, int depth ) {
      Log.info(Sys.GBM__,"============================================================== ");
      Log.info(Sys.GBM__,"Average squared prediction error for tree of depth "+depth+" is "+(_sum/nrows));
      Log.info(Sys.GBM__,"Total of "+_err+" errors on "+nrows+" rows, with "+_tree._len+" nodes");
    }
  }
}
