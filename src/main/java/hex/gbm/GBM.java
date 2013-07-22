package hex.gbm;

import java.util.ArrayList;
import java.util.Arrays;
import water.*;
import water.H2O.H2OCountedCompleter;
import water.fvec.*;
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
    final int ncols = fr._vecs.length-1; // Last column is the response column
    Vec vs[] = fr._vecs;

    // Response column is the last one in the frame
    Vec vresponse = vs[ncols];

    // Make a new Vec to hold the split-number for each row (initially all zero).
    Vec vnids = Vec.makeZero(vs[0]);
    fr.add("NIDs",vnids);
    // Make a new Vec to hold the prediction value for each row
    Vec vpred  = Vec.makeZero(vs[0]);
    fr.add("Predictions",vpred);

    // Initially setup as-if an empty-split had just happened
    Tree tree = new Tree();
    Histogram hists[] = new Histogram[ncols];
    for( int j=0; j<ncols; j++ )
      hists[j] = new Histogram(fr._names[j],vs[j].length(),vs[j].min(),vs[j].max(),vs[j]._isInt);
    Tree.Node root = tree.newNode(null,hists);
    int leaf = 0; // Define a "working set" of leaf splits, from here to tree._len

    // One Big Loop till the tree is of proper depth.
    for( int depth=0; depth<maxDepth; depth++ ) {

      // Report the average prediction error
      CalcError ce = new CalcError().doAll(vresponse,vpred);
      double errAvg = ce._sum/fr._vecs[0].length();
      Log.unwrap(System.out,"============================================================== ");
      Log.unwrap(System.out,"Average squared prediction error for tree of depth "+depth+" is "+errAvg);
      Log.unwrap(System.out,"Total of "+ce._err+" errors on "+vpred.length()+" rows, with "+tree._len+" nodes");

      // Build a histogram with a pass over the data.
      Histogram hs[][] = new BuildHistogram(tree,leaf,ncols).doAll(fr)._hcs;
      // Reassign the new Histogram back into the Tree
      final int tmax = tree._len; // Number of total splits
      for( int i=leaf; i<tmax; i++ )
        tree.n(i)._hs = hs[i-leaf];

      // Build up the next-generation tree splits from the current histograms.
      // Nearly all leaves will split one more level.  This loop nest is
      //           O( #active_splits * #bins * #ncols )
      // but is NOT over all the data.
      for( ; leaf<tmax; leaf++ )
        pickSplits(tree, tree.n(leaf), fr, ncols);

      // "Score" each row against the new splits.  Assign a new split.
      new ScoreAndAssign(tree).doAll(fr);
    }

    // One more pass for final prediction error
    CalcError ce = new CalcError().doAll(vresponse,vpred);
    double errAvg = ce._sum/fr._vecs[0].length();
    Log.unwrap(System.out,"============================================================== ");
    Log.unwrap(System.out,"Average squared prediction error for tree of depth "+maxDepth+" is "+errAvg);
    Log.unwrap(System.out,"Total of "+ce._err+" errors on "+vpred.length()+" rows, with "+tree._len+" nodes");

    // Remove temp vectors
    fr.remove("Predictions");
    fr.remove("NIDs");
    UKV.remove(vnids._key);
    UKV.remove(vpred._key);
  }

  // --------------------------------------------------------------------------
  // Build up the next-generation tree splits from the current histograms.
  // Nearly all leaves will split one more level.  This loop nest is
  //           O( #active_splits * #bins * #ncols )
  // but is NOT over all the data.
  private void pickSplits( Tree tree, Tree.Node t, Frame fr, int ncols ) {
    // Adjust for observed min/max in the histograms, as some of the bins might
    // already be zero from a prior split (i.e., the maximal element in column
    // 7 went to the "other split"), leaving behind a smaller new maximal element.
    for( int j=0; j<ncols; j++ ) { // For every column in the new split
      Histogram h = t._hs[j];      // Old histogram of column
      if( h != null ) h.tightenMinMax();
    }
        
    // Compute best split
    int col = t.bestSplit();      // Best split-point for this tree

    // Split the best split.  This is a BIN-way split; each bin gets it's own
    // subtree.
    Histogram splitH = t._hs[col];// Histogram of the column being split
    int l = splitH._bins.length;  // Number of split choices
    assert l > 1;                 // Should always be some bins to split between
    for( int i=0; i<l; i++ ) {    // For all split-points
      Histogram nhists[] = splitH.split(col,i,t._hs,fr,ncols);
      if( nhists != null )      // Add a new (unsplit) Tree
        t._ns[i]=tree.newNode(t,nhists)._nid;
    }
    t.clean();              // Toss old histogram data
  }

  // --------------------------------------------------------------------------
  // Compute sum-squared-error.  Should use the recursive-mean technique.
  private static class CalcError extends MRTask2<CalcError> {
    double _sum;
    long _err;
    @Override public void map( Chunk ys, Chunk preds ) {
      for( int i=0; i<ys._len; i++ ) {
        assert !ys.isNA0(i);
        double y    = ys   .at0(i);
        double pred = preds.at0(i);
        _sum += (y-pred)*(y-pred);
        if( (int)y != (int)(pred+0.5) ) _err++;
      }
    }
    @Override public void reduce( CalcError t ) { _sum += t._sum; _err += t._err; }
  }

  // --------------------------------------------------------------------------
  // Collect histogram data for each row, in it's own Tree.Node split.
  // Collect counts, mean, variance, min, max per bin, per column.
  private static class BuildHistogram extends MRTask2<BuildHistogram> {
    final Tree _tree;           // Read-only, shared (except at the histograms in the Tree.Nodes)
    final int _ncols;
    final int _leaf;
    Histogram _hcs[][];         // Output: histograms-per-nid-per-column
    BuildHistogram(Tree tree, int leaf, int ncols) { _tree=tree; _leaf=leaf; _ncols=ncols; }

    @Override public void map( Chunk[] chks ) {
      Chunk nids = chks[chks.length-2];
      // We need private (local) space to gather the histograms.
      // Make local clones of all the histograms that appear in this chunk.
      _hcs = new Histogram[_tree._len-_leaf][]; // A leaf-biased array of all active histograms
      for( int i=0; i<nids._len; i++ ) {
        int nid = (int)nids.at80(i); // Node id for this row
        if( nid==-1 ) continue; // Row does not need to split again (generally perfect prediction already)
        if( _hcs[nid-_leaf] == null ) { // Lazily manifest this histogram for 'nid'
          _hcs[nid-_leaf] = new Histogram[_ncols];
          Histogram hs[] = _tree.n(nid)._hs; // The existing column of Histograms
          for( int j=0; j<_ncols; j++ )      // Make private copies
            if( hs[j] != null )
              _hcs[nid-_leaf][j] = hs[j].copy();
        }
      }

      // Now revisit all rows again, bumping the local histogram counts
      Chunk ys = chks[chks.length-3];
      for( int i=0; i<nids._len; i++ ) {
        int nid = (int)nids.at80(i); // Node id for this row
        if( nid==-1 ) continue; // Row does not need to split again (generally perfect prediction already)
        Histogram hs[] = _hcs[nid-_leaf];
        double y = ys.at0(i);        // Response variable for row
        for( int j=0; j<_ncols; j++) // For all columns
          if( hs[j] != null ) // Some columns are ignored, since already split to death
            hs[j].incr(chks[j].at0(i),y);
      }
    }
    @Override public void reduce( BuildHistogram bh ) {
      for( int i=0; i<_hcs.length; i++ ) {
        Histogram hs1[] = _hcs[i], hs2[] = bh._hcs[i];
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
  // "Score" each row against the new splits.  Assign a new split.
  private static class ScoreAndAssign extends MRTask2<ScoreAndAssign> {
    final Tree _tree;
    ScoreAndAssign(Tree tree) { _tree=tree; }
    @Override public void map( Chunk[] chks ) {
      Chunk preds = chks[chks.length-1];
      Chunk nids  = chks[chks.length-2];
      for( int i=0; i<nids._len; i++ ) {
        int oldNid = (int)nids.at80(i); // Get Tree.Node to decide from
        if( oldNid==-1 ) continue;      // row already predicts perfectly
        Tree.Node t = _tree.n(oldNid);  // This row is being split in Tree t
        double d = chks[t._col].at0(i); // Value to split on for this row
        int bin = t._hs[t._col].bin(d); // Bin in the histogram in the tree
        double pred = t._hs[t._col].mean(bin);// Current prediction
        int newNid = t._ns[bin];// Tree.Node split
        nids .set80(i,newNid);  // Save the new split# for this row
        preds.set80(i,pred);    // Save the new prediction also
      }
    }
  }

}
