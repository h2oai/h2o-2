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
  public static GBM start(Key dest, final Frame fr, final Vec vresponse, final int maxDepth) {
    final GBM job = new GBM(dest, fr);
    H2O.submitTask(job.start(new H2OCountedCompleter() {
        @Override public void compute2() { job.run(fr,vresponse,maxDepth); tryComplete(); }
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
  private void run(Frame fr, Vec vresponse, int maxDepth) {
    Timer t_gbm = new Timer();
    final int  ncols = fr.numCols();
    final long nrows = fr.numRows();
    final int ymin = (int)vresponse.min();
    short nclass = vresponse._isInt ? (short)(vresponse.max()-ymin+1) : 1;
    assert 1 <= nclass && nclass < 1000; // Arbitrary cutoff for too many classes

    // Add in Vecs after the response column which holds the row-by-row
    // residuals: the (actual minus prediction), for each class.  The
    // prediction is a probability distribution across classes: every class is
    // assigned a probability from 0.0 to 1.0, and the sum of probs is 1.0.
    //
    // The initial prediction is just the class distribution.  The initial
    // residuals are then basically the actual class minus the average class.
    float preds[] = buildResiduals(nclass,fr,ncols,nrows,vresponse,ymin);
    DTree init_tree = new DTree(fr._names,ncols,nclass);
    new GBMDecidedNode(init_tree,preds);

    DTree forest[] = new DTree[] {init_tree};
    // Initial scoring
    //new BulkScore(forest,ncols,nclass,ymin,1.0f,false).doIt(fr,vresponse).report( Sys.GBM__, nrows, maxDepth );

    // Build trees until we hit the limit
    for( int tid=1; tid<20; tid++)
      forest = buildNextTree(fr,vresponse,forest,ncols,nrows,nclass,ymin,maxDepth,1);

    Log.info(Sys.GBM__,"GBM Modeling done in "+t_gbm);

    // One more pass for final prediction error
    Timer t_score = new Timer();
    new BulkScore(forest,ncols,nclass,ymin,1.0f,false).doIt(fr,vresponse).report( Sys.GBM__, nrows, maxDepth );
    Log.info(Sys.GBM__,"GBM final Scoring done in "+t_score);

    // Remove temp vector; cleanup the Frame
    while( fr.numCols() > ncols+1 )
      UKV.remove(fr.remove(fr.numCols()-1)._key);
  }


  // Build the next tree, which is trying to correct the residual error from the prior trees.
  private static DTree[] buildNextTree(Frame fr, Vec vresponse, DTree forest[], final int ncols, long nrows, final short nclass, int ymin, int maxDepth, final int tid) {
    // Make a new Vec to hold the split-number for each row (initially all zero).
    Vec vnids = vresponse.makeZero();
    fr.add("NIDs",vnids);

    // Initially setup as-if an empty-split had just happened
    final DTree tree = new DTree(fr._names,ncols,nclass);
    new GBMUndecidedNode(tree,-1,DBinHistogram.initialHist(fr,ncols,nclass)); // The "root" node
    int leaf = 0; // Define a "working set" of leaf splits, from here to tree._len
    // Add tree to the end of the forest
    forest = Arrays.copyOf(forest,forest.length+1);
    forest[forest.length-1] = tree;

    // ----
    // One Big Loop till the tree is of proper depth.
    // Adds a layer to the tree each pass.
    int depth=0;
    for( ; depth<maxDepth; depth++ ) {

      // Fuse 2 conceptual passes into one:
      // Pass 1: Score a prior DHistogram, and make new DTree.Node assignments
      // to every row.  This involves pulling out the current assigned Node,
      // "scoring" the row against that Node's decision criteria, and assigning
      // the row to a new child Node (and giving it an improved prediction).
      // Pass 2: Build new summary DHistograms on the new child Nodes every row
      // got assigned into.  Collect counts, mean, variance, min, max per bin,
      // per column.
      ScoreBuildHistogram sbh = new ScoreBuildHistogram(new DTree[]{tree},new int[]{leaf},ncols,nclass,ymin,fr).doAll(fr);

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
        GBMDecidedNode dn = new GBMDecidedNode((GBMUndecidedNode)tree.undecided(leaf));
        //System.out.println(dn);
      }

      // Level-by-level scoring, within a tree.
      //new BulkScore(forest,ncols,nclass,ymin,1.0f,false).doIt(fr,vresponse).report( Sys.GBM__, nrows, depth );

      // If we did not make any new splits, then the tree is split-to-death
      if( tmax == tree._len ) break;
    }

    // For each observation, find the residual(error) between predicted and desired.
    // Desired is in the old residual columns; predicted is in the decision nodes.
    // Replace the old residual columns with new residuals.
    new MRTask2() {
      @Override public void map( Chunk chks[] ) {
        Chunk ns = chks[chks.length-1];
        for( int i=0; i<ns._len; i++ ) {  // For all rows
          boolean frunk = (tid==1) && (ns.at80(i)==41);
          DecidedNode node = tree.decided((int)ns.at80(i));
          float preds[] = node._pred[node.bin(chks,i)];
          for( int c=0; c<nclass; c++ ) {
            double actual = chks[ncols+c].at0(i);
            double residual = actual-preds[c];
            chks[ncols+c].set40(i,(float)residual);
          }
        }
      }
    }.doAll(fr);

    // Remove the NIDs column
    assert fr._names[fr.numCols()-1].equals("NIDs");
    UKV.remove(fr.remove(fr.numCols()-1)._key);

    // Print the generated tree
    //System.out.println(tree.root().toString2(new StringBuilder(),0));
    
    // Tree-by-tree scoring
    new BulkScore(forest,ncols,nclass,ymin,1.0f,false).doIt(fr,vresponse).report( Sys.GBM__, nrows, depth );
    return forest;
  }


  // Add in Vecs after the response column which holds the row-by-row
  // residuals: the (actual minus prediction), for each class.  The
  // prediction is a probability distribution across classes: every class is
  // assigned a probability from 0.0 to 1.0, and the sum of probs is 1.0.
  //
  // The initial prediction is just the class distribution.  The initial
  // residuals are then basically the actual class minus the average class.
  private static float[] buildResiduals(short nclass, final Frame fr, final int ncols, long nrows, Vec vresponse, final int ymin ) {
    // Find the initial prediction - the current average response variable.
    float preds[] = new float[nclass];
    if( nclass == 1 ) {
      fr.add("Residual-"+fr._names[ncols],vresponse.makeCon(vresponse.mean()));
      throw H2O.unimpl();
    } else {
      long cs[] = new ClassDist(nclass,ymin).doAll(vresponse)._cs;
      String[] domain = vresponse.domain();
      for( int i=0; i<nclass; i++ ) {
        preds[i] = (float)cs[i]/nrows; // Prediction is just class average
        fr.add("Residual-"+domain[i],vresponse.makeCon(-preds[i]));
      }
    }

    // Compute initial residuals with no trees: prediction-actual e.g. if the
    // class choices are A,B,C with equal probability, and the row is actually
    // a 'B', the residual is {-0.33,1-0.33,-0.33}
    fr.add("response",vresponse);
    new MRTask2() {
      @Override public void map( Chunk chks[] ) {
        Chunk cy = chks[chks.length-1];   // Response as last chunk
        for( int i=0; i<cy._len; i++ ) {  // For all rows
          int cls = (int)cy.at80(i)-ymin; // Class
          Chunk res = chks[ncols+cls];    // Residual column for this class
          res.set80(i,1.0f+res.at0(i));   // Fix residual for actual class
        }
      }
    }.doAll(fr);
    fr.remove(ncols+nclass);    // Remove last (response) col

    return preds;
  }

  // ---
  // GBM DTree decision node: same as the normal DecidedNode, but
  // specifies a decision algorithm given complete histograms on all
  // columns.  GBM algo: find the lowest error amongst *all* columns.
  static class GBMDecidedNode extends DecidedNode<GBMUndecidedNode> {
    GBMDecidedNode( GBMUndecidedNode n ) { super(n); }
    GBMDecidedNode( DTree t, float[] p ) { super(t,p); }

    @Override GBMUndecidedNode makeUndecidedNode(DTree tree, int nid, DHistogram[] nhists ) { 
      return new GBMUndecidedNode(tree,nid,nhists); 
    }

    // Find the column with the best split (lowest score).  Unlike RF, GBM
    // scores on all columns and selects splits on all columns.
    @Override int bestCol( GBMUndecidedNode u ) {
      DHistogram hs[] = u._hs;
      double bs = Double.MAX_VALUE; // Best score
      int idx = -1;             // Column to split on
      for( int i=0; i<hs.length; i++ ) {
        if( hs[i]==null || hs[i].nbins() <= 1 ) continue;
        double s = hs[i].score();
        if( s < bs ) { bs = s; idx = i; }
      }
      return idx;
    }
  }

  // GBM DTree undecided node: same as the normal UndecidedNode, but specifies
  // a list of columns to score on now, and then decide over later.
  // GBM algo: use all columns
  static class GBMUndecidedNode extends UndecidedNode {
    GBMUndecidedNode( DTree tree, int pid, DHistogram hs[] ) { super(tree,pid,hs); }

    // Randomly select mtry columns to 'score' in following pass over the data.
    // In GBM, we use all columns (as opposed to RF, which uses a random subset).
    @Override int[] scoreCols( DHistogram[] hs ) { return null; }
  }
}
