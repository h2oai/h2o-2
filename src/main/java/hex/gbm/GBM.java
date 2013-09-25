package hex.gbm;

import hex.gbm.DTree.*;
import java.util.Arrays;
import jsr166y.CountedCompleter;
import water.*;
import water.H2O.H2OCountedCompleter;
import water.Job.FrameJob;
import water.api.*;
import water.fvec.*;
import water.util.*;
import water.util.Log.Tag.Sys;

// Gradient Boosted Trees
//
// Based on "Elements of Statistical Learning, Second Edition, page 387"
public class GBM extends SharedTreeModelBuilder {
  static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  @API(help = "Learning rate, from 0. to 1.0", filter = Default.class, dmin=0, dmax=1)
  public double learn_rate = 0.2;

  @API(help = "The GBM Model")
  public GBMModel gbm_model;

  public static class GBMModel extends DTree.TreeModel {
    static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
    static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

    public GBMModel(Key key, Key dataKey, Frame fr, int ntrees, DTree[][] forest, double [] errs, int ymin, long [][] cm){
      super(key,dataKey,fr,ntrees,forest,errs,ymin,cm);
    }
  }
  public Frame score( Frame fr ) { return gbm_model.score(fr,true);  }

  protected Log.Tag.Sys logTag() { return Sys.GBM__; }
  public static final String KEY_PREFIX = "__GBMModel_";
  public GBM() { super("Distributed GBM",KEY_PREFIX); }

  /** Return the query link to this page */
  public static String link(Key k, String content) {
    RString rs = new RString("<a href='GBM.query?source=%$key'>%content</a>");
    rs.replace("key", k.toString());
    rs.replace("content", content);
    return rs.toString();
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
  @Override protected Response serve() {
    startBuildModel();
    return GBMProgressPage.redirect(this, self(),dest());
  }

  protected void buildModel( final Frame fr, final Frame frm, final Key outputKey, final Key dataKey, final int ncols, final long nrows, final char nclass, final int ymin, final Timer t_build ) {
    gbm_model = new GBMModel(outputKey,dataKey,frm,ntrees,new DTree[0][], null, ymin, null);
    DKV.put(outputKey, gbm_model);

    H2O.submitTask(start(new H2OCountedCompleter() {
      @Override public void compute2() {
        // The Initial Forest is empty
        DTree forest[/*Trees*/][/*nclass*/] = new DTree[0][];

        // Build trees until we hit the limit
        for( int tid=0; tid<ntrees; tid++) {
          if( cancelled() ) break;

          // Step 2a: Compute prob distribution from prior tree results: 
          //   Work <== f(Tree)
          new ComputeProb(ncols,nclass).doAll(fr);
          for( int i=0; i<nrows; i++ )
            System.out.println(fr.toString(i));

          // Step 2b i: Compute residuals from the probability distribution
          //   Work <== f(Work)
          new ComputeRes(ncols,nclass,ymin).doAll(fr);
          for( int i=0; i<nrows; i++ )
            System.out.println(fr.toString(i));

          
          forest = buildNextTree(fr,forest,ncols,nrows,nclass,ymin);
        //  // System.out.println("Tree #" + forest.length + ":\n" +  forest[forest.length-1].compress().toString());
        //  // Tree-by-tree scoring
        //  BulkScore bs2 = new BulkScore(forest,tid,ncols,nclass,ymin,1.0f).doAll(fr).report( Sys.GBM__, max_depth );
        //  _errs = Arrays.copyOf(_errs,_errs.length+1);
        //  _errs[_errs.length-1] = (float)bs2._sum/nrows;
        //  gbm_model = new GBMModel(outputKey, dataKey,frm, ntrees,forest, _errs, ymin,bs2._cm);
        //  DKV.put(outputKey, gbm_model);
        }

        cleanUp(fr,ncols,t_build); // Shared cleanup
        tryComplete();
      }
      @Override public boolean onExceptionalCompletion(Throwable ex, CountedCompleter caller) {
        ex.printStackTrace();
        GBM.this.cancel(ex.getMessage());
        return true;
      }
    }));
  }

  // --------------------------------------------------------------------------
  // Compute Probability Distribution from prior tree results.
  // Prob_k = exp(Work_k)/sum_all_K exp(Work_k)
  class ComputeProb extends MRTask2<ComputeProb> {
    final int _ncols;
    final char _nclass;
    ComputeProb( int ncols, char nclass ) { _ncols=ncols; _nclass=nclass; }
    private Chunk tree( Chunk chks[], int c) { return DTree.chk_tree(chks,_ncols,_nclass,c); }
    private Chunk work( Chunk chks[], int c) { return DTree.chk_work(chks,_ncols,_nclass,c); }
    @Override public void map( Chunk chks[] ) {
      Chunk ys = DTree.chk_resp(chks,_ncols,_nclass);
      double ds[] = new double[_nclass];
      for( int row=0; row<ys._len; row++ ) {
        double sum=0;
        for( int c=0; c<_nclass; c++ ) // Sum across of likelyhoods
          sum+=(ds[c]=Math.exp(tree(chks,c).at0(row)));
        for( int c=0; c<_nclass; c++ ) // Save as a probability distribution
          work(chks,c).set0(row,ds[c]/sum);
      }
    }
  }

  // --------------------------------------------------------------------------
  // Compute Residuals from Actuals
  class ComputeRes extends MRTask2<ComputeRes> {
    final int _ncols, _ymin;
    final char _nclass;
    ComputeRes( int ncols, char nclass, int ymin ) { _ncols=ncols; _nclass=nclass; _ymin = ymin; }
    private Chunk work( Chunk chks[], int c) { return DTree.chk_work(chks,_ncols,_nclass,c); }
    @Override public void map( Chunk chks[] ) {
      Chunk ys = DTree.chk_resp(chks,_ncols,_nclass);
      for( int row=0; row<ys._len; row++ ) {
        int y = (int)ys.at8(row)-_ymin; // zero-based response variable
        for( int c=0; c<_nclass; c++ ) {
          Chunk pc = work(chks,c);
          pc.set0(row, (y==c?1:0)-pc.at0(row) );
        }
      }
    }
  }


  // --------------------------------------------------------------------------
  // Build the next tree, which is trying to correct the residual error from the prior trees.
  private DTree[][] buildNextTree(Frame fr, DTree forest[][], final int ncols, long nrows, final char nclass, int ymin) {
    // Add tree to the end of the forest
    DTree[][] oldForest = forest;
    forest = Arrays.copyOf(forest,forest.length+1);
    throw H2O.unimpl();
    //forest[forest.length-1] = tree;
    //
    //// Initially setup as-if an empty-split had just happened
    //final DTree tree = new DTree(fr._names,ncols,(char)nbins,nclass,min_rows);
    //new GBMUndecidedNode(tree,-1,DBinHistogram.initialHist(fr,ncols,(char)nbins)); // The "root" node
    //int leaf = 0; // Define a "working set" of leaf splits, from here to tree._len
    //
    //// ----
    //// One Big Loop till the tree is of proper depth.
    //// Adds a layer to the tree each pass.
    //int depth=0;
    //for( ; depth<max_depth; depth++ ) {
    //  if( cancelled() ) return oldForest;
    //
    //  // Fuse 2 conceptual passes into one:
    //  // Pass 1: Score a prior DHistogram, and make new DTree.Node assignments
    //  // to every row.  This involves pulling out the current assigned Node,
    //  // "scoring" the row against that Node's decision criteria, and assigning
    //  // the row to a new child Node (and giving it an improved prediction).
    //  // Pass 2: Build new summary DHistograms on the new child Nodes every row
    //  // got assigned into.  Collect counts, mean, variance, min, max per bin,
    //  // per column.
    //  ScoreBuildHistogram sbh = new ScoreBuildHistogram(new DTree[]{tree},new int[]{leaf},ncols,nclass,ymin,fr).doAll(fr);
    //  //System.out.println(sbh.profString());
    //
    //  // Reassign the new DHistogram back into the DTree
    //  final int tmax = tree._len; // Number of total splits
    //  for( int i=leaf; i<tmax; i++ )
    //    tree.undecided(i)._hs = sbh.getFinalHisto(0,i);
    //
    //  // Build up the next-generation tree splits from the current histograms.
    //  // Nearly all leaves will split one more level.  This loop nest is
    //  //           O( #active_splits * #bins * #ncols )
    //  // but is NOT over all the data.
    //  for( ; leaf<tmax; leaf++ ) {
    //    System.out.println(tree.undecided(leaf));
    //    // Replace the Undecided with the Split decision
    //    GBMDecidedNode dn = new GBMDecidedNode((GBMUndecidedNode)tree.undecided(leaf));
    //    System.out.println(dn);
    //  }
    //
    //  // If we did not make any new splits, then the tree is split-to-death
    //  if( tmax == tree._len ) break;
    //}
    //
    //// Scale the tree down by the learning rate
    //for( int i=0; i<tree._len; i++ ) {
    //  if( tree.node(i) instanceof DecidedNode ) {
    //    float pred[][] = tree.decided(i)._pred;
    //    if( pred != null )
    //      for( int b=0; b<pred.length; b++ )
    //        for( int c=0; c<pred[b].length; c++ )
    //          pred[b][c] *= learn_rate;
    //  }
    //}
    //
    //// For each observation, find the residual(error) between predicted and desired.
    //// Desired is in the old residual columns; predicted is in the decision nodes.
    //// Replace the old residual columns with new residuals.
    //new MRTask2() {
    //  @Override public void map( Chunk chks[] ) {
    //    Chunk nids = DTree.chk_nids(chks,ncols,nclass,1,0);
    //    for( int i=0; i<nids._len; i++ ) {  // For all rows
    //      DecidedNode node = tree.decided((int)nids.at80(i));
    //      float preds[] = node._pred[node.bin(chks,i)];
    //      for( int c=0; c<nclass; c++ ) {
    //        Chunk cres = DTree.chk_work(chks,ncols,nclass,c);
    //        double actual = cres.at0(i);
    //        double residual = actual-preds[c];
    //        cres.set0(i,(float)residual);
    //      }
    //    }
    //  }
    //}.doAll(fr);

    // Print the generated tree
    //System.out.println(tree.root().toString2(new StringBuilder(),0));

    //return forest;
  }


  // Add in Vecs after the response column which holds the row-by-row
  // residuals: the (actual minus prediction), for each class.  The
  // prediction is a probability distribution across classes: every class is
  // assigned a probability from 0.0 to 1.0, and the sum of probs is 1.0.
  //
  // The initial prediction is just the class distribution.  The initial
  // residuals are then basically the actual class minus the average class.
  private void buildResiduals(final char nclass, final Frame fr, final int ncols, long nrows, final int ymin ) {
    String[] domain = vresponse.domain();
    // Find the initial prediction - the current average response variable.
    if( nclass == 1 ) {
      fr.add("Residual",vresponse.makeCon(vresponse.mean()));
      throw H2O.unimpl();
    } else {
      float f = 1.0f/nclass;    // Prediction is same for all classes
      for( int i=0; i<nclass; i++ )
        fr.add("Residual-"+domain[i],vresponse.makeCon(-f));
    }
    // Build a set of predictions that's the sum across all trees.
    for( int i=0; i<nclass; i++ )   // All Zero cols
      fr.add("Pred-"+domain[i],vresponse.makeZero());

    // Compute initial residuals with no trees: prediction-actual e.g. if the
    // class choices are A,B,C with equal probability, and the row is actually
    // a 'B', the residual is {-0.33,1-0.33,-0.33}
    new MRTask2() {
      @Override public void map( Chunk chks[] ) {
        Chunk cy = DTree.chk_resp(chks,ncols,nclass);
        for( int i=0; i<cy._len; i++ ) {  // For all rows
          if( cy.isNA0(i) ) continue;     // Ignore NA results
          int cls = (int)cy.at80(i)-ymin; // Class
          Chunk res = DTree.chk_work(chks,ncols,nclass,cls);    // Residual column for this class
          res.set0(i,1.0f+(float)res.at0(i)); // Fix residual for actual class
        }
      }
    }.doAll(fr);
  }

  // ---
  // GBM DTree decision node: same as the normal DecidedNode, but
  // specifies a decision algorithm given complete histograms on all
  // columns.  GBM algo: find the lowest error amongst *all* columns.
  static class GBMDecidedNode extends DecidedNode<GBMUndecidedNode> {
    GBMDecidedNode( GBMUndecidedNode n ) { super(n); }
    GBMDecidedNode( DTree t, double p ) { super(t,p); }

    @Override GBMUndecidedNode makeUndecidedNode(DTree tree, int nid, DBinHistogram[] nhists ) {
      return new GBMUndecidedNode(tree,nid,nhists);
    }

    // Find the column with the best split (lowest score).  Unlike RF, GBM
    // scores on all columns and selects splits on all columns.
    @Override DTree.Split bestCol( GBMUndecidedNode u ) {
      DTree.Split best = new DTree.Split(-1,-1,false,0L,0L,Double.MAX_VALUE,Double.MAX_VALUE,0,0);
      DHistogram hs[] = u._hs;
      if( hs == null ) return best;
      for( int i=0; i<hs.length; i++ ) {
        if( hs[i]==null || hs[i].nbins() <= 1 ) continue;
        DTree.Split s = hs[i].scoreMSE(i,u._tree._names[i]);
        if( s.mse() < best.mse() ) best = s;
        if( s.mse() <= 0 ) break; // No point in looking further!
      }
      return best;
    }
  }

  // GBM DTree undecided node: same as the normal UndecidedNode, but specifies
  // a list of columns to score on now, and then decide over later.
  // GBM algo: use all columns
  static class GBMUndecidedNode extends UndecidedNode {
    GBMUndecidedNode( DTree tree, int pid, DBinHistogram hs[] ) { super(tree,pid,hs); }

    // Randomly select mtry columns to 'score' in following pass over the data.
    // In GBM, we use all columns (as opposed to RF, which uses a random subset).
    @Override int[] scoreCols( DHistogram[] hs ) { return null; }
  }
}
