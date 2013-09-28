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

  public static class GBMModel extends DTree.TreeModel {
    static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
    static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

    public GBMModel(Key key, Key dataKey, Frame fr, int ntrees, DTree[][] forest, double [] errs, int ymin, long [][] cm){
      super(key,dataKey,fr,ntrees,forest,errs,ymin,cm);
    }
  }
  public Frame score( Frame fr ) { return ((GBMModel)UKV.get(dest())).score(fr,true);  }

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

  protected void buildModel( final Frame fr, final Frame frm, final Key outputKey, final Key dataKey, final Timer t_build ) {
    GBMModel gbm_model = new GBMModel(outputKey,dataKey,frm,ntrees,new DTree[0][], null, _ymin, null);
    DKV.put(outputKey, gbm_model);

    H2O.submitTask(start(new H2OCountedCompleter() {
      @Override public void compute2() {
        // The Initial Forest is empty
        DTree forest[/*Trees*/][/*nclass*/] = new DTree[0][];

        // Build trees until we hit the limit
        for( int tid=0; tid<ntrees; tid++) {

          // ESL2, page 387
          // Step 2a: Compute prob distribution from prior tree results: 
          //   Work <== f(Tree)
          new ComputeProb().doAll(fr);

          // ESL2, page 387
          // Step 2b i: Compute residuals from the probability distribution
          //   Work <== f(Work)
          new ComputeRes().doAll(fr);
          
          // ESL2, page 387, Step 2b ii, iii, iv
          forest = buildNextKTrees(fr,forest);
          if( cancelled() ) break; // If canceled during building, do not bulkscore

          // Check latest predictions
          Score sc = new Score().doAll(fr).report(Sys.GBM__,forest);
          _errs = Arrays.copyOf(_errs,_errs.length+1);
          _errs[_errs.length-1] = (float)sc._sum/_nrows;
          GBMModel gbm_model = new GBMModel(outputKey, dataKey,frm, ntrees,forest, _errs, _ymin,sc._cm);
          DKV.put(outputKey, gbm_model);
        }

        cleanUp(fr,t_build); // Shared cleanup
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
  // Work <== f(Tree)
  class ComputeProb extends MRTask2<ComputeProb> {
    @Override public void map( Chunk chks[] ) {
      Chunk ys = chk_resp(chks);
      double ds[] = new double[_nclass];
      for( int row=0; row<ys._len; row++ ) {
        double sum = score0(chks,ds,row);
        for( int k=0; k<_nclass; k++ ) // Save as a probability distribution
          chk_work(chks,k).set0(row,(float)(ds[k]/sum));
      }
    }
  }

  // Read the 'tree' columns, do model-specific math and put the results in the
  // ds[] array, and return the sum.  Dividing any ds[] element by the sum
  // turns the results into a probability distribution.
  @Override protected double score0( Chunk chks[], double ds[/*nclass*/], int row ) {
    double sum=0;
    for( int k=0; k<_nclass; k++ ) // Sum across of likelyhoods
      sum+=(ds[k]=Math.exp(chk_tree(chks,k).at0(row)));
    return sum;
  }

  // --------------------------------------------------------------------------
  // Compute Residuals from Actuals
  // Work <== f(Work)
  class ComputeRes extends MRTask2<ComputeRes> {
    @Override public void map( Chunk chks[] ) {
      Chunk ys = chk_resp(chks);
      for( int row=0; row<ys._len; row++ ) {
        int y = (int)ys.at80(row)-_ymin; // zero-based response variable
        for( int k=0; k<_nclass; k++ ) {
          Chunk wk = chk_work(chks,k);
          wk.set0(row, (y==k?1f:0f)-(float)wk.at0(row) );
        }
      }
    }
  }

  // --------------------------------------------------------------------------
  // Build the next k-trees, which is trying to correct the residual error from
  // the prior trees.  From LSE2, page 387.  Step 2b ii, iii.
  private DTree[][] buildNextKTrees(Frame fr, DTree forest[][]) {
    String domain[] = fr.vecs()[_ncols].domain(); // For printing

    // We're going to build K (nclass) trees - each focused on correcting
    // errors for a single class.
    final DTree[] ktrees = new DTree[_nclass];
    for( int k=0; k<_nclass; k++ ) {
      // Initially setup as-if an empty-split had just happened
      ktrees[k] = new DTree(fr._names,_ncols,(char)nbins,(char)_nclass,min_rows);
      new GBMUndecidedNode(ktrees[k],-1,DBinHistogram.initialHist(fr,_ncols,(char)nbins)); // The "root" node
    }

    // Add ktrees to the end of the forest
    DTree[][] oldForest = forest;
    forest = Arrays.copyOf(forest,forest.length+1);
    forest[forest.length-1] = ktrees;
    int[] leafs = new int[_nclass]; // Define a "working set" of leaf splits, from here to tree._len

    // ----
    // ESL2, page 387.  Step 2b ii.
    // One Big Loop till the ktrees are of proper depth.
    // Adds a layer to the trees each pass.
    int depth=0;
    for( ; depth<max_depth; depth++ ) {
      if( cancelled() ) return oldForest;

      // Build K trees, one per class.
      // Fuse 2 conceptual passes into one:
      // Pass 1: Score a prior DHistogram, and make new DTree.Node assignments
      // to every row.  This involves pulling out the current assigned Node,
      // "scoring" the row against that Node's decision criteria, and assigning
      // the row to a new child Node (and giving it an improved prediction).
      // Pass 2: Build new summary DHistograms on the new child Nodes every row
      // got assigned into.  Collect counts, mean, variance, min, max per bin,
      // per column.
      System.out.println("QQ"+Arrays.toString(leafs));
      ScoreBuildHistogram sbh = new ScoreBuildHistogram(ktrees,leafs).doAll(fr);
      //System.out.println(sbh.profString());

      // Build up the next-generation tree splits from the current histograms.
      // Nearly all leaves will split one more level.  This loop nest is
      //           O( #active_splits * #bins * #ncols )
      // but is NOT over all the data.
      boolean did_split=false;
      for( int k=0; k<_nclass; k++ ) {
        DTree tree = ktrees[k]; // Tree for class K
        int tmax = tree._len;   // Number of total splits in tree K
        for( int leaf=leafs[k]; leaf<tmax; leaf++ ) { // Visit all the new splits (leaves)
          UndecidedNode udn = tree.undecided(leaf);
          udn._hs = sbh.getFinalHisto(k,leaf);

          // Check for a constant response & stub out the tree
          DBinHistogram h=null;
          for( int i=0; i<_ncols; i++ )
            if( udn._hs[i] instanceof DBinHistogram ) 
              { h=(DBinHistogram)udn._hs[i]; break; }
          if( h.isConstantResponse() ) {
            new GBMLeafNode(tree,-1); // Stub the root
            tmax = tree._len;           // No new leaves
            break;                      // Done with this tree
          }

          //System.out.println("Class "+domain[k]+", "+udn);
          // Replace the Undecided with the Split decision
          GBMDecidedNode dn = new GBMDecidedNode((GBMUndecidedNode)udn);
          //System.out.println(dn);
          did_split = true;
        }
        leafs[k]=tmax;          // Setup leafs for next tree level
      }
    
      // If we did not make any new splits, then the tree is split-to-death
      if( !did_split ) break;
    }

    // Each tree bottomed-out in a DecidedNode; go 1 more level and insert
    // LeafNodes to hold predictions.
    for( int k=0; k<_nclass; k++ ) {
      DTree tree = ktrees[k];
      int leaf = leafs[k] = tree._len;
      for( int nid=0; nid<leaf; nid++ ) {
        if( tree.node(nid) instanceof DecidedNode ) {
          DecidedNode dn = tree.decided(nid);
          for( int i=0; i<dn._nids.length; i++ ) {
            int cnid = dn._nids[i];
            if( cnid == -1 ||   // Bottomed out
                tree.node(cnid) instanceof UndecidedNode ) // Or chopped off for depth
              dn._nids[i] = new GBMLeafNode(tree,nid)._nid;
          }
        }
      }
    }

    // ----
    // ESL2, page 387.  Step 2b iii.  Compute the gammas, and store them back
    // into the tree leaves.  Includes learn_rate.
    // gamma_i_k = (nclass-1)/nclass * (sum res_i / sum (|res_i|*(1-|res_i|)))
    GammaPass gp = new GammaPass(ktrees,leafs).doAll(fr);
    double m1class = (double)(_nclass-1)/_nclass; // K-1/K
    for( int k=0; k<_nclass; k++ ) {
      final DTree tree = ktrees[k];
      for( int i=0; i<tree._len-leafs[k]; i++ ) {
        assert gp._gss[k][i] != 0 : "Split until an empty leaf? nid="+(leafs[k]+i)+"  tree="+k;
        double g = learn_rate*m1class*gp._rss[k][i]/gp._gss[k][i];
        ((LeafNode)tree.node(leafs[k]+i))._pred = g;
      }
    }

    // ----
    // ESL2, page 387.  Step 2b iv.  Cache the sum of all the trees, plus the
    // new tree, in the 'tree' columns.  Also, zap the NIDs for next pass.
    // Tree <== f(Tree)
    // Nids <== 0
    new MRTask2() {
      @Override public void map( Chunk chks[] ) {
        // For all tree/klasses
        for( int k=0; k<_nclass; k++ ) {
          final DTree tree = ktrees[k];
          final Chunk nids = chk_nids(chks,k);
          final Chunk ct   = chk_tree(chks,k);
          for( int row=0; row<nids._len; row++ ) {
            int nid = (int)nids.at80(row);
            ct.set0(row, ct.at0(row) + ((LeafNode)tree.node(nid))._pred);
            nids.set0(row,0);
          }            
        }
      }
    }.doAll(fr);

    // Print the generated K trees
    for( int k=0; k<_nclass; k++ )
      System.out.println(ktrees[k].root().toString2(new StringBuilder(),0));

    return forest;
  }


  // ---
  // ESL2, page 387.  Step 2b iii.
  // Nids <== f(Nids)
  private class GammaPass extends MRTask2<GammaPass> {
    final DTree _trees[]; // Read-only, shared (except at the histograms in the Nodes)
    final int   _leafs[]; // Number of active leaves (per tree)
    // Per leaf: sum(res);
    double _rss[/*tree/klass*/][/*tree-relative node-id*/];
    // Per leaf:  sum(|res|*1-|res|)
    double _gss[/*tree/klass*/][/*tree-relative node-id*/];
    GammaPass(DTree trees[], int leafs[]) { _leafs=leafs; _trees=trees; }
    @Override public void map( Chunk[] chks ) {
      _gss = new double[_nclass][];
      _rss = new double[_nclass][];
      // For all tree/klasses
      for( int k=0; k<_nclass; k++ ) {
        final DTree tree = _trees[k];
        final int   leaf = _leafs[k];
        // A leaf-biased array of all active histograms
        final double gs[] = _gss[k] = new double[tree._len-leaf];
        final double rs[] = _rss[k] = new double[tree._len-leaf];
        final Chunk nids = chk_nids(chks,k);
        final Chunk wrks = chk_work(chks,k);
        for( int row=0; row<nids._len; row++ ) {
          int nid = (int)nids.at80(row); // Get Node to decide from
          if( tree.node(nid) instanceof UndecidedNode )
            nid = tree.node(nid)._pid;
          int leafnid = tree.decided(nid).ns(chks,row);
          assert leaf <= leafnid && leafnid < tree._len;
          assert tree.node(leafnid) instanceof LeafNode;
          nids.set0(row,leafnid);
          double res = wrks.at0(row);
          double ares = Math.abs(res);
          gs[leafnid-leaf] += ares*(1-ares);
          rs[leafnid-leaf] += res;
        }
      }
    }
    @Override public void reduce( GammaPass gp ) { 
      Utils.add(_gss,gp._gss); 
      Utils.add(_rss,gp._rss); 
    }
  }

  // ---
  // GBM DTree decision node: same as the normal DecidedNode, but
  // specifies a decision algorithm given complete histograms on all
  // columns.  GBM algo: find the lowest error amongst *all* columns.
  static class GBMDecidedNode extends DecidedNode<GBMUndecidedNode> {
    GBMDecidedNode( GBMUndecidedNode n ) { super(n); }
    @Override GBMUndecidedNode makeUndecidedNode(DTree tree, int nid, DBinHistogram[] nhists ) {
      return new GBMUndecidedNode(tree,nid,nhists);
    }

    // Find the column with the best split (lowest score).  Unlike RF, GBM
    // scores on all columns and selects splits on all columns.
    @Override DTree.Split bestCol( GBMUndecidedNode u ) {
      DTree.Split best = new DTree.Split(-1,-1,false,Double.MAX_VALUE,Double.MAX_VALUE,0L,0L);
      DHistogram hs[] = u._hs;
      if( hs == null ) return best;
      for( int i=0; i<hs.length; i++ ) {
        if( hs[i]==null || hs[i].nbins() <= 1 ) continue;
        DTree.Split s = hs[i].scoreMSE(i);
        if( s.se() < best.se() ) best = s;
        if( s.se() <= 0 ) break; // No point in looking further!
      }
      return best;
    }
  }

  // ---
  // GBM DTree undecided node: same as the normal UndecidedNode, but specifies
  // a list of columns to score on now, and then decide over later.
  // GBM algo: use all columns
  static class GBMUndecidedNode extends UndecidedNode {
    GBMUndecidedNode( DTree tree, int pid, DBinHistogram hs[] ) { super(tree,pid,hs); }
    // Randomly select mtry columns to 'score' in following pass over the data.
    // In GBM, we use all columns (as opposed to RF, which uses a random subset).
    @Override int[] scoreCols( DHistogram[] hs ) { return null; }
  }

  // ---
  static class GBMLeafNode extends LeafNode {
    GBMLeafNode( DTree tree, int pid ) { super(tree,pid); }
    GBMLeafNode( DTree tree, int pid, int nid ) { super(tree,pid,nid); }
    // Insert just the predictions: a single byte/short if we are predicting a
    // single class, or else the full distribution.
    @Override protected AutoBuffer compress(AutoBuffer ab) { return ab.put4f((float)_pred); }
    @Override protected int size() { return 4; }
  }
}
