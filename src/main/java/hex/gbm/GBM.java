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
public class GBM extends FrameJob {
  static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  @API(help="", required=true, filter=GBMVecSelect.class)
  Vec vresponse;
  class GBMVecSelect extends VecSelect { GBMVecSelect() { super("source"); } }

  @API(help = "Number of trees", filter = NtreesFilter.class)
  int ntrees = 10;
  public class NtreesFilter implements Filter {
    @Override public boolean run(Object value) {
      int ntrees = (Integer)value;
      return 1 <= ntrees && ntrees <= 1000000;
    }
  }

  @API(help = "Maximum tree depth", filter = MaxDepthFilter.class)
  int max_depth = 8;
  public class MaxDepthFilter implements Filter {
    @Override public boolean run(Object value) { return 1 <= (Integer)value; }
  }

  @API(help = "Fewest allowed observations in a leaf", filter = MinRowsFilter.class)
  int min_rows = 5;
  public class MinRowsFilter implements Filter {
    @Override public boolean run(Object value) { return (Integer)value >= 1; }
  }

  @API(help = "Number of bins to split the column", filter = NBinsFilter.class)
  char nbins = 50;
  public class NBinsFilter implements Filter {
    @Override public boolean run(Object value) { return (Integer)value >= 2; }
  }

  @API(help = "Learning rate, from 0. to 1.0", filter = LearnRateFilter.class)
  double learn_rate = 0.1;
  public class LearnRateFilter implements Filter {
    @Override public boolean run(Object value) {
      double learn_rate = (Double)value;
      return 0.0 < learn_rate && learn_rate <= 1.0;
    }
  }

  @API(help = "The GBM Model")
  GBMModel gbm_model;

  // Overall prediction error as I add trees
  transient private float _errs[];

  public float progress(){
    DTree.TreeModel m = DKV.get(dest()).get();
    return m.forest.length/(float)m.N;
  }
  public static class GBMModel extends DTree.TreeModel {
    static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
    static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.
    public GBMModel(Key key, Key dataKey, Frame fr, int ntrees, DTree[] forest, float [] errs, String [] domain, int ymin, long [][] cm){
      super(key,dataKey,fr,ntrees,forest,errs,domain,ymin,cm);
    }
    @Override protected double score0(double[] data) {
      throw new RuntimeException("TODO: Score me");
    }
  }
  public Vec score( Frame fr ) { return gbm_model.score(fr,Key.make());  }

  public static final String KEY_PREFIX = "__GBMModel_";
  public static final Key makeKey() { return Key.make(KEY_PREFIX + Key.make());  }
  public GBM() { super("Distributed GBM",makeKey()); }

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
    final Frame fr = new Frame(source); // Local copy for local hacking

    // Doing classification only right now...
    if( !vresponse.isEnum() ) vresponse.asEnum();

    // While I'd like the Frames built custom for each call, with excluded
    // columns already removed - for now check to see if the response column is
    // part of the frame and remove it up front.
    for( int i=0; i<fr.numCols(); i++ )
      if( fr._vecs[i]==vresponse )
        fr.remove(i);

    String vname="response";
    for( int i=0; i<fr.numCols(); i++ )
      if( fr._vecs[i]==vresponse ) {
        vname=fr._names[i];
        fr.remove(i);
      }

    // Ignore-columns-code goes here....

    buildModel(fr,vname);
    return GBMProgressPage.redirect(this, self(),dest());
  }

  private void buildModel( final Frame fr, String vname ) {
    final Timer t_gbm = new Timer();
    final Frame frm = new Frame(fr); // Local copy for local hacking
    frm.add(vname,vresponse);        // Hardwire response as last vector

    assert 1 <= min_rows;
    final int  ncols = fr.numCols();
    final long nrows = fr.numRows();
    final int ymin = (int)vresponse.min();
    final char nclass = vresponse.isInt() ? (char)(vresponse.max()-ymin+1) : 1;
    assert 1 <= nclass && nclass < 1000; // Arbitrary cutoff for too many classes
    final String domain[] = nclass > 1 ? vresponse.domain() : null;
    _errs = new float[0];     // No trees yet
    final Key outputKey = dest();
    final Key dataKey = null;
    gbm_model = new GBMModel(outputKey,dataKey,frm,ntrees,new DTree[0], null, domain, ymin, null);
    DKV.put(outputKey, gbm_model);

    H2O.submitTask(start(new H2OCountedCompleter() {
      @Override public void compute2() {
        // Add in Vecs after the response column which holds the row-by-row
        // residuals: the (actual minus prediction), for each class.  The
        // prediction is a probability distribution across classes: every class is
        // assigned a probability from 0.0 to 1.0, and the sum of probs is 1.0.
        //
        // The initial prediction is just the class distribution.  The initial
        // residuals are then basically the actual class minus the average class.
        float preds[] = buildResiduals(nclass,fr,ncols,nrows,ymin);
        DTree init_tree = new DTree(fr._names,ncols,nbins,nclass,min_rows);
        new GBMDecidedNode(init_tree,preds);
        DTree forest[] = new DTree[] {init_tree};
        BulkScore bs = new BulkScore(forest,ncols,nclass,ymin,1.0f,false).doIt(fr,vresponse).report( Sys.GBM__, nrows, 0 );
        _errs = new float[]{(float)bs._err/nrows}; // Errors for exactly 1 tree
        gbm_model = new GBMModel(outputKey,dataKey,frm,ntrees,forest, _errs, domain, ymin,bs._cm);
        DKV.put(outputKey, gbm_model);

        // Build trees until we hit the limit
        for( int tid=1; tid<ntrees; tid++) {
          if(GBM.this.cancelled())break;
          forest = buildNextTree(fr,forest,ncols,nrows,nclass,ymin);

          // Tree-by-tree scoring
          Timer t_score = new Timer();
          BulkScore bs2 = new BulkScore(forest,ncols,nclass,ymin,1.0f,false).doIt(fr,vresponse).report( Sys.GBM__, nrows, max_depth );
          _errs = Arrays.copyOf(_errs,_errs.length+1);
          _errs[_errs.length-1] = (float)bs2._err/nrows;
          gbm_model = new GBMModel(outputKey, dataKey,frm, ntrees,forest, _errs, domain, ymin,bs2._cm);
          DKV.put(outputKey, gbm_model);
          Log.info(Sys.GBM__,"GBM final Scoring done in "+t_score);
        }
        Log.info(Sys.GBM__,"GBM Modeling done in "+t_gbm);
        // Remove temp vectors; cleanup the Frame
        while( fr.numCols() > ncols )
          UKV.remove(fr.remove(fr.numCols()-1)._key);
        GBM.this.remove();
        tryComplete();
      }
      @Override public boolean onExceptionalCompletion(Throwable ex, CountedCompleter caller) {
        ex.printStackTrace();
        GBM.this.cancel(ex.getMessage());
        return true;
      }
    }));
  }

  // Build the next tree, which is trying to correct the residual error from the prior trees.
  private DTree[] buildNextTree(Frame fr, DTree forest[], final int ncols, long nrows, final char nclass, int ymin) {
    // Make a new Vec to hold the split-number for each row (initially all zero).
    Vec vnids = vresponse.makeZero();
    fr.add("NIDs",vnids);
    // Initially setup as-if an empty-split had just happened
    final DTree tree = new DTree(fr._names,ncols,nbins,nclass,min_rows);
    new GBMUndecidedNode(tree,-1,DBinHistogram.initialHist(fr,ncols,nbins,nclass)); // The "root" node
    int leaf = 0; // Define a "working set" of leaf splits, from here to tree._len
    // Add tree to the end of the forest
    forest = Arrays.copyOf(forest,forest.length+1);
    forest[forest.length-1] = tree;

    // ----
    // One Big Loop till the tree is of proper depth.
    // Adds a layer to the tree each pass.
    int depth=0;
    for( ; depth<max_depth; depth++ ) {

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

    // Scale the tree down by the learning rate
    for( int i=0; i<tree._len; i++ ) {
      if( tree.node(i) instanceof DecidedNode ) {
        float pred[][] = tree.decided(i)._pred;
        if( pred != null )
          for( int b=0; b<pred.length; b++ )
            for( int c=0; c<pred[b].length; c++ )
              pred[b][c] *= learn_rate;
      }
    }

    // For each observation, find the residual(error) between predicted and desired.
    // Desired is in the old residual columns; predicted is in the decision nodes.
    // Replace the old residual columns with new residuals.
    new MRTask2() {
      @Override public void map( Chunk chks[] ) {
        Chunk ns = chks[chks.length-1];
        for( int i=0; i<ns._len; i++ ) {  // For all rows
          DecidedNode node = tree.decided((int)ns.at80(i));
          float preds[] = node._pred[node.bin(chks,i)];
          for( int c=0; c<nclass; c++ ) {
            double actual = chks[ncols+c].at0(i);
            double residual = actual-preds[c];
            chks[ncols+c].set0(i,(float)residual);
          }
        }
      }
    }.doAll(fr);

    // Remove the NIDs column
    assert fr._names[fr.numCols()-1].equals("NIDs");
    UKV.remove(fr.remove(fr.numCols()-1)._key);

    // Print the generated tree
    //System.out.println(tree.root().toString2(new StringBuilder(),0));

    return forest;
  }


  // Add in Vecs after the response column which holds the row-by-row
  // residuals: the (actual minus prediction), for each class.  The
  // prediction is a probability distribution across classes: every class is
  // assigned a probability from 0.0 to 1.0, and the sum of probs is 1.0.
  //
  // The initial prediction is just the class distribution.  The initial
  // residuals are then basically the actual class minus the average class.
  private float[] buildResiduals(char nclass, final Frame fr, final int ncols, long nrows, final int ymin ) {
    // Find the initial prediction - the current average response variable.
    float preds[] = new float[nclass];
    if( nclass == 1 ) {
      fr.add("Residual",vresponse.makeCon(vresponse.mean()));
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
          res.set0(i,1.0f+(float)res.at0(i)); // Fix residual for actual class
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

    @Override GBMUndecidedNode makeUndecidedNode(DTree tree, int nid, DBinHistogram[] nhists ) {
      return new GBMUndecidedNode(tree,nid,nhists);
    }

    // Find the column with the best split (lowest score).  Unlike RF, GBM
    // scores on all columns and selects splits on all columns.
    @Override DTree.Split bestCol( GBMUndecidedNode u ) {
      DTree.Split best = new DTree.Split(-1,-1,0L,0L,Double.MAX_VALUE,Double.MAX_VALUE,null,null);
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
