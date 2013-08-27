package hex.gbm;

import hex.gbm.DTree.*;
import hex.rng.MersenneTwisterRNG;
import java.util.Arrays;
import java.util.Random;
import water.*;
import water.api.DocGen;
import water.Job.FrameJob;
import water.fvec.Chunk;
import water.fvec.Frame;
import water.fvec.Vec;
import water.util.Log.Tag.Sys;
import water.util.Log;
import water.util.RString;

// Gradient Boosted Trees
public class GBM extends FrameJob {

  static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  @API(help="", required=true, filter=DRFVecSelect.class)
  Vec vresponse;
  class DRFVecSelect extends VecSelect { DRFVecSelect() { super("source"); } }

  @API(help = "Learning rate, from 0. to 1.0", filter = LearnRateFilter.class)
  double learn_rate = 0.1f;
  public class LearnRateFilter implements Filter {
    @Override public boolean run(Object value) { 
      double learn_rate = (Double)value; 
      return 0.0 < learn_rate && learn_rate <= 1.0;
    }
  }

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


  // JSON Output Fields
  @API(help="Class names")
  public String domain[];
  
  @API(help="Confusion Matrix[actual_class][predicted_class]")
  long cm[/*actual*/][/*predicted*/]; // Confusion matrix

  @API(help="Error by tree")
  float errs[/*ntrees*/]; // Error rate, as trees are added


  public static final String KEY_PREFIX = "__GBMModel_";
  public static final Key makeKey() { return Key.make(KEY_PREFIX + Key.make());  }
  public GBM() { super("Distributed Gradiant Boosted Forest",makeKey()); }

  /** Return the query link to this page */
  public static String link(Key k, String content) {
    RString rs = new RString("<a href='GBM.query?source=%$key'>%content</a>");
    rs.replace("key", k.toString());
    rs.replace("content", content);
    return rs.toString();
  }

  @Override public boolean toHTML( StringBuilder sb ) {
    DocGen.HTML.title(sb,description);
    DocGen.HTML.section(sb,"Confusion Matrix");

    // Top row of CM
    DocGen.HTML.arrayHead(sb);
    sb.append("<tr class='warning'>");
    sb.append("<th>Actual / Predicted</th>"); // Row header
    for( int i=0; i<domain.length; i++ )
      sb.append("<th>").append(domain[i]).append("</th>");
    sb.append("<th>Error</th>");
    sb.append("</tr>");

    // Main CM Body
    long tsum=0, terr=0;                   // Total observations & errors
    for( int i=0; i<domain.length; i++ ) { // Actual loop
      sb.append("<tr>");
      sb.append("<th>").append(domain[i]).append("</th>");// Row header
      long sum=0, err=0;                     // Per-class observations & errors
      for( int j=0; j<domain.length; j++ ) { // Predicted loop
        sb.append(i==j ? "<td style='background-color:LightGreen'>":"<td>");
        sb.append(cm[i][j]).append("</td>");
        sum += cm[i][j];              // Per-class observations
        if( i != j ) err += cm[i][j]; // and errors
      }
      sb.append(String.format("<th>%5.3f = %d / %d</th>", (double)err/sum, err, sum));
      tsum += sum;  terr += err; // Bump totals
    }
    sb.append("</tr>");

    // Last row of CM
    sb.append("<tr>");
    sb.append("<th>Totals</th>");// Row header
    for( int j=0; j<domain.length; j++ ) { // Predicted loop
      long sum=0;
      for( int i=0; i<domain.length; i++ ) sum += cm[i][j];
      sb.append("<td>").append(sum).append("</td>");
    }
    sb.append(String.format("<th>%5.3f = %d / %d</th>", (double)terr/tsum, terr, tsum));
    sb.append("</tr>");

    DocGen.HTML.arrayTail(sb);

    DocGen.HTML.section(sb,"Error Rate by Tree");
    DocGen.HTML.arrayHead(sb);
    sb.append("<tr><th>Trees</th>");
    for( int i=0; i<errs.length; i++ )
      sb.append("<td>").append(i+1).append("</td>");
    sb.append("</tr>");
    sb.append("<tr><th class='warning'>Error Rate</th>");
    for( int i=0; i<errs.length; i++ )
      sb.append("<td>").append(errs[i]).append("</td>");
    sb.append("</tr>");

    DocGen.HTML.arrayTail(sb);
    return true;
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
    Timer t_gbm = new Timer();
    final Frame fr = new Frame(source); // Local copy for local hacking
    // While I'd like the Frames built custom for each call, with excluded
    // columns already removed - for now check to see if the response column is
    // part of the frame and remove it up front.
    for( int i=0; i<fr.numCols(); i++ )
      if( fr._vecs[i]==vresponse )
        fr.remove(i);

    final int  ncols = fr.numCols();
    final long nrows = fr.numRows();
    final int ymin = (int)vresponse.min();
    short nclass = vresponse._isInt ? (short)(vresponse.max()-ymin+1) : 1;
    assert 1 <= nclass && nclass < 1000; // Arbitrary cutoff for too many classes
    domain = vresponse.domain();
    errs = new float[0];         // No trees yet

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
    for( int tid=1; tid<ntrees; tid++)
      forest = buildNextTree(fr,vresponse,forest,ncols,nrows,nclass,ymin,max_depth,1);

    Log.info(Sys.GBM__,"GBM Modeling done in "+t_gbm);

    // One more pass for final prediction error
    Timer t_score = new Timer();
    cm = new BulkScore(forest,ncols,nclass,ymin,1.0f,false).doIt(fr,vresponse).report( Sys.GBM__, nrows, max_depth )._cm;
    Log.info(Sys.GBM__,"GBM final Scoring done in "+t_score);

    // Remove temp vectors; cleanup the Frame
    while( fr.numCols() > ncols )
      UKV.remove(fr.remove(fr.numCols()-1)._key);

    return new Response(Response.Status.done, this, -1, -1, null);
  }


  // Build the next tree, which is trying to correct the residual error from the prior trees.
  private DTree[] buildNextTree(Frame fr, Vec vresponse, DTree forest[], final int ncols, long nrows, final short nclass, int ymin, int max_depth, final int tid) {
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
    long err = new BulkScore(forest,ncols,nclass,ymin,1.0f,false).doIt(fr,vresponse).report( Sys.GBM__, nrows, depth )._err;
    errs = Arrays.copyOf(errs,errs.length+1);
    errs[errs.length-1] = (float)err/nrows;

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
          res.set40(i,1.0f+(float)res.at0(i));   // Fix residual for actual class
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
