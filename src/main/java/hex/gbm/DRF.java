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

// Random Forest Trees
public class DRF extends FrameJob {
  static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  @API(help="", required=true, filter=DRFVecSelect.class)
  Vec vresponse;
  class DRFVecSelect extends VecSelect { DRFVecSelect() { super("source"); } }

  @API(help = "Number of trees", filter = NtreesFilter.class)
  int ntrees = 50;
  public class NtreesFilter implements Filter {
    @Override public boolean run(Object value) { 
      int ntrees = (Integer)value; 
      return 1 <= ntrees && ntrees <= 1000000;
    }
  }

  @API(help = "Maximum tree depth", filter = MaxDepthFilter.class)
  int max_depth = 50;
  public class MaxDepthFilter implements Filter {
    @Override public boolean run(Object value) { return 1 <= (Integer)value; }
  }

  @API(help = "Columns to randomly select at each level, or -1 for sqrt(#cols)", filter = MTriesFilter.class)
  int mtries = -1;
  public class MTriesFilter implements Filter {
    @Override public boolean run(Object value) { 
      int mtries = (Integer)value; 
      if( mtries == -1 ) return true;
      if( mtries <=  0 ) return false;
      return mtries <= source.numCols();
    }
  }

  @API(help = "Sample rate, from 0. to 1.0", filter = SampleRateFilter.class)
  float sample_rate = 0.6666667f;
  public class SampleRateFilter implements Filter {
    @Override public boolean run(Object value) { 
      float sample_rate = (Float)value; 
      return 0.0 < sample_rate && sample_rate <= 1.0;
    }
  }

  @API(help = "Seed for the random number generator", filter = Default.class)
  long seed = new Random().nextLong();


  // JSON Output Fields
  @API(help="Classes")
  public String domain[];
  
  @API(help="Confusion Matrix")
  long _cm[/*actual*/][/*predicted*/]; // Confusion matrix
  public long[][] cm() { return _cm; }

  public static final String KEY_PREFIX = "__DRFModel_";
  public static final Key makeKey() { return Key.make(KEY_PREFIX + Key.make());  }
  public DRF() { super("Distributed Random Forest",makeKey()); }

  /** Return the query link to this page */
  public static String link(Key k, String content) {
    RString rs = new RString("<a href='DRF.query?source=%$key'>%content</a>");
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
        sb.append(_cm[i][j]).append("</td>");
        sum += _cm[i][j];       // Per-class observations
        if( i != j ) err += _cm[i][j]; // and errors
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
      for( int i=0; i<domain.length; i++ ) sum += _cm[i][j];
      sb.append("<td>").append(sum).append("</td>");
    }
    sb.append(String.format("<th>%5.3f = %d / %d</th>", (double)terr/tsum, terr, tsum));
    sb.append("</tr>");

    DocGen.HTML.arrayTail(sb);
    return true;
  }


  // ==========================================================================

  // Compute a DRF tree.  

  // Start by splitting all the data according to some criteria (minimize
  // variance at the leaves).  Record on each row which split it goes to, and
  // assign a split number to it (for next pass).  On *this* pass, use the
  // split-number to build a per-split histogram, with a per-histogram-bucket
  // variance.

  // Compute a single DRF tree from the Frame.  Last column is the response
  // variable.  Depth is capped at max_depth.
  @Override protected Response serve() {
    Timer t_drf = new Timer();
    final Frame fr = new Frame(source); // Local copy for local hacking
    if( !vresponse.isEnum() ) vresponse.asEnum();
    // While I'd like the Frames built custom for each call, with excluded
    // columns already removed - for now check to see if the response column is
    // part of the frame and remove it up front.
    for( int i=0; i<fr.numCols(); i++ )
      if( fr._vecs[i]==vresponse )
        fr.remove(i);

    final int mtrys = (mtries==-1) ? Math.max((int)Math.sqrt(fr.numCols()),1) : mtries;
    assert 0 <= ntrees && ntrees < 1000000;
    assert 1 <= mtrys && mtrys <= fr.numCols() : "Too large mtrys="+mtrys+", ncols="+fr.numCols();
    assert 0.0 < sample_rate && sample_rate <= 1.0;
    final String names[] = fr._names;
    final int  ncols = fr.numCols();
    final long nrows = fr.numRows();
    final int  ymin  = (int)vresponse.min();
    short nclass = vresponse.isInt() ? (short)(vresponse.max()-ymin+1) : 1;
    assert 1 <= nclass && nclass < 1000; // Arbitrary cutoff for too many classes
    domain = nclass > 1 ? vresponse.domain() : null;

    // Fill in the response variable column(s)
    if( nclass == 1 ) {
      fr.add("response",vresponse);
    } else {
      // A vector of {0,..,0,1,0,...}
      // A single 1.0 in the actual class.
      for( int i=0; i<nclass; i++ ) 
        fr.add(domain[i],vresponse.makeZero());
      fr.add("response",vresponse);
      // Set a single 1.0 in the response for that class
      new MRTask2() {
        @Override public void map( Chunk chks[] ) {
          Chunk cy = chks[chks.length-1];
          for( int i=0; i<cy._len; i++ ) {
            if( cy.isNA0(i) ) continue;
            int cls = (int)cy.at80(i) - ymin;
            chks[ncols+cls].set0(i,1.0f);
          }
        }
      }.doAll(fr);
      fr.remove(ncols+nclass);
    }

    // The RNG used to pick split columns
    Random rand = new MersenneTwisterRNG(new int[]{(int)(seed>>32L),(int)seed});

    // Initially setup as-if an empty-split had just happened
    DHistogram hs[] = DBinHistogram.initialHist(fr,ncols,nclass);
    DRFTree forest[] = new DRFTree[ntrees];
    Vec[] nids = new Vec[ntrees];

    // ----
    // Only work on so many trees at once, else get GC issues.
    // Hand the inner loop a smaller set of trees.
    final int NTREE=2;          // Limit of 5 trees at once
    int depth=0;
    for( int st = 0; st < ntrees; st+= NTREE ) {
      int xtrees = Math.min(NTREE,ntrees-st);
      DRFTree someTrees[] = new DRFTree[xtrees];
      int someLeafs[] = new int[xtrees];

      for( int t=0; t<xtrees; t++ ) {
        int idx = st+t;
        // Make a new Vec to hold the split-number for each row (initially all zero).
        Vec vec = vresponse.makeZero();
        nids[idx] = vec;
        forest[idx] = someTrees[t] = new DRFTree(fr,ncols,nclass,hs,mtrys,rand.nextLong());
        if( sample_rate < 1.0 )
          new Sample(someTrees[t],sample_rate).doAll(vec);
        fr.add("NIDs"+t,vec);
      }

      // Make NTREE trees at once
      int d = makeSomeTrees(st, someTrees,someLeafs, xtrees, max_depth, fr, vresponse, ncols, nclass, ymin, nrows, sample_rate);
      if( d>depth ) depth=d;    // Actual max depth used

      // Remove temp vectors; cleanup the Frame
      for( int t=0; t<xtrees; t++ )
        UKV.remove(fr.remove(fr.numCols()-1)._key);
    }
    Log.info(Sys.DRF__,"DRF done in "+t_drf);

    // One more pass for final prediction error
    _cm = new BulkScore(forest,ncols,nclass,ymin,sample_rate,true).doIt(fr,vresponse).report( Sys.DRF__, nrows, depth )._cm;
    
    return new Response(Response.Status.done, this, -1, -1, null);
  }

  // ----
  // One Big Loop till the tree is of proper depth.
  // Adds a layer to the tree each pass.
  public int makeSomeTrees( int st, DRFTree trees[], int leafs[], int ntrees, int max_depth, Frame fr, Vec vresponse, int ncols, short nclass, int ymin, long nrows, double sample_rate ) {
    for( int depth=0; depth<max_depth; depth++ ) {
      Timer t_pass = new Timer();

      // Fuse 2 conceptual passes into one:
      // Pass 1: Score a prior DHistogram, and make new DTree.Node assignments
      // to every row.  This involves pulling out the current assigned Node,
      // "scoring" the row against that Node's decision criteria, and assigning
      // the row to a new child Node (and giving it an improved prediction).
      // Pass 2: Build new summary DHistograms on the new child Nodes every row
      // got assigned into.  Collect counts, mean, variance, min, max per bin,
      // per column.
      ScoreBuildHistogram sbh = new ScoreBuildHistogram(trees,leafs,ncols,nclass,ymin,fr).doAll(fr);

      // Reassign the new DHistograms back into the DTrees
      for( int t=0; t<ntrees; t++ ) {
        final int tmax = trees[t]._len; // Number of total splits
        final DTree tree = trees[t];
        long sum=0;
        for( int i=leafs[t]; i<tmax; i++ ) {
          DHistogram hs[] = sbh.getFinalHisto(t,i);
          tree.undecided(i)._hs = hs;
          for( DHistogram h : hs )
            if( h != null ) sum += h.byteSize();
        }
        //System.out.println("Tree#"+(st+t)+", leaves="+(trees[t]._len-leafs[t])+", histo size="+PrettyPrint.bytes(sum)+", time="+t_pass);
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
          //System.out.println("Tree#"+(st+t)+", "+tree.undecided(leaf));
          // Replace the Undecided with the Split decision
          new DRFDecidedNode((DRFUndecidedNode)tree.undecided(leaf));
        }
        leafs[t] = leaf;
        // If we did not make any new splits, then the tree is split-to-death
        if( tmax < tree._len ) still_splitting = true;
      }

      // If all trees are done, then so are we
      if( !still_splitting ) return depth;
      //new BulkScore(trees,ncols,nclass,ymin,(float)sample_rate,true).doIt(fr,vresponse).report( Sys.DRF__, nrows, depth );
    }
    return max_depth;
  }

  // A standard DTree with a few more bits.  Support for sampling during
  // training, and replaying the sample later on the identical dataset to
  // e.g. compute OOBEE.
  static class DRFTree extends DTree {
    final int _mtrys;           // Number of columns to choose amongst in splits
    final long _seed;           // RNG seed; drives sampling seeds
    final long _seeds[];        // One seed for each chunk, for sampling
    final transient Random _rand; // RNG for split decisions & sampling
    DRFTree( Frame fr, int ncols, int nclass, DHistogram hs[], int mtrys, long seed ) { 
      super(fr._names, ncols, nclass); 
      _mtrys = mtrys; 
      _seed = seed;                  // Save for any replay scenarios
      _rand = new MersenneTwisterRNG(new int[]{(int)(seed>>32),(int)seed});
      _seeds = new long[fr._vecs[0].nChunks()];
      for( int i=0; i<_seeds.length; i++ )
        _seeds[i] = _rand.nextLong();
      new DRFUndecidedNode(this,-1,hs); // The "root" node 
    }
    // Return a deterministic chunk-local RNG.  Can be kinda expensive.
    @Override public Random rngForChunk( int cidx ) {
      long seed = _seeds[cidx];
      return new MersenneTwisterRNG(new int[]{(int)(seed>>32),(int)seed});
    }
  }

  // DRF DTree decision node: same as the normal DecidedNode, but specifies a
  // decision algorithm given complete histograms on all columns.  
  // DRF algo: find the lowest error amongst a random mtry columns.
  static class DRFDecidedNode extends DecidedNode<DRFUndecidedNode> {
    DRFDecidedNode( DRFUndecidedNode n ) { super(n); }

    @Override DRFUndecidedNode makeUndecidedNode(DTree tree, int nid, DHistogram[] nhists ) { 
      return new DRFUndecidedNode(tree,nid,nhists); 
    }

    // Find the column with the best split (lowest score).
    @Override int bestCol( DRFUndecidedNode u ) {
      double bs = Double.MAX_VALUE; // Best score
      int idx = -1;                 // Column to split on
      for( int i=0; i<u._scoreCols.length; i++ ) {
        int col = u._scoreCols[i];
        double s = u._hs[col].score();
        if( s < bs ) { bs = s; idx = col; }
        if( s <= 0 ) break;     // No point in looking further!
      }
      return idx;
    }
  }

  // DRF DTree undecided node: same as the normal UndecidedNode, but specifies
  // a list of columns to score on now, and then decide over later.
  // DRF algo: pick a random mtry columns
  static class DRFUndecidedNode extends UndecidedNode {
    DRFUndecidedNode( DTree tree, int pid, DHistogram hs[] ) { super(tree,pid,hs); }

    // Randomly select mtry columns to 'score' in following pass over the data.
    @Override int[] scoreCols( DHistogram[] hs ) {
      DRFTree tree = (DRFTree)_tree;
      int[] cols = new int[hs.length];
      int len=0;
      // Gather all active columns to choose from.  Ignore columns we
      // previously ignored, or columns with 1 bin (nothing to split), or
      // histogramed bin min==max (means the predictors are constant).
      for( int i=0; i<hs.length; i++ ) {
        if( hs[i]==null ) continue; // Ignore not-tracked cols
        if( hs[i]._min == hs[i]._max ) continue; // predictor min==max, does not distinguish
        if( hs[i] instanceof DBinHistogram && hs[i].nbins() <= 1 ) 
          continue;             // cols with 1 bin (will not split)
        cols[len++] = i;        // Gather active column
      }
      int choices = len;        // Number of columns I can choose from
      if( choices == 0 ) {
        for( int i=0; i<hs.length; i++ ) {
          String s;
          if( hs[i]==null ) s="null";
          else if( hs[i]._min == hs[i]._max ) s="min==max=="+hs[i]._min;
          else if( hs[i] instanceof DBinHistogram && hs[i].nbins() <= 1 ) 
            s="nbins="+hs[i].nbins();
          else s="unk";
          System.out.println("No choices, hists="+s);
        }
      }
      assert choices > 0;

      // Draw up to mtry columns at random without replacement.
      double bs = Double.MAX_VALUE; // Best score
      for( int i=0; i<tree._mtrys; i++ ) {
        if( len == 0 ) break;   // Out of choices!
        int idx2 = tree._rand.nextInt(len);
        int col = cols[idx2];     // The chosen column
        cols[idx2] = cols[--len]; // Compress out of array; do not choose again
        cols[len] = col;          // Swap chosen in just after 'len'
      }
      assert choices - len > 0;
      return Arrays.copyOfRange(cols,len,choices);
    }
  }

  // Determinstic sampling
  static class Sample extends MRTask2<Sample> {
    final DRFTree _tree;
    final float _rate;
    Sample( DRFTree tree, double rate ) { _tree = tree; _rate = (float)rate; }
    @Override public void map( Chunk nids ) {
      Random rand = _tree.rngForChunk(nids.cidx());
      for( int i=0; i<nids._len; i++ )
        if( rand.nextFloat() >= _rate )
          nids.set0(i,-2);     // Flag row as being ignored by sampling
    }
  }
}
