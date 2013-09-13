package hex.gbm;

import hex.ScoreTask;
import hex.gbm.DTree.*;
import hex.rng.MersenneTwisterRNG;
import java.util.Arrays;
import java.util.Random;
import jsr166y.CountedCompleter;
import water.*;
import water.H2O.H2OCountedCompleter;
import water.Job.FrameJob;
import water.api.DRFProgressPage;
import water.api.DocGen;
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

  @API(help="Response vector", required=true, filter=DRFVecSelect.class)
  Vec vresponse;
  class DRFVecSelect extends VecClassSelect { DRFVecSelect() { super("source"); } }

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

  @API(help = "Fewest allowed observations in a leaf", filter = MinRowsFilter.class)
  int min_rows = 5;
  public class MinRowsFilter implements Filter {
    @Override public boolean run(Object value) { return (Integer)value >= 1; }
  }

  @API(help = "Number of bins to split the column", filter = NBinsFilter.class)
  int nbins = 100;
  public class NBinsFilter implements Filter {
    @Override public boolean run(Object value) { return (Integer)value >= 2; }
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

  @API(help = "The DRF Model")
  DRFModel drf_model;

  // Overall prediction error as I add trees
  transient private float _errs[];

  public float progress(){
    DTree.TreeModel m = DKV.get(dest()).get();
    return m.treeBits.length/(float)m.N;
  }
  public static class DRFModel extends DTree.TreeModel {
    static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
    static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.
    public DRFModel(Key key, Key dataKey, Frame fr, int ntrees, DTree[] forest, float [] errs, int ymin, long [][] cm){
      super(key,dataKey,fr,ntrees,forest,errs,ymin,cm);
    }
    @Override protected double score0(double[] data) {
      throw new RuntimeException("TODO: Score me");
    }
  }
  public Vec score( Frame fr ) { return drf_model.score(fr,true);  }

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
    final Frame fr = new Frame(source); // Local copy for local hacking

    // Doing classification only right now...
    if( !vresponse.isEnum() ) vresponse.asEnum();

    // While I'd like the Frames built custom for each call, with excluded
    // columns already removed - for now check to see if the response column is
    // part of the frame and remove it up front.
    String vname="response";
    for( int i=0; i<fr.numCols(); i++ )
      if( fr._vecs[i]==vresponse ) {
        vname=fr._names[i];
        fr.remove(i);
      }

    // Ignore-columns-code goes here....

    buildModel(fr,vname);
    return DRFProgressPage.redirect(this, self(),dest());
  }

  private void buildModel( final Frame fr, String vname ) {
    final Timer t_drf = new Timer();
    final Frame frm = new Frame(fr); // Local copy for local hacking
    frm.add(vname,vresponse);        // Hardwire response as last vector

    final int mtrys = (mtries==-1) ? Math.max((int)Math.sqrt(fr.numCols()),1) : mtries;
    assert 0 <= ntrees && ntrees < 1000000;
    assert 1 <= mtrys && mtrys <= fr.numCols() : "Too large mtrys="+mtrys+", ncols="+fr.numCols();
    assert 0.0 < sample_rate && sample_rate <= 1.0;
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
    drf_model = new DRFModel(outputKey,dataKey,frm,ntrees,new DTree[0],null, ymin, null);
    DKV.put(outputKey, drf_model);

    H2O.submitTask(start(new H2OCountedCompleter() {
      @Override public void compute2() {

        // Fill in the response variable column(s)
        if( nclass == 1 ) {
          fr.add("response",vresponse);
        } else {
          // A vector of {0,..,0,1,0,...}
          // A single 1.0 in the actual class.
          for( int i=0; i<nclass; i++ )
            fr.add(domain[i],vresponse.makeZero());
          // Set a single 1.0 in the response for that class
          fr.add("response",vresponse);
          new Set1Task(ymin,ncols).doAll(fr);
          fr.remove(ncols+nclass);
        }
        
        // The RNG used to pick split columns
        Random rand = new MersenneTwisterRNG(new int[]{(int)(seed>>32L),(int)seed});
        
        // Initially setup as-if an empty-split had just happened
        DBinHistogram hs[] = DBinHistogram.initialHist(fr,ncols,(char)nbins,nclass);
        DRFTree forest[] = new DRFTree[0];

        // ----
        // Only work on so many trees at once, else get GC issues.
        // Hand the inner loop a smaller set of trees.
        final int NTREE=2;          // Limit of 5 trees at once
        int depth=0;
        for( int st = 0; st < ntrees; st+= NTREE ) {
          if( DRF.this.cancelled() ) break;
          int xtrees = Math.min(NTREE,ntrees-st);
          DRFTree someTrees[] = new DRFTree[xtrees];
          int someLeafs[] = new int[xtrees];
          forest = Arrays.copyOf(forest,forest.length+xtrees);
          
          for( int t=0; t<xtrees; t++ ) {
            int idx = st+t;
            // Make a new Vec to hold the split-number for each row (initially all zero).
            Vec vec = vresponse.makeZero();
            forest[idx] = someTrees[t] = new DRFTree(fr,ncols,(char)nbins,nclass,min_rows,hs,mtrys,rand.nextLong());
            if( sample_rate < 1.0 )
              new Sample(someTrees[t],sample_rate).doAll(vec);
            fr.add("NIDs"+t,vec);
          }
          
          // Make NTREE trees at once
          int d = makeSomeTrees(st, someTrees,someLeafs, xtrees, max_depth, fr, vresponse, ncols, nclass, ymin, nrows, sample_rate);
          if( d>depth ) depth=d;    // Actual max depth used
          
          BulkScore bs = new BulkScore(forest,ncols,nclass,ymin,sample_rate,true).doIt(fr,vresponse).report( Sys.DRF__, depth );
          int old = _errs.length;
          _errs = Arrays.copyOf(_errs,st+xtrees);
          for( int i=old; i<_errs.length; i++ ) _errs[i] = Float.NaN;
          _errs[_errs.length-1] = (float)bs._sum/nrows;
          drf_model = new DRFModel(outputKey,dataKey,frm,ntrees,forest, _errs, ymin,bs._cm);
          DKV.put(outputKey, drf_model);

          // Remove temp vectors; cleanup the Frame
          for( int t=0; t<xtrees; t++ )
            UKV.remove(fr.remove(fr.numCols()-1)._key);
        }
        Log.info(Sys.DRF__,"DRF done in "+t_drf);
        
        // Remove temp vectors; cleanup the Frame
        while( fr.numCols() > ncols )
          UKV.remove(fr.remove(fr.numCols()-1)._key);
        DRF.this.remove();
        tryComplete();
      }
      @Override public boolean onExceptionalCompletion(Throwable ex, CountedCompleter caller) {
        ex.printStackTrace();
        DRF.this.cancel(ex.getMessage());
        return true;
      }
    }));
  }

  private static class Set1Task extends MRTask2<Set1Task> {
    final int _ymin, _ncols;
    Set1Task( int ymin, int ncols ) { _ymin = ymin; _ncols = ncols; }
    @Override public void map( Chunk chks[] ) {
      Chunk cy = chks[chks.length-1];
      for( int i=0; i<cy._len; i++ ) {
        if( cy.isNA0(i) ) continue;
        int cls = (int)cy.at80(i) - _ymin;
        chks[_ncols+cls].set0(i,1.0f);
      }
    }
  }

  // ----
  // One Big Loop till the tree is of proper depth.
  // Adds a layer to the tree each pass.
  public int makeSomeTrees( int st, DRFTree trees[], int leafs[], int ntrees, int max_depth, Frame fr, Vec vresponse, int ncols, char nclass, int ymin, long nrows, double sample_rate ) {
    int depth=0;
    for( ; depth<max_depth; depth++ ) {
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
          //if( hs != null ) for( DHistogram h : hs )
          //  if( h != null ) sum += h.byteSize();
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
      if( !still_splitting ) break;
      //new BulkScore(trees,ncols,nclass,ymin,(float)sample_rate,true).doIt(fr,vresponse).report( Sys.DRF__, depth );
    }

    // Print the generated trees
    //for( int t=0; t<ntrees; t++ )
    //  System.out.println(trees[t].root().toString2(new StringBuilder(),0));

    return depth;
  }

  // A standard DTree with a few more bits.  Support for sampling during
  // training, and replaying the sample later on the identical dataset to
  // e.g. compute OOBEE.
  static class DRFTree extends DTree {
    final int _mtrys;           // Number of columns to choose amongst in splits
    final long _seed;           // RNG seed; drives sampling seeds
    final long _seeds[];        // One seed for each chunk, for sampling
    final transient Random _rand; // RNG for split decisions & sampling
    DRFTree( Frame fr, int ncols, char nbins, char nclass, int min_rows, DBinHistogram hs[], int mtrys, long seed ) {
      super(fr._names, ncols, nbins, nclass, min_rows);
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

    @Override DRFUndecidedNode makeUndecidedNode(DTree tree, int nid, DBinHistogram[] nhists ) {
      return new DRFUndecidedNode(tree,nid,nhists);
    }

    // Find the column with the best split (lowest score).
    @Override DTree.Split bestCol( DRFUndecidedNode u ) {
      DTree.Split best = DTree.Split.make(-1,-1,false,0L,0L,Double.MAX_VALUE,Double.MAX_VALUE,(float[])null,null);
      if( u._hs == null ) return best;
      for( int i=0; i<u._scoreCols.length; i++ ) {
        int col = u._scoreCols[i];
        DTree.Split s = u._hs[col].scoreMSE(col,u._tree._names[col]);
        if( s.mse() < best.mse() ) best = s;
        if( s.mse() <= 0 ) break; // No point in looking further!
      }
      return best;
    }
  }

  // DRF DTree undecided node: same as the normal UndecidedNode, but specifies
  // a list of columns to score on now, and then decide over later.
  // DRF algo: pick a random mtry columns
  static class DRFUndecidedNode extends UndecidedNode {
    DRFUndecidedNode( DTree tree, int pid, DBinHistogram hs[] ) { super(tree,pid,hs); }

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
        if( hs[i].nbins() <= 1 ) continue; // cols with 1 bin (will not split)
        cols[len++] = i;        // Gather active column
      }
      int choices = len;        // Number of columns I can choose from
      if( choices == 0 ) {
        for( int i=0; i<hs.length; i++ ) {
          String s;
          if( hs[i]==null ) s="null";
          else if( hs[i]._min == hs[i]._max ) s=hs[i]._name+"=min==max=="+hs[i]._min;
          else if( hs[i].nbins() <= 1 )       s=hs[i]._name+"=nbins="    +hs[i].nbins();
          else                                s=hs[i]._name+"=unk";
          System.out.println("No choices, hists="+s);
        }
        System.out.println(this);
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
