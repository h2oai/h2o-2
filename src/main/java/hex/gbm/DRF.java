package hex.gbm;

import hex.gbm.DTree.DecidedNode;
import hex.gbm.DTree.UndecidedNode;
import hex.rng.MersenneTwisterRNG;

import java.util.Arrays;
import java.util.Random;

import jsr166y.CountedCompleter;
import water.*;
import water.H2O.H2OCountedCompleter;
import water.api.DRFProgressPage;
import water.api.DocGen;
import water.fvec.*;
import water.util.*;
import water.util.Log.Tag.Sys;

// Random Forest Trees
public class DRF extends SharedTreeModelBuilder {
  static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  @API(help = "Columns to randomly select at each level, or -1 for sqrt(#cols)", filter = Default.class, lmin=-1, lmax=100000)
  int mtries = -1;

  @API(help = "Sample rate, from 0. to 1.0", filter = Default.class, dmin=0, dmax=1)
  float sample_rate = 0.6666667f;

  @API(help = "Seed for the random number generator", filter = Default.class)
  long seed = new Random().nextLong();

  public static class DRFModel extends DTree.TreeModel {
    static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
    static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.
    public DRFModel(Key key, Key dataKey, Frame fr, int ntrees, int ymin) { super(key,dataKey,fr,ntrees,ymin); }
    public DRFModel(DRFModel prior, DTree[] trees, double err, long [][] cm) { super(prior, trees, err, cm); }
    @Override protected float[] score0(double data[], float preds[]) {
      Arrays.fill(preds,0);
      throw H2O.unimpl();
      //for( CompressedTree t : treeBits )
      //  t.addScore(preds, data);
      //float sum=0;
      //for( float f : preds ) sum += f;
      //// We have an (near)integer sum of votes - one per voting tree.  If OOBEE
      //// was used, the votes will be roughly equal to one minus the sampling
      //// ratio.  Estimate the number of votes.
      //int votes = Math.round(sum);
      //// After adding all trees, divide by tree-count to get a distribution
      //for( int i=0; i<preds.length; i++ )
      //  preds[i] /= votes;
      //DTree.correctDistro(preds);
      //assert DTree.checkDistro(preds) : "Funny distro";
      //return preds;
    }
  }
  public Frame score( Frame fr ) { return ((DRFModel)UKV.get(dest())).score(fr,true);  }

  @Override protected Log.Tag.Sys logTag() { return Sys.DRF__; }
  public DRF() { description = "Distributed RF"; }

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
    startBuildModel();
    return DRFProgressPage.redirect(this, self(),dest());
  }

  @Override protected void buildModel( final Frame fr, final Frame frm, final Key outputKey, final Key dataKey, final Timer t_build ) {
    final int mtrys = (mtries==-1) ? Math.max((int)Math.sqrt(_ncols),1) : mtries;
    assert 1 <= mtrys && mtrys <= _ncols : "Too large mtrys="+mtrys+", ncols="+_ncols;
    assert 0.0 < sample_rate && sample_rate <= 1.0;
    DRFModel drf_model0 = new DRFModel(outputKey,dataKey,frm,ntrees, _ymin);
    DKV.put(outputKey, drf_model0);

    H2O.submitTask(start(new H2OCountedCompleter() {
      @Override public void compute2() {
        throw H2O.unimpl();
        //// Set a single 1.0 in the response for that class
        //if( nclass > 1 )
        //  new Set1Task(ymin,ncols,nclass).doAll(fr);
        //
        //// The RNG used to pick split columns
        //Random rand = new MersenneTwisterRNG(new int[]{(int)(seed>>32L),(int)seed});
        //
        //// Initially setup as-if an empty-split had just happened
        //DBinHistogram hs[] = DBinHistogram.initialHist(fr,ncols,(char)nbins,nclass);
        //DRFTree forest[] = new DRFTree[0];
        //
        //// ----
        //// Only work on so many trees at once, else get GC issues.
        //// Hand the inner loop a smaller set of trees.
        //final int NTREE=2;          // Limit of 5 trees at once
        //int depth=0;
        //for( int st = 0; st < ntrees; st+= NTREE ) {
        //  if( cancelled() ) break;
        //  int xtrees = Math.min(NTREE,ntrees-st);
        //  DRFTree someTrees[] = new DRFTree[xtrees];
        //  int someLeafs[] = new int[xtrees];
        //  forest = Arrays.copyOf(forest,forest.length+xtrees);
        //
        //  for( int t=0; t<xtrees; t++ ) {
        //    int idx = st+t;
        //    forest[idx] = someTrees[t] = new DRFTree(fr,ncols,(char)nbins,nclass,min_rows,hs,mtrys,rand.nextLong());
        //    Vec vec = vresponse.makeZero();
        //    // Make a new Vec to hold the split-number for each row (initially
        //    // all zero).  If sampling, flag out some rows for OOBEE scoring.
        //    if( sample_rate < 1.0 )
        //      new Sample(someTrees[t],sample_rate).doAll(vec);
        //    fr.add("NIDs"+t,vec);
        //  }
        //
        //  // Make NTREE trees at once
        //  int d = makeSomeTrees(st, someTrees,someLeafs, xtrees, max_depth, fr, vresponse, sample_rate);
        //  if( d>depth ) depth=d;    // Actual max depth used
        //
        //  BulkScore bs = new BulkScore(forest,forest.length-xtrees,ncols,nclass,ymin,sample_rate).doAll(fr).report( Sys.DRF__, depth );
        //  int old = _errs.length;
        //  _errs = Arrays.copyOf(_errs,st+xtrees);
        //  for( int i=old; i<_errs.length; i++ ) _errs[i] = Float.NaN;
        //  _errs[_errs.length-1] = (double)bs._sum/nrows;
        //  drf_model1 = new DRFModel(drf_model1,forest, _errs, ymin,bs._cm);
        //  DKV.put(outputKey, drf_model1);
        //
        //  // Remove temp vectors; cleanup the Frame
        //  for( int t=0; t<xtrees; t++ )
        //    UKV.remove(fr.remove(fr.numCols()-1)._key);
        //}
        //cleanUp(fr,t_build); // Shared cleanup
        //tryComplete();
      }
      @Override public boolean onExceptionalCompletion(Throwable ex, CountedCompleter caller) {
        ex.printStackTrace();
        DRF.this.cancel(ex.getMessage());
        return true;
      }
    }));
  }

  private class Set1Task extends MRTask2<Set1Task> {
    final int _ymin;
    final char _nclass;
    Set1Task( int ymin, char nclass ) { _ymin = ymin; _nclass=nclass; }
    @Override public void map( Chunk chks[] ) {
      Chunk cy = chk_resp(chks);
      for( int i=0; i<cy._len; i++ ) {
        if( cy.isNA0(i) ) continue;
        int cls = (int)cy.at80(i) - _ymin;
        chk_work(chks,cls).set0(i,1.0f);
      }
    }
  }

  // ----
  // One Big Loop till the tree is of proper depth.
  // Adds a layer to the tree each pass.
  public int makeSomeTrees( int st, DRFTree trees[], int leafs[], int ntrees, int max_depth, Frame fr, Vec vresponse, double sample_rate ) {
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
      ScoreBuildHistogram sbh = new ScoreBuildHistogram(trees,leafs).doAll(fr);
      //System.out.println(sbh.profString());

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
    }

    // Print the generated trees
    //for( int t=0; t<ntrees; t++ )
    //  System.out.println(trees[t].root().toString2(new StringBuilder(),0));

    return depth;
  }

  // Read the 'tree' columns, do model-specific math and put the results in the
  // ds[] array, and return the sum.  Dividing any ds[] element by the sum
  // turns the results into a probability distribution.
  @Override protected double score0( Chunk chks[], double ds[/*nclass*/], int row ) {
    double sum=0;
    for( int k=0; k<_nclass; k++ ) // Sum across of likelyhoods
      sum+=(ds[k]=chk_tree(chks,k).at0(row));
    return sum;
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
      _seeds = new long[fr.vecs()[0].nChunks()];
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

    @Override DRFUndecidedNode makeUndecidedNode(DBinHistogram[] nhists ) {
      return new DRFUndecidedNode(_tree,_nid,nhists);
    }

    // Find the column with the best split (lowest score).
    @Override DTree.Split bestCol( DRFUndecidedNode u ) {
      DTree.Split best = new DTree.Split(-1,-1,false,Double.MAX_VALUE,Double.MAX_VALUE,0L,0L);
      if( u._hs == null ) return best;
      for( int i=0; i<u._scoreCols.length; i++ ) {
        int col = u._scoreCols[i];
        DTree.Split s = u._hs[col].scoreMSE(col);
        if( s.se() < best.se() ) best = s;
        if( s.se() <= 0 ) break; // No point in looking further!
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
    Sample( DRFTree tree, float rate ) { _tree = tree; _rate = rate; }
    @Override public void map( Chunk nids ) {
      Random rand = _tree.rngForChunk(nids.cidx());
      for( int i=0; i<nids._len; i++ )
        if( rand.nextFloat() >= _rate )
          nids.set0(i,-2);     // Flag row as being ignored by sampling
    }
  }
}
