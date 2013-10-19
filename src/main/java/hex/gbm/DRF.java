package hex.gbm;

import hex.gbm.DTree.*;
import hex.rng.MersenneTwisterRNG;

import java.util.Arrays;
import java.util.Random;

import water.*;
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
  long seed = 0x1321e74a0192470cL; // Only one hardcoded seed to receive the same results between runs

  @API(help = "Stop criterium for tree grow.", filter = Default.class, lmin=-1, lmax=1000 )
  int nodesize = -1; // nodesize = 1 for classification, else = 5

  public static class DRFModel extends DTree.TreeModel {
    static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
    static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.
    public DRFModel(Key key, Key dataKey, Key testKey, String names[], String domains[][], int ntrees) { super(key,dataKey,testKey,names,domains,ntrees); }
    public DRFModel(DRFModel prior, DTree[] trees, double err, long [][] cm) { super(prior, trees, err, cm); }
    @Override protected float[] score0(double data[], float preds[]) {
      Arrays.fill(preds,0);
      float[] p = super.score0(data, preds);
      float sum=0;
      for( float f : preds ) sum += f;
      // We have an (near)integer sum of votes - one per voting tree.
      int votes = Math.round(sum);
      // After adding all trees, divide by tree-count to get a distribution
      for( int i=0; i<preds.length; i++ )
        preds[i] /= votes;
      //DTree.correctDistro(preds);
      //assert DTree.checkDistro(preds) : "Funny distro";
      //MMSystem.err.println("data=" + Arrays.toString(data) + ", prediction = "+Arrays.toString(p));
      return p;
    }
  }
  public Frame score( Frame fr ) { return ((DRFModel)UKV.get(dest())).score(fr,true);  }

  @Override protected Log.Tag.Sys logTag() { return Sys.DRF__; }
  public DRF() { description = "Distributed RF"; ntrees = 50; max_depth = 50; }

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
  @Override protected void logStart() {
    Log.info("Starting DRF model build...");
    super.logStart();
    Log.info("sample_rate: " + sample_rate);
  }

  @Override protected void exec() {
    logStart();
    buildModel();
  }

  @Override protected Response redirect() {
    return DRFProgressPage.redirect(this, self(), dest());
  }

  @Override protected void buildModel( final Frame fr, String names[], String domains[][], final Key outputKey, final Key dataKey, final Key testKey, final Timer t_build ) {
    final int cmtries = (mtries==-1) ? // classification: mtry=sqrt(_ncols), regression: mtry=_ncols/3
        ( classification ? Math.max((int)Math.sqrt(_ncols),1) : Math.max(_ncols/3,1))  : mtries;
    final int cnodesize = (nodesize==-1) ? (classification ? 1 : 5) : nodesize; // computed nodesize
    assert 1 <= cmtries && cmtries <= _ncols : "Too large mtries="+cmtries+", ncols="+_ncols;
    assert 0.0 < sample_rate && sample_rate <= 1.0;
    DRFModel model = new DRFModel(outputKey,dataKey,testKey,names,domains,ntrees);
    DKV.put(outputKey, model);
    // --
    System.err.println("Class distribution: " + Arrays.toString(_distribution));

    // The RNG used to pick split columns
    Random rand = new MersenneTwisterRNG((int)(seed>>32L),(int)seed);

    // Set a single 1.0 in the response for that class
    new Set1Task().doAll(fr);

    // Build trees until we hit the limit
    for( int tid=0; tid<ntrees; tid++) {
      // At each iteration build K trees (K = nclass = response column domain size)
      // TODO: parallelize ? build more than k trees at each time, we need to care about temporary data
      // Idea: launch more DRF at once.
      Timer t_kTrees = new Timer();
      DTree[] ktrees = buildNextKTrees(fr,cmtries,cnodesize,sample_rate,rand);
      Log.info(Sys.DRF__, "Tree "+(tid+1)+"x"+_nclass+" produced in "+t_kTrees);
      if( cancelled() ) break; // If canceled during building, do not bulkscore
      // Print Frame:
      //System.err.println(fr.toStringAll());

      // Do validation or OOBEE scoring only if trees are produced fast enough.
      Score sc = new Score().doIt(model, fr, validation, _validResponse).report(Sys.DRF__,tid+1,ktrees);
      model = new DRFModel(model, ktrees, (float)sc._sum/_nrows, sc._cm);
      DKV.put(outputKey, model);
    }

    cleanUp(fr,t_build); // Shared cleanup
  }

  private class Set1Task extends MRTask2<Set1Task> {
    @Override public void map( Chunk chks[] ) {
      Chunk cy = chk_resp(chks);
      for( int i=0; i<cy._len; i++ ) {
        if( cy.isNA0(i) ) continue;
        int cls = (int)cy.at80(i);
        chk_work(chks,cls).set0(i,1.0f);
      }
    }
  }

  // --------------------------------------------------------------------------
  // Build the next random k-trees
  private DTree[] buildNextKTrees(Frame fr, int mtrys, int nodesize, float sample_rate, Random rand) {
    // We're going to build K (nclass) trees - each focused on correcting
    // errors for a single class.
    final DTree[] ktrees = new DTree[_nclass];
    for( int k=0; k<_nclass; k++ ) {
      // Initially setup as-if an empty-split had just happened
      if( _distribution[k] != 0 ) {
        ktrees[k] = new DRFTree(fr,_ncols,(char)nbins,(char)_nclass,min_rows,mtrys,rand.nextLong());
        new DRFUndecidedNode(ktrees[k],-1,DBinHistogram.initialHist(fr,_ncols,(char)nbins)); // The "root" node
      }
    }
    // Sample
    for( int k=0; k<_nclass; k++) {
      if (ktrees[k] != null) new Sample(((DRFTree)ktrees[k]), sample_rate).doAll(vec_nids(fr,k));
    }
    int[] leafs = new int[_nclass]; // Define a "working set" of leaf splits, from here to tree._len

    //System.err.println("----> " + fr.toStringAll());

    // ----
    // One Big Loop till the ktrees are of proper depth.
    // Adds a layer to the trees each pass.
    int depth=0;
    for( ; depth<max_depth; depth++ ) {
      if( cancelled() ) return null;

      // Build K trees, one per class.
      // Fuse 2 conceptual passes into one:
      // Pass 1: Score a prior DHistogram, and make new DTree.Node assignments
      // to every row.  This involves pulling out the current assigned Node,
      // "scoring" the row against that Node's decision criteria, and assigning
      // the row to a new child Node (and giving it an improved prediction).
      // Pass 2: Build new summary DHistograms on the new child Nodes every row
      // got assigned into.  Collect counts, mean, variance, min, max per bin,
      // per column.
      ScoreBuildHistogram sbh = new ScoreBuildHistogram(ktrees,leafs).doAll(fr);
      //System.out.println(sbh.profString());

      // Build up the next-generation tree splits from the current histograms.
      // Nearly all leaves will split one more level.  This loop nest is
      //           O( #active_splits * #bins * #ncols )
      // but is NOT over all the data.
      boolean did_split=false;
      for( int k=0; k<_nclass; k++ ) {
        DTree tree = ktrees[k]; // Tree for class K
        if( tree == null ) continue;
        int tmax = tree._len;   // Number of total splits in tree K
        for( int leaf=leafs[k]; leaf<tmax; leaf++ ) { // Visit all the new splits (leaves)
          UndecidedNode udn = tree.undecided(leaf);
          udn._hs = sbh.getFinalHisto(k,leaf);
          //System.out.println("Class "+(domain!=null?domain[k]:k)+",\n  Undecided node:"+udn);
          // Replace the Undecided with the Split decision
          DRFDecidedNode dn = new DRFDecidedNode((DRFUndecidedNode)udn);
          //System.out.println("  --> Decided node: " + dn);
          if( dn._split._col == -1 ) udn.do_not_split();
          else did_split = true;
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
      if( tree == null ) continue;
      int leaf = leafs[k] = tree._len;
      for( int nid=0; nid<leaf; nid++ ) {
        if( tree.node(nid) instanceof DecidedNode ) {
          DecidedNode dn = tree.decided(nid);
          for( int i=0; i<dn._nids.length; i++ ) {
            int cnid = dn._nids[i];
            if( cnid == -1 || // Bottomed out (predictors or responses known constant)
                tree.node(cnid) instanceof UndecidedNode || // Or chopped off for depth
                (tree.node(cnid) instanceof DecidedNode &&  // Or not possible to split
                 ((DecidedNode)tree.node(cnid))._split._col==-1) ) {
              DRFLeafNode nleaf = new DRFLeafNode(tree,nid);
              dn._nids[i] = nleaf._nid; // Mark a leaf here
            }
          }
          // Handle the trivial non-splitting tree
          if( nid==0 && dn._split._col == -1 )
            new DRFLeafNode(tree,-1,0);
        }
      }
    } // -- k-trees are done

    // ----
    // Collect votes for the tree.
    CollectPreds gp = new CollectPreds(ktrees,leafs).doAll(fr);
    for( int k=0; k<_nclass; k++ ) {
      final DTree tree = ktrees[k];
      if( tree == null ) continue;
      for( int i=0; i<tree._len-leafs[k]; i++ ) {
        // setup prediction for k-tree's i-th leaf
        ((LeafNode)tree.node(leafs[k]+i))._pred = gp._votes[k][i];;
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
          if( tree == null ) continue;
          final Chunk nids = chk_nids(chks,k);
          final Chunk ct   = chk_tree(chks,k);
          for( int row=0; row<nids._len; row++ ) {
            int nid = (int)nids.at80(row);
            // Track only prediction for oob rows
            if (isOOBRow(nid)) {
              //if (isDecidedRow(nid)) continue;
              nid = oob2Nid(nid);
              ct.set0(row, (float)(ct.at0(row) + ((LeafNode)tree.node(nid))._pred));
            }
            nids.set0(row,0);
          }
        }
      }
    }.doAll(fr);

    // Print the generated K trees
    printGenerateTrees(ktrees);

    return ktrees;
  }

  private void printGenerateTrees(DTree[] trees) {
    for( int k=0; k<_nclass; k++ )
      if( trees[k] != null )
        System.out.println(trees[k].root().toString2(new StringBuilder(),0));
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

  // Collect and write predictions into leafs.
  private class CollectPreds extends MRTask2<CollectPreds> {
    final DTree _trees[]; // Read-only, shared (except at the histograms in the Nodes)
    final int   _leafs[]; // Number of active leaves (per tree)
    // Per leaf: sum(votes);
    double _votes[/*tree/klass*/][/*tree-relative node-id*/];
    CollectPreds(DTree trees[], int leafs[]) { _leafs=leafs; _trees=trees; }
    @Override public void map( Chunk[] chks ) {
      _votes = new double[_nclass][];
      // For all tree/klasses
      for( int k=0; k<_nclass; k++ ) {
        final DTree tree = _trees[k];
        final int   leaf = _leafs[k];
        if( tree == null ) continue; // Empty class is ignored
        // A leaf-biased array of all active Tree leaves.
        final double vs[] = _votes[k] = new double[tree._len-leaf];
        final Chunk nids = chk_nids(chks,k); // Node-ids  for this tree/class
        final Chunk vss = chk_work(chks,k); // Residuals for this tree/class
        // If we have all constant responses, then we do not split even the
        // root and the residuals should be zero.
        if( tree.root() instanceof LeafNode ) continue;
        for( int row=0; row<nids._len; row++ ) { // For all rows
          int nid = (int)nids.at80(row);         // Get Node to decide from
          boolean oobrow = false;
          if (isOOBRow(nid)) { oobrow = true; nid = oob2Nid(nid); }
          if( tree.node(nid) instanceof UndecidedNode ) // If we bottomed out the tree
            nid = tree.node(nid)._pid;                  // Then take parent's decision
          DecidedNode dn = tree.decided(nid);           // Must have a decision point
          if( dn._split._col == -1 )     // Unable to decide?
            dn = tree.decided(nid = tree.node(nid)._pid); // Then take parent's decision
          int leafnid = dn.ns(chks,row); // Decide down to a leafnode
          assert leaf <= leafnid && leafnid < tree._len;
          assert tree.node(leafnid) instanceof LeafNode;
          nids.set0(row,(oobrow ? nid2Oob(leafnid) : leafnid));
          // Note: I can which leaf/region I end up in, but I do not care for
          // the prediction presented by the tree.  For GBM, we compute the
          // sum-of-residuals (and sum/abs/mult residuals) for all rows in the
          // leaf, and get our prediction from that.
          if (!oobrow) {
            double v = vss.at0(row);
            // How many rows in this leaf has predicted k-class.
            vs[leafnid-leaf] += v;
          }
        }
      }
    }
    @Override public void reduce( CollectPreds gp ) {
      Utils.add(_votes,gp._votes);
    }
  }

  // A standard DTree with a few more bits.  Support for sampling during
  // training, and replaying the sample later on the identical dataset to
  // e.g. compute OOBEE.
  static class DRFTree extends DTree {
    final int _mtrys;           // Number of columns to choose amongst in splits
    final long _seed;           // RNG seed; drives sampling seeds
    final long _seeds[];        // One seed for each chunk, for sampling
    final transient Random _rand; // RNG for split decisions & sampling
    DRFTree( Frame fr, int ncols, char nbins, char nclass, int min_rows, int mtrys, long seed ) {
      super(fr._names, ncols, nbins, nclass, min_rows);
      _mtrys = mtrys;
      _seed = seed;                  // Save for any replay scenarios
      _rand = new MersenneTwisterRNG((int)(seed>>32),(int)seed);
      _seeds = new long[fr.vecs()[0].nChunks()];
      for( int i=0; i<_seeds.length; i++ )
        _seeds[i] = _rand.nextLong();
    }
    // Return a deterministic chunk-local RNG.  Can be kinda expensive.
    @Override public Random rngForChunk( int cidx ) {
      long seed = _seeds[cidx];
      return new MersenneTwisterRNG((int)(seed>>32),(int)seed);
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
        if( s == null ) continue;
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

  static class DRFLeafNode extends LeafNode {
    DRFLeafNode( DTree tree, int pid ) { super(tree,pid); }
    DRFLeafNode( DTree tree, int pid, int nid ) { super(tree,pid,nid); }
    // Insert just the predictions: a single byte/short if we are predicting a
    // single class, or else the full distribution.
    @Override protected AutoBuffer compress(AutoBuffer ab) { assert !Double.isNaN(_pred); return ab.put4f((float)_pred); }
    @Override protected int size() { return 4; }
  }

  // Deterministic sampling
  static class Sample extends MRTask2<Sample> {
    final DRFTree _tree;
    final float _rate;
    Sample( DRFTree tree, float rate ) { _tree = tree; _rate = rate; }
    @Override public void map( Chunk nids ) {
      Random rand = _tree.rngForChunk(nids.cidx());
      for( int i=0; i<nids._len; i++ )
        if( rand.nextFloat() >= _rate )
          nids.set0(i, OUT_OF_BAG);     // Flag row as being ignored by sampling
    }
  }
}
