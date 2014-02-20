package hex.gbm;

import hex.ConfusionMatrix;
import hex.rng.MersenneTwisterRNG;

import java.util.Arrays;
import java.util.Random;

import jsr166y.CountedCompleter;
import water.*;
import water.H2O.H2OCountedCompleter;
import water.Job.ValidatedJob;
import water.api.DocGen;
import water.fvec.*;
import water.util.*;
import water.util.Log.Tag.Sys;

// Build (distributed) Trees.  Used for both Gradiant Boosted Method and Random
// Forest, and really could be used for any decision-tree builder.
//
// While this is a wholly H2O-design, we found these papers afterwards that
// describes our design fairly well.
//   Parallel GBRT http://www.cse.wustl.edu/~kilian/papers/fr819-tyreeA.pdf
//   Streaming parallel decision tree http://jmlr.org/papers/volume11/ben-haim10a/ben-haim10a.pdf
// Note that our dynamic Histogram technique is different (surely faster, and
// probably less mathematically clean).  I'm sure a host of other smaller details
// differ also - but in the Big Picture the paper and our algorithm are similar.

public abstract class SharedTreeModelBuilder<TM extends DTree.TreeModel> extends ValidatedJob {
  static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  @API(help = "Number of trees", filter = Default.class, lmin=1, lmax=1000000)
  public int ntrees = 50;

  @API(help = "Maximum tree depth", filter = Default.class, lmin=1, lmax=10000)
  public int max_depth = 5;

  @API(help = "Fewest allowed observations in a leaf (in R called 'nodesize')", filter = Default.class, lmin=1)
  public int min_rows = 10;

  @API(help = "Build a histogram of this many bins, then split at the best point", filter = Default.class, lmin=2, lmax=10000)
  public int nbins = 20;

  @API(help = "Perform scoring after each iteration (can be slow)", filter = Default.class)
  public boolean score_each_iteration = false;

  // Overall prediction Mean Squared Error as I add trees
  transient protected double _errs[];

  @API(help = "Active feature columns")
  protected int _ncols;

  @API(help = "Rows in training dataset")
  protected long _nrows;

  @API(help = "Number of classes")
  protected int _nclass;

  @API(help = "Class distribution")
  protected long _distribution[];

  private transient boolean _gen_enum; // True if we need to cleanup an enum response column at the end

  private transient Frame _adaptedValidation;     // Validation dataset is already adapted to a produced model
  private transient Frame _onlyAdaptedValidation; // Frame containing only adapted part of validation which needs to be clean-up at the end of computation
  private transient int[][] _modelMap;            // Transformation for model response to common domain
  private transient int[][] _validMap;            // Transformation for validation response to common domain

  /** Maximal number of supported levels in response. */
  public static final int MAX_SUPPORTED_LEVELS = 1000;

  /** Marker for already decided row. */
  static public final int DECIDED_ROW = -1;
  /** Marker for sampled out rows */
  static public final int OUT_OF_BAG = -2;


  ConfusionMatrix[] _cms;
  double _auc;

  @Override public float progress(){
    Value value = DKV.get(dest());
    DTree.TreeModel m = value != null ? (DTree.TreeModel) value.get() : null;
    return m == null ? 0 : (float)m.treeBits.length/(float)m.N;
  }

  @Override protected void logStart() {
    super.logStart();
    Log.info("    ntrees: " + ntrees);
    Log.info("    max_depth: " + max_depth);
    Log.info("    min_rows: " + min_rows);
    Log.info("    nbins: " + nbins);
  }

  // Verify input parameters
  @Override protected void init() {
    super.init();
    // Check parameters
    assert 0 <= ntrees && ntrees < 1000000; // Sanity check
    // Should be handled by input
    //assert response.isEnum() : "Response is not enum";
    assert (classification && (response.isInt() || response.isEnum())) ||   // Classify Int or Enums
           (!classification && !response.isEnum()) : "Classification="+classification + " and response="+response.isInt();  // Regress  Int or Float

    if (source.numRows()==0)
      throw new IllegalArgumentException("Cannot build a model on empty dataset!");
    if (source.numRows() - response.naCnt() <=0)
      throw new IllegalArgumentException("Dataset contains too many NAs!");

    _ncols = _train.length;
    _nrows = source.numRows() - response.naCnt();

    assert (_nrows>0) : "Dataset contains no rows - validation of input parameters is probably broken!";
    // Transform response to enum
    if( !response.isEnum() && classification ) {
      response = response.toEnum();
      _gen_enum = true;
    }
    _nclass = response.isEnum() ? (char)(response.domain().length) : 1;
    _errs = new double[0];                // No trees yet
    if (classification && _nclass <= 1)
      throw new IllegalArgumentException("Constant response column!");
    if (_nclass > MAX_SUPPORTED_LEVELS)
      throw new IllegalArgumentException("Too many levels in response column!");
    if (_nclass == 2) {
      _cms = new ConfusionMatrix[DEFAULT_THRESHOLDS.length];
      for(int i = 0; i < _cms.length; ++i)
        _cms[i] = new ConfusionMatrix(2);
    }
  }

  // --------------------------------------------------------------------------
  // Driver for model-building.
  public void buildModel( ) {
    final Key outputKey = dest();
    String sd = input("source");
    final Key dataKey = (sd==null||sd.length()==0)?null:Key.make(sd);
    String sv = input("validation");
    final Key testKey = (sv==null||sv.length()==0)?dataKey:Key.make(sv);

    // Lock the input datasets against deletes
    source.read_lock(self());
    if( validation != null && !source._key.equals(validation._key) )
      validation.read_lock(self());

    Frame fr = new Frame(_names, _train);
    fr.add(_responseName,response);
    final Frame frm = new Frame(fr); // Model-Frame; no extra columns
    String names[] = frm.names();
    String domains[][] = frm.domains();

    // For doing classification on Integer (not Enum) columns, we want some
    // handy names in the Model.  This really should be in the Model code.
    String[] domain = response.domain();
    if( domain == null && _nclass > 1 ) // No names?  Something is wrong since we converted response to enum already !
      assert false : "Response domain' names should be always presented in case of classification";
    if( domain == null ) domain = new String[] {"r"}; // For regression, give a name to class 0

    // Find the class distribution
    _distribution = _nclass > 1 ? new ClassDist(_nclass).doAll(response)._ys : null;

    // Also add to the basic working Frame these sets:
    //   nclass Vecs of current forest results (sum across all trees)
    //   nclass Vecs of working/temp data
    //   nclass Vecs of NIDs, allowing 1 tree per class

    // Current forest values: results of summing the prior M trees
    for( int i=0; i<_nclass; i++ )
      fr.add("Tree_"+domain[i], response.makeZero());

    // Initial work columns.  Set-before-use in the algos.
    for( int i=0; i<_nclass; i++ )
      fr.add("Work_"+domain[i], response.makeZero());

    // One Tree per class, each tree needs a NIDs.  For empty classes use a -1
    // NID signifying an empty regression tree.
    for( int i=0; i<_nclass; i++ )
      fr.add("NIDs_"+domain[i], response.makeCon(_distribution==null ? 0 : (_distribution[i]==0?-1:0)));

    // Compute confusion matrix domain:
    // - if validation is specified then it is union of train and validation response domains
    //   else it is only domain of response column.
    String[] cmDomain = null;
    if (validation!=null && _nclass > 1) {
      // Collect domain for validation response
      Vec validResponse = validation.vec(names[names.length-1]).toEnum(); // toEnum call require explicit delete of created vector
      String[] validationDomain = validResponse.domain();
      cmDomain = Utils.union(domain, validationDomain);
      UKV.remove(validResponse._key);
    }

    // Timer  for model building
    Timer bm_timer =  new Timer();
    // Create an initial model
    TM model = makeModel(outputKey, dataKey, testKey, names, domains, cmDomain);
    // Save the model ! (delete_and_lock has side-effect of saving model into DKV)
    model.delete_and_lock(self());
    // Prepare adapted validation dataset if it is necessary for classification (we do not need to care about regression)
    if (validation!=null && _nclass > 1) {
      Frame[] av = model.adapt(validation, false);
      _adaptedValidation     = av[0]; // adapted validation data for model
      _onlyAdaptedValidation = av[1]; // only adapted
    }

    try {
      // Compute the model
      model = buildModel(model, fr, names, domains, cmDomain, bm_timer);
    } finally {
      model.unlock(self());  // Update and unlock model
      cleanUp(fr,bm_timer);  // Shared cleanup
    }
  }

  // Shared cleanup
  protected void cleanUp(Frame fr, Timer t_build) {
    Log.info(logTag(),"Modeling done in "+t_build);

    // Remove temp vectors; cleanup the Frame
    while( fr.numCols() > _ncols+1/*Do not delete the response vector*/ )
      UKV.remove(fr.remove(fr.numCols()-1)._key);
    // If we made a response column with toEnum, nuke it.
    if( _gen_enum ) UKV.remove(response._key);
    // Delete adapted part of validation dataset
    if( _onlyAdaptedValidation != null ) _onlyAdaptedValidation.delete();

    // Unlock the input datasets against deletes
    source.unlock(self());
    if( validation != null && !source._key.equals(validation._key) )
      validation.unlock(self());

    remove();                   // Remove Job
  }

  transient long _timeLastScoreStart, _timeLastScoreEnd, _firstScore;
  protected TM doScoring(TM model, Frame fr, DTree[] ktrees, int tid, DTree.TreeModel.TreeStats tstats, boolean finalScoring, boolean oob, boolean build_tree_per_node ) {
    long now = System.currentTimeMillis();
    if( _firstScore == 0 ) _firstScore=now;
    long sinceLastScore = now-_timeLastScoreStart;
    Score sc = null;
    // If validation is specified we use a model for scoring, so we need to update it!
    // First we save model with trees and then update it with resulting error
    // Double update - before scoring
    model = makeModel(model, ktrees, tstats);
    model.update(self());
    if( score_each_iteration ||
        finalScoring ||
        (now-_firstScore < 4000) || // Score every time for 4 secs
        // Throttle scoring to keep the cost sane; limit to a 10% duty cycle & every 4 secs
        (sinceLastScore > 4000 && // Limit scoring updates to every 4sec
         (double)(_timeLastScoreEnd-_timeLastScoreStart)/sinceLastScore < 0.1) ) { // 10% duty cycle
      _timeLastScoreStart = now;
      // Perform scoring
      sc = new Score().doIt(model, fr, validation, _adaptedValidation, oob, build_tree_per_node).report(logTag(),tid,ktrees);
      _timeLastScoreEnd = System.currentTimeMillis();
    }
    // Double update - after scoring
    model = makeModel(model,
                      sc==null ? Double.NaN : sc.mse(),
                      sc==null ? null : (_nclass>1? new ConfusionMatrix(sc._cm):null));
    model.update(self());
    return model;
  }

  // --------------------------------------------------------------------------
  // Convenvience accessor for a complex chunk layout.
  // Wish I could name the array elements nicer...
  protected Chunk chk_resp( Chunk chks[]        ) { return chks[_ncols]; }
  protected Chunk chk_tree( Chunk chks[], int c ) { return chks[_ncols+1+c]; }
  protected Chunk chk_work( Chunk chks[], int c ) { return chks[_ncols+1+_nclass+c]; }
  protected Chunk chk_nids( Chunk chks[], int t ) { return chks[_ncols+1+_nclass+_nclass+t]; }

  protected final Vec vec_nids( Frame fr, int t) { return fr.vecs()[_ncols+1+_nclass+_nclass+t]; }
  protected final Vec vec_resp( Frame fr, int t) { return fr.vecs()[_ncols]; }

  // --------------------------------------------------------------------------
  // Fuse 2 conceptual passes into one:
  //
  // Pass 1: Score a prior partially-built tree model, and make new Node
  //         assignments to every row.  This involves pulling out the current
  //         assigned DecidedNode, "scoring" the row against that Node's
  //         decision criteria, and assigning the row to a new child
  //         UndecidedNode (and giving it an improved prediction).
  //
  // Pass 2: Build new summary DHistograms on the new child UndecidedNodes
  //         every row got assigned into.  Collect counts, mean, variance, min,
  //         max per bin, per column.
  //
  // The result is a set of DHistogram arrays; one DHistogram array for
  // each unique 'leaf' in the tree being histogramed in parallel.  These have
  // node ID's (nids) from 'leaf' to 'tree._len'.  Each DHistogram array is
  // for all the columns in that 'leaf'.
  //
  // The other result is a prediction "score" for the whole dataset, based on
  // the previous passes' DHistograms.
  public class ScoreBuildHistogram extends MRTask2<ScoreBuildHistogram> {
    final int   _k;    // Which tree
    final DTree _tree; // Read-only, shared (except at the histograms in the Nodes)
    final int   _leaf; // Number of active leaves (per tree)
    // Histograms for every tree, split & active column
    final DHistogram _hcs[/*tree-relative node-id*/][/*column*/];
    final boolean _subset;      // True if working a subset of cols
    public ScoreBuildHistogram(H2OCountedCompleter cc, int k, DTree tree, int leaf, DHistogram hcs[][], boolean subset) {
      super(cc);
      _k   = k;
      _tree= tree;
      _leaf= leaf;
      _hcs = hcs;
      _subset = subset;
    }

    // Once-per-node shared init
    @Override public void setupLocal( ) {
      // Init all the internal tree fields after shipping over the wire
      _tree.init_tree();
      // Allocate local shared memory histograms
      for( int l=_leaf; l<_tree._len; l++ ) {
        DTree.UndecidedNode udn = _tree.undecided(l);
        DHistogram hs[] = _hcs[l-_leaf];
        int sCols[] = udn._scoreCols;
        if( sCols != null ) { // Sub-selecting just some columns?
          for( int j=0; j<sCols.length; j++) // For tracked cols
            hs[sCols[j]].init();
        } else {                // Else all columns
          for( int j=0; j<_ncols; j++) // For all columns
            if( hs[j] != null )        // Tracking this column?
              hs[j].init();
        }
      }
    }

    @Override public void map( Chunk[] chks ) {
      assert chks.length==_ncols+4;
      final Chunk tree = chks[_ncols+1];
      final Chunk wrks = chks[_ncols+2];
      final Chunk nids = chks[_ncols+3];

      // Pass 1: Score a prior partially-built tree model, and make new Node
      // assignments to every row.  This involves pulling out the current
      // assigned DecidedNode, "scoring" the row against that Node's decision
      // criteria, and assigning the row to a new child UndecidedNode (and
      // giving it an improved prediction).
      int nnids[] = new int[nids._len];
      if( _leaf > 0)            // Prior pass exists?
        score_decide(chks,nids,wrks,tree,nnids);
      else                      // Just flag all the NA rows
        for( int row=0; row<nids._len; row++ )
          if( isDecidedRow((int)nids.at0(row)) ) nnids[row] = -1;

      // Pass 2: accumulate all rows, cols into histograms
      if( _subset ) accum_subset(chks,nids,wrks,nnids);
      else          accum_all   (chks,     wrks,nnids);
    }

    @Override public void reduce( ScoreBuildHistogram sbh ) {
      // Merge histograms
      if( sbh._hcs == _hcs ) return; // Local histograms all shared; free to merge
      // Distributed histograms need a little work
      for( int i=0; i<_hcs.length; i++ ) {
        DHistogram hs1[] = _hcs[i], hs2[] = sbh._hcs[i];
        if( hs1 == null ) _hcs[i] = hs2;
        else if( hs2 != null )
          for( int j=0; j<hs1.length; j++ )
            if( hs1[j] == null ) hs1[j] = hs2[j];
            else if( hs2[j] != null )
              hs1[j].add(hs2[j]);
      }
    }

    // Pass 1: Score a prior partially-built tree model, and make new Node
    // assignments to every row.  This involves pulling out the current
    // assigned DecidedNode, "scoring" the row against that Node's decision
    // criteria, and assigning the row to a new child UndecidedNode (and
    // giving it an improved prediction).
    private void score_decide(Chunk chks[], Chunk nids, Chunk wrks, Chunk tree, int nnids[]) {
      for( int row=0; row<nids._len; row++ ) { // Over all rows
        int nid = (int)nids.at80(row);         // Get Node to decide from
        if( isDecidedRow(nid)) {               // already done
          nnids[row] = (nid-_leaf);
          continue;
        }
        // Score row against current decisions & assign new split
        boolean oob = isOOBRow(nid);
        if( oob ) nid = oob2Nid(nid); // sampled away - we track the position in the tree
        DTree.DecidedNode dn = _tree.decided(nid);
        if( dn._split._col == -1 ) { // Might have a leftover non-split
          nid = dn._pid;             // Use the parent split decision then
          int xnid = oob ? nid2Oob(nid) : nid;
          nids.set0(row, xnid);
          nnids[row] = xnid-_leaf;
          dn = _tree.decided(nid); // Parent steers us
        }
        assert !isDecidedRow(nid);
        nid = dn.ns(chks,row); // Move down the tree 1 level
        if( !isDecidedRow(nid) ) {
          int xnid = oob ? nid2Oob(nid) : nid;
          nids.set0(row, xnid);
          nnids[row] = xnid-_leaf;
        } else {
          nnids[row] = nid-_leaf;
        }
      }
    }

    // All rows, some cols, accumulate histograms
    private void accum_subset(Chunk chks[], Chunk nids, Chunk wrks, int nnids[]) {
      for( int row=0; row<nnids.length; row++ ) { // Over all rows
        int nid = nnids[row];                     // Get Node to decide from
        if( nid >= 0 ) {        // row already predicts perfectly or OOB
          assert !Double.isNaN(wrks.at0(row)); // Already marked as sampled-away
          DHistogram nhs[] = _hcs[nid];
          int sCols[] = _tree.undecided(nid+_leaf)._scoreCols; // Columns to score (null, or a list of selected cols)
          for( int j=0; j<sCols.length; j++) { // For tracked cols
            final int c = sCols[j];
            nhs[c].incr((float)chks[c].at0(row),wrks.at0(row)); // Histogram row/col
          }
        }
      }
    }

    // All rows, all cols, accumulate histograms.  This is the hot hot inner
    // loop of GBM, so we do some non-standard optimizations.  The rows in this
    // chunk are spread out amongst a modest set of NodeIDs/splits.  Normally
    // we would visit the rows in row-order, but this visits the NIDs in random
    // order.  The hot-part of this code updates the histograms racily (via
    // atomic updates) - once-per-row.  This optimized version updates the
    // histograms once-per-NID, but requires pre-sorting the rows by NID.
    private void accum_all(Chunk chks[], Chunk wrks, int nnids[]) {
      final DHistogram hcs[][] = _hcs;
      // Sort the rows by NID, so we visit all the same NIDs in a row
      // Find the count of unique NIDs in this chunk
      int nh[] = new int[hcs.length+1];
      for( int i : nnids ) if( i >= 0 ) nh[i+1]++;
      // Rollup the histogram of rows-per-NID in this chunk
      for( int i=0; i<hcs.length; i++ ) nh[i+1] += nh[i];
      // Splat the rows into NID-groups
      int rows[] = new int[nnids.length];
      for( int row=0; row<nnids.length; row++ )
        if( nnids[row] >= 0 )
          rows[nh[nnids[row]]++] = row;
      // rows[] has Chunk-local ROW-numbers now, in-order, grouped by NID.
      // nh[] lists the start of each new NID, and is indexed by NID+1.
      accum_all2(chks,wrks,nh,rows);
    }

    // For all columns, for all NIDs, for all ROWS...
    private void accum_all2(Chunk chks[], Chunk wrks, int nh[], int[] rows) {
      final DHistogram hcs[][] = _hcs;
      // Local temp arrays, no atomic updates.
      int    bins[] = new int   [nbins];
      double sums[] = new double[nbins];
      double ssqs[] = new double[nbins];
      // For All Columns
      for( int c=0; c<_ncols; c++) { // for all columns
        Chunk chk = chks[c];
        // For All NIDs
        for( int n=0; n<hcs.length; n++ ) {
          final DRealHistogram rh = ((DRealHistogram)hcs[n][c]);
          if( rh==null ) continue; // Ignore untracked columns in this split
          final int lo = n==0 ? 0 : nh[n-1];
          final int hi = nh[n];
          float min = rh._min2;
          float max = rh._maxIn;
          // While most of the time we are limited to nbins, we allow more bins
          // in a few cases (top-level splits have few total bins across all
          // the (few) splits) so it's safe to bin more; also categoricals want
          // to split one bin-per-level no matter how many levels).
          if( rh._bins.length >= bins.length ) { // Grow bins if needed
            bins = new int   [rh._bins.length];
            sums = new double[rh._bins.length];
            ssqs = new double[rh._bins.length];
          }

          // Gather all the data for this set of rows, for 1 column and 1 split/NID
          // Gather min/max, sums and sum-squares.
          for( int xrow=lo; xrow<hi; xrow++ ) {
            int row = rows[xrow];
            float col_data = (float)chk.at0(row);
            if( col_data < min ) min = col_data;
            if( col_data > max ) max = col_data;
            int b = rh.bin(col_data); // Compute bin# via linear interpolation
            bins[b]++;                // Bump count in bin
            double resp = wrks.at0(row);
            sums[b] += resp;
            ssqs[b] += resp*resp;
          }

          // Add all the data into the Histogram (atomically add)
          rh.setMin(min);       // Track actual lower/upper bound per-bin
          rh.setMax(max);
          for( int b=0; b<rh._bins.length; b++ ) { // Bump counts in bins
            if( bins[b] != 0 ) { Utils.AtomicIntArray.add(rh._bins,b,bins[b]); bins[b]=0; }
            if( ssqs[b] != 0 ) { rh.incr1(b,sums[b],ssqs[b]); sums[b]=ssqs[b]=0; }
          }
        }
      }
    }
  }


  // --------------------------------------------------------------------------
  // Build an entire layer of all K trees
  protected DHistogram[][][] buildLayer(final Frame fr, final DTree ktrees[], final int leafs[], final DHistogram hcs[][][], boolean subset, boolean build_tree_per_node) {
    // Build K trees, one per class.

    // Build up the next-generation tree splits from the current histograms.
    // Nearly all leaves will split one more level.  This loop nest is
    //           O( #active_splits * #bins * #ncols )
    // but is NOT over all the data.
    H2OCountedCompleter sb1ts[] = new H2OCountedCompleter[_nclass];
    Vec vecs[] = fr.vecs();
    for( int k=0; k<_nclass; k++ ) {
      final DTree tree = ktrees[k]; // Tree for class K
      if( tree == null ) continue;
      // Build a frame with just a single tree (& work & nid) columns, so the
      // nested MRTask2 ScoreBuildHistogram in ScoreBuildOneTree does not try
      // to close other tree's Vecs when run in parallel.
      Frame fr2 = new Frame(Arrays.copyOf(fr._names,_ncols+1), Arrays.copyOf(vecs,_ncols+1));
      fr2.add(fr._names[_ncols+1+k],vecs[_ncols+1+k]);
      fr2.add(fr._names[_ncols+1+_nclass+k],vecs[_ncols+1+_nclass+k]);
      fr2.add(fr._names[_ncols+1+_nclass+_nclass+k],vecs[_ncols+1+_nclass+_nclass+k]);
      // Start building one of the K trees in parallel
      H2O.submitTask(sb1ts[k] = new ScoreBuildOneTree(k,tree,leafs,hcs,fr2, subset, build_tree_per_node));
    }
    // Block for all K trees to complete.
    boolean did_split=false;
    for( int k=0; k<_nclass; k++ ) {
      final DTree tree = ktrees[k]; // Tree for class K
      if( tree == null ) continue;
      sb1ts[k].join();
      if( ((ScoreBuildOneTree)sb1ts[k])._did_split ) did_split=true;
    }
    // The layer is done.
    return did_split ? hcs : null;
  }

  private class ScoreBuildOneTree extends H2OCountedCompleter {
    final int _k;               // The tree
    final DTree _tree;
    final int _leafs[/*nclass*/];
    final DHistogram _hcs[/*nclass*/][][];
    final Frame _fr2;
    final boolean _build_tree_per_node;
    final boolean _subset;      // True if working a subset of cols
    boolean _did_split;
    ScoreBuildOneTree( int k, DTree tree, int leafs[], DHistogram hcs[][][], Frame fr2, boolean subset, boolean build_tree_per_node ) {
      _k    = k;
      _tree = tree;
      _leafs= leafs;
      _hcs  = hcs;
      _fr2  = fr2;
      _subset = subset;
      _build_tree_per_node = build_tree_per_node;
    }
    @Override public void compute2() {
      // Fuse 2 conceptual passes into one:
      // Pass 1: Score a prior DHistogram, and make new Node assignments
      // to every row.  This involves pulling out the current assigned Node,
      // "scoring" the row against that Node's decision criteria, and assigning
      // the row to a new child Node (and giving it an improved prediction).
      // Pass 2: Build new summary DHistograms on the new child Nodes every row
      // got assigned into.  Collect counts, mean, variance, min, max per bin,
      // per column.
      new ScoreBuildHistogram(this,_k,_tree,_leafs[_k],_hcs[_k],_subset).dfork(0,_fr2,_build_tree_per_node);
    }
    @Override public void onCompletion(CountedCompleter caller) {
      ScoreBuildHistogram sbh = (ScoreBuildHistogram)caller;
      //System.out.println(sbh.profString());

      final int leafk = _leafs[_k];
      int tmax = _tree.len();   // Number of total splits in tree K
      for( int leaf=leafk; leaf<tmax; leaf++ ) { // Visit all the new splits (leaves)
        DTree.UndecidedNode udn = _tree.undecided(leaf);
        //System.out.println((_nclass==1?"Regression":("Class "+_fr2.vecs()[_ncols]._domain[_k]))+",\n  Undecided node:"+udn);
        // Replace the Undecided with the Split decision
        DTree.DecidedNode dn = makeDecided(udn,sbh._hcs[leaf-leafk]);
        //System.out.println("--> Decided node: " + dn +
        //                   "  > Split: " + dn._split + " L/R:" + dn._split.rowsLeft()+" + "+dn._split.rowsRight());
        if( dn._split.col() == -1 ) udn.do_not_split();
        else _did_split = true;
      }
      _leafs[_k]=tmax;          // Setup leafs for next tree level
      int new_leafs = _tree.len()-tmax;
      _hcs[_k] = new DHistogram[new_leafs][/*ncol*/];
      for( int nl = tmax; nl<_tree.len(); nl ++ )
        _hcs[_k][nl-tmax] = _tree.undecided(nl)._hs;
      _tree.depth++;            // Next layer done
    }
  }

  // Builder-specific decision node
  protected abstract DTree.DecidedNode makeDecided( DTree.UndecidedNode udn, DHistogram hs[] );

  // --------------------------------------------------------------------------
  private static class ClassDist extends MRTask2<ClassDist> {
    ClassDist(int nclass) { _nclass = nclass; }
    final int _nclass;
    long _ys[];
    @Override public void map(Chunk ys) {
      _ys = new long[_nclass];
      for( int i=0; i<ys._len; i++ )
        if( !ys.isNA0(i) )
          _ys[(int)ys.at80(i)]++;
    }
    @Override public void reduce( ClassDist that ) { Utils.add(_ys,that._ys); }
  }

  // --------------------------------------------------------------------------
  // Read the 'tree' columns, do model-specific math and put the results in the
  // fs[] array, and return the sum.  Dividing any fs[] element by the sum
  // turns the results into a probability distribution.
  protected abstract float score1( Chunk chks[], float fs[/*nclass*/], int row );

  // Score the *tree* columns, and produce a confusion matrix
  public class Score extends MRTask2<Score> {
    /* @OUT */ long    _cm[/*actual*/][/*predicted*/]; // Confusion matrix
    /* @OUT */ double  _sum;                           // Sum-squared-error
    /* @OUT */ long    _snrows;                        // Count of voted-on rows
    /* @IN */  boolean _oob;
    /* @IN */  boolean _validation;
    //double _auc;               //Area under the ROC curve for _nclass == 2

    public double   sum()   { return _sum; }
    public long[][] cm ()   { return _cm;  }
    public long     nrows() { return _snrows; }
    public double   mse()   { return sum() / nrows(); }
   // public double   auc()   { return _auc; }

    /**
     * Compute CM & MSE on either the training or testing dataset.
     *
     * @param model a model which is used to perform computation
     * @param fr    a model training frame
     * @param validation a test frame or null
     * @param oob   perform out-of-bag validation on training frame
     * @param build_tree_per_node
     * @return
     */
    public Score doIt(Model model, Frame fr, Frame validation, Frame adaptedValidation, boolean oob, boolean build_tree_per_node) {
      assert !oob || validation==null : "Validation frame cannot be specified if oob validation is demanded!"; // oob => validation==null
      _oob = oob;
      // No validation frame is specified, so perform computation on training data
      if( validation == null ) return doAll(fr, build_tree_per_node);
      _validation = true;
      // Validation: need to score the set, getting a probability distribution for each class
      // Frame has nclass vectors (nclass, or 1 for regression), for classification it
      Frame res = model.score(adaptedValidation, false); // For classification: predicted values (~ values in res[0]) are in interval 0..domain().length-1, for regression just single column.
      Frame adapValidation = new Frame(adaptedValidation); // adapted validation dataset
      // All columns including response of validation frame are already adapted to model
      if (_nclass>1) { // Classification
        for( int i=0; i<_nclass; i++ ) // Distribution of response classes
          adapValidation.add("ClassDist"+i,res.vecs()[i+1]);
        adapValidation.add("Prediction",res.vecs()[0]); // Predicted values
      } else { // Regression
        adapValidation.add("Prediction",res.vecs()[0]);
      }
      // Compute a CM & MSE
      doAll(adapValidation, build_tree_per_node);
      // Remove temporary result
      res.delete();
      return this;
    }

    @Override public void map( Chunk chks[] ) {
      Chunk ys = chk_resp(chks); // Response
      _cm = new long[_nclass][_nclass];
      float fs[] = new float[_nclass+1];
      // Score all Rows
      for( int row=0; row<ys._len; row++ ) {
        if( ys.isNA0(row) ) continue; // Ignore missing response vars
        float sum;
        if( _validation ) {     // Passed in a class distribution from scoring
          for( int i=0; i<_nclass; i++ )
            fs[i+1] = (float)chks[i+_ncols+1].at0(row); // Get the class distros
          if (_nclass > 1 ) sum = 1.0f; // Sum of a distribution is 1.0 for classification
          else sum = fs[0];    // Sum is the same as prediction for regression.
        } else {               // Passed in the model-specific columns
          sum = score1(chks,fs,row);
        }
        float err;  int ycls=0;
        if (_oob && inBagRow(chks, row)) continue; // score only on out-of-bag rows
        if( _nclass > 1 ) {    // Classification
          if( sum == 0 ) {       // This tree does not predict this row *at all*?
            err = 1.0f-1.0f/_nclass; // Then take ycls=0, uniform predictive power
          } else {
            ycls = (int)ys.at80(row); // Response class from 0 to nclass-1
            if (ycls >= _nclass) continue;
            assert 0 <= ycls && ycls < _nclass : "weird ycls="+ycls+", y="+ys.at0(row);
            err = Float.isInfinite(sum)
              ? (Float.isInfinite(fs[ycls+1]) ? 0f : 1f)
              : 1.0f-fs[ycls+1]/sum; // Error: distance from predicting ycls as 1.0
          }
          assert !Double.isNaN(err) : "fs[cls]="+fs[ycls+1] + ", sum=" + sum;
        } else {                // Regression
          err = (float)ys.at0(row) - sum;
        }
        _sum += err*err;               // Squared error
        assert !Double.isNaN(_sum);
        // Pick highest prob for our prediction.
        if (_nclass > 1) { // fill CM only for classification
          if(_nclass == 2) { //binomial classification -> compute AUC, draw ROC
            for(int i = 0; i < _cms.length; ++i)
              _cms[i].add(ycls, ( (1 - (fs[ycls+1] / sum) )>= DEFAULT_THRESHOLDS[i])?1:0);
          }
          int best = _validation ? (int) chks[_ncols+1+_nclass].at80(row) : Model.getPrediction(fs, row);
          _cm[ycls][best]++;      // Bump Confusion Matrix also
        }
        _snrows++;
      }
      if(_nclass == 2) {
        _auc = computeAUC(_cms);
      }
    }

    @Override public void reduce( Score t ) { _sum += t._sum; Utils.add(_cm,t._cm); _snrows += t._snrows;}

    public Score report( Sys tag, int ntree, DTree[] trees ) {
      assert !Double.isNaN(_sum);
      Log.info(tag,"============================================================== ");
      int lcnt=0;
      if( trees!=null ) for( DTree t : trees ) if( t != null ) lcnt += t._len;
      long err=_snrows;
      for( int c=0; c<_nclass; c++ ) err -= _cm[c][c];
      Log.info(tag,"Mean Squared Error is "+(_sum/_snrows)+", with "+ntree+"x"+_nclass+" trees (average of "+((float)lcnt/_nclass)+" nodes)");
      if( _nclass > 1 )
        Log.info(tag,"Total of "+err+" errors on "+_snrows+" rows, CM= "+Arrays.deepToString(_cm));
      else
        Log.info("Reported on "+_snrows+" rows.");
      return this;
    }
  }

  double[] tprs;
  double[] fprs;
  public double computeAUC(ConfusionMatrix[] cms) {
    if( cms == null ) return -1;
      tprs = new double[cms.length];
      fprs = new double[cms.length];
      double auc = 0;           // Area-under-ROC
      double TPR_pre = 1;
      double FPR_pre = 1;
      for( int t = 0; t < cms.length; ++t ) {
        double TPR = 1 - cms[t].classErr(1); // =TP/(TP+FN) = true -positive-rate
        double FPR = cms[t].classErr(0); // =FP/(FP+TN) = false-positive-rate
        auc += trapeziod_area(FPR_pre, FPR, TPR_pre, TPR);
        TPR_pre = TPR;
        FPR_pre = FPR;
        tprs[t] = TPR;
        fprs[t] = FPR;
      }
      auc += trapeziod_area(FPR_pre, 0, TPR_pre, 0);
      return Math.round(1000*auc)*0.001;
  }

  private double trapeziod_area(double x1, double x2, double y1, double y2) {
    double base = Math.abs(x1 - x2);
    double havg = 0.5 * (y1 + y2);
    return base * havg;
  }

  @Override public String speedDescription() { return "time/tree"; }
  @Override public long speedValue() {
    Value value = DKV.get(dest());
    DTree.TreeModel m = value != null ? (DTree.TreeModel) value.get() : null;
    long numTreesBuiltSoFar = m == null ? 0 : m.treeBits.length;
    long sv = (numTreesBuiltSoFar <= 0) ? 0 : (runTimeMs() / numTreesBuiltSoFar);
    return sv;
  }

  protected abstract water.util.Log.Tag.Sys logTag();
  protected abstract TM buildModel( TM initialModel, Frame fr, String names[], String domains[][], String[] cmDomain, Timer t_build );

  protected abstract TM makeModel( Key outputKey, Key dataKey, Key testKey, String names[], String domains[][], String[] cmDomain);
  protected abstract TM makeModel( TM model, double err, ConfusionMatrix cm);
  protected abstract TM makeModel( TM model, DTree ktrees[], DTree.TreeModel.TreeStats tstats);

  protected boolean inBagRow(Chunk[] chks, int row) { return false; }

  static public final boolean isOOBRow(int nid)     { return nid <= OUT_OF_BAG; }
  static public final boolean isDecidedRow(int nid) { return nid == DECIDED_ROW; }
  static public final int     oob2Nid(int oobNid)   { return -oobNid + OUT_OF_BAG; }
  static public final int     nid2Oob(int nid)      { return -nid + OUT_OF_BAG; }

  // Helper to unify use of M-T RNG
  public static Random createRNG(long seed) {
    return new MersenneTwisterRNG(new int[] { (int)(seed>>32L),(int)seed });
  }

  // helper for debugging
  static protected void printGenerateTrees(DTree[] trees) {
    for( int k=0; k<trees.length; k++ )
      if( trees[k] != null )
        System.out.println(trees[k].root().toString2(new StringBuilder(),0));
  }

  public static double[] DEFAULT_THRESHOLDS = new double[] { 0,0.001,0.002,0.003,0.004,0.005,0.006,0.007,0.008,0.009,0.01,0.011,0.012,0.013,0.014,0.015,0.016,0.017,0.018,0.019,0.02,0.021,0.022,0.023,0.024,0.025,0.026,0.027,0.028,0.029,0.03,0.031,0.032,0.033,0.034,0.035,0.036,0.037,0.038,0.039,0.04,0.041,0.042,0.043,0.044,0.045,0.046,0.047,0.048,0.049,0.05,0.051,0.052,0.053,0.054,0.055,0.056,0.057,0.058,0.059,0.06,0.061,0.062,0.063,0.064,0.065,0.066,0.067,0.068,0.069,0.07,0.071,0.072,0.073,0.074,0.075,0.076,0.077,0.078,0.079,0.08,0.081,0.082,0.083,0.084,0.085,0.086,0.087,0.088,0.089,0.09,0.091,0.092,0.093,0.094,0.095,0.096,0.097,0.098,0.099,0.1,0.101,0.102,0.103,0.104,0.105,0.106,0.107,0.108,0.109,0.11,0.111,0.112,0.113,0.114,0.115,0.116,0.117,0.118,0.119,0.12,0.121,0.122,0.123,0.124,0.125,0.126,0.127,0.128,0.129,0.13,0.131,0.132,0.133,0.134,0.135,0.136,0.137,0.138,0.139,0.14,0.141,0.142,0.143,0.144,0.145,0.146,0.147,0.148,0.149,0.15,0.151,0.152,0.153,0.154,0.155,0.156,0.157,0.158,0.159,0.16,0.161,0.162,0.163,0.164,0.165,0.166,0.167,0.168,0.169,0.17,0.171,0.172,0.173,0.174,0.175,0.176,0.177,0.178,0.179,0.18,0.181,0.182,0.183,0.184,0.185,0.186,0.187,0.188,0.189,0.19,0.191,0.192,0.193,0.194,0.195,0.196,0.197,0.198,0.199,0.2,0.201,0.202,0.203,0.204,0.205,0.206,0.207,0.208,0.209,0.21,0.211,0.212,0.213,0.214,0.215,0.216,0.217,0.218,0.219,0.22,0.221,0.222,0.223,0.224,0.225,0.226,0.227,0.228,0.229,0.23,0.231,0.232,0.233,0.234,0.235,0.236,0.237,0.238,0.239,0.24,0.241,0.242,0.243,0.244,0.245,0.246,0.247,0.248,0.249,0.25,0.251,0.252,0.253,0.254,0.255,0.256,0.257,0.258,0.259,0.26,0.261,0.262,0.263,0.264,0.265,0.266,0.267,0.268,0.269,0.27,0.271,0.272,0.273,0.274,0.275,0.276,0.277,0.278,0.279,0.28,0.281,0.282,0.283,0.284,0.285,0.286,0.287,0.288,0.289,0.29,0.291,0.292,0.293,0.294,0.295,0.296,0.297,0.298,0.299,0.3,0.301,0.302,0.303,0.304,0.305,0.306,0.307,0.308,0.309,0.31,0.311,0.312,0.313,0.314,0.315,0.316,0.317,0.318,0.319,0.32,0.321,0.322,0.323,0.324,0.325,0.326,0.327,0.328,0.329,0.33,0.331,0.332,0.333,0.334,0.335,0.336,0.337,0.338,0.339,0.34,0.341,0.342,0.343,0.344,0.345,0.346,0.347,0.348,0.349,0.35,0.351,0.352,0.353,0.354,0.355,0.356,0.357,0.358,0.359,0.36,0.361,0.362,0.363,0.364,0.365,0.366,0.367,0.368,0.369,0.37,0.371,0.372,0.373,0.374,0.375,0.376,0.377,0.378,0.379,0.38,0.381,0.382,0.383,0.384,0.385,0.386,0.387,0.388,0.389,0.39,0.391,0.392,0.393,0.394,0.395,0.396,0.397,0.398,0.399,0.4,0.401,0.402,0.403,0.404,0.405,0.406,0.407,0.408,0.409,0.41,0.411,0.412,0.413,0.414,0.415,0.416,0.417,0.418,0.419,0.42,0.421,0.422,0.423,0.424,0.425,0.426,0.427,0.428,0.429,0.43,0.431,0.432,0.433,0.434,0.435,0.436,0.437,0.438,0.439,0.44,0.441,0.442,0.443,0.444,0.445,0.446,0.447,0.448,0.449,0.45,0.451,0.452,0.453,0.454,0.455,0.456,0.457,0.458,0.459,0.46,0.461,0.462,0.463,0.464,0.465,0.466,0.467,0.468,0.469,0.47,0.471,0.472,0.473,0.474,0.475,0.476,0.477,0.478,0.479,0.48,0.481,0.482,0.483,0.484,0.485,0.486,0.487,0.488,0.489,0.49,0.491,0.492,0.493,0.494,0.495,0.496,0.497,0.498,0.499,0.5,0.501,0.502,0.503,0.504,0.505,0.506,0.507,0.508,0.509,0.51,0.511,0.512,0.513,0.514,0.515,0.516,0.517,0.518,0.519,0.52,0.521,0.522,0.523,0.524,0.525,0.526,0.527,0.528,0.529,0.53,0.531,0.532,0.533,0.534,0.535,0.536,0.537,0.538,0.539,0.54,0.541,0.542,0.543,0.544,0.545,0.546,0.547,0.548,0.549,0.55,0.551,0.552,0.553,0.554,0.555,0.556,0.557,0.558,0.559,0.56,0.561,0.562,0.563,0.564,0.565,0.566,0.567,0.568,0.569,0.57,0.571,0.572,0.573,0.574,0.575,0.576,0.577,0.578,0.579,0.58,0.581,0.582,0.583,0.584,0.585,0.586,0.587,0.588,0.589,0.59,0.591,0.592,0.593,0.594,0.595,0.596,0.597,0.598,0.599,0.6,0.601,0.602,0.603,0.604,0.605,0.606,0.607,0.608,0.609,0.61,0.611,0.612,0.613,0.614,0.615,0.616,0.617,0.618,0.619,0.62,0.621,0.622,0.623,0.624,0.625,0.626,0.627,0.628,0.629,0.63,0.631,0.632,0.633,0.634,0.635,0.636,0.637,0.638,0.639,0.64,0.641,0.642,0.643,0.644,0.645,0.646,0.647,0.648,0.649,0.65,0.651,0.652,0.653,0.654,0.655,0.656,0.657,0.658,0.659,0.66,0.661,0.662,0.663,0.664,0.665,0.666,0.667,0.668,0.669,0.67,0.671,0.672,0.673,0.674,0.675,0.676,0.677,0.678,0.679,0.68,0.681,0.682,0.683,0.684,0.685,0.686,0.687,0.688,0.689,0.69,0.691,0.692,0.693,0.694,0.695,0.696,0.697,0.698,0.699,0.7,0.701,0.702,0.703,0.704,0.705,0.706,0.707,0.708,0.709,0.71,0.711,0.712,0.713,0.714,0.715,0.716,0.717,0.718,0.719,0.72,0.721,0.722,0.723,0.724,0.725,0.726,0.727,0.728,0.729,0.73,0.731,0.732,0.733,0.734,0.735,0.736,0.737,0.738,0.739,0.74,0.741,0.742,0.743,0.744,0.745,0.746,0.747,0.748,0.749,0.75,0.751,0.752,0.753,0.754,0.755,0.756,0.757,0.758,0.759,0.76,0.761,0.762,0.763,0.764,0.765,0.766,0.767,0.768,0.769,0.77,0.771,0.772,0.773,0.774,0.775,0.776,0.777,0.778,0.779,0.78,0.781,0.782,0.783,0.784,0.785,0.786,0.787,0.788,0.789,0.79,0.791,0.792,0.793,0.794,0.795,0.796,0.797,0.798,0.799,0.8,0.801,0.802,0.803,0.804,0.805,0.806,0.807,0.808,0.809,0.81,0.811,0.812,0.813,0.814,0.815,0.816,0.817,0.818,0.819,0.82,0.821,0.822,0.823,0.824,0.825,0.826,0.827,0.828,0.829,0.83,0.831,0.832,0.833,0.834,0.835,0.836,0.837,0.838,0.839,0.84,0.841,0.842,0.843,0.844,0.845,0.846,0.847,0.848,0.849,0.85,0.851,0.852,0.853,0.854,0.855,0.856,0.857,0.858,0.859,0.86,0.861,0.862,0.863,0.864,0.865,0.866,0.867,0.868,0.869,0.87,0.871,0.872,0.873,0.874,0.875,0.876,0.877,0.878,0.879,0.88,0.881,0.882,0.883,0.884,0.885,0.886,0.887,0.888,0.889,0.89,0.891,0.892,0.893,0.894,0.895,0.896,0.897,0.898,0.899,0.9,0.901,0.902,0.903,0.904,0.905,0.906,0.907,0.908,0.909,0.91,0.911,0.912,0.913,0.914,0.915,0.916,0.917,0.918,0.919,0.92,0.921,0.922,0.923,0.924,0.925,0.926,0.927,0.928,0.929,0.93,0.931,0.932,0.933,0.934,0.935,0.936,0.937,0.938,0.939,0.94,0.941,0.942,0.943,0.944,0.945,0.946,0.947,0.948,0.949,0.95,0.951,0.952,0.953,0.954,0.955,0.956,0.957,0.958,0.959,0.96,0.961,0.962,0.963,0.964,0.965,0.966,0.967,0.968,0.969,0.97,0.971,0.972,0.973,0.974,0.975,0.976,0.977,0.978,0.979,0.98,0.981,0.982,0.983,0.984,0.985,0.986,0.987,0.988,0.989,0.99,0.991,0.992,0.993,0.994,0.995,0.996,0.997,0.998,0.999,1 };
}
