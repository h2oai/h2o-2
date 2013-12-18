package hex.gbm;

import hex.rng.MersenneTwisterRNG;
import java.util.Arrays;
import java.util.Random;
import jsr166y.ForkJoinTask;
import jsr166y.RecursiveAction;
import water.*;
import water.H2O.H2OCountedCompleter;
import water.Job.ValidatedJob;
import water.api.DocGen;
import water.fvec.*;
import water.util.*;
import water.util.Log.Tag.Sys;

public abstract class SharedTreeModelBuilder<TM extends DTree.TreeModel> extends ValidatedJob {
  static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  @API(help = "Number of trees", filter = Default.class, lmin=1, lmax=1000000)
  public int ntrees = 100;

  @API(help = "Maximum tree depth", filter = Default.class, lmin=1, lmax=10000)
  public int max_depth = 5;

  @API(help = "Fewest allowed observations in a leaf (in R called 'nodesize')", filter = Default.class, lmin=1)
  public int min_rows = 10;

  @API(help = "Build a histogram of this many bins, then split at the best point", filter = Default.class, lmin=2, lmax=100000)
  public int nbins = 100;

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

  /** Maximal number of supported levels in response. */
  public static final int MAX_SUPPORTED_LEVELS = 1000;

  /** Marker for already decided row. */
  static public final int DECIDED_ROW = -1;
  /** Marker for sampled out rows */
  static public final int OUT_OF_BAG = -2;

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
  }

  // --------------------------------------------------------------------------
  // Driver for model-building.
  public void buildModel( ) {
    final Key outputKey = dest();
    String sd = input("source");
    final Key dataKey = (sd==null||sd.length()==0)?null:Key.make(sd);
    String sv = input("validation");
    final Key testKey = (sv==null||sv.length()==0)?dataKey:Key.make(sv);

    Frame fr = new Frame(_names, _train);
    fr.add(_responseName,response);
    final Frame frm = new Frame(fr); // Model-Frame; no extra columns
    String names[] = frm.names();
    String domains[][] = frm.domains();

    // For doing classification on Integer (not Enum) columns, we want some
    // handy names in the Model.  This really should be in the Model code.
    String[] domain = response.domain();
    if( domain == null && _nclass > 1 ) // No names?  Something is wrong since we converted response to enum
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

    // Tail-call position: this forks off in the background, and this call
    // returns immediately.  The actual model build is merely kicked off.
    buildModel(fr,names,domains,outputKey, dataKey, testKey, new Timer());
  }

  // Shared cleanup
  protected void cleanUp(Frame fr, Timer t_build) {
    Log.info(logTag(),"Modeling done in "+t_build);

    // Remove temp vectors; cleanup the Frame
    while( fr.numCols() > _ncols+1/*Do not delete the response vector*/ )
      UKV.remove(fr.remove(fr.numCols()-1)._key);
    // If we made a response column with toEnum, nuke it.
    if( _gen_enum ) UKV.remove(response._key);
    remove();                   // Remove Job
  }

  transient long _timeLastScoreStart, _timeLastScoreEnd;
  protected TM doScoring(TM model, Key outputKey, Frame fr, DTree[] ktrees, int tid, DTree.TreeModel.TreeStats tstats, boolean finalScoring, boolean oob ) {
    long now = System.currentTimeMillis();
    long sinceLastScore = now-_timeLastScoreStart;
    Score sc = null;
    if( finalScoring ||
        // Throttle scoring to keep the cost sane; limit to a 10% duty cycle & every 4 secs
        (sinceLastScore > 4000 && // Limit scoring updates to every 4sec
         (double)(_timeLastScoreEnd-_timeLastScoreStart)/sinceLastScore < 0.1) ) { // 10% duty cycle
      _timeLastScoreStart = now;
      sc = new Score().doIt(model, fr, validation, oob).report(logTag(),tid,ktrees);
      _timeLastScoreEnd = System.currentTimeMillis();
    }
    model = makeModel(model, finalScoring?null:ktrees, 
                      sc==null ? Double.NaN : sc._sum/_nrows, 
                      sc==null ? null : sc._cm, tstats);
    DKV.put(outputKey, model);
    return model;
  }

  // --------------------------------------------------------------------------
  // Convenvience accessor for a complex chunk layout.
  // Wish I could name the array elements nicer...
  private Chunk[] chk_check( Chunk chks[] ) {
    assert chks.length == _ncols+1/*response*/+_nclass/*prob dist so far*/+_nclass/*tmp*/+_nclass/*NIDs, one tree per class*/;
    return chks;
  }
  protected Chunk chk_resp( Chunk chks[]        ) { return chks[_ncols]; }
  protected Chunk chk_tree( Chunk chks[], int c ) { return chks[_ncols+1+c]; }
  protected Chunk chk_work( Chunk chks[], int c ) { return chks[_ncols+1+_nclass+c]; }
  protected Chunk chk_nids( Chunk chks[], int t ) { return chks[_ncols+1+_nclass+_nclass+t]; }

  protected final Vec vec_work( Frame fr, int c) { return fr.vecs()[_ncols+1+_nclass+c]; }
  protected final Vec vec_nids( Frame fr, int t) { return fr.vecs()[_ncols+1+_nclass+_nclass+t]; }

  // --------------------------------------------------------------------------
  // Fuse 2 conceptual passes into one:
  //
  // Pass 1: Score a prior partially-built tree model, and make new Node
  //         assignments to every row.  This involves pulling out the current
  //         assigned DecidedNode, "scoring" the row against that Node's
  //         decision criteria, and assigning the row to a new child
  //         UndecidedNode (and giving it an improved prediction).
  //
  // Pass 2: Build new summary DSharedHistograms on the new child
  //         UndecidedNodes every row got assigned into.  Collect counts, mean,
  //         variance, min, max per bin, per column.
  //
  // The result is a set of DSharedHistogram arrays; one DSharedHistogram array
  // for each unique 'leaf' in the tree being histogramed in parallel.  These
  // have node ID's (nids) from 'leaf' to 'tree._len'.  Each DSharedHistogram
  // array is for all the columns in that 'leaf'.
  //
  // The other result is a prediction "score" for the whole dataset, based on
  // the previous passes' DHistograms.
  public class ScoreBuildHistogram extends MRTask2<ScoreBuildHistogram> {
    final DTree _trees[]; // Read-only, shared (except at the histograms in the Nodes)
    final int   _leafs[]; // Number of active leaves (per tree)
    // Histograms for every tree, split & active column
    final DSharedHistogram _hcs[/*tree/klass*/][/*tree-relative node-id*/][/*column*/];
    public ScoreBuildHistogram(DTree trees[], int leafs[], DSharedHistogram hcs[][][]) {
      assert trees.length==_nclass; // One tree per-class
      assert leafs.length==_nclass; // One count of leaves per-class
      _trees=trees;
      _leafs=leafs;
      _hcs = hcs;
    }

    // Once-per-node shared init
    @Override public void setupLocal( ) { 
      // Init all the internal tree fields after shipping over the wire
      for( DTree dt : _trees ) if( dt != null ) dt.init_tree(); 
      // Allocate local shared memory histograms
      for( int k=0; k<_nclass; k++ ) {
        if( _trees[k] != null ) { // Ignore unused classes
          for( int l=_leafs[k]; l<_trees[k]._len; l++ ) {
            DTree.UndecidedNode udn = _trees[k].undecided(l);
            DSharedHistogram hs[] = _hcs[k][l-_leafs[k]];
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
      }
    }

    @Override public void map( Chunk[] chks ) {
      // For all klasses
      ForkJoinTask sb1t[] = new ForkJoinTask[_nclass];
      for( int k=0; k<_nclass; k++ )
        if( _trees[k] != null ) // Ignore unused classes
          sb1t[k] = new ScoreBuildOneTree(_trees,_leafs,_hcs,chks,k).fork();
      for( int k=0; k<_nclass; k++ )
        if( _trees[k] != null ) // Ignore unused classes
          sb1t[k].join();
    }

    @Override public void reduce( ScoreBuildHistogram sbh ) {
      // Merge histograms
      if( sbh._hcs == _hcs ) return;
      assert _hcs.length==_nclass; // One tree per class
      for( int k=0; k<_nclass; k++ ) {
        DSharedHistogram hcs[/*leaf#*/][/*col*/] = _hcs[k];
        if( hcs == null ) _hcs[k] = sbh._hcs[k];
        else for( int i=0; i<hcs.length; i++ ) {
          DSharedHistogram hs1[] = hcs[i], hs2[] = sbh._hcs[k][i];
          if( hs1 == null ) hcs[i] = hs2;
          else if( hs2 != null )
            for( int j=0; j<hs1.length; j++ )
              if( hs1[j] == null ) hs1[j] = hs2[j];
              else if( hs2[j] != null )
                hs1[j].add(hs2[j]);
        }
      }
    }
  }

  public class ScoreBuildOneTree extends RecursiveAction {
    final DTree _trees[]; // Read-only, shared (except at the histograms in the Nodes)
    final int   _leafs[]; // Number of active leaves (per tree)
    final DSharedHistogram _hcs[/*tree/klass*/][/*tree-relative node-id*/][/*column*/];
    final Chunk _chks[];
    final int _treenum;
    ScoreBuildOneTree( DTree trees[], int leafs[], DSharedHistogram hcs[][][], Chunk chks[], int treenum ) {
      _trees = trees; _leafs = leafs; _hcs = hcs; _chks = chks; _treenum = treenum;
    }

    @Override public void compute() {
      final int k = _treenum;
      final Chunk chks[] = _chks;
      final DTree tree = _trees[k];
      final int leaf = _leafs[k]; // Number of active leafs per tree for given class
      // A leaf-biased array of all active histograms
      final DSharedHistogram hcs[][] = _hcs[k];
      final Chunk nids = chk_nids(chks,k);
      final Chunk wrks = chk_work(chks,k); // What we predict on

      // Pass 1: Score a prior partially-built tree model, and make new Node
      // assignments to every row.  This involves pulling out the current
      // assigned DecidedNode, "scoring" the row against that Node's decision
      // criteria, and assigning the row to a new child UndecidedNode (and
      // giving it an improved prediction).
      for( int row=0; row<nids._len; row++ ) { // Over all rows
        int nid = (int)nids.at80(row); // Get Node to decide from
        if (isDecidedRow(nid)) continue; // already done
        if (isOOBRow(nid)) { // sampled away - we track the position in the tree
          if ( leaf > 0) {
            int nnid = oob2Nid(nid);
            DTree.DecidedNode dn = tree.decided(nnid);
            if( dn._split._col == -1 ) nids.set0(row,nid2Oob(nnid = dn._pid)); // Might have a leftover non-split
            if( nnid != -1 ) nnid = tree.decided(nnid).ns(chks,row); // Move down the tree 1 level
            if( nnid != -1 ) nids.set0(row,nid2Oob(nnid));
          }
          continue;
        }

        // Score row against current decisions & assign new split
        if( leaf > 0 ) {      // Prior pass exists?
          DTree.DecidedNode dn = tree.decided(nid);
          if( dn._split._col == -1 ) nids.set0(row,(nid = dn._pid)); // Might have a leftover non-split
          if( nid != -1 ) nid = tree.decided(nid).ns(chks,row); // Move down the tree 1 level
          if( nid != -1 ) nids.set0(row,nid);
        }

        // Pass 1.9
        if( nid < leaf ) continue; // row already predicts perfectly

        // Pass 2.0
        double y = wrks.at0(row);       // Response for this row
        if( Double.isNaN(y) ) continue; // No response, cannot train
        DSharedHistogram nhs[] = hcs[nid-leaf];
        int sCols[] = tree.undecided(nid)._scoreCols; // Columns to score (null, or a list of selected cols)

        if( sCols != null ) { // Sub-selecting just some columns?
          for( int j=0; j<sCols.length; j++) { // For tracked cols
            final int c = sCols[j];
            nhs[c].incr((float)chks[c].at0(row),y); // Histogram row/col
          }
        } else {                // Else all columns
          for( int j=0; j<_ncols; j++) // for all columns
            if( nhs[j] != null )       // Tracking this column?
              nhs[j].incr((float)chks[j].at0(row),y); // Histogram row/col
        }
      }
    }
  }

  
  // --------------------------------------------------------------------------
  // Builder-specific decision node
  protected abstract DTree.DecidedNode makeDecided( DTree.UndecidedNode udn, DSharedHistogram hs[] );

  protected DSharedHistogram[][][] buildLayer(Frame fr, DTree ktrees[], int leafs[], DSharedHistogram hcs[][][]) {
    // Build K trees, one per class.
    // Fuse 2 conceptual passes into one:
    // Pass 1: Score a prior DHistogram, and make new Node assignments
    // to every row.  This involves pulling out the current assigned Node,
    // "scoring" the row against that Node's decision criteria, and assigning
    // the row to a new child Node (and giving it an improved prediction).
    // Pass 2: Build new summary DHistograms on the new child Nodes every row
    // got assigned into.  Collect counts, mean, variance, min, max per bin,
    // per column.

    ScoreBuildHistogram sbh = new ScoreBuildHistogram(ktrees,leafs,hcs).doAll(fr);
    //System.out.println(sbh.profString());
    hcs = sbh._hcs;             // Get the new histograms

    // Build up the next-generation tree splits from the current histograms.
    // Nearly all leaves will split one more level.  This loop nest is
    //           O( #active_splits * #bins * #ncols )
    // but is NOT over all the data.
    boolean did_split=false;
    for( int k=0; k<_nclass; k++ ) {
      DTree tree = ktrees[k]; // Tree for class K
      if( tree == null ) continue;
      int tmax = tree.len();   // Number of total splits in tree K
      for( int leaf=leafs[k]; leaf<tmax; leaf++ ) { // Visit all the new splits (leaves)
        DTree.UndecidedNode udn = tree.undecided(leaf);
        //System.out.println("Class "+(response._domain[k])+",\n  Undecided node:"+udn);
        // Replace the Undecided with the Split decision
        DTree.DecidedNode dn = makeDecided(udn,hcs[k][leaf-leafs[k]]);
        //System.out.println("--> Decided node: " + dn);
        //System.out.println("  > Split: " + dn._split + " Total rows: " + (dn._split.rowsLeft()+dn._split.rowsRight()));
        if( dn._split.col() == -1 ) udn.do_not_split();
        else did_split = true;
      }
      leafs[k]=tmax;          // Setup leafs for next tree level
      int new_leafs = tree.len()-tmax;
      hcs[k] = new DSharedHistogram[new_leafs][/*ncol*/];
      for( int nl = tmax; nl<tree.len(); nl ++ )
        hcs[k][nl-tmax] = tree.undecided(nl)._hs;
      tree.depth++;           // Next layer done
    }
    return did_split ? hcs : null;
  }

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
  // ds[] array, and return the sum.  Dividing any ds[] element by the sum
  // turns the results into a probability distribution.
  protected abstract double score0( Chunk chks[], double ds[/*nclass*/], int row );

  // Score the *tree* columns, and produce a confusion matrix
  public class Score extends MRTask2<Score> {
    long _cm[/*actual*/][/*predicted*/]; // Confusion matrix
    double _sum;                // Sum-squared-error
    long _snrows;               // Count of voted-on rows
    /* @IN */ boolean _oob;

    public double   sum()   { return _sum; }
    public long[][] cm ()   { return _cm;  }
    public long     nrows() { return _snrows; }

    // Compute CM & MSE on either the training or testing dataset
    public Score doIt(Model model, Frame fr, Frame validation, boolean oob) {
      assert !oob || validation==null ; // oob => validation==null
      _oob = oob;
      // No validation, so do on training data
      if( validation == null ) return doAll(fr);
      // Validation: need to score the set, getting a probability distribution for each class
      // Frame has nclass vectors (nclass, or 1 for regression)
      Frame res = model.score(validation);
      // Adapt the validation set to the model
      Frame frs[] = model.adapt(validation,false);
      Frame adapValidation = frs[0]; // adapted validation dataset
      // All columns including response of validation frame are already adapted to model
      if (_nclass>1) { // Classification
        for( int i=0; i<_nclass; i++ )
          adapValidation.add("Work"+i,res.vecs()[i+1]);
      } else { // Regression
        adapValidation.add("Work"+0,res.vecs()[0]);
      }
      // Compute a CM & MSE
      doAll(adapValidation);
      // Remove the extra adapted Vecs
      frs[1].remove();
      // Remove temporary result
      return this;
    }

    @Override public void map( Chunk chks[] ) {
      Chunk ys = chk_resp(chks); // Response
      _cm = new long[_nclass][_nclass];
      double ds[] = new double[_nclass];
      int ties[] = new int[_nclass];
      // Score all Rows
      for( int row=0; row<ys._len; row++ ) {
        if( ys.isNA0(row) ) continue; // Ignore missing response vars
        double sum = score0(chks,ds,row);
        double err;  int ycls=0;
        if( _nclass > 1 ) {    // Classification
          if( sum == 0 ) {       // This tree does not predict this row *at all*?
            err = 1.0f-1.0f/_nclass; // Then take ycls=0, uniform predictive power
            if (_oob) continue; // it is in-bag row (no vote by any tree)
          } else {
            ycls = (int)ys.at80(row); // Response class from 0 to nclass-1
            if (ycls >= _nclass) continue;
            assert 0 <= ycls && ycls < _nclass : "weird ycls="+ycls+", y="+ys.at0(row);
            err = Double.isInfinite(sum)
              ? (Double.isInfinite(ds[ycls]) ? 0 : 1)
              : 1.0-ds[ycls]/sum; // Error: distance from predicting ycls as 1.0
          }
          assert !Double.isNaN(err) : "ds[cls]="+ds[ycls] + ", sum=" + sum;
        } else {                // Regression
          err = ys.at0(row) - sum;
        }
        _sum += err*err;               // Squared error
        assert !Double.isNaN(_sum);
        // Pick highest prob for our prediction.  Count all ties for best.
        int best=0, tie_cnt=0;
        for( int c=1; c<_nclass; c++ )
          if( ds[best] < ds[c] ) { best=c; ties[  tie_cnt=0]=c; }
          else if( ds[best] == ds[c] ) {   ties[++tie_cnt  ]=c; }
        // Break ties psuedo-randomly: (row# mod #ties).
        if( tie_cnt > 1 ) { best = ties[row%tie_cnt]; }
        _cm[ycls][best]++;      // Bump Confusion Matrix also
        _snrows++;
      }
    }
    @Override public void reduce( Score t ) { _sum += t._sum; Utils.add(_cm,t._cm); _snrows += t._snrows; }

    public Score report( Sys tag, int ntree, DTree[] trees ) {
      assert !Double.isNaN(_sum);
      int lcnt=0;
      Log.info(tag,"============================================================== ");
      if (trees==null) {
        Log.info("No trees...");
      } else {
        for( DTree t : trees ) if( t != null ) lcnt += t._len;
        long err=_snrows;
        for( int c=0; c<_nclass; c++ ) err -= _cm[c][c];
        Log.info(tag,"Mean Squared Error is "+(_sum/_snrows)+", with "+ntree+"x"+_nclass+" trees (average of "+((float)lcnt/_nclass)+" nodes)");
        if( _nclass > 1 )
          Log.info(tag,"Total of "+err+" errors on "+_snrows+" rows, CM= "+Arrays.deepToString(_cm));
      }
      return this;
    }
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
  protected abstract void buildModel( Frame fr, String names[], String domains[][], Key outputKey, Key dataKey, Key testKey, Timer t_build );

  protected abstract TM makeModel( TM model, DTree ktrees[], double err, long cm[][], DTree.TreeModel.TreeStats tstats);

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
}
