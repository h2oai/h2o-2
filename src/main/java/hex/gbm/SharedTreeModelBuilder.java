package hex.gbm;

import java.util.Arrays;

import water.*;
import water.Job.ModelJob;
import water.Request2.MultiVecSelect;
import water.Request2.VecClassSelect;
import water.api.*;
import water.api.Request.API;
import water.api.Request.Default;
import water.fvec.*;
import water.util.*;
import water.util.Log.Tag.Sys;

public abstract class SharedTreeModelBuilder extends ModelJob {
  static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  @API(help="", required=true, filter=TVecSelect.class)
  public Vec vresponse;
  class TVecSelect extends VecClassSelect { TVecSelect() { super("source"); } }

  @API(help="columns to ignore",required=false,filter=TMultiVecSelect.class)
  public int [] ignored_cols = new int []{};
  class TMultiVecSelect extends MultiVecSelect { TMultiVecSelect() { super("source");} }

  @API(help = "Number of trees", filter = Default.class, lmin=1, lmax=1000000)
  public int ntrees = 100;

  @API(help = "Maximum tree depth", filter = Default.class, lmin=0, lmax=10000)
  public int max_depth = 5;

  @API(help = "Fewest allowed observations in a leaf", filter = Default.class, lmin=1)
  public int min_rows = 10;

  @API(help = "Build a histogram of this many bins, then split at the best point", filter = Default.class, lmin=2, lmax=100000)
  public int nbins = 1024;

  // Overall prediction Mean Squared Error as I add trees
  transient protected double _errs[];

  @API(help = "Active feature columns")
  protected int _ncols;

  @API(help = "Rows in training dataset")
  protected long _nrows;

  @API(help = "Number of classes")
  protected int _nclass;

  @API(help = "Minimum class number, generally 0 or 1")
  protected int _ymin;

  @API(help = "Class distribution, ymin based")
  protected long _distribution[];

  @Override public float progress(){
    DTree.TreeModel m = DKV.get(dest()).get();
    return (float)m.treeBits.length/(float)m.N;
  }

  // --------------------------------------------------------------------------
  // Driver for model-building.
  public void startBuildModel( ) {
    final Frame fr = new Frame(source); // Local copy for local hacking
    fr.remove(ignored_cols);
    // Doing classification only right now...
    if( !vresponse.isEnum() ) vresponse.asEnum();

    // While I'd like the Frames built custom for each call, with excluded
    // columns already removed - for now check to see if the response column is
    // part of the frame and remove it up front.
    String vname="response";
    for( int i=0; i<fr.numCols(); i++ )
      if( fr.vecs()[i]==vresponse ) {
        vname=fr._names[i];
        fr.remove(i);
      }

    assert 0 <= ntrees && ntrees < 1000000; // Sanity check
    assert 1 <= min_rows;
    _ncols = fr.numCols();      // Feature set
    _nrows = fr.numRows() - vresponse.naCnt();
    _ymin = (int)vresponse.min();
    _nclass = vresponse.isInt() ? (char)(vresponse.max()-_ymin+1) : 1;
    assert 1 <= _nclass && _nclass <= 1000; // Arbitrary cutoff for too many classes
    _errs = new double[0];                // No trees yet
    final Key outputKey = dest();
    final Key dataKey = null;
    String[] domain = vresponse.domain();

    fr.add(vname,vresponse);         // Hardwire response as last vector
    final Frame frm = new Frame(fr); // Model-Frame; no extra columns

    // Find the class distribution
    _distribution = new ClassDist().doAll(vresponse)._ys;

    // Also add to the basic working Frame these sets:
    //   nclass Vecs of current forest results (sum across all trees)
    //   nclass Vecs of working/temp data
    //   nclass Vecs of NIDs, allowing 1 tree per class

    // Current forest values: results of summing the prior M trees
    for( int i=0; i<_nclass; i++ )
      fr.add("Tree_"+domain[i], vresponse.makeZero());

    // Initial work columns.  Set-before-use in the algos.
    for( int i=0; i<_nclass; i++ )
      fr.add("Work_"+domain[i], vresponse.makeZero());

    // One Tree per class, each tree needs a NIDs.  For empty classes use a -1
    // NID signifying an empty regression tree.
    for( int i=0; i<_nclass; i++ )
      fr.add("NIDs_"+domain[i], vresponse.makeCon(_distribution[i]==0?-1:0));

    // Tail-call position: this forks off in the background, and this call
    // returns immediately.  The actual model build is merely kicked off.
    buildModel(fr,frm,outputKey, dataKey, new Timer());
  }

  // Shared cleanup
  protected void cleanUp(Frame fr, Timer t_build) {
    Log.info(logTag(),"Modeling done in "+t_build);

    // Remove temp vectors; cleanup the Frame
    while( fr.numCols() > _ncols+1/*Do not delete the response vector*/ )
      UKV.remove(fr.remove(fr.numCols()-1)._key);
    remove();                   // Remove Job
  }

  // --------------------------------------------------------------------------
  // Convenvience accessor for a complex chunk layout.
  // Wish I could name the array elements nicer...
  private Chunk[] chk_check( Chunk chks[] ) {
    assert chks.length == _ncols+1/*response*/+_nclass/*prob dist so far*/+_nclass/*tmp*/+_nclass/*NIDs, one tree per class*/;
    return chks;
  }
  Chunk chk_resp( Chunk chks[]        ) { return chk_check(chks)[_ncols]; }
  Chunk chk_tree( Chunk chks[], int c ) { return chk_check(chks)[_ncols+1+c]; }
  Chunk chk_work( Chunk chks[], int c ) { return chk_check(chks)[_ncols+1+_nclass+c]; }
  Chunk chk_nids( Chunk chks[], int t ) { return chk_check(chks)[_ncols+1+_nclass+_nclass+t]; }

  // --------------------------------------------------------------------------
  // Fuse 2 conceptual passes into one:
  //
  // Pass 1: Score a prior partially-built tree model, and make new Node
  //         assignments to every row.  This involves pulling out the current
  //         assigned DecidedNode, "scoring" the row against that Node's
  //         decision criteria, and assigning the row to a new child
  //         UndecidedNode (and giving it an improved prediction).
  //
  // Pass 2: Build new summary DHistograms on the new child UndecidedNodes every
  //         row got assigned into.  Collect counts, mean, variance, min, max
  //         per bin, per column.
  //
  // The result is a set of DHistogram arrays; one DHistogram array for each
  // unique 'leaf' in the tree being histogramed in parallel.  These have node
  // ID's (nids) from 'leaf' to 'tree._len'.  Each DHistogram array is for all
  // the columns in that 'leaf'.
  //
  // The other result is a prediction "score" for the whole dataset, based on
  // the previous passes' DHistograms.
  class ScoreBuildHistogram extends MRTask2<ScoreBuildHistogram> {
    final DTree _trees[]; // Read-only, shared (except at the histograms in the Nodes)
    final int   _leafs[]; // Number of active leaves (per tree)
    // Histograms for every tree, split & active column
    DHistogram _hcs[/*tree/klass*/][/*tree-relative node-id*/][/*column*/];
    ScoreBuildHistogram(DTree trees[], int leafs[]) {
      assert trees.length==_nclass; // One tree per-class
      assert leafs.length==_nclass; // One count of leaves per-class
      _trees=trees;
      _leafs=leafs;
    }

    // Init all the internal tree fields after shipping over the wire
    @Override public void setupLocal( ) { for( DTree dt : _trees ) if( dt != null ) dt.init_tree(); }

    public DHistogram[] getFinalHisto( int k, int nid ) {
      DHistogram hs[] = _hcs[k][nid-_leafs[k]];
      if( hs == null ) return null; // Can happen if the split is all NA's
      // Having gather min/max/mean/class/etc on all the data, we can now
      // tighten the min & max numbers.
      for( int j=0; j<hs.length; j++ ) {
        DHistogram h = hs[j];    // Old histogram of column
        if( h != null ) h.tightenMinMax();
      }
      return hs;
    }

    @Override public void map( Chunk[] chks ) {
      // We need private (local) space to gather the histograms.
      // Make local clones of all the histograms that appear in this chunk.
      _hcs = new DHistogram[_nclass][][];

      // For all klasses
      for( int k=0; k<_nclass; k++ ) {
        final DTree tree = _trees[k];
        if( tree == null ) continue; // Ignore unused classes
        final int leaf   = _leafs[k];
        // A leaf-biased array of all active histograms
        final DHistogram hcs[][] = _hcs[k] = new DHistogram[tree._len-leaf][];
        final Chunk nids = chk_nids(chks,k);
        final Chunk wrks = chk_work(chks,k); // We are predicting the residuals

        // Pass 1: Score a prior partially-built tree model, and make new Node
        // assignments to every row.  This involves pulling out the current
        // assigned DecidedNode, "scoring" the row against that Node's decision
        // criteria, and assigning the row to a new child UndecidedNode (and
        // giving it an improved prediction).
        for( int i=0; i<nids._len; i++ ) {
          int nid = (int)nids.at80(i); // Get Node to decide from
          if( nid==-2 ) continue; // sampled away

          // Score row against current decisions & assign new split
          if( leaf > 0 ) {      // Prior pass exists?
            DTree.DecidedNode dn = tree.decided(nid);
            if( dn._split._col == -1 ) nids.set0(i,(nid = dn._pid)); // Might have a leftover non-split
            nid = tree.decided(nid).ns(chks,i); // Move down the tree 1 level
            if( nid != -1 ) nids.set0(i,nid);
          }

          // Pass 1.9
          if( nid < leaf ) continue; // row already predicts perfectly

          // We need private (local) space to gather the histograms.
          // Make local clones of all the histograms that appear in this chunk.
          DHistogram nhs[] = hcs[nid-leaf];
          if( nhs != null ) continue; // Already have histograms
          // Lazily manifest this histogram for tree-node 'nid'
          nhs = hcs[nid-leaf] = new DHistogram[_ncols];
          DHistogram ohs[] = tree.undecided(nid)._hs; // The existing column of Histograms
          int sCols[] = tree.undecided(nid)._scoreCols;
          if( sCols != null ) { // Sub-selecting just some columns?
            // For just the selected columns make Big Histograms
            for( int j=0; j<sCols.length; j++ ) { // Make private copies
              int idx = sCols[j];                 // Just the selected columns
              nhs[idx] = ohs[idx].bigCopy();
            }
            // For all the rest make small Histograms
            for( int j=0; j<nhs.length; j++ )
              if( ohs[j] != null && nhs[j]==null )
                nhs[j] = ohs[j].smallCopy();
          } else {              // Selecting all columns
            // Default: make big copies of all
            for( int j=0; j<nhs.length; j++ )
                if( ohs[j] != null )
                  nhs[j] = ohs[j].bigCopy();
          }
        }

        // Pass 2: Build new summary DHistograms on the new child
        // UndecidedNodes every row got assigned into.  Collect counts, mean,
        // variance, min, max per bin, per column.
        for( int row=0; row<nids._len; row++ ) { // For all rows
          int nid = (int)nids.at80(row);         // Get Node to decide from
          if( nid<leaf ) continue; // row already predicts perfectly or sampled away
          DHistogram nhs[] = hcs[nid-leaf];

          double y = wrks.at0(row);      // Response for this row
          for( int j=0; j<_ncols; j++) { // For all columns
            DHistogram nh = nhs[j];
            if( nh == null ) continue; // Not tracking this column?
            float col_data = (float)chks[j].at0(row);
            if( nh instanceof DBinHistogram ) // Big histogram
              ((DBinHistogram)nh).incr(row,col_data,y);
            else              nh .incr(col_data); // Small histogram
          }
        }
      }
    }

    @Override public void reduce( ScoreBuildHistogram sbh ) {
      // Merge histograms
      assert _hcs.length==_nclass; // One tree per class
      for( int k=0; k<_nclass; k++ ) {
        DHistogram hcs[/*leaf#*/][/*col*/] = _hcs[k];
        if( hcs == null ) _hcs[k] = sbh._hcs[k];
        else for( int i=0; i<hcs.length; i++ ) {
          DHistogram hs1[] = hcs[i], hs2[] = sbh._hcs[k][i];
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

  // --------------------------------------------------------------------------
  private class ClassDist extends MRTask2<ClassDist> {
    long _ys[];
    @Override public void map(Chunk ys) {
      _ys = new long[_nclass];
      for( int i=0; i<ys._len; i++ )
        if( !ys.isNA0(i) )
          _ys[(int)ys.at80(i)-_ymin]++;
    }
    @Override public void reduce( ClassDist that ) { Utils.add(_ys,that._ys); }
  }

  // --------------------------------------------------------------------------
  // Read the 'tree' columns, do model-specific math and put the results in the
  // ds[] array, and return the sum.  Dividing any ds[] element by the sum
  // turns the results into a probability distribution.
  protected abstract double score0( Chunk chks[], double ds[/*nclass*/], int row );

  // Score the *tree* columns, and produce a confusion matrix
  protected class Score extends MRTask2<Score> {
    long _cm[/*actual*/][/*predicted*/]; // Confusion matrix
    double _sum;                // Sum-squared-error
    @Override public void map( Chunk chks[] ) {
      Chunk ys = chk_resp(chks); // Response
      _cm = new long[_nclass][_nclass];
      double ds[] = new double[_nclass];
      // Score all Rows
      for( int row=0; row<ys._len; row++ ) {
        if( ys.isNA0(row) ) continue; // Ignore missing response vars
        int ycls = (int)ys.at80(row)-_ymin; // Response class from 0 to nclass-1
        assert 0 <= ycls && ycls < _nclass : "weird ycls="+ycls+", y="+ys.at0(row)+", ymin="+_ymin;
        double sum = score0(chks,ds,row);
        double err = Double.isInfinite(sum)
          ? (Double.isInfinite(ds[ycls]) ? 0 : 1)
          : 1.0-ds[ycls]/sum; // Error: distance from predicting ycls as 1.0
        assert !Double.isNaN(err) : ds[ycls] + " " + sum;
        _sum += err*err;               // Squared error
        assert !Double.isNaN(_sum);
        int best=0;                    // Pick highest prob for our prediction
        for( int c=1; c<_nclass; c++ )
          if( ds[best] < ds[c] ) best=c;
        _cm[ycls][best]++;      // Bump Confusion Matrix also
      }
    }
    @Override public void reduce( Score t ) { _sum += t._sum; Utils.add(_cm,t._cm); }

    public Score report( Sys tag, int ntree, DTree[] trees ) {
      assert !Double.isNaN(_sum);
      int lcnt=0;
      for( DTree t : trees ) if( t != null ) lcnt += t._len;
      long err=_nrows;
      for( int c=0; c<_nclass; c++ ) err -= _cm[c][c];
      Log.info(tag,"============================================================== ");
      Log.info(tag,"Mean Squared Error for forest is "+(_sum/_nrows));
      Log.info(tag,"Total of "+err+" errors on "+_nrows+" rows, with "+ntree+"x"+_nclass+" trees (average of "+((float)lcnt/_nclass)+" nodes)");
      System.out.println("CM= "+Arrays.deepToString(_cm));
      return this;
    }
  }

  protected abstract water.util.Log.Tag.Sys logTag();
  protected abstract void buildModel( Frame fr, Frame frm, Key outputKey, Key dataKey, Timer t_build );
}
