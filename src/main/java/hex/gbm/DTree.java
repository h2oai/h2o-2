package hex.gbm;

import java.util.Arrays;
import java.util.Random;
import water.*;
import water.H2O.H2OCountedCompleter;
import water.fvec.*;
import water.util.Log.Tag.Sys;
import water.util.Log;
import water.util.Utils;

/**
   A Decision Tree, laid over a Frame of Vecs, and built distributed.

   This class defines an explicit Tree structure, as a collection of {@code
   Tree} {@code Node}s.  The Nodes are numbered with a unique {@code _nid}.
   Users need to maintain their own mapping from their data to a {@code _nid},
   where the obvious technique is to have a Vec of {@code _nid}s (ints), one
   per each element of the data Vecs.

   Each {@code Node} has a {@code DHistogram}, describing summary data about the
   rows.  The DHistogram requires a pass over the data to be filled in, and we
   expect to fill in all rows for Nodes at the same depth at the same time.
   i.e., a single pass over the data will fill in all leaf Nodes' DHistograms
   at once.

   @author Cliff Click
*/
class DTree extends Iced {
  final String[] _names; // Column names
  final int _ncols;      // Active training columns
  final int _nclass;     // #classes, or 0 for regression trees
  final int _min_rows;   // Fewest allowed rows in any split
  private Node[] _ns;    // All the nodes in the tree.  Node 0 is the root.
  int _len;              // Resizable array
  DTree( String[] names, int ncols, int nclass, int min_rows ) { 
    _names = names; _ncols = ncols; _nclass=nclass; _min_rows = min_rows; _ns = new Node[1]; }

  public final Node root() { return _ns[0]; }

  // Return Node i
  public final Node node( int i ) {
    if( i >= _len ) throw new ArrayIndexOutOfBoundsException(i);
    return _ns[i];
  }
  public final UndecidedNode undecided( int i ) { return (UndecidedNode)node(i); }
  public final   DecidedNode   decided( int i ) { return (  DecidedNode)node(i); }

  // Get a new node index, growing innards on demand
  private int newIdx() {
    if( _len == _ns.length ) _ns = Arrays.copyOf(_ns,_len<<1);
    return _len++;
  }

  // Return a deterministic chunk-local RNG.  Can be kinda expensive.
  // Override this in, e.g. Random Forest algos, to get a per-chunk RNG
  public Random rngForChunk( int cidx ) { throw H2O.fail(); }

  // Abstract node flavor
  static abstract class Node extends Iced {
    transient DTree _tree;    // Make transient, lest we clone the whole tree
    final int _pid;           // Parent node id, root has no parent and uses -1
    final int _nid;           // My node-ID, 0 is root
    Node( DTree tree, int pid, int nid ) {
      _tree = tree;
      _pid=pid;
      tree._ns[_nid=nid] = this;
    }

    // Recursively print the decision-line from tree root to this child.
    StringBuilder printLine(StringBuilder sb ) {
      if( _pid==-1 ) return sb.append("[root]");
      DecidedNode parent = _tree.decided(_pid);
      parent.printLine(sb).append(" to ");
      return parent.printChild(sb,_nid);
    }
    abstract public StringBuilder toString2(StringBuilder sb, int depth);
  }

  // An UndecidedNode: Has a DHistogram which is filled in (in parallel with other
  // histograms) in a single pass over the data.  Does not contain any
  // split-decision.
  static abstract class UndecidedNode extends Node {
    DHistogram _hs[];            // DHistograms per column
    int _scoreCols[];            // A list of columns to score; could be null for all
    UndecidedNode( DTree tree, int pid, DHistogram hs[] ) {
      super(tree,pid,tree.newIdx());
      _hs=hs;
      assert hs.length==tree._ncols;
      _scoreCols = scoreCols(hs);
    }

    // Pick a random selection of columns to compute best score.
    // Can return null for 'all columns'.
    abstract int[] scoreCols( DHistogram[] hs );

    @Override public String toString() {
      final int nclass = _tree._nclass;
      final String colPad="  ";
      final int cntW=4, mmmW=4, menW=4, varW=4;
      final int colW=cntW+1+mmmW+1+mmmW+1+nclass*(menW+1)+varW;
      StringBuilder sb = new StringBuilder();
      sb.append("Nid# ").append(_nid).append(", ");
      printLine(sb).append("\n");
      final int ncols = _hs.length;
      for( int j=0; j<ncols; j++ )
        if( _hs[j] != null )
          p(sb,_hs[j]._name+String.format(", err=%5.2f %4.1f",_hs[j].score(),_hs[j]._min),colW).append(colPad);
      sb.append('\n');
      for( int j=0; j<ncols; j++ ) {
        if( _hs[j] == null ) continue;
        p(sb,"cnt" ,cntW).append('/');
        p(sb,"min" ,mmmW).append('/');
        p(sb,"max" ,mmmW).append('/');
        for( int c=0; c<nclass; c++ )
          p(sb,Integer.toString(c),menW).append('/');
        p(sb,"var" ,varW).append(colPad);
      }
      sb.append('\n');
      for( int i=0; i<DHistogram.BINS; i++ ) {
        for( int j=0; j<ncols; j++ ) {
          DHistogram h = _hs[j];
          if( h == null ) continue;
          if( i < h.nbins() ) {
            p(sb, h.bins(i),cntW).append('/');
            p(sb, h.mins(i),mmmW).append('/');
            p(sb, h.maxs(i),mmmW).append('/');
            for( int c=0; c<nclass; c++ )
              p(sb,h.mean(i,c),menW).append('/');
            p(sb, h.var (i),varW).append(colPad);
          } else {
            p(sb,"",colW).append(colPad);
          }
        }
        sb.append('\n');
      }
      sb.append("Nid# ").append(_nid);
      return sb.toString();
    }
    static private StringBuilder p(StringBuilder sb, String s, int w) {
      return sb.append(Log.fixedLength(s,w));
    }
    static private StringBuilder p(StringBuilder sb, long l, int w) {
      return p(sb,Long.toString(l),w);
    }
    static private StringBuilder p(StringBuilder sb, float d, int w) {
      String s = Float.isNaN(d) ? "NaN" :
        ((d==Float.MAX_VALUE || d==-Float.MAX_VALUE) ? " -" :
         Float.toString(d));
      if( s.length() <= w ) return p(sb,s,w);
      s = String.format("%4.2f",d);
      if( s.length() > w )
        s = String.format("%4.1f",d);
      if( s.length() > w )
        s = String.format("%4.0f",d);
      return p(sb,s,w);
    }

    @Override public StringBuilder toString2(StringBuilder sb, int depth) {
      for( int d=0; d<depth; d++ ) sb.append("  ");
      sb.append("Undecided\n");
      return sb;
    }
  }

  // Internal tree nodes which split into several children over a single
  // column.  Includes a split-decision: which child does this Row belong to?
  // Does not contain a histogram describing how the decision was made.
  static abstract class DecidedNode<UDN extends UndecidedNode> extends Node {
    final int _col;             // Column we split over
    final float _bmin, _step;   // Binning info of column
    // The following arrays are all based on a bin# extracted from linear
    // interpolation of _col, _min and _step.
    final int   _nids[];          // Children NIDS for an n-way split
    // A prediction class-vector for each split.  Can be NULL if we have a
    // child (which carries his own prediction).
    final float _pred[/*split*/][/*class*/];

    // Make a correctly flavored Undecided
    abstract UDN makeUndecidedNode(DTree tree, int nid, DHistogram[] nhists );

    // Pick the best column from the given histograms
    abstract int bestCol( UDN udn );

    DecidedNode( UDN n ) {
      super(n._tree,n._pid,n._nid); // Replace Undecided with this DecidedNode
      int col = bestCol(n);         // Best split-point for this tree

      // If I have 2 identical predictor rows leading to 2 different responses,
      // then this dataset cannot distinguish these rows... and we have to bail
      // out here.
      if( col == -1 ) {
        DecidedNode p = n._tree.decided(_pid);
        _col  = p._col;  // Just copy the parent data over, for the predictions
        _bmin = p._bmin;
        _step = p._step;
        _nids = new int[p._nids.length];
        Arrays.fill(_nids,-1);  // No further splits
        _pred = p._pred;
        return;
      }
      _col = col;               // Assign split-column choice

      // From the splitting Undecided, get the column, min, max
      DHistogram uhs[] = n._hs; // Histogram from Undecided
      DBinHistogram splitH = (DBinHistogram)uhs[_col];// DHistogram of the column being split
      assert splitH._nbins > 1; // Should always be some bins to split between
      assert splitH._step  > 0;
      int nbins = splitH._nbins;// Number of split choices
      _bmin = splitH._bmin;     // Binning info
      _step = splitH._step;
      _nids = new int[nbins];
      int nclass = _tree._nclass;
      _pred = new float[nbins][nclass];
      int ncols = _tree._ncols;      // ncols: all predictor columns
      int min_rows = _tree._min_rows;
      for( int b=0; b<nbins; b++ ) { // For all split-points
        // Setup for children splits
        DHistogram nhists[] = splitH.split(col,b,uhs,_tree._names,ncols,min_rows);
        assert nhists==null || nhists.length==ncols;
        _nids[b] = nhists == null ? -1 : makeUndecidedNode(_tree,_nid,nhists)._nid;
        // Also setup predictions locally
        for( int c=0; c<nclass; c++ ) // Copy the class counts into the decision
          _pred[b][c] = splitH.mean(b,c);
        // If the split has no counts for a bin, that just means no training
        // data landed there.  Actual (or test) data certainly can land in that
        // bin - so give it a prediction from the parent.
        if( splitH._bins[b] == 0 ) {
          // Tree root (no parent) and no training data?
          if( _pid == -1 ) {
            Arrays.fill(_pred[b],1.0f/nclass);
          } else {     // Else get parent & use parent's prediction for our bin
            _pred[b] = null;
            DecidedNode p = n._tree.decided(_pid);
            for( int i=0; i<p._nids.length; i++ )
              if( p._nids[i]==_nid ) {
                _pred[b] = p._pred[i];
                break;
              }
            assert _pred[b] != null;
          }
        }
      }
    }
  
    // DecidedNode with a pre-cooked response and no children
    DecidedNode( DTree tree, float pred[] ) {
      super(tree,-1,tree.newIdx());
      _col = -1;
      _bmin = _step = Float.NaN;
      _nids = new int[] { -1 }; // 1 bin, no children
      _pred = new float[][] { pred };
    }

    // Bin #.
    public int bin( Chunk chks[], int i ) {
      if( _nids.length == 1 ) return 0;
      if( chks[_col].isNA0(i) ) return i%_nids.length; // Missing data: pseudo-random bin select
      float d = (float)chks[_col].at0(i); // Value to split on for this row
      // Note that during *scoring* (as opposed to training), we can be exposed
      // to data which is outside the bin limits, so we must cap at both ends.
      int idx1 = (int)((d-_bmin)/_step); // Interpolate bin#
      int bin = Math.max(Math.min(idx1,_nids.length-1),0);// Cap at length
      return bin;
    }

    public int ns( Chunk chks[], int i ) { return _nids[bin(chks,i)]; }

    @Override public String toString() {
      String n= " <= "+_tree._names[_col]+" < ";
      String s = new String();
      float f = _bmin;
      for( int i=0; i<_nids.length; i++ )
        s += f+n+(f+=_step)+" = "+Arrays.toString(_pred[i])+"\n";
      return s;
    }

    StringBuilder printChild( StringBuilder sb, int nid ) {
      for( int i=0; i<_nids.length; i++ )
        if( _nids[i]==nid )
          return sb.append("[").append(_bmin+i*_step).append(" <= ").
            append(_tree._names[_col]).append(" < ").append(_bmin+(i+1)*_step).append("]");
      throw H2O.fail();
    }  

    @Override public StringBuilder toString2(StringBuilder sb, int depth) {
      for( int i=0; i<_nids.length; i++ ) {
        for( int d=0; d<depth; d++ ) sb.append("  ");
        (_col >= 0 ? sb.append(_tree._names[_col]).append(" < ").append(_bmin+_step*(1+i)) : sb.append("init")).append(":").append(Arrays.toString(_pred[i])).append("\n");
        if( _nids[i] >= 0 ) _tree.node(_nids[i]).toString2(sb,depth+1);
      }
      return sb;
    }
  }

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
  static class ScoreBuildHistogram extends MRTask2<ScoreBuildHistogram> {
    final Frame _fr;
    final DTree _trees[]; // Read-only, shared (except at the histograms in the Nodes)
    final int   _leafs[]; // Number of active leaves (per tree)
    final int _ncols;
    final short _nclass;        // One for regression, else #classes
    // Bias classes to zero; e.g. covtype classes range from 1-7 so this is 1.
    // e.g. prostate classes range 0-1 so this is 0
    final int _ymin;
    // Histograms for every tree, split & active column
    DHistogram _hcs[/*tree id*/][/*tree-relative node-id*/][/*column*/];
    ScoreBuildHistogram(DTree trees[], int leafs[], int ncols, short nclass, int ymin, Frame fr) {
      _trees=trees;
      _leafs=leafs;
      _ncols=ncols;
      _nclass = nclass;
      _ymin = ymin;
      _fr = fr;
    }

    // Init all the internal tree fields after shipping over the wire
    @Override public void init( ) {
      for( DTree dt : _trees )
        for( int j=0; j<dt._len; j++ )
          dt._ns[j]._tree = dt;
    }

    public DHistogram[] getFinalHisto( int tid, int nid ) {
      DHistogram hs[] = _hcs[tid][nid-_leafs[tid]];
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
      assert _ncols+_nclass/*response-vector*/+_trees.length == chks.length
        : "Missing columns?  ncols="+_ncols+", "+_nclass+" for response-vector, ntrees="+_trees.length+", and found "+chks.length+" vecs";

      // We need private (local) space to gather the histograms.
      // Make local clones of all the histograms that appear in this chunk.
      _hcs = new DHistogram[_trees.length][][];

      // For all trees
      for( int t=0; t<_trees.length; t++ ) {
        final DTree tree = _trees[t];
        final int leaf = _leafs[t];
        // A leaf-biased array of all active histograms
        final DHistogram hcs[][] = _hcs[t] = new DHistogram[tree._len-leaf][];
        final Chunk nids = chks[_ncols+_nclass/*response-vector*/+t];

        // Pass 1: Score a prior partially-built tree model, and make new Node
        // assignments to every row.  This involves pulling out the current
        // assigned DecidedNode, "scoring" the row against that Node's decision
        // criteria, and assigning the row to a new child UndecidedNode (and
        // giving it an improved prediction).
        for( int i=0; i<nids._len; i++ ) {
          int nid = (int)nids.at80(i); // Get Node to decide from
          if( nid==-2 ) continue; // sampled away
        
          // Score row against current decisions & assign new split
          if( leaf > 0 && (nid = tree.decided(nid).ns(chks,i)) != -1 ) // Prior pass exists?
            nids.set0(i,nid);
        
          // Pass 1.9
          if( nid < leaf ) continue; // row already predicts perfectly
        
          // We need private (local) space to gather the histograms.
          // Make local clones of all the histograms that appear in this chunk.
          DHistogram nhs[] = hcs[nid-leaf];
          if( nhs == null ) {     // Lazily manifest this histogram for 'nid'
            nhs = hcs[nid-leaf] = new DHistogram[_ncols];
            DHistogram ohs[] = tree.undecided(nid)._hs; // The existing column of Histograms
            int sCols[] = tree.undecided(nid)._scoreCols;
            if( sCols != null ) {
              // For just the selected columns make Big Histograms
              for( int j=0; j<sCols.length; j++ ) { // Make private copies
                int idx = sCols[j];                 // Just the selected columns
                nhs[idx] = ohs[idx].bigCopy();
              }
              // For all the rest make small Histograms
              for( int j=0; j<nhs.length; j++ )
                if( ohs[j] != null && nhs[j]==null )
                  nhs[j] = ohs[j].smallCopy();
            } else {
              // Default: make big copies of all
              for( int j=0; j<nhs.length; j++ )
                if( ohs[j] != null )
                  nhs[j] = ohs[j].bigCopy();
            }
          }
        }
        
        // Pass 2: Build new summary DHistograms on the new child
        // UndecidedNodes every row got assigned into.  Collect counts, mean,
        // variance, min, max per bin, per column.
        for( int i=0; i<nids._len; i++ ) { // For all rows
          int nid = (int)nids.at80(i);     // Get Node to decide from
          if( nid<leaf ) continue; // row already predicts perfectly or sampled away
          DHistogram nhs[] = hcs[nid-leaf];

          for( int j=0; j<_ncols; j++) { // For all columns
            DHistogram nh = nhs[j];
            if( nh == null ) continue; // Not tracking this column?
            float f = (float)chks[j].at0(i);
            nh.incr(f);         // Small histogram
            if( nh instanceof DBinHistogram ) // Big histogram
              ((DBinHistogram)nh).incr(i,f,chks,_ncols);
          }
        }
      }
    }

    @Override public void reduce( ScoreBuildHistogram sbh ) {
      // Merge histograms
      for( int t=0; t<_hcs.length; t++ ) {
        DHistogram hcs[][] = _hcs[t];
        for( int i=0; i<hcs.length; i++ ) {
          DHistogram hs1[] = hcs[i], hs2[] = sbh._hcs[t][i];
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
  // Compute sum-squared-error.  Should use the recursive-mean technique.
  public static class BulkScore extends MRTask2<BulkScore> {
    final DTree _trees[]; // Read-only, shared (except at the histograms in the Nodes)
    final int _ncols;     // Number of predictor columns
    final int _nclass;    // Number of classes
    // Bias classes to zero; e.g. covtype classes range from 1-7 so this is 1.
    // e.g. prostate classes range 0-1 so this is 0
    final int _ymin;
    // Out-Of-Bag-Error-Estimate.  This is fairly specific to Random Forest,
    // and involves scoring each tree only on rows for which is was not
    // trained, which only makes sense when scoring the Forest while the
    // training data is handy, i.e., scoring during & after training.
    // Pass in a 1.0 if turned off.
    final float _sampleRate;
    // Scoring is based on average of all trees - or just the plain sum?
    final boolean _doAvg;
    // OUTPUT fields
    long _cm[/*actual*/][/*predicted*/]; // Confusion matrix
    double _sum;                // Sum-squared-error
    long _err;                  // Total absolute errors

    BulkScore( DTree trees[], int ncols, int nclass, int ymin, float sampleRate, boolean doAvg ) {
      _trees = trees; _ncols = ncols;
      _nclass = nclass; _ymin = ymin;
      _sampleRate = sampleRate; _doAvg = doAvg;
    }

    // Compare the vresponse Vec passed in against our prediction.
    public BulkScore doIt( Frame fr, Vec vresp ) {
      assert fr.numCols() >= _ncols+_nclass;
      fr.add("response",vresp);
      doAll(fr);
      fr.remove(fr.numCols()-1);
      return this;
    }

    // Init all the internal tree fields after shipping over the wire
    @Override public void init( ) {
      for( DTree dt : _trees )
        for( int j=0; j<dt._len; j++ )
          dt._ns[j]._tree = dt;
    }

    @Override public void map( Chunk chks[] ) {
      Chunk ys = chks[chks.length-1]; // Response
      _cm = new long[_nclass][_nclass];

      // Get an array of RNGs to replay the sampling in reverse, only for OOBEE.
      // Note the fairly expense MerseenTwisterRNG built per-tree (per-chunk).
      Random rands[] = null;
      if( _sampleRate < 1.0f ) {      // oobee vs full scoring?
        rands = new Random[_trees.length];
        for( int t=0; t<_trees.length; t++ )
          rands[t] = _trees[t].rngForChunk(ys.cidx());
      }
 
      // Score all Rows
      float pred[] = new float[_nclass]; // Shared temp array for computing predictions
      for( int i=0; i<ys._len; i++ ) {
        float err = score0( chks, i, (float)(ys.at0(i)-_ymin), pred, rands );
        _sum += err*err;        // Squared error
      }
    }

    @Override public void reduce( BulkScore t ) {
      _sum += t._sum;
      _err += t._err;
      Utils.add(_cm,t._cm);
    }

    // Return a relative error: response-prediction.  The prediction is a
    // vector; typically a class distribution.  If the response is also a
    // vector we return the Euclidean distance.  If the response is a single
    // class variable we instead return the squared-error of the prediction for
    // the class.  We also count absolute errors when we predict the majority class.
    private float score0( Chunk chks[], int i, float y, float pred[], Random rands[] ) {
      Arrays.fill(pred,0);      // Recycled temp array
      int nt = 0;               // Number of trees not sampled-away

      // For all trees
      for( int t=0; t<_trees.length; t++ ) {
        // For OOBEE error, do not score rows on trees trained on that row
        if( rands != null && !(rands[t].nextFloat() >= _sampleRate) ) continue;
        if( Float.isNaN(y) ) continue; // Ignore missing response vars
        nt++;
        final DTree tree = _trees[t];
        // "score" this row on this tree.  Apply the tree decisions at each
        // point, walking down the tree to a leaf.
        DecidedNode prev = null;
        Node node = tree.root();
        while( node instanceof DecidedNode ) { // While tree-walking
          prev = (DecidedNode)node;
          int nid = prev.ns(chks,i);
          if( nid == -1 ) break;
          node = tree.node(nid);
          assert node._tree==tree;
        }
        // We hit the end of the tree walk.  Get this tree's prediction.
        int bin = prev.bin(chks,i);    // Which bin did we decide on?
        Utils.add(pred,prev._pred[bin]); // Add the prediction vectors, tree-by-tree
      } // End of for-all trees

      // Having computed the votes across all trees, find the majority class
      // and it's error rate.
      if( nt == 0 ) return 0;   // OOBEE: all rows trained, so no rows scored

      if( _doAvg )              // Average (or not) sum of trees?
        for( int c=0; c<_nclass; c++ )
          pred[c] /= nt;        // Average every prediction across trees

      // Regression?
      if( _nclass == 1 )        // Single-class ==> Regression?
        return y-pred[0];       // Prediction: sum of trees

      // Classification?
      // Pick max class in predicted response vector
      int best=0;               // Find largest class
      float sum=pred[0];        // Also compute sum of classes
      for( int c=1; c<_nclass; c++ ) {
        sum += pred[c];
        if( pred[c] > pred[best] ) best=c;
      }

      assert 1-.00001 <= sum && sum <= 1+.00001 : "Expect predictions to be a probability distribution but found "+Arrays.toString(pred)+"="+sum+", scoring row "+i;
      int ycls = (int)y;         // Response class from 0 to nclass-1
      assert 0 <= ycls && ycls < _nclass : "weird ycls="+ycls+", y="+y+", ymin="+_ymin;
      if( best != ycls ) _err++; // Absolute prediction error; off-diagonal sum
      _cm[ycls][best]++;         // Confusion Matrix
      //for( int x=0; x<chks.length; x++ )
      //  System.out.print(chks[x].at(i)+",");
      //System.out.println(" pred="+Arrays.toString(pred)+(best==ycls?"":", ERROR"));
      float ypred = pred[ycls];  // Predict max class
      if( ypred > 1.0f ) ypred = 1.0f;
      return 1.0f - ypred;       // Error from 0 to 1.0
    }

    public BulkScore report( Sys tag, long nrows, int depth ) {
      int lcnt=0;
      for( int t=0; t<_trees.length; t++ ) lcnt += _trees[t]._len;
      Log.info(tag,"============================================================== ");
      Log.info(tag,"Average squared prediction error for tree of depth "+depth+" is "+(_sum/nrows));
      Log.info(tag,"Total of "+_err+" errors on "+nrows+" rows, with "+_trees.length+" trees (average of "+((float)lcnt/_trees.length)+" nodes)");
      return this;
    }
  }

  // Compute class distributions
  static class ClassDist extends MRTask2<ClassDist> {
    final short _nclass;
    final int _ymin;
    long _cs[];
    ClassDist( short nclass, int ymin ) { _nclass = nclass; _ymin = ymin; }
    @Override public void map( Chunk cr ) {
      _cs = new long[_nclass];
      for( int i=0; i<cr._len; i++ )
        _cs[(int)cr.at80(i)-_ymin]++;
    }
    @Override public void reduce( ClassDist cd ) { Utils.add(_cs,cd._cs); }
  }
}
