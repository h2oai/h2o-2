package hex.gbm;

import java.util.Arrays;
import java.util.Random;
import water.*;
import water.H2O.H2OCountedCompleter;
import water.fvec.*;
import water.util.Log.Tag.Sys;
import water.util.Log;

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
  final int _nclass;     // #classes, or 0 for regression rtees
  private Node[] _ns;    // All the nodes in the tree.  Node 0 is the root.
  int _len;              // Resizable array
  DTree( String[] names, int ncols, int nclass ) { _names = names; _ncols = ncols; _nclass=nclass; _ns = new Node[1]; }

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
      final String colPad="  ";
      final int cntW=4, mmmW=4, varW=4;
      final int colW=cntW+1+mmmW+1+mmmW+1+mmmW+1+varW;
      StringBuilder sb = new StringBuilder();
      sb.append("Nid# ").append(_nid).append(", ");
      printLine(sb).append("\n");
      final int ncols = _hs.length;
      for( int j=0; j<ncols; j++ )
        if( _hs[j] != null )
          p(sb,_hs[j]._name+String.format(", err=%5.2f %f",_hs[j].score(),_hs[j]._min),colW).append(colPad);
      sb.append('\n');
      for( int j=0; j<ncols; j++ ) {
        if( _hs[j] == null ) continue;
        p(sb,"cnt" ,cntW).append('/');
        p(sb,"min" ,mmmW).append('/');
        p(sb,"max" ,mmmW).append('/');
        if( _tree._nclass == 0 ) {
          p(sb,"mean",mmmW).append('/');
          p(sb,"var" ,varW).append(colPad);
        } else {
          p(sb,"C0",mmmW).append('-');
          p(sb,"C"+(_tree._nclass-1),varW).append(colPad);
        }
      }
      sb.append('\n');
      for( int i=0; i<DHistogram.BINS; i++ ) {
        for( int j=0; j<ncols; j++ ) {
          if( _hs[j] == null ) continue;
          if( i < _hs[j].nbins() ) {
            p(sb,Long.toString(_hs[j].bins(i)),cntW).append('/');
            p(sb,              _hs[j].mins(i) ,mmmW).append('/');
            p(sb,              _hs[j].maxs(i) ,mmmW).append('/');
            if( _tree._nclass==0 ) {  // Regression
              p(sb,              _hs[j]. mean(i) ,mmmW).append('/');
              p(sb,              _hs[j]. var (i) ,varW).append(colPad);
            } else {            // Classification
              StringBuilder sb2 = new StringBuilder();
              long N = _hs[j].bins(i);
              for( int k = 0; k<_tree._nclass; k++ )
                sb2.append(_hs[j].clss(i,k)).append(',');
              p(sb,sb2.toString(),mmmW+1+varW).append(colPad);
            }
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
    static private StringBuilder p(StringBuilder sb, float d, int w) {
      String s = Double.isNaN(d) ? "NaN" :
        ((d==Float.MAX_VALUE || d==-Float.MAX_VALUE) ? " -" : 
         Double.toString(d));
      if( s.length() <= w ) return p(sb,s,w);
      s = String.format("%4.1f",d);
      if( s.length() > w )
        s = String.format("%4.0f",d);
      return sb.append(s);
    }
  }

  // Internal tree nodes which split into several children over a single
  // column.  Includes a split-decision: which child does this Row belong to?
  // Does not contain a histogram describing how the decision was made.
  static abstract class DecidedNode<UDN extends UndecidedNode> extends Node {
    final int _col;             // Column we split over
    final float _min, _step;    // Binning info of column
    // The following arrays are all based on a bin# extracted from linear
    // interpolation of _col, _min and _step.

    // For classification, we return a zero-based class, and the prediction is
    // a value from zero to 1 describing how weak our guess is.  For instance,
    // during tree-building we decide to build a DecidedNode from 10 rows; 8
    // rows are class A, and 2 rows are class B.  Then our class is A, but our
    // error is 0.2 (2 out of 10 wrong).
    final int   _ns[];            // An n-way split node
    final float _mins[], _maxs[]; // Hang onto for printing purposes
    // For Regressions:
    //  ycls[x][0] == count of rows for this leaf
    //  pred[x] == split X's prediction, typically mean of response variable
    // For Classifications:
    //  ycls[x][] == Class distribution of leaves
    final long  _ycls[/*split*/][/*class*/]; // Class distribution
    final float _pred[/*split*/]; // Regression: this is the prediction

    // Make a correctly flavored Undecided
    abstract UDN makeUndecidedNode(DTree tree, int nid, DHistogram[] nhists );

    // Pick the best column from the given histograms
    abstract int bestCol( UDN udn );

    DecidedNode( UDN n ) {
      super(n._tree,n._pid,n._nid); // Replace Undecided with this DecidedNode
      int col = bestCol(n);         // Best split-point for this tree

      // If I have 2 identical predictor rows leading to 2 different responses,
      // then this dataset cannot distinguish these rows... and we have to bail
      // out here.  Note that the column picked here is strictly to grab the
      // average prediction; we will not split.
      boolean canDecide = true;
      if( col == -1 ) {
        canDecide = false;
        for( int i=0; i<n._hs.length; i++ )
          if( n._hs[i]!=null && n._hs[i].nbins() > 1 )
            { col = i; break; } // Take some random junky column
      }
      _col = col;

      // From the splitting Undecided, get the column, min, max
      DBinHistogram splitH = (DBinHistogram)n._hs[_col];// DHistogram of the column being split
      int nums = splitH.nbins(); // Number of split choices
      assert nums > 1;           // Should always be some bins to split between
      _min  = splitH._bmin;      // Binning info
      _step = splitH._step;
      assert _step > 0;
      _mins = splitH._mins;     // Hang onto for printing purposes
      _maxs = splitH._maxs;     // Hang onto for printing purposes
      _ns = new int[nums];
      int nclass = _tree._nclass; // Number of classes
      _ycls = new long[nums][nclass==0 ? 1 : nclass];
      _pred = new float[nums];
      int ncols = _tree._ncols;     // ncols: all columns, minus response
      for( int i=0; i<nums; i++ ) { // For all split-points
        // Setup for children splits
        DHistogram nhists[] = canDecide ? splitH.split(_col,i,n._hs,_tree._names,ncols) : null;
        assert nhists==null || nhists.length==ncols;
        _ns[i] = nhists == null ? -1 : makeUndecidedNode(_tree,_nid,nhists)._nid;
        // Also setup predictions locally
        if( nclass == 0 )  {                     // Regression?
          _ycls[i] = new long[]{splitH.bins(i)}; // Number of entries in bin
          _pred[i] = splitH. mean(i);            // Prediction is mean of bin
        } else {                                 // Classification?
          for( int c=0; c<nclass; c++ )          // Copy the class counts into the decision
            _ycls[i][c] = splitH.clss(i,c);
        }
      }
    }

    // Bin #.
    public int bin( Chunk chks[], int i ) {
      float d = (float)chks[_col].at0(i); // Value to split on for this row
      // Note that during *scoring* (as opposed to training), we can be exposed
      // to data which is outside the bin limits, so we must cap at both ends.
      int idx1 = (int)((d-_min)/_step);     // Interpolate bin#
      int bin = Math.max(Math.min(idx1,_ns.length-1),0);// Cap at length
      return bin;
    }

    public int ns( Chunk chks[], int i ) { return _ns[bin(chks,i)]; }

    @Override public String toString() {
      String n= " <= "+_tree._names[_col]+" <= ";
      String s = new String();
      for( int i=0; i<_ns.length; i++ ) 
        s += _mins[i]+n+_maxs[i]+" = "+Arrays.toString(_ycls[i])+"\n";
      return s+" "+_min+" "+_step;
    }

    StringBuilder printChild( StringBuilder sb, int nid ) {
      for( int i=0; i<_ns.length; i++ ) 
        if( _ns[i]==nid ) 
          return sb.append("[").append(_mins[i]).append(" <= ").
            append(_tree._names[_col]).append(" <= ").append(_maxs[i]).append("]");
      throw H2O.fail();
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
    final short _nclass;        // Zero for regression, else #classes
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
      assert hs != null : "no histo tree "+tid+" nid="+nid+", pid="+_trees[tid].node(nid)._pid;
      // Having gather min/max/mean/class/etc on all the data, we can now
      // tighten the min & max numbers.
      for( int j=0; j<hs.length; j++ ) {
        DHistogram h = hs[j];    // Old histogram of column
        if( h != null ) h.tightenMinMax();
      }
      return hs;
    }

    @Override public void map( Chunk[] chks ) {
      assert _ncols+1/*response variable*/+_trees.length == chks.length 
        : "Missing columns?  ncols="+_ncols+", 1 for response, ntrees="+_trees.length+", and found "+chks.length+" vecs";
      Chunk ys = chks[_ncols];

      // We need private (local) space to gather the histograms.
      // Make local clones of all the histograms that appear in this chunk.
      _hcs = new DHistogram[_trees.length][][];

      // For all trees
      for( int t=0; t<_trees.length; t++ ) {
        final DTree tree = _trees[t];
        final int leaf = _leafs[t];
        // A leaf-biased array of all active histograms
        final DHistogram hcs[][] = _hcs[t] = new DHistogram[tree._len-leaf][]; 
        final Chunk nids = chks[_ncols+1/*response col*/+t];

        // Pass 1 & 2
        for( int i=0; i<nids._len; i++ ) {
          int nid = (int)nids.at80(i); // Get Node to decide from
          if( nid<0 ) continue; // row already predicts perfectly or sampled away
          
          // Pass 1: Score row against current decisions & assign new split
          int oldnid=nid;
          if( leaf > 0 )        // Prior pass exists?
            nids.set80(i,nid = tree.decided(nid).ns(chks,i));
          
          // Pass 1.9
          if( nid==-1 ) continue;         // row already predicts perfectly
          
          // We need private (local) space to gather the histograms.
          // Make local clones of all the histograms that appear in this chunk.
          DHistogram nhs[] = hcs[nid-leaf];
          if( nhs == null ) {     // Lazily manifest this histogram for 'nid'
            nhs = hcs[nid-leaf] = new DHistogram[_ncols];
            int sCols[] = tree.undecided(nid)._scoreCols;
            DHistogram ohs[] = tree.undecided(nid)._hs; // The existing column of Histograms
            // For just the selected columns make Big Histograms
            for( int j=0; j<sCols.length; j++ ) { // Make private copies
              int idx = sCols[j];                 // Just the selected columns
              nhs[idx] = ohs[idx].bigCopy();
            }
            // For all the rest make small Histograms
            for( int j=0; j<nhs.length; j++ )
              if( ohs[j] != null && nhs[j]==null )
                nhs[j] = ohs[j].smallCopy();
          }
        }
          
        // Pass 2
        if( _nclass == 0 ) countRegression(tree,nids,ys,chks,hcs,leaf);
        else               countClasses   (tree,nids,ys,chks,hcs,leaf);
      }
    }

    // Pass 2: Count Regression for rows & columns
    private void countRegression( DTree tree, Chunk nids, Chunk ys, Chunk chks[], DHistogram hcs[][], int leaf ) {
      for( int i=0; i<nids._len; i++ ) { // Bump the local histogram counts
        int nid = (int)nids.at80(i);     // Get Node to decide from
        if( nid<0 ) continue; // row already predicts perfectly or sampled away
        DHistogram nhs[] = hcs[nid-leaf];
        float y = (float)ys.at0(i);
        for( int j=0; j<_ncols; j++) { // For all columns
          DHistogram nh = nhs[j];
          if( nh != null ) {
            float f = (float)chks[j].at0(i);
            nh.incr(f);
            if( nh instanceof DBinHistogram ) ((DBinHistogram)nh).incr(f,y);
          }
        }
      }
    }

    // Pass 2: Count Classes for rows & columns
    private void countClasses( DTree tree, Chunk nids, Chunk ys, Chunk chks[], DHistogram hcs[][], int leaf ) {
      for( int i=0; i<nids._len; i++ ) { // Bump the local histogram counts
        int nid = (int)nids.at80(i);     // Get Node to decide from
        if( nid<0 ) continue; // row already predicts perfectly or sampled away
        DHistogram nhs[] = hcs[nid-leaf];
        int ycls = (int)ys.at80(i) - _ymin;

        int sCols[] = tree.undecided(nid)._scoreCols;
        for( int idx=0; idx<sCols.length; idx++) { // For all columns
          int j = sCols[idx];                      // Just the selected columns
          DHistogram nh = nhs[j];
          float f = (float)chks[j].at0(i);
          nh.incr(f);
          ((DBinHistogram)nh).incr(f,ycls);
        }
        //for( int j=0; j<_ncols; j++) { // For all columns
        //  DHistogram nh = nhs[j];
        //  if( nh != null ) {
        //    float f = (float)chks[j].at0(i);
        //    nh.incr(f);
        //    if( nh instanceof DBinHistogram ) ((DBinHistogram)nh).incr(f,ycls);
        //  }
        //}
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
    final int _ncols;     // Number of columns actively being worked
    final int _nclass;    // Zero for regression, else #classes
    // Bias classes to zero; e.g. covtype classes range from 1-7 so this is 1.
    // e.g. prostate classes range 0-1 so this is 0
    final int _ymin;
    // Out-Of-Bag-Error-Estimate.  This is fairly specific to Random Forest,
    // and involves scoring each tree only on rows for which is was not
    // trained, which only makes sense when scoring the Forest while the
    // training data is handy, i.e., scoring during & after training.
    // Pass in a 1.0 if turned off.
    final float _rate;
    // OUTPUT fields
    long _cm[/*actual*/][/*predicted*/]; // Confusion matrix
    double _sum;                // Sum-squared-error
    long _err;                  // Total absolute errors

    BulkScore( DTree trees[], int ncols, int nclass, int ymin, float sampleRate ) { 
      _trees = trees; _ncols = ncols; 
      _nclass = nclass; _ymin = ymin; 
      _rate = sampleRate;
    }

    // Init all the internal tree fields after shipping over the wire
    @Override public void init( ) {
      for( DTree dt : _trees )
        for( int j=0; j<dt._len; j++ )
          dt._ns[j]._tree = dt;
    }

    @Override public void map( Chunk chks[] ) {
      Chunk ys = chks[_ncols];
      _cm = new long[_nclass][_nclass];

      // Get an array of RNGs to replay the sampling in reverse, only for OOBEE.
      // Note the fairly expense MerseenTwisterRNG built per-tree (per-chunk).
      Random rands[] = null;
      if( _rate < 1.0f ) {      // oobee vs full scoring?
        rands = new Random[_trees.length];
        for( int t=0; t<_trees.length; t++ )
          rands[t] = _trees[t].rngForChunk(ys.cidx());
      }

      // Score all Rows
      long clss[] = new long[_nclass]; // Shared temp array for computing classes
      for( int i=0; i<ys._len; i++ ) {
        float err = score0( chks, i, (float)ys.at0(i), clss, rands );
        _sum += err*err;        // Squared error
      }
    }

    @Override public void reduce( BulkScore t ) { 
      _sum += t._sum; 
      _err += t._err; 
      for( int i=0; i<_nclass; i++ )
        for( int j=0; j<_nclass; j++ )
          _cm[i][j] += t._cm[i][j];
    }

    // Return a relative error.  For regression it's y-mean.  For classification, 
    // it's the %-tage of the response class out of all rows in the leaf, plus
    // a count of absolute errors when we predict the majority class.
    private float score0( Chunk chks[], int i, float y, long clss[], Random rands[] ) {
      float sum=0;             // Regression: average across trees
      Arrays.fill(clss,0);      // Recycled temp array

      // For all trees
      for( int t=0; t<_trees.length; t++ ) {
        // For OOBEE error, do not score rows on trees trained on that row
        if( rands != null && !(rands[t].nextFloat() >= _rate) ) continue;

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
        // We hit the end of the tree walk.  Get this tree's prediction
        if( _nclass == 0 ) {    // Regression?
          sum += regressScore(prev,clss,chks,i,true,sum);
        } else {                // Classification?
          classScore(prev,clss,chks,i,true);
        }
      } // End of for-all trees

      // Having computed the votes across all trees, find the majority class
      // and it's error rate.
      if( _nclass == 0 ) {
        long rows = clss[0];         // Find total rows trained
        if( clss[0] == 0 ) return 0; // OOBEE: all rows trained, so no rows scored
        float prediction = sum/rows; // Average of trees is prediction
        return prediction - y;        // Error
      } else {
        long rows = clss[0];    // Find total rows trained
        int best=0;             // Find largest class across all trees
        for( int c=1; c<_nclass; c++ ) {
          rows += clss[c];      // Total rows trained
          if( clss[c] > clss[best] ) best=c;
        }
        if( rows == 0 ) return 0;  // OOBEE: all rows trained, so no rows scored
        int ycls = (int)y-_ymin;   // Zero-based response class
        if( best != ycls ) _err++; // Absolute prediction error
        _cm[ycls][best]++;         // Confusion Matrix
        return (float)(rows-clss[ycls])/rows; // Error
      }
    }

    // Compute classification score: add this leaf decision node's response-
    // variable distribution to the total forest distribution.  If the selected
    // bin is empty - we've got no distribution.  Go back up 1 layer and take
    // the (weaker) distribution from the parent.
    private void classScore( DecidedNode prev, long clss[], Chunk chks[], int i, boolean recur ) {
      int bin = prev.bin(chks,i);    // Which bin did we decide on?
      long[] ycls = prev._ycls[bin]; // Classes for that bin
      long bits=0;
      for( int c=0; c<_nclass; c++ ) {
        clss[c] += ycls[c];     // Compute distribution
        bits |= ycls[c];        // Detect empty distribution
      }
      if( bits == 0 && prev._pid != -1 ) { // No class prediction here, and we can back up a layer?
        assert recur;
        prev = prev._tree.decided(prev._pid); // Backup 1 layer in tree
        classScore(prev,clss,chks,i,false);   // And predict again (weaker)
      }
    }

    private float regressScore( DecidedNode prev, long clss[], Chunk chks[], int i, boolean recur, float sum ) {
      int bin = prev.bin(chks,i);    // Which bin did we decide on?
      long[] ycls = prev._ycls[bin]; // Classes for that bin
      long num = ycls[0];            // "classes" is really just bin-count
      clss[0] += num;                // More total rows
      sum += prev._pred[bin]*num;   // More total regression count
      if( num==0 ) {
        prev = prev._tree.decided(prev._pid); // Backup 1 layer in tree
        return regressScore(prev,clss,chks,i,false,sum); // And predict again (weaker)
      }
      return sum;
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
}
