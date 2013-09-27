package hex.gbm;

import hex.rf.Tree.TreeVisitor;
import java.util.*;
import water.*;
import water.api.DocGen;
import water.api.Request.API;
import water.fvec.*;
import water.util.*;
import water.util.Log.Tag.Sys;

/**
   A Decision Tree, laid over a Frame of Vecs, and built distributed.

   This class defines an explicit Tree structure, as a collection of {@code
   DTree} {@code Node}s.  The Nodes are numbered with a unique {@code _nid}.
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
  final char _nbins;     // Max number of bins to split over
  final char _nclass;    // #classes, or 1 for regression trees
  final int _min_rows;   // Fewest allowed rows in any split
  private Node[] _ns;    // All the nodes in the tree.  Node 0 is the root.
  int _len;              // Resizable array
  DTree( String[] names, int ncols, char nbins, char nclass, int min_rows ) {
    _names = names; _ncols = ncols; _nbins=nbins; _nclass=nclass; _min_rows = min_rows; _ns = new Node[1]; }

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


  // --------------------------------------------------------------------------
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
    Node( DTree tree, int pid ) {
      _tree = tree;
      _pid=pid;
      _nid=tree.newIdx();
      tree._ns[_nid] = this;
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

  // --------------------------------------------------------------------------
  // Records a column, a bin to split at within the column, and the MSE.
  static class Split extends Iced {
    final int _col, _bin;       // Column to split, bin where being split
    final boolean _equal;       // Split is < or == ?
    final double _se;           // Squared error whole split
    final long _n0, _n1;        // Rows in each final split

    Split( int col, int bin, boolean equal, double se, long n0, long n1 ) {
      _col = col;  _bin = bin;  _equal = equal;
      _n0 = n0;  _n1 = n1;  _se = se;
    }

    // Split-at dividing point
    float splat(DHistogram hs[]) {  return ((DBinHistogram)hs[_col]).binAt(_bin); }

    // Split a DBinHistogram.  Return null if there is no point in splitting
    // this bin further (such as there's fewer than min_row elements, or zero
    // errpr in the response column).  Return an array of DBinHistograms (one
    // per column), which are bounded by the split bin-limits.  If the column
    // has constant data, or was not being tracked by a prior DBinHistogram
    // (for being constant data from a prior split), then that column will be
    // null in the returned array.
    public DBinHistogram[] split( int splat, char nbins, int min_rows, DHistogram hs[] ) {
      long n = splat==0 ? _n0 : _n1;
      if( n < min_rows || n <= 1 ) return null; // Too few elements
      if( _se <= 1e-30 ) return null; // No point in splitting a perfect prediction

      // Build a next-gen split point from the splitting bin
      int cnt=0;                  // Count of possible splits
      DBinHistogram nhists[] = new DBinHistogram[hs.length]; // A new histogram set
      for( int j=0; j<hs.length; j++ ) { // For every column in the new split
        DHistogram h = hs[j];            // old histogram of column
        if( h == null ) continue;        // Column was not being tracked?
        // min & max come from the original column data, since splitting on an
        // unrelated column will not change the j'th columns min/max.
        float min = h._min, max = h._max;
        // Tighter bounds on the column getting split: exactly each new
        // DBinHistogram's bound are the bins' min & max.
        if( _col==j ) {
          if( _equal ) {        // Equality split; no change on unequals-side
            if( splat == 1 ) max=min = h.mins(_bin); // but know exact bounds on equals-side
          } else {              // Less-than split
            if( splat == 0 ) max = h.maxs(_bin-1); // Max from next-smallest bin
            else             min = h.mins(_bin  ); // Min from this bin
          }
        }
        if( min == max ) continue; // This column will not split again
        if( min >  max ) continue; // Happens for all-NA subsplits
        nhists[j] = new DBinHistogram(h._name,nbins,h._isInt,min,max,n);
        cnt++;                    // At least some chance of splitting
      }
      return cnt == 0 ? null : nhists;
    }

    public static StringBuilder ary2str( StringBuilder sb, int w, long xs[] ) {
      sb.append('[');
      for( long x : xs ) UndecidedNode.p(sb,x,w).append(",");
      return sb.append(']');
    }
    public static StringBuilder ary2str( StringBuilder sb, int w, float xs[] ) {
      sb.append('[');
      for( float x : xs ) UndecidedNode.p(sb,x,w).append(",");
      return sb.append(']');
    }
    public static StringBuilder ary2str( StringBuilder sb, int w, double xs[] ) {
      sb.append('[');
      for( double x : xs ) UndecidedNode.p(sb,(float)x,w).append(",");
      return sb.append(']');
    }
    @Override public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("{"+_col+"/");
      UndecidedNode.p(sb,_bin,2);
      sb.append(", se=").append(_se);
      sb.append(", n0=").append(_n0);
      sb.append(", n1=").append(_n1);
      return sb.append("}").toString();
    }
  }

  // --------------------------------------------------------------------------
  // An UndecidedNode: Has a DHistogram which is filled in (in parallel with other
  // histograms) in a single pass over the data.  Does not contain any
  // split-decision.
  static abstract class UndecidedNode extends Node {
    DHistogram _hs[];      // DHistograms per column
    int _scoreCols[];      // A list of columns to score; could be null for all
    UndecidedNode( DTree tree, int pid, DBinHistogram hs[] ) {
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
        p(sb,"mean",menW).append('/');
        p(sb,"var" ,varW).append(colPad);
      }
      sb.append('\n');

      // Max bins
      int nbins=0;
      for( int j=0; j<ncols; j++ )
        if( _hs[j] != null && _hs[j].nbins() > nbins ) nbins = _hs[j].nbins();

      for( int i=0; i<nbins; i++ ) {
        for( int j=0; j<ncols; j++ ) {
          DHistogram h = _hs[j];
          if( h == null ) continue;
          if( i < h.nbins() ) {
            p(sb, h.bins(i),cntW).append('/');
            p(sb, h.mins(i),mmmW).append('/');
            p(sb, h.maxs(i),mmmW).append('/');
            p(sb, h.mean(i),menW).append('/');
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
    static private StringBuilder p(StringBuilder sb, double d, int w) {
      String s = Double.isNaN(d) ? "NaN" :
        ((d==Float.MAX_VALUE || d==-Float.MAX_VALUE || d==Double.MAX_VALUE || d==-Double.MAX_VALUE) ? " -" :
         Double.toString(d));
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

  // --------------------------------------------------------------------------
  // Internal tree nodes which split into several children over a single
  // column.  Includes a split-decision: which child does this Row belong to?
  // Does not contain a histogram describing how the decision was made.
  static abstract class DecidedNode<UDN extends UndecidedNode> extends Node {
    final Split _split;         // Split: col, equal/notequal/less/greater, nrows, MSE
    final float _splat;         // Split At point: lower bin-edge of split
    // _equals\_nids[] \   0   1
    // ----------------+----------
    //       F         |   <   >=
    //       T         |  !=   ==
    final int _nids[];          // Children NIDS for the split

    transient byte _nodeType; // Complex encoding: see the compressed struct comments
    transient int _size = 0;  // Compressed byte size of this subtree

    // Make a correctly flavored Undecided
    abstract UDN makeUndecidedNode(DTree tree, int nid, DBinHistogram[] nhists );

    // Pick the best column from the given histograms
    abstract Split bestCol( UDN udn );

    DecidedNode( UDN n ) {
      super(n._tree,n._pid,n._nid); // Replace Undecided with this DecidedNode
      _split = bestCol(n);          // Best split-point for this tree

      // If I have 2 identical predictor rows leading to 2 different responses,
      // then this dataset cannot distinguish these rows... and we have to bail
      // out here.
      if( _split._col == -1 || _split._bin == 0/*bin 0 is NO SPLIT*/ ) {
        throw H2O.unimpl();
        //DecidedNode p = n._tree.decided(_pid);
        //_col  = p._col;  // Just copy the parent data over, for the predictions
        //_equal = p._equal;
        //_splat = p._splat;
        //_nids = new int[p._nids.length];
        //Arrays.fill(_nids,-1);  // No further splits
        //_pred = p._pred;
        //return;
      }
      _splat = _split.splat(n._hs); // Split-at value
      _nids = new int[2];        // Split into 2 subsets
      final char nclass  = _tree._nclass;
      final char nbins   = _tree._nbins;
      final int min_rows = _tree._min_rows;

      for( int b=0; b<2; b++ ) { // For all split-points
        // Setup for children splits
        DBinHistogram nhists[] = _split.split(b,nbins,min_rows,n._hs);
        assert nhists==null || nhists.length==_tree._ncols;
        _nids[b] = nhists == null ? -1 : makeUndecidedNode(_tree,_nid,nhists)._nid;
      }
    }

    // Bin #.
    public int bin( Chunk chks[], int row ) {
      assert _nids.length == 2 : Arrays.toString(_nids)+", pid="+_pid+" and "+this;
      if( chks[_split._col].isNA0(row) ) // Missing data?
        return row%_nids.length;         // pseudo-random bin select
      float d = (float)chks[_split._col].at0(row); // Value to split on for this row
      // Note that during *scoring* (as opposed to training), we can be exposed
      // to data which is outside the bin limits.
      return _split._equal ? (d != _splat ? 0 : 1) : (d < _splat ? 0 : 1);
    }

    public int ns( Chunk chks[], int i ) { return _nids[bin(chks,i)]; }

    @Override public String toString() {
      if( _split._col == -1 ) return "Decided has col = -1";
      int col = _split._col;
      if( _split._equal )
        return
          _tree._names[col]+" != "+_splat+"\n"+
          _tree._names[col]+" == "+_splat+"\n";
      return
        _tree._names[col]+" < "+_splat+"\n"+
        _splat+" <="+_tree._names[col]+"\n";
    }

    StringBuilder printChild( StringBuilder sb, int nid ) {
      int i = _nids[0]==nid ? 0 : 1;
      assert _nids[i]==nid : "No child nid "+nid+"? " +Arrays.toString(_nids);
      sb.append("[").append(_tree._names[_split._col]);
      sb.append(_split._equal
                ? (i==0 ? " != " : " == ")
                : (i==0 ? " <  " : " >= "));
      sb.append(_splat).append("]");
      return sb;
    }

    @Override public StringBuilder toString2(StringBuilder sb, int depth) {
      for( int i=0; i<_nids.length; i++ ) {
        for( int d=0; d<depth; d++ ) sb.append("  ");
        if( _split._col < 0 ) sb.append("init");
        else {
          sb.append(_tree._names[_split._col]);
          sb.append(_split._equal
                    ? (i==0 ? " != " : " == ")
                    : (i==0 ? " <  " : " >= "));
          sb.append(_splat).append("\n");
        }
        if( _nids[i] >= 0 && _nids[i] < _tree._len ) 
          _tree.node(_nids[i]).toString2(sb,depth+1);
      }
      return sb;
    }

    // Return a DecidedNode child or null.  Can be null if there is no tree node
    // or because there is an UndecidedNode (happens at max-tree-depth).
    private DecidedNode getSubTree(int i){
      Node n;
      return (_nids[i] != -1) && ((n = _tree.node(_nids[i])) instanceof DecidedNode)
          ? (DecidedNode)n : null;
    }

    // Size of child-i (must be a leaf).
    // Also sets _nodeType.
    private int leafSz( int i ) {
      throw H2O.unimpl();
      //assert getSubTree(i)==null; // leaf
      //if( singleClass(i) ) {
      //  _nodeType |= (byte)( 8 << i*2);
      //  return (_tree._nclass < 256)?1:2;
      //} else {
      //  _nodeType |= (byte)(24 << i*2);
      //  int sz = 4*(_tree._nclass);
      //  if(sz > 255)_nodeType |= (sz < 65535)?2:3;
      //  return sz;
      //}
    }

    // Size of this subtree
    public final int size(){
      if( _size != 0 ) return _size; // Cached size

      assert _nodeType == 0:"unexpected node type: " + _nodeType;
      if( _split._equal ) _nodeType |= (byte)4;

      int res = 7; // 1B node type + flags, 2B colId, 4B float split val
      throw H2O.unimpl();
      //// left child
      //DecidedNode child;
      //if((child = getSubTree(0)) != null) {
      //  int lsz = child.size();
      //  int slen = lsz < 256 ? 1 : (lsz < 65535 ? 2 : 3);
      //  _nodeType |= slen;      // Set the size-skip bits
      //  res += lsz + slen;
      //} else
      //  res += leafSz(0);
      //// right child
      //if(_nids.length > 1)
      //  res += ((child = getSubTree(1)) != null) ? child.size() : leafSz(1);
      //assert res != 0;
      //return (_size = res);
    }

    // Insert just the predictions: a single byte/short if we are predicting a
    // single class, or else the full distribution.
    private AutoBuffer compressLeaf(AutoBuffer ab, int i) {
      throw H2O.unimpl();
      //// just put the predictions in
      //if( singleClass(i) ) {
      //  int c = Utils.maxIndex(_pred[i]); // The One non-zero Class
      //  if(_tree._nclass < 256) ab.put1(       c);
      //  else                    ab.put2((short)c);
      //} else ab.put8d(f);
      //return ab;
    }

    // Compress this tree into the AutoBuffer
    public AutoBuffer compress(AutoBuffer ab) {
      int pos = ab.position();
      if( _nodeType == 0 ) size(); // Sets _nodeType & _size both
      ab.put1(_nodeType);          // Includes left-child skip-size bits
      ab.put2((short)_split._col);
      ab.put4f(_splat);
      int last = _nids.length-1;
      assert last <= 1; // can not handle more than 2 nodes
      DecidedNode child;
      for( int i = 0; i < last; ++i ) {
        if((child = getSubTree(i)) != null) { // we have left subtree, set the skip sz
          int sz = child.size();
          if(sz < 256)         ab.put1(       sz);
          else if (sz < 65535) ab.put2((short)sz);
          else                 ab.put3(       sz);
          // now write the subtree in
          child.compress(ab);
        } else compressLeaf(ab, i);
      }
      // last node, no need for skip # bytes info
      if((child = getSubTree(last)) != null)
        child.compress(ab);
      else compressLeaf(ab, last);
      assert _size == ab.position()-pos:"reported size = " + _size + " , real size = " + (ab.position()-pos);
      return ab;
    }
  }

  // Convenvience accessor for a complex chunk layout.
  // Wish I could name the array elements nicer...
  private static Chunk[] chk_check( Chunk chks[], int ncols, char nclass ) {
    assert chks.length == ncols+1/*response*/+nclass/*prob dist so far*/+nclass/*tmp*/+nclass/*NIDs, one tree per class*/;
    return chks;
  }
  static Chunk chk_resp( Chunk chks[], int ncols, char nclass        ) { return chk_check(chks,ncols,nclass)[ncols]; }
  static Chunk chk_tree( Chunk chks[], int ncols, char nclass, int c ) { return chk_check(chks,ncols,nclass)[ncols+1+c]; }
  static Chunk chk_work( Chunk chks[], int ncols, char nclass, int c ) { return chk_check(chks,ncols,nclass)[ncols+1+nclass+c]; }
  static Chunk chk_nids( Chunk chks[], int ncols, char nclass, int t ) { return chk_check(chks,ncols,nclass)[ncols+1+nclass+nclass+t]; }

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
    final DTree _trees[]; // Read-only, shared (except at the histograms in the Nodes)
    final int   _leafs[]; // Number of active leaves (per tree)
    final int _ncols;
    final char _nclass;         // One for regression, else #classes
    // Bias classes to zero; e.g. covtype classes range from 1-7 so this is 1.
    // e.g. prostate classes range 0-1 so this is 0
    final int _ymin;
    // Histograms for every tree, split & active column
    DHistogram _hcs[/*tree/klass*/][/*tree-relative node-id*/][/*column*/];
    ScoreBuildHistogram(DTree trees[], int leafs[], int ncols, char nclass, int ymin) {
      assert trees.length==nclass; // One tree per-class
      assert leafs.length==nclass; // One count of leaves per-class
      _trees=trees;
      _leafs=leafs;
      _ncols=ncols;
      _nclass = nclass;
      _ymin = ymin;
    }

    // Init all the internal tree fields after shipping over the wire
    @Override public void setupLocal( ) {
      for( DTree dt : _trees )
        for( int j=0; j<dt._len; j++ )
          dt._ns[j]._tree = dt;
    }

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
        final int leaf   = _leafs[k];
        // A leaf-biased array of all active histograms
        final DHistogram hcs[][] = _hcs[k] = new DHistogram[tree._len-leaf][];
        final Chunk nids = chk_nids(chks,_ncols,_nclass,k);
        final Chunk wrks = chk_work(chks,_ncols,_nclass,k); // We are predicting the residuals

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

          double y = wrks.at(row);       // Response for this row
          for( int j=0; j<_ncols; j++) { // For all columns
            DHistogram nh = nhs[j];
            if( nh == null ) continue; // Not tracking this column?
            float col_data = (float)chks[j].at0(row);
            nh.incr(col_data);                // Small histogram
            if( nh instanceof DBinHistogram ) // Big histogram
              ((DBinHistogram)nh).incr(row,col_data,y);
          }
        }
      }
    }

    @Override public void reduce( ScoreBuildHistogram sbh ) {
      // Merge histograms
      assert _hcs.length==_nclass; // One tree per class
      for( int k=0; k<_nclass; k++ ) {
        DHistogram hcs[/*leaf#*/][/*col*/] = _hcs[k];
        for( int i=0; i<hcs.length; i++ ) {
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
  // Compute mean-squared-error.
  public static class BulkScore extends MRTask2<BulkScore> {
    final DTree _trees[]; // Read-only, shared (except at the histograms in the Nodes)
    final int _ncols;     // Number of predictor columns
    final char _nclass;   // Number of classes
    // Bias classes to zero; e.g. covtype classes range from 1-7 so this is 1.
    // e.g. prostate classes range 0-1 so this is 0
    final int _ymin;
    // Out-Of-Bag-Error-Estimate.  This is fairly specific to Random Forest,
    // and involves scoring each tree only on rows for which is was not
    // trained, which only makes sense when scoring the Forest while the
    // training data is handy, i.e., scoring during & after training.
    // Pass in a 1.0 if turned off.
    final float _sampleRate;
    // OUTPUT fields
    long _cm[/*actual*/][/*predicted*/]; // Confusion matrix
    double _sum;                // Sum-squared-error
    long _err, _nrows;          // Total absolute errors, actual rows trained

    BulkScore( DTree trees[][], int doneTrees, int ncols, char nclass, int ymin, float sampleRate ) {
      _trees = trees[trees.length-1]; _ncols = ncols;
      _nclass = nclass; _ymin = ymin;
      _sampleRate = sampleRate;
    }

    // Init all the internal tree fields after shipping over the wire
    @Override public void setupLocal( ) {
      for( DTree dt : _trees )
        for( int j=0; j<dt._len; j++ )
          dt._ns[j]._tree = dt;
    }

    @Override public void map( Chunk chks[] ) {
      Chunk ys = chk_resp(chks,_ncols,_nclass); // Response
      // Prediction vector @ chks.length-_nclass to chks.length-1
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
      for( int row=0; row<ys._len; row++ ) {
        double err = score0( chks, row, (float)(ys.at0(row)-_ymin), rands );
        if( !Double.isNaN(err) ) { // Skip rows trained in *all* trees for OOBEE
          _sum += err*err;        // Squared error
          _nrows++;
        }
      }
    }

    @Override public void reduce( BulkScore t ) {
      _sum += t._sum;
      _err += t._err;
      _nrows += t._nrows;
      Utils.add(_cm,t._cm);
    }

    // Return a relative error: response-prediction.  The prediction is a
    // vector; typically a class distribution.  If the response is also a
    // vector we return the Euclidean distance.  If the response is a single
    // class variable we instead return the squared-error of the prediction for
    // the class.  We also count absolute errors when we predict the majority class.
    private double score0( Chunk chks[], int row, float y, Random rands[] ) {
      if( Float.isNaN(y) ) return Double.NaN; // Ignore missing response vars
      int ycls = (int)y;        // Response class from 0 to nclass-1
      assert 0 <= ycls && ycls < _nclass : "weird ycls="+ycls+", y="+y+", ymin="+_ymin;
      int best= 0;              // Find largest class
      double best_score = Double.MIN_VALUE;
      double score = Double.NaN;

      // For remaining trees
      for( int t=0; t<_trees.length; t++ ) {
        // For OOBEE error, do not score rows on trees trained on that row
        if( rands != null && !(rands[t].nextFloat() >= _sampleRate) ) continue;
        final DTree tree = _trees[t];
        // "score" this row on this tree.  Apply the tree decisions at each
        // point, walking down the tree to a leaf.
        DecidedNode prev = null;
        Node node = tree.root();
        while( node instanceof DecidedNode ) { // While tree-walking
          prev = (DecidedNode)node;
          int nid = prev.ns(chks,row); // Get node's split decision
          if( nid == -1 ) break;       // Decision is: take prediction Here
          node = tree.node(nid);       // Else work down 1 tree level
        }

        // We hit the end of the tree walk.  Get this tree's prediction.
        int bin = prev.bin(chks,row);    // Which bin did we decide on?
        throw H2O.unimpl();
        //double pred = prev._split._preds[bin]; // Prediction
        //for( int c=0; c<_nclass; c++ ) { // Add into the 
        //  Chunk C = chk_pred(chks,_ncols,_nclass,c);
        //  float f = (float)(C.at0(row)+preds[c]);
        //  C.set0(row,f);        // Set incremented score back in
        //  if( f > best_score ) { best = c; best_score = f; }
        //  if( c == ycls ) score = f; // Also capture prediction for correct class
        //}
      } // End of for-all trees

      // No trees means predict avg class
      if( _trees.length==0 ) { best=0; best_score=score=1.0/_nclass; }

      // Having computed the votes across all trees, find the majority class
      // and it's error rate.
      if( Double.isNaN(score) ) return Double.NaN; // OOBEE: all rows trained, so no rows scored

      // Regression?
      if( _nclass == 1 )        // Single-class ==> Regression?
        throw H2O.unimpl(); // set directly into 
      //return y-pred[0];       // Prediction: sum of trees

      // Classification?
      if( best != ycls ) _err++; // Absolute prediction error; off-diagonal sum
      _cm[ycls][best]++;         // Confusion Matrix

      //for( int x=0; x<_ncols; x++ )
      //  System.out.print(String.format("%5.2f,",chks[x].at(i)));
      //System.out.print(" | ");
      //for( int x=_ncols; x<chks.length; x++ )
      //  System.out.print(String.format("%5.2f,",chks[x].at(i)));
      //System.out.println(" pred="+pred[ycls]+","+Arrays.toString(pred)+(best==ycls?"":", ERROR"));

      return 1.0 - Math.min(score,1.0); // Error from 0 to 1.0
    }

    public BulkScore report( Sys tag, int depth ) {
      int lcnt=0;
      for( int t=0; t<_trees.length; t++ ) lcnt += _trees[t]._len;
      Log.info(tag,"============================================================== ");
      Log.info(tag,"Average squared prediction error for tree of depth "+depth+" is "+(_sum/_nrows));
      Log.info(tag,"Total of "+_err+" errors on "+_nrows+" rows, with "+_trees.length+" trees (average of "+((float)lcnt/_trees.length)+" nodes)");
      return this;
    }

    //private String crashReport( int row, float sum, float[] pred, int[] nids ) {
    //  String s = "Expect predictions to be a probability distribution but found "+Arrays.toString(pred)+"="+sum+", scoring row "+row+"\n";
    //  for( int t=0; t<nids.length; t++ ) {
    //    if( nids[t]== -1 ) s += "Skipping tree "+t+"\n";
    //    else {
    //      DecidedNode dn = _trees[t].decided(nids[t]);
    //      s += "Tree "+t+" = "+dn + "\n";
    //    }
    //  }
    //  return s;
    //}
  }

  // --------------------------------------------------------------------------
  public static abstract class TreeModel extends Model {
    static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
    static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.
    @API(help="Expected max trees")                public final int N;
    @API(help="MSE rate as trees are added")       public final double [] errs;
    @API(help="Min class - to zero-bias the CM")   public final int ymin;
    @API(help="Actual trees built (probably < N)") public final CompressedTree [/*N*/][/*nclass*/] treeBits;

    // For classification models, we'll do a Confusion Matrix right in the
    // model (for now - really should be seperate).
    @API(help="Confusion Matrix computed on training dataset, cm[actual][predicted]") public final long cm[][];

    public TreeModel(Key key, Key dataKey, Frame fr, int ntrees, DTree[][] forest, double [] errs, int ymin, long [][] cm) {
      super(key,dataKey,fr);
      this.N = ntrees; this.errs = errs; this.ymin = ymin; this.cm = cm;
      assert forest.length==0 || forest[0].length == nclasses()-ymin;
      treeBits = new CompressedTree[forest.length][];
      for( int i=0; i<forest.length; i++ ) {
        CompressedTree ts[] = treeBits[i] = new CompressedTree[forest[0].length];
        for( int c=0; c<ts.length; c++ )
          treeBits[i][c] = forest[i][c].compress();
      }
    }

    // Number of trees actually in the model (instead of expected/planned)
    public int numTrees() { return treeBits.length; }

    @Override protected float[] score0(double data[], float preds[]) {
      Arrays.fill(preds,1.0f/nclasses());
      throw H2O.unimpl();
      //for( CompressedTree t : treeBits )
      //  t.addScore(preds, data);
      //return preds;
    }

    public void generateHTML(String title, StringBuilder sb) {
      DocGen.HTML.title(sb,title);
      DocGen.HTML.paragraph(sb,"Model Key: "+_selfKey);
      DocGen.HTML.paragraph(sb,water.api.GeneratePredictions2.link(_selfKey,"Predict!"));
      String[] domain = _domains[_domains.length-1]; // Domain of response col

      // Top row of CM
      if( cm != null ) {
        assert ymin+cm.length==domain.length;
        DocGen.HTML.section(sb,"Confusion Matrix");
        DocGen.HTML.arrayHead(sb);
        sb.append("<tr class='warning'>");
        sb.append("<th>Actual / Predicted</th>"); // Row header
        for( int i=0; i<cm.length; i++ )
          sb.append("<th>").append(domain[i+ymin]).append("</th>");
        sb.append("<th>Error</th>");
        sb.append("</tr>");

        // Main CM Body
        long tsum=0, terr=0;                   // Total observations & errors
        for( int i=0; i<cm.length; i++ ) { // Actual loop
          sb.append("<tr>");
          sb.append("<th>").append(domain[i+ymin]).append("</th>");// Row header
          long sum=0, err=0;                     // Per-class observations & errors
          for( int j=0; j<cm[i].length; j++ ) { // Predicted loop
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
        for( int j=0; j<cm.length; j++ ) { // Predicted loop
          long sum=0;
          for( int i=0; i<cm.length; i++ ) sum += cm[i][j];
          sb.append("<td>").append(sum).append("</td>");
        }
        sb.append(String.format("<th>%5.3f = %d / %d</th>", (double)terr/tsum, terr, tsum));
        sb.append("</tr>");
        DocGen.HTML.arrayTail(sb);
      }

      if( errs != null ) {
        DocGen.HTML.section(sb,"Mean Squared Error by Tree");
        DocGen.HTML.arrayHead(sb);
        sb.append("<tr><th>Trees</th>");
        for( int i=0; i<errs.length; i++ )
          sb.append("<td>").append(i).append("</td>");
        sb.append("</tr>");
        sb.append("<tr><th class='warning'>MSE</th>");
        for( int i=0; i<errs.length; i++ )
          sb.append(String.format("<td>%5.3f</td>",errs[i]));
        sb.append("</tr>");
        DocGen.HTML.arrayTail(sb);
      }
    }

    // --------------------------------------------------------------------------
    // Highly compressed tree encoding:
    //    tree: 1B nodeType, 2B colId, 4B splitVal, left-tree-size, left, right
    //    nodeType: (from lsb): 
    //        2 bits ( 1,2) skip-tree-size-size, 
    //        1 bit  ( 4) operator flag (0 -> <, 1 -> == ), 
    //        1 bit  ( 8) left leaf flag, 
    //        1 bit  (16) left leaf type flag, 
    //        1 bit  (32) right leaf flag, 
    //        1 bit  (64) right leaf type flag
    //    left, right: tree | prediction
    //    prediction: 1 or 2 bytes (small leaf) or array of floats with len=nclass
    public static class CompressedTree extends Iced {
      final byte [] _bits;
      final int _nclass;
      public CompressedTree( byte [] bits, int nclass ) { _bits = bits; _nclass = nclass; }
      public float[] addScore( final float preds[], final double row[] ) {
        // Predictions are stored biased by the minimum class, but the scoring
        // logic assumes the full class size.  Bias results.
        int ymin = preds.length - _nclass;
        AutoBuffer ab = new AutoBuffer(_bits);
        while(true) {
          int nodeType = ab.get1();
          int colId = ab.get2();
          float splitVal = ab.get4f();
          if( colId == 65535 ) return scoreLeaf(ab, preds, ymin, (nodeType&16)==16);

          boolean equal = ((nodeType&4)==4);
          // Compute the amount to skip.
          int lmask =  nodeType & 0x1B;
          int rmask = (nodeType & 0x60) >> 2;
          int skip = 0;
          switch(lmask) {
          case 1:  skip = ab.get1();  break;
          case 2:  skip = ab.get2();  break;
          case 3:  skip = ab.get3();  break;
          case 8:  skip = _nclass < 256?1:2;  break; // Small leaf
          case 24: skip = _nclass*4;  break; // skip the p-distribution
          default: assert false:"illegal lmask value " + lmask;
          }

          if( ( equal && ((float)row[colId]) == splitVal) ||
              (!equal && ((float)row[colId]) >= splitVal) ) {
            ab.position(ab.position()+skip); // Skip right subtree
            lmask = rmask;                   // And set the leaf bits into common place
          }
          if( (lmask&8)==8 ) return scoreLeaf(ab,preds,ymin, (lmask&16)==16);
        }
      }
      
      private float[] scoreLeaf(AutoBuffer ab, float preds[], int ymin, boolean big) {
        if( !big )              // Small leaf?
          preds[ymin+(_nclass < 256 ? ab.get1() : ab.get2())] += 1.0f;
        else                    // Big leaf
          for( int i = 0; i < _nclass; ++i )
            preds[ymin+i] += ab.get4f();
        return preds;
      }
    }
    
    /** Abstract visitor class for serialized trees.*/
    public static abstract class TreeVisitor<T extends Exception> {
      // Override these methods to get walker behavior.  
      protected void pre ( int col, float fcmp, boolean equal ) throws T { }
      protected void mid ( int col, float fcmp, boolean equal ) throws T { }
      protected void post( int col, float fcmp, boolean equal ) throws T { }
      protected void leaf( int pclass )                         throws T { }
      protected void leaf( float preds[] )                      throws T { }
      long  result( ) { return 0; } // Override to return simple results

      protected final TreeModel _tm;
      protected final CompressedTree _ct;
      private final AutoBuffer _ts;
      private final float _preds[]; // Reused to hold a
      public TreeVisitor( TreeModel tm, CompressedTree ct ) { 
        _tm = tm;
        _ts = new AutoBuffer((_ct=ct)._bits); 
        _preds = new float[ct._nclass+tm.ymin]; 
      }
      
      // Call either the single-class leaf or the full-prediction leaf
      private final void leaf2( int mask ) throws T {
        assert (mask& 8)== 8;   // Is a leaf
        if( (mask&16) == 0 )    // Small leaf?
          // Call the leaf with a single class prediction
          leaf(_tm.ymin+(_ct._nclass < 256 ? _ts.get1() : _ts.get2()));
        else {
          for( int i = 0; i < _ct._nclass; ++i )
            _preds[_tm.ymin+i] = _ts.get4f();
          leaf(_preds);
        }
      }

      public final void visit() throws T {
        int nodeType = _ts.get1();
        int col = _ts.get2();
        float fcmp = _ts.get4f();
        if( col==65535 ) { leaf2(nodeType); return; }
        boolean equal = ((nodeType&4)==4);
        // Compute the amount to skip.
        int lmask =  nodeType & 0x1B;
        int rmask = (nodeType & 0x60) >> 2;
        int skip = 0;
        switch(lmask) {
        case 1:  skip = _ts.get1();  break;
        case 2:  skip = _ts.get2();  break;
        case 3:  skip = _ts.get3();  break;
        case 8:  skip = _ct._nclass < 256?1:2;  break; // Small leaf
        case 24: skip = _ct._nclass*4;  break; // skip the p-distribution
        default: assert false:"illegal lmask value " + lmask;
        }
        pre (col,fcmp,equal);   // Pre-walk
        if( (lmask & 0x8)==8 ) leaf2(lmask);  else  visit();
        mid (col,fcmp,equal);   // Mid-walk
        if( (rmask & 0x8)==8 ) leaf2(rmask);  else  visit();
        post(col,fcmp,equal);
      }
    }
    
    StringBuilder toString(CompressedTree ct, final StringBuilder sb ) {
      new TreeVisitor<RuntimeException>(this,ct) {
        int _depth;
        @Override protected void pre( int col, float fcmp, boolean equal ) {
          for( int i=0; i<_depth; i++ ) sb.append("  ");
          sb.append(_names[col]).append(equal?"==":"< ").append(fcmp).append('\n');
          _depth++;
        }
        @Override protected void post( int col, float fcmp, boolean equal ) { _depth--; }
        @Override protected void leaf( int pclass ) {
          for( int i=0; i<_depth; i++ ) sb.append("  ");
          sb.append("[").append(classNames()[pclass]).append("]\n");
        }
        @Override protected void leaf( float preds[]  ) {
          for( int i=0; i<_depth; i++ ) sb.append("  ");
          String domain[] = classNames();
          sb.append("[");
          for( int c=_tm.ymin; c<preds.length; c++ )
            sb.append(domain[c]).append('=').append(preds[c]).append(',');
          sb.append("]\n");
        }
      }.visit();
      return sb;
    }
  }

  // Build a compressed-tree struct
  public TreeModel.CompressedTree compress() {
    int sz = decided(0).size();
    AutoBuffer ab = new AutoBuffer(sz);
    int pos = ab.position();    // For asserts
    decided(0).compress(ab);        // Compress whole tree
    assert ab.position() - pos == sz;
    return new TreeModel.CompressedTree(ab.buf(),_nclass);
  }

  // Due to roundoff error, we often get "ugly" distributions.  Correct where obvious.
  static void correctDistro( float fs[] ) {
    if( fs == null ) return;
    double sum=0;
    int max=0;
    for( int i=0; i<fs.length; i++ ) {
      sum += fs[i];
      if( fs[i] > max ) max = i;
    }
    if( Double.isNaN(sum) ) return; // Assume this is intended
    if( sum < 0.5 ) {        // GBM: expect 0.0 distro
      assert Math.abs(sum)<0.0001 : Arrays.toString(fs);// Really busted?
      if( sum != 0.0 ) {     // Not a 0.0?
        sum /= fs.length;    // Recenter the distro around 0
        for( int i=0; i<fs.length; i++ )
          fs[i] -= sum;
      }
    } else {                  // DRF: expect 1.0 distro
      assert Math.abs(sum-1.0)<0.0001 : Arrays.toString(fs);// Really busted?
      if( fs[max] >= 1.0 ) {  // If max class >= 1.0, force a clean distro
        Arrays.fill(fs,0);    // All zeros, except the 1.0
        fs[max] = 1.0f;       // 
      } else                  // Else adjust the max to clean out error
        fs[max] += 1.0-sum;
    }
    sum=0;
    for( float f : fs ) sum+=f;
    assert Math.abs((sum < 0.5 ? 0 : 1)-sum)<0.000001 : Arrays.toString(fs);
  }

}
