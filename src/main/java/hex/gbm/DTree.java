package hex.gbm;

import hex.ConfusionMatrix;

import java.util.Arrays;
import java.util.Random;

import water.*;
import water.api.DocGen;
import water.api.Request.API;
import water.fvec.Chunk;
import water.fvec.Frame;
import water.util.Log;

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
  // One-time local init after wire transfer
  void init_tree( ) { for( int j=0; j<_len; j++ ) _ns[j]._tree = this; }

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
    Node( DTree tree, int pid ) { this(tree,pid,tree.newIdx()); }

    // Recursively print the decision-line from tree root to this child.
    StringBuilder printLine(StringBuilder sb ) {
      if( _pid==-1 ) return sb.append("[root]");
      DecidedNode parent = _tree.decided(_pid);
      parent.printLine(sb).append(" to ");
      return parent.printChild(sb,_nid);
    }
    abstract protected StringBuilder toString2(StringBuilder sb, int depth);
    abstract protected AutoBuffer compress(AutoBuffer ab);
    abstract protected int size();
  }

  // --------------------------------------------------------------------------
  // Records a column, a bin to split at within the column, and the MSE.
  static class Split extends Iced {
    final int _col, _bin;       // Column to split, bin where being split
    final boolean _equal;       // Split is < or == ?
    final double _se0, _se1;    // Squared error of each subsplit
    final long _n0, _n1;        // Rows in each final split

    Split( int col, int bin, boolean equal, double se0, double se1, long n0, long n1 ) {
      _col = col;  _bin = bin;  _equal = equal;
      _n0 = n0;  _n1 = n1;  _se0 = se0;  _se1 = se1;
    }
    public final double se() { return _se0+_se1; }

    // Split-at dividing point.  Don't use the step*bin+bmin, due to roundoff
    // error we can have that point be slightly higher or lower than the bin
    // min/max - which would allow values outside the stated bin-range into the
    // split sub-bins.  Always go for a value which splits the nearest two
    // elements.
    float splat(DHistogram hs[]) {
      DBinHistogram h = ((DBinHistogram)hs[_col]);
      assert _bin > 0 && _bin < h._nbins;
      if( _equal ) { assert h._bins[_bin]!=0 && h._mins[_bin]==h._maxs[_bin]; return h._mins[_bin]; }
      int x=_bin-1;
      while( x >= 0 && h._bins[x]==0 ) x--;
      int n=_bin;
      while( n < h._bins.length && h._bins[n]==0 ) n++;
      if( x <               0 ) return h._mins[n];
      if( n >= h._bins.length ) return h._maxs[x];
      return (h._maxs[x]+h._mins[n])/2;
    }

    // Split a DBinHistogram.  Return null if there is no point in splitting
    // this bin further (such as there's fewer than min_row elements, or zero
    // error in the response column).  Return an array of DBinHistograms (one
    // per column), which are bounded by the split bin-limits.  If the column
    // has constant data, or was not being tracked by a prior DBinHistogram
    // (for being constant data from a prior split), then that column will be
    // null in the returned array.
    public DBinHistogram[] split( int splat, char nbins, int min_rows, DHistogram hs[] ) {
      long n = splat==0 ? _n0 : _n1;
      if( n < min_rows || n <= 1 ) return null; // Too few elements
      double se = splat==0 ? _se0 : _se1;
      if( se <= 1e-30 ) return null; // No point in splitting a perfect prediction

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
      sb.append(", se0=").append(_se0);
      sb.append(", se1=").append(_se1);
      sb.append(", n0=" ).append(_n0 );
      sb.append(", n1=" ).append(_n1 );
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

    // Make the parent of this Node use a -1 NID to prevent the split that this
    // node otherwise induces.  Happens if we find out too-late that we have a
    // perfect prediction here, and we want to turn into a leaf.
    void do_not_split( ) {
      if( _pid == -1 ) return;
      DecidedNode dn = _tree.decided(_pid);
      for( int i=0; i<dn._nids.length; i++ )
        if( dn._nids[i]==_nid ) 
          { dn._nids[i] = -1; return; }
      throw H2O.fail();
    }

    @Override public String toString() {
      final int nclass = _tree._nclass;
      final String colPad="  ";
      final int cntW=4, mmmW=4, menW=5, varW=5;
      final int colW=cntW+1+mmmW+1+mmmW+1+menW+1+varW;
      StringBuilder sb = new StringBuilder();
      sb.append("Nid# ").append(_nid).append(", ");
      printLine(sb).append("\n");
      if( _hs == null ) return sb.append("_hs==null").toString();
      final int ncols = _hs.length;
      for( int j=0; j<ncols; j++ )
        if( _hs[j] != null )
          p(sb,_hs[j]._name+String.format(", %4.1f",_hs[j]._min),colW).append(colPad);
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
         (d==0?" 0":Double.toString(d)));
      if( s.length() <= w ) return p(sb,s,w);
      s = String.format("% 4.2f",d);
      if( s.length() > w )
        s = String.format("% 4.1f",d);
      if( s.length() > w )
        s = String.format("%4.0f",d);
      return p(sb,s,w);
    }

    @Override public StringBuilder toString2(StringBuilder sb, int depth) {
      for( int d=0; d<depth; d++ ) sb.append("  ");
      return sb.append("Undecided\n");
    }
    @Override protected AutoBuffer compress(AutoBuffer ab) { throw H2O.fail(); }
    @Override protected int size() { throw H2O.fail(); }
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
    abstract UDN makeUndecidedNode(DBinHistogram[] nhists );

    // Pick the best column from the given histograms
    abstract Split bestCol( UDN udn );

    DecidedNode( UDN n ) {
      super(n._tree,n._pid,n._nid); // Replace Undecided with this DecidedNode
      _nids = new int[2];           // Split into 2 subsets
      _split = bestCol(n);          // Best split-point for this tree
      if( _split._col == -1 ) {     // No good split?
        // Happens because the predictor columns cannot split the responses -
        // which might be because all predictor columns are now constant, or
        // because all responses are now constant.
        _splat = Float.NaN;
        Arrays.fill(_nids,-1);
        return;
      }

      _splat = _split.splat(n._hs); // Split-at value
      final char nclass  = _tree._nclass;
      final char nbins   = _tree._nbins;
      final int min_rows = _tree._min_rows;

      for( int b=0; b<2; b++ ) { // For all split-points
        // Setup for children splits
        DBinHistogram nhists[] = _split.split(b,nbins,min_rows,n._hs);
        assert nhists==null || nhists.length==_tree._ncols;
        _nids[b] = nhists == null ? -1 : makeUndecidedNode(nhists)._nid;
      }
    }

    // Bin #.
    public int bin( Chunk chks[], int row ) {
      if( chks[_split._col].isNA0(row) ) // Missing data?
        return 0;                        // NAs always to bin 0
      float d = (float)chks[_split._col].at0(row); // Value to split on for this row
      // Note that during *scoring* (as opposed to training), we can be exposed
      // to data which is outside the bin limits.
      return _split._equal ? (d != _splat ? 0 : 1) : (d < _splat ? 0 : 1);
    }

    public int ns( Chunk chks[], int row ) { return _nids[bin(chks,row)]; }

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
        sb.append(_nid).append(" ");
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

    // Size of this subtree; sets _nodeType also
    @Override public final int size(){
      if( _size != 0 ) return _size; // Cached size

      assert _nodeType == 0:"unexpected node type: " + _nodeType;
      if( _split._equal ) _nodeType |= (byte)4;

      int res = 7; // 1B node type + flags, 2B colId, 4B float split val

      Node left = _tree.node(_nids[0]);
      int lsz = left.size();
      res += lsz;
      if( left instanceof LeafNode ) _nodeType |= (byte)(24 << 0*2);
      else {
        int slen = lsz < 256 ? 1 : (lsz < 65535 ? 2 : 3);
        _nodeType |= slen; // Set the size-skip bits
        res += slen;
      }
      
      Node rite = _tree.node(_nids[1]);
      if( rite instanceof LeafNode ) _nodeType |= (byte)(24 << 1*2);
      res += rite.size();
      assert (_nodeType&0x1B) != 27;
      assert res != 0;
      return (_size = res);
    }

    // Compress this tree into the AutoBuffer
    @Override public AutoBuffer compress(AutoBuffer ab) {
      int pos = ab.position();
      if( _nodeType == 0 ) size(); // Sets _nodeType & _size both
      ab.put1(_nodeType);          // Includes left-child skip-size bits
      assert _split._col != -1;    // Not a broken root non-decision?
      ab.put2((short)_split._col);
      ab.put4f(_splat);
      Node left = _tree.node(_nids[0]);
      if( (_nodeType&3) > 0 ) { // Size bits are optional for leaves
        int sz = left.size();
        if(sz < 256)         ab.put1(       sz);
        else if (sz < 65535) ab.put2((short)sz);
        else                 ab.put3(       sz);
      }
      // now write the subtree in
      left.compress(ab);
      Node rite = _tree.node(_nids[1]);
      rite.compress(ab);
      assert _size == ab.position()-pos:"reported size = " + _size + " , real size = " + (ab.position()-pos);
      return ab;
    }
  }

  static abstract class LeafNode extends Node {
    double _pred;
    LeafNode( DTree tree, int pid ) { super(tree,pid); }
    LeafNode( DTree tree, int pid, int nid ) { super(tree,pid,nid); }
    @Override public String toString() { return "Leaf#"+_nid+" = "+_pred; }
    @Override public final StringBuilder toString2(StringBuilder sb, int depth) {
      for( int d=0; d<depth; d++ ) sb.append("  ");
      sb.append(_nid).append(" ");
      return sb.append("pred=").append(_pred).append("\n");
    }
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

    public TreeModel(Key key, Key dataKey, Frame fr, int ntrees, int ymin) {
      super(key,dataKey,fr);
      this.N = ntrees; this.errs = new double[0]; this.ymin = ymin; this.cm = null;
      treeBits = new CompressedTree[0][];
    }
    public TreeModel(TreeModel prior, DTree[] trees, double err, long [][] cm) {
      super(prior._selfKey,prior._dataKey,prior._names,prior._domains);
      this.N = prior.N; this.ymin = prior.ymin; this.cm = cm;
      errs = Arrays.copyOf(prior.errs,prior.errs.length+1);
      errs[errs.length-1] = err;
      assert trees.length == nclasses()-ymin : "Trees="+trees.length+" nclasses()="+nclasses()+" ymin="+ymin;
      treeBits = Arrays.copyOf(prior.treeBits,prior.treeBits.length+1);
      CompressedTree ts[] = treeBits[treeBits.length-1] = new CompressedTree[trees.length];
      for( int c=0; c<trees.length; c++ )
        if( trees[c] != null )
            ts[c] = trees[c].compress();
    }

    // Number of trees actually in the model (instead of expected/planned)
    public int numTrees() { return treeBits.length; }

    @Override public ConfusionMatrix cm() { return cm == null ? null : new ConfusionMatrix(cm); }

    @Override protected float[] score0(double data[], float preds[]) {
      Arrays.fill(preds,0);
      for( CompressedTree ts[] : treeBits )
        for( int c=0; c<ts.length; c++ )
          if( ts[c] != null )
            preds[c] += ts[c].score(data);
      return preds;
    }

    public void generateHTML(String title, StringBuilder sb) {
      DocGen.HTML.title(sb,title);
      DocGen.HTML.paragraph(sb,"Model Key: "+_selfKey);
      DocGen.HTML.paragraph(sb,water.api.Predict.link(_selfKey,"Predict!"));
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
    //        1 bit  ( 4) operator flag (0 --> <, 1 --> == ), 
    //        1 bit  ( 8) left leaf flag, 
    //        1 bit  (16) left leaf type flag, (unused)
    //        1 bit  (32) right leaf flag, 
    //        1 bit  (64) right leaf type flag (unused)
    //    left, right: tree | prediction
    //    prediction: 4 bytes of float
    public static class CompressedTree extends Iced {
      final byte [] _bits;
      final int _nclass;
      public CompressedTree( byte [] bits, int nclass ) { _bits = bits; _nclass = nclass; }
      float score( final double row[] ) {
        AutoBuffer ab = new AutoBuffer(_bits);
        while(true) {
          int nodeType = ab.get1();
          int colId = ab.get2();
          if( colId == 65535 ) return scoreLeaf(ab);
          float splitVal = ab.get4f();

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
          case 24: skip = 4;          break; // skip the prediction
          default: assert false:"illegal lmask value " + lmask+" at "+ab.position()+" in bitpile "+Arrays.toString(_bits);
          }

          if( !Double.isNaN(row[colId]) ) // NaNs always go to bin 0
            if( ( equal && ((float)row[colId]) == splitVal) ||
                (!equal && ((float)row[colId]) >= splitVal) ) {
              ab.position(ab.position()+skip); // Skip right subtree
              lmask = rmask;                   // And set the leaf bits into common place
            }
          if( (lmask&8)==8 ) return scoreLeaf(ab);
        }
      }
      
      private float scoreLeaf( AutoBuffer ab ) { return ab.get4f(); }
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
    int sz = root().size();
    if( root() instanceof LeafNode ) sz += 3; // Oops - tree-stump
    AutoBuffer ab = new AutoBuffer(sz);
    if( root() instanceof LeafNode ) // Oops - tree-stump
      ab.put1(0).put2((char)65535); // Flag it special so the decompress doesn't look for top-level decision
    root().compress(ab);      // Compress whole tree
    assert ab.position() == sz;
    return new TreeModel.CompressedTree(ab.buf(),_nclass);
  }
}
