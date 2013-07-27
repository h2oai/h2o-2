package hex.gbm;

import java.util.ArrayList;
import java.util.Arrays;
import water.*;
import water.H2O.H2OCountedCompleter;
import water.fvec.*;
import water.util.Log;

/**
   A Decision Tree, laid over a Frame of Vecs, and built distributed.

   This class defines an explicit Tree structure, as a collection of {@code
   Tree} {@code Node}s.  The Nodes are numbered with a unique {@code _nid}.
   Users need to maintain their own mapping from their data to a {@code _nid},
   where the obvious technique is to have a Vec of {@code _nid}s (ints), one
   per each element of the data Vecs.

   Each {@code Node} has a {@code Histogram}, describing summary data about the
   rows.  The Histogram requires a pass over the data to be filled in, and we
   expect to fill in all rows for Nodes at the same depth at the same time.
   i.e., a single pass over the data will fill in all leaf Nodes' Histograms
   at once.

   @author Cliff Click
*/

class Tree extends Iced {
  final String[] _names; // Column names
  private Node[] _ns;    // All the nodes in the tree.  Node 0 is the root.
  int _len;              // Resizable array
  Tree( String[] names ) { _names = names; _ns = new Node[1]; }

  public final Node root() { return _ns[0]; }

  // Return Tree.Node i
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

  // Abstract node flavor
  static abstract class Node extends Iced {
    transient Tree _tree;
    final int _pid;             // Parent node id, root has no parent and uses -1
    final int _nid;             // My node-ID, 0 is root
    Node( Tree tree, int pid, int nid ) { 
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

  // An UndecidedNode: Has a Histogram which is filled in (in parallel with other
  // histograms) in a single pass over the data.  Does not contain any
  // split-decision.
  static class UndecidedNode extends Node {
    Histogram _hs[];            // Histograms per column
    UndecidedNode( Tree tree, int pid, Histogram hs[] ) { super(tree,pid,tree.newIdx()); _hs=hs; }

    // Find the column with the best split (lowest score).
    int bestDecided() {
      double bs = Double.MAX_VALUE; // Best score
      int idx = -1;             // Column to split on
      for( int i=0; i<_hs.length; i++ ) {
        if( _hs[i]==null || _hs[i]._bins.length == 1 ) continue;
        double s = _hs[i].score();
        if( s < bs ) { bs = s; idx = i; }
      }
      return idx;
    }

    @Override public String toString() {
      final String colPad="  ";
      final int cntW=4, mmmW=4, varW=4;
      final int colW=cntW+1+mmmW+1+mmmW+1+mmmW+1+varW;
      StringBuilder sb = new StringBuilder();
      sb.append("Nid# ").append(_nid).append(", ");
      printLine(sb).append("\n");
      int numClasses = 0;       // Assume Regression
      final int ncols = _hs.length;
      for( int j=0; j<ncols; j++ )
        if( _hs[j] != null ) {
          p(sb,_hs[j]._name+String.format(", err=%5.2f",_hs[j].score()),colW).append(colPad);
          if( _hs[j]._clss != null ) numClasses = _hs[j]._clss[0].length; // Classification
        }
      sb.append('\n');
      for( int j=0; j<ncols; j++ ) {
        if( _hs[j] == null ) continue;
        p(sb,"cnt" ,cntW).append('/');
        p(sb,"min" ,mmmW).append('/');
        p(sb,"max" ,mmmW).append('/');
        if( numClasses == 0 ) {
          p(sb,"mean",mmmW).append('/');
          p(sb,"var" ,varW).append(colPad);
        } else {
          p(sb,"C0",mmmW).append('-');
          p(sb,"C"+(numClasses-1),varW).append(colPad);
        }
      }
      sb.append('\n');
      for( int i=0; i<Histogram.BINS; i++ ) {
        for( int j=0; j<ncols; j++ ) {
          if( _hs[j] == null ) continue;
          if( i < _hs[j]._bins.length ) {
            p(sb,Long.toString(_hs[j]._bins[i]),cntW).append('/');
            p(sb,              _hs[j]._mins[i] ,mmmW).append('/');
            p(sb,              _hs[j]._maxs[i] ,mmmW).append('/');
            if( numClasses==0 ) { // Regression
              p(sb,              _hs[j]. mean(i) ,mmmW).append('/');
              p(sb,              _hs[j]. var (i) ,varW).append(colPad);
            } else {            // Classification
              StringBuilder sb2 = new StringBuilder();
              long N = _hs[j]._bins[i];
              long cls[] = _hs[j]._clss[i];
              for( int k = 0; k<cls.length; k++ )
                sb2.append(cls[k]).append(',');
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
    static private StringBuilder p(StringBuilder sb, double d, int w) {
      String s = Double.isNaN(d) ? "NaN" :
        ((d==Double.MAX_VALUE || d==-Double.MAX_VALUE) ? " -" : 
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
  static class DecidedNode extends Node {
    final int _col;             // Column we split over
    final double _min, _step;   // Binning info of column
    // The following arrays are all based on a bin# extracted from linear
    // interpolation of _col, _min and _step.

    // For classification, we return a zero-based class, and the prediction is
    // a value from zero to 1 describing how weak our guess is.  For instance,
    // during tree-building we decide to build a DecidedNode from 10 rows; 8
    // rows are class A, and 2 rows are class B.  Then our class is A, but our
    // error is 0.2 (2 out of 10 wrong).
    final int _ns[];            // An n-way split node
    final int _ycls[];          // Classification: this is the class
    final double _pred[];       // Regression: this is the prediction
    final double _mins[], _maxs[];  // Hang onto for printing purposes

    DecidedNode( UndecidedNode n ) {
      super(n._tree,n._pid,n._nid); // Replace Undecided with this DecidedNode
      _col = n.bestDecided();       // Best split-point for this tree
      // From the splitting Undecided, get the column, min, max
      Histogram splitH = n._hs[_col];// Histogram of the column being split
      int nums = splitH._nbins;      // Number of split choices
      assert nums > 1;          // Should always be some bins to split between
      _min  = splitH._min ;     // Binning info
      _step = splitH._step;
      assert _step > 0;
      _mins = splitH._mins;     // Hang onto for printing purposes
      _maxs = splitH._maxs;     // Hang onto for printing purposes
      _ns = new int[nums];
      _ycls = new int[nums];
      _pred = new double[nums];
      int ncols = _tree._names.length-1; // ncols: all columns, minus response
      for( int i=0; i<nums; i++ ) { // For all split-points
        // Setup for children splits
        Histogram nhists[] = splitH.split(_col,i,n._hs,_tree._names,ncols);
        _ns[i] = nhists == null ? -1 : new UndecidedNode(_tree,_nid,nhists)._nid;
        // Also setup predictions locally
        if( splitH._clss == null )   // Regression?
          _pred[i] = splitH.mean(i); // Prediction is mean of bin
        else {                       // Classification
          double num = splitH._bins[i];      // Number of entries this histogram
	  long clss[] = splitH._clss[i];     // Class histogram
          int best=0;                        // Largest class so far
	  for( int k=1; k<clss.length; k++ ) // Find largest class
	    if( clss[best] < clss[k] ) best = k;
          _pred[i] = (num-clss[_ycls[i]=best])/num;
        }
      }
    }

    // Bin #.
    public int bin( Chunk[] chks, int i ) {
      double d = chks[_col].at0(i);         // Value to split on for this row
      int idx1 = (int)((d-_min)/_step);     // Interpolate bin#
      assert idx1 >= 0;                     // Expect sanity
      int bin = Math.min(idx1,_ns.length-1);// Cap at length
      return bin;
    }

    public int ns( Chunk[] chks, int i ) { return _ns[bin(chks,i)]; }

    @Override public String toString() {
      throw H2O.unimpl();
    }

    StringBuilder printChild( StringBuilder sb, int nid ) {
      for( int i=0; i<_ns.length; i++ ) 
        if( _ns[i]==nid ) 
          return sb.append("[").append(_mins[i]).append(" <= ").
            append(_tree._names[_col]).append(" <= ").append(_maxs[i]).append("]");
      throw H2O.fail();
    }    
  }
}
