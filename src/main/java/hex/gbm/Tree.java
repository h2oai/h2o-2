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
  private Node[] _ns = new Node[1]; // All the nodes in the tree.  Node 0 is the root.
  int _len;                         // Resizable array

  // Return Tree.Node i
  public final Node n( int i ) { 
    if( i >= _len ) throw new ArrayIndexOutOfBoundsException(i); 
    return _ns[i]; 
  }

  // Add a new Node to Tree, growing innards on demand
  Node newNode( Node parent, Histogram hs[] ) {
    if( _len == _ns.length ) _ns = Arrays.copyOf(_ns,_len<<1);
    int i=_len++;
    return (_ns[i] = new Node(this,parent==null?-1:parent._nid,i,hs));
  }

  // Inner Tree.Node.  Has a Histogram which is filled in (in parallel with
  // other histograms) in a single pass over the data.  Has a split decision
  // and a regression/classification filled in after the Histogram is filled in.
  static class Node extends Iced {
    transient Tree _tree;
    final int _nid;             // My node-ID, 0 is root
    final int _pid;             // Parent node id, root has no parent and uses -1
    int[] _ns;                  // Child node ids.  Null if no split decision has been made.

    final Histogram _hs[];      // Histograms per column
    int _col;                   // Column we split over

    private Node( Tree tree, int pid, int nid, Histogram hs[] ) { _tree=tree; _nid=nid; _pid= pid; _hs = hs; }

    // Find the column with the best split (lowest score).
    // Also setup leaf Trees for when we split this Tree
    int bestSplit() {
      assert _ns==null;
      double bs = Double.MAX_VALUE; // Best score
      int idx = -1;             // Column to split on
      for( int i=0; i<_hs.length; i++ ) {
        if( _hs[i]==null || _hs[i]._bins.length == 1 ) continue;
        double s = _hs[i].scoreVar();
        if( s < bs ) { bs = s; idx = i; }
      }
      // Split on column 'idx' with score 'bs'
      _ns = new int[_hs[idx]._bins.length];
      Arrays.fill(_ns,-1);      // Mark as "no child split assigned yet"
      return (_col=idx);        // Record & return split column
    }

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
          p(sb,_hs[j]._name+String.format(", %5.2f",_hs[j].scoreVar()),colW).append(colPad);
      sb.append('\n');
      for( int j=0; j<ncols; j++ ) {
        if( _hs[j] == null ) continue;
        p(sb,"cnt" ,cntW).append('/');
        p(sb,"min" ,mmmW).append('/');
        p(sb,"max" ,mmmW).append('/');
        p(sb,"mean",mmmW).append('/');
        p(sb,"var" ,varW).append(colPad);
      }
      sb.append('\n');
      for( int i=0; i<Histogram.BINS; i++ ) {
        for( int j=0; j<ncols; j++ ) {
          if( _hs[j] == null ) continue;
          if( i < _hs[j]._bins.length ) {
            p(sb,Long.toString(_hs[j]._bins[i]),cntW).append('/');
            p(sb,              _hs[j]._mins[i] ,mmmW).append('/');
            p(sb,              _hs[j]._maxs[i] ,mmmW).append('/');
            p(sb,              _hs[j]. mean(i) ,mmmW).append('/');
            p(sb,              _hs[j]. var (i) ,varW).append(colPad);
            //p(sb,              _hs[j]. mse (i) ,varW).append(colPad);
          } else {
            p(sb,"",colW).append(colPad);
          }
        }
        sb.append('\n');
      }
      sb.append("Nid# ").append(_nid);
      if( _ns != null ) sb.append(", split on column "+_col+", "+_hs[_col]._name);
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

    StringBuilder printLine(StringBuilder sb ) {
      if( _pid==-1 ) return sb.append("root");
      Node parent = _tree.n(_pid);
      Histogram h = parent._hs[parent._col];
      for( int i=0; i<parent._ns.length; i++ )
        if( parent._ns[i]==_nid )
          return parent.printLine(sb.append("[").append(h._mins[i]).append(" <= ").append(h._name).
                                  append(" <= ").append(h._maxs[i]).append("] from "));
      throw H2O.fail();
    }
  }
}
