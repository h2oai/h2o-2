package water.exec;

import java.util.*;

import water.*;
import water.fvec.*;
import water.util.Utils;
import water.nbhm.NonBlockingHashMap;

/** Parse a generic R string and build an AST, in the context of an H2O Cloud
 *  @author cliffc@0xdata.com
 */

// --------------------------------------------------------------------------
// R's Apply.  Function is limited to taking a single column and returning
// a single column.  Double is limited to 1 or 2, statically determined.
class ASTRApply extends ASTOp {
  static final String VARS[] = new String[]{ "", "ary", "dbl1.2", "fcn"};
  ASTRApply( ) { super(VARS,
                       new Type[]{ Type.ARY, Type.ARY, Type.DBL, Type.fcn(new Type[]{Type.dblary(),Type.ARY}) },
                       OPF_PREFIX,
                       OPP_PREFIX,
                       OPA_RIGHT); }
  protected ASTRApply( String vars[], Type ts[], int form, int prec, int asso) { super(vars,ts,form,prec,asso); }
  @Override String opStr(){ return "apply";}
  @Override ASTOp make() {return new ASTRApply();}
  @Override void apply(Env env, int argcnt) {
    // Peek everything from the stack
    final ASTOp op = env.fcn(-1);    // ary->dblary but better be ary[,1]->dblary[,1]
    double d = env.dbl(-2);    // MARGIN: ROW=1, COLUMN=2 selector
    Frame fr = env.ary(-3);    // The Frame to work on
    if( d==2 || d== -1 ) {     // Work on columns?
      int ncols = fr.numCols();

      double ds[][] = null; // If results are doubles, gather in small array
      Frame fr2 = null;     // If the results are Vecs, gather them in this Frame
      String err = "apply requires that "+op+" return 1 column";
      if( op._t.ret().isDbl() ) ds = new double[ncols][1];
      else                     fr2 = new Frame(new String[0],new Vec[0]);

      // Apply the function across columns
      try {
        Vec vecs[] = fr.vecs();
        for( int i=0; i<ncols; i++ ) {
          env.push(op);
          env.push(new Frame(new String[]{fr._names[i]},new Vec[]{vecs[i]}));
          env.fcn(-2).apply(env, 2);
          if( ds != null ) {    // Doubles or Frame results?
            ds[i][0] = env.popDbl();
          } else {                // Frame results
            if( env.ary(-1).numCols() != 1 )
              throw new IllegalArgumentException(err);
            fr2.add(fr._names[i], env.popAry().theVec(err));
          }
        }
      } catch( IllegalArgumentException iae ) { 
        env.subRef(fr2,null); 
        throw iae; 
      }
      env.pop(4);
      if( ds != null ) env.push(TestUtil.frame(new String[]{"C1"},ds));
      else { env.push(1);  env._ary[env._sp-1] = fr2;  }
      assert env.isAry();
      return;
    }
    if( d==1 || d==-2) {      // Work on rows
      // apply on rows is essentially a map function
      Type ts[] = new Type[2];
      ts[0] = Type.unbound();
      ts[1] = Type.ARY;
      Type ft1 = Type.fcn(ts);
      Type ft2 = op._t.find();  // Should be a function type
      if( !ft1.union(ft2) ) {
        if( ft2._ts.length != 2 )
          throw new IllegalArgumentException("FCN " + op.toString() + " cannot accept one argument.");
        if( !ft2._ts[1].union(ts[1]) )
          throw new IllegalArgumentException("Arg " + op._vars[1] + " typed " + ft2._ts[1].find() + " but passed as " + ts[1]);
        assert false;
      }
      // find out return type
      final double[] rowin = new double[fr.vecs().length];
      for (int c = 0; c < rowin.length; c++) rowin[c] = fr.vecs()[c].at(0);
      final double[] rowout = op.map(env,rowin,null);
      final Env env0 = env;
      MRTask2 mrt = new MRTask2() {
        @Override
        public void map(Chunk[] cs, NewChunk[] ncs) {
          for (int i = 0; i < cs[0]._len; i++) {
            for (int c = 0; c < cs.length; c++) rowin[c] = cs[c].at0(i);
            op.map(env0, rowin, rowout);
            for (int c = 0; c < ncs.length; c++) ncs[c].addNum(rowout[c]);
          }
        }
      };
      String[] names = new String[rowout.length];
      for (int i = 0; i < names.length; i++) names[i] = "C"+(i+1);
      Frame res = mrt.doAll(rowout.length,fr).outputFrame(names, null);
      env.poppush(4,res,null);
      return;
    }
    throw new IllegalArgumentException("MARGIN limited to 1 (rows) or 2 (cols)");
  }
}

// --------------------------------------------------------------------------
// Same as "apply" but defaults to columns.  
class ASTSApply extends ASTRApply {
  static final String VARS[] = new String[]{ "", "ary", "fcn"};
  ASTSApply( ) { super(VARS,
                       new Type[]{ Type.ARY, Type.ARY, Type.fcn(new Type[]{Type.dblary(),Type.ARY}) },
                       OPF_PREFIX,
                       OPP_PREFIX,
                       OPA_RIGHT); }
  @Override String opStr(){ return "sapply";}
  @Override ASTOp make() {return new ASTSApply();}
  @Override void apply(Env env, int argcnt) {
    // Stack: SApply, ary, fcn
    //   -->: RApply, ary, 2, fcn
    assert env.isFcn(-3);
    env._fcn[env._sp-3] = new ASTRApply();
    ASTOp fcn = env.popFcn();   // Pop, no ref-cnt
    env.push(2.0);
    env.push(1);
    env._fcn[env._sp-1] = fcn;  // Push, no ref-cnt
    super.apply(env,argcnt+1);
  }
}

// --------------------------------------------------------------------------
// PLYR's DDPLY.  GroupBy by any other name.  Type signature:
//   #RxN  ddply(RxC,subC, 1xN function( subRxC ) { ... } ) 
//   R - Rows in original frame
//   C - Cols in original frame
//   subC - Subset of C; either a single column entry, or a 1 Vec frame with a list of columns.
//   subR - Subset of R, where all subC values are the same.
//   N - Return column(s).
//  #R - # of unique combos in the original "subC" set

class ASTddply extends ASTOp {
  static final String VARS[] = new String[]{ "#RxN", "RxC", "subC", "fcn_subRxC"};
  ASTddply( ) { super(VARS,
                      new Type[]{ Type.ARY, Type.ARY, Type.dblary(), Type.fcn(new Type[]{Type.dblary(),Type.ARY}) },
                      OPF_PREFIX,
                      OPP_PREFIX,
                      OPA_RIGHT); }
  @Override String opStr(){ return "ddply";}
  @Override ASTOp make() {return new ASTddply();}
  @Override void apply(Env env, int argcnt) {
    // Peek everything from the stack
    // Function to execute on the groups
    final ASTOp op = env.fcn(-1); // ary->dblary: subRxC -> 1xN
    
    Frame fr = env.ary(-3);    // The Frame to work on
    final int ncols = fr.numCols();

    // Either a single column, or a collection of columns to group on.
    int cols[];
    if( !env.isAry(-2) ) {      // Single column?
      if( Double.isNaN(env.dbl(-2)) ) throw new IllegalArgumentException("NA not a valid column");
      cols = new int[]{(int)env.dbl(-2)-1};
    } else {                    // Else a collection of columns?
      Frame cs = env.ary(-2);
      if( cs.numCols() !=  1  ) throw new IllegalArgumentException("Only one column-of-columns for column selection");
      if( cs.numRows() > 1000 ) throw new IllegalArgumentException("Too many columns selected");
      cols = new int[(int)cs.numRows()];
      Vec vec = cs.vecs()[0];
      for( int i=0; i<cols.length; i++ ) 
        if( vec.isNA(i) ) throw new IllegalArgumentException("NA not a valid column");
        else cols[i] = (int)vec.at8(i)-1;
    }
    // Another check for sane columns
    for( int c : cols )
      if( c < 0 || c >= fr.numCols() )
        throw new IllegalArgumentException("Column "+(c+1)+" out of range for frame columns "+fr.numCols());

    // Random implementation hacks/notes...

    // Was pondering a SIMD-like execution model, running the fcn "once" - but
    // in parallel for all groups.  But this isn't going to work: each fcn
    // execution will take different control paths.  Also the functions side-
    // effects' must only happen once, and they will make multiple passes over
    // the Frame passed in.  
    //
    // GroupIDs' can vary from 1 group to 1-per-row.  Are formed by the cross-
    // product of the selection cols.  Will be hashed to find Group - NBHML
    // mapping row-contents to group.  Index is a sample row.  NBHML per-node,
    // plus roll-ups.  Result/Value is Group structure pointing to NewChunks
    // holding row indices.  

    // Hack-Pass-1: Get a Vec-per-group holding row#s in that group.  Chunks
    // are node-local and hold node-local rows.  Vecs are made lazily via
    // hash-table mapping.  After the first roll-up we'll know which groups
    // exist.  Can compress the row#'s into Chunks also.

    // Hack-Pass-2: ForAllGroups, launch parallel ddply execution tasks with a
    // bogus Frame, with magical Vecs that use the Group(s) to define their
    // rows.  All the usual Chunk.at0 or Vec.at calls work, except the Chunk
    // does a row-indirection to find the actual Chunk data.
    
    // Pass-1: Find Groups.
    // Build a NBHML mapping 1st row-# in group to double[] holding selection cols.
    ddplyPass1 p1 = new ddplyPass1(cols).doAll(fr);
    System.out.print(p1.toString());
    
    env.pop(4);
    // Push empty frame for debuggging
    env.push(new Frame(new String[0],new Vec[0]));
  }

  // ---
  // Pass1: Find unique groups, based on a subset of columns
  private static class ddplyPass1 extends MRTask2<ddplyPass1> {
    public int _cols[];         // Selection columns
    public transient NonBlockingHashMap<Entry,Object> _uniques;
    public static final Object V = "";
    // A stupid class to give double[] sane equals & hashCode
    private static class Entry extends Iced {
      public double _ds[];
      Entry( int len ) { _ds = new double[len]; }
      @Override public boolean equals( Object o ) { 
        return o instanceof Entry && Arrays.equals(_ds,((Entry)o)._ds); }
      @Override public int hashCode() {
        int sum=0;
        for( double d : _ds ) sum += Double.doubleToRawLongBits(d);
        return sum;
      }
      @Override public String toString() { return Arrays.toString(_ds); }
    }
    ddplyPass1( int cols[] ) { _cols = cols; }
    @Override public void setupLocal() {
      _uniques = new NonBlockingHashMap<Entry,Object>();
    }
    @Override public void map( Chunk chks[] ) {
      Entry e = new Entry(_cols.length);
      int len = chks[_cols[0]]._len;
      for( int row=0; row<len; row++ ) {
        // Temp array holding the column-selection data
        for( int c=0; c<_cols.length; c++ )   // For all selection cols
          e._ds[c] = chks[_cols[c]].at0(row); // Load into working array
        if( _uniques.putIfAbsent(e,V)==null ) // Add group signature if not already present
          e = new Entry(_cols.length);        // Was added; so 'e' in hashset; need a new 'e'
      }
    }
    @Override public void reduce( ddplyPass1 p1 ) {
      if( _uniques == p1._uniques ) return; // Trivally true
      _uniques.putAll(p1._uniques);         // Smash hashmaps together
    }
    @Override public String toString() { return _uniques.toString(); }
    // Custom serialization for NBHM.  Much nicer when these are auto-gen'd.
    @Override public AutoBuffer write( AutoBuffer ab ) {
      super.write(ab);
      ab.putA4(_cols);
      if( _uniques == null ) return ab.put4(0);
      ab.put4(_uniques.size());
      for( Entry e : _uniques.keySet() ) ab.put(e);
      return ab;
    }
    
    @Override public ddplyPass1 read( AutoBuffer ab ) {
      super.read(ab);
      assert _uniques == null;
      _uniques = new NonBlockingHashMap<Entry,Object>();
      _cols = ab.getA4();
      int len = ab.get4();
      for( int i=0; i<len; i++ )
        _uniques.put((Entry)ab.get(),V);
      return this;
    }
    public void copyOver(DTask that) {
      ddplyPass1 p1 = (ddplyPass1)that;
      super.copyOver(p1);
      p1._cols = _cols;
      p1._uniques = _uniques;
    }
  }

}
