package water.exec;

import java.util.*;
import water.H2O;
import water.MRTask2;
import water.fvec.*;

/** Parse a generic R string and build an AST, in the context of an H2O Cloud
 *  @author cliffc@0xdata.com
 */

// --------------------------------------------------------------------------
public abstract class ASTOp extends AST {
  static final HashMap<String,ASTOp> OPS = new HashMap();
  static {
    // Unary ops
    put(new ASTIsNA());
    put(new ASTSgn ());
    put(new ASTNrow());
    put(new ASTNcol());

    // Binary ops
    put(new ASTPlus());
    put(new ASTSub ());
    put(new ASTMul ());
    put(new ASTDiv ());
    put(new ASTMin ());

    // Misc
    put(new ASTCat ());
    put(new ASTReduce());
    put(new ASTIfElse());
  }
  static private void put(ASTOp ast) { OPS.put(ast.opStr(),ast); }

  // All fields are final, because functions are immutable
  final String _vars[];         // Variable names
  ASTOp( String vars[], Type ts[] ) { super(Type.fcn(0,ts)); _vars=vars; }

  abstract String opStr();
  abstract ASTOp make();

  @Override public String toString() {
    String s = _t._ts[0]+" "+opStr()+"(";
    int len=_vars.length;
    for( int i=1; i<len-1; i++ )
      s += _t._ts[i]+" "+_vars[i]+", ";
    return s + _t._ts[len-1]+" "+_vars[len-1]+")";
  }
  public String toString(boolean verbose) {
    if( !verbose ) return toString(); // Just the fun name& arg names
    return toString();
  }

  // Parse an OP or return null.
  static ASTOp parse(Exec2 E) {
    int x = E._x;
    String id = E.isID();
    if( id == null ) return null;
    ASTOp op = OPS.get(id);
    if( op != null ) return op.make();
    E._x = x;                 // Roll back, no parse happened
    // Attempt a user-mode function parse
    return ASTFunc.parseFcn(E);
  }

  @Override void exec(Env env) { env.push(this); }
  abstract void apply(Env env, int argcnt);
}

abstract class ASTUniOp extends ASTOp {
  static final String VARS[] = new String[]{ "", "x"};
  static Type[] newsig() {
    Type t1 = Type.dblary();
    return new Type[]{Type.anyary(new Type[]{t1}),t1};
  }
  ASTUniOp( ) { super(VARS,newsig()); }
  double op( double d ) { throw H2O.fail(); }
  protected ASTUniOp( String[] vars, Type[] types ) { super(vars,types); }
  @Override void apply(Env env, int argcnt) {
    // Expect we can broadcast across all functions as needed.
    if( !env.isFrame() ) { env.poppush(op(env.popDbl())); return; }
    Frame fr = env.popFrame();
    final ASTUniOp uni = this;  // Final 'this' so can use in closure
    Frame fr2 = new MRTask2() {
        @Override public void map( Chunk chks[], NewChunk nchks[] ) {
          for( int i=0; i<nchks.length; i++ ) {
            NewChunk n =nchks[i];
            Chunk c = chks[i];
            int rlen = c._len;
            for( int r=0; r<rlen; r++ )
              n.addNum(uni.op(c.at0(r)));
          }
        }
      }.doAll(fr.numCols(),fr).outputFrame(fr._names, fr.domains());
    env.subRef(fr);
    env.pop();                  // Pop self
    env.push(fr2);
  }
}

class ASTIsNA extends ASTUniOp { String opStr(){ return "is.na"; } ASTOp make() {return new ASTIsNA();} double op(double d) { return Double.isNaN(d)?1:0;}}
class ASTSgn  extends ASTUniOp { String opStr(){ return "sgn" ; } ASTOp make() {return new ASTSgn ();} double op(double d) { return Math.signum(d);}}
class ASTNrow extends ASTUniOp {
  ASTNrow() { super(VARS,new Type[]{Type.DBL,Type.ARY}); }
  @Override String opStr() { return "nrow"; }
  @Override ASTOp make() {return this;}
  @Override void apply(Env env, int argcnt) {
    Frame fr = env.popFrame();
    double d = fr.numRows();
    env.subRef(fr);
    env.poppush(d);
  }
}
class ASTNcol extends ASTUniOp {
  ASTNcol() { super(VARS,new Type[]{Type.DBL,Type.ARY}); }
  @Override String opStr() { return "ncol"; }
  @Override ASTOp make() {return this;}
  @Override void apply(Env env, int argcnt) {
    Frame fr = env.popFrame();
    double d = fr.numCols();
    env.subRef(fr);
    env.poppush(d);
  }
}

abstract class ASTBinOp extends ASTOp {
  static final String VARS[] = new String[]{ "", "x","y"};
  static Type[] newsig() {
    Type t1 = Type.dblary(), t2 = Type.dblary();
    return new Type[]{Type.anyary(new Type[]{t1,t2}),t1,t2};
  }
  ASTBinOp( ) { super(VARS, newsig()); }
  abstract double op( double d0, double d1 );
  @Override void apply(Env env, int argcnt) {
    // Expect we can broadcast across all functions as needed.
    Frame fr0 = null, fr1 = null;
    double d0=0, d1=0;
    if( env.isFrame() ) fr1 = env.popFrame(); else d1 = env.popDbl();
    if( env.isFrame() ) fr0 = env.popFrame(); else d0 = env.popDbl();
    if( fr0==null && fr1==null ) {
      env.poppush(op(d0,d1));
      return;
    }
    final boolean lf = fr0 != null;
    final boolean rf = fr1 != null;
    final double fd0 = d0;
    final double fd1 = d1;
    Frame fr  = null;           // Do-All frame
    int ncols = 0;              // Result column count
    if( fr0 !=null ) {          // Left?
      ncols = fr0.numCols();
      if( fr1 != null ) {
        if( fr0.numCols() != fr1.numCols() ||
            fr0.numRows() != fr1.numRows() )
          throw new IllegalArgumentException("Arrays must be same size: "+fr0+" vs "+fr1);
        fr = new Frame(fr0).add(fr1);
      } else {
        fr = fr0;
      }
    } else {
      ncols = fr1.numCols();
      fr = fr1;
    }
    final ASTBinOp bin = this;  // Final 'this' so can use in closure

    // Run an arbitrary binary op on one or two frames & scalars
    Frame fr2 = new MRTask2() {
        @Override public void map( Chunk chks[], NewChunk nchks[] ) {
          for( int i=0; i<nchks.length; i++ ) {
            NewChunk n =nchks[i];
            Chunk c0= !lf ? null : chks[i];
            Chunk c1= !rf ? null : chks[i+(lf?nchks.length:0)];
            int rlen = (lf ? c0 : c1)._len;
            for( int r=0; r<rlen; r++ )
              n.addNum(bin.op(lf ? c0.at0(r) : fd0, rf ? c1.at0(r) : fd1));
          }
        }
      }.doAll(ncols,fr).outputFrame(fr._names,fr.domains());
    if( fr0 != null ) env.subRef(fr0);
    if( fr1 != null ) env.subRef(fr1);
    env.pop();
    env.push(fr2);
  }
}
class ASTPlus extends ASTBinOp { String opStr(){ return "+"  ;} ASTOp make() {return new ASTPlus();} double op(double d0, double d1) { return d0+d1;}}
class ASTSub  extends ASTBinOp { String opStr(){ return "-"  ;} ASTOp make() {return new ASTSub ();} double op(double d0, double d1) { return d0-d1;}}
class ASTMul  extends ASTBinOp { String opStr(){ return "*"  ;} ASTOp make() {return new ASTMul ();} double op(double d0, double d1) { return d0*d1;}}
class ASTDiv  extends ASTBinOp { String opStr(){ return "/"  ;} ASTOp make() {return new ASTDiv ();} double op(double d0, double d1) { return d0/d1;}}
class ASTMin  extends ASTBinOp { String opStr(){ return "min";} ASTOp make() {return new ASTMin ();} double op(double d0, double d1) { return Math.min(d0,d1);}}

class ASTReduce extends ASTOp {
  static final String VARS[] = new String[]{ "", "op2", "ary"};
  static final Type   TYPES[]= new Type  []{ Type.ARY, Type.fcn(0,new Type[]{Type.DBL,Type.DBL,Type.DBL}), Type.ARY };
  ASTReduce( ) { super(VARS,TYPES); }
  @Override String opStr(){ return "Reduce";}
  @Override ASTOp make() {return this;}
  @Override void apply(Env env, int argcnt) { throw H2O.unimpl(); }
}

// Variable length; instances will be created of required length
class ASTCat extends ASTOp {
  @Override String opStr() { return "c"; }
  ASTCat( ) { super(new String[]{"cat","dbls"},
                    new Type[]{Type.ARY,Type.varargs(Type.DBL)}); }
  @Override ASTOp make() {return this;}
  @Override void apply(Env env, int argcnt) {
    AppendableVec av = new AppendableVec(Vec.newKey());
    NewChunk nc = new NewChunk(av,0);
    for( int i=0; i<argcnt-1; i++ )
      nc.addNum(env.dbl(-argcnt+1+i));
    nc.close(0,null);
    Vec v = av.close(null);
    env.pop(argcnt);
    env.push(new Frame(new String[]{"c"}, new Vec[]{v}));
  }
}

// Selective return
class ASTIfElse extends ASTOp {
  static final String VARS[] = new String[]{"ifelse","tst","true","false"};
  static Type[] newsig() {
    Type t1 = Type.unbound(0);
    return new Type[]{t1,Type.DBL,t1,t1};
  }
  ASTIfElse( ) { super(VARS, newsig()); }
  @Override ASTOp make() {return new ASTIfElse();}
  @Override String opStr() { return "ifelse"; }
  // Parse an infix trinary ?: operator
  static AST parse(Exec2 E, AST tst) {
    if( !E.peek('?') ) return null;
    int x=E._x;
    AST tru=E.xpeek(':',E._x,parseCXExpr(E));
    if( tru == null ) E.throwErr("Missing expression in trinary",x);
    x = E._x;
    AST fal=parseCXExpr(E);
    if( fal == null ) E.throwErr("Missing expression in trinary",x);
    return ASTApply.make(new AST[]{new ASTIfElse(),tst,tru,fal},E,x);
  }
  @Override void apply(Env env, int argcnt) { throw H2O.unimpl(); }
}

// --------------------------------------------------------------------------
// R's Apply.  Function is limited to taking a single column and returning
// a single column.  Double is limited to 1 or 2, statically determined.
class ASTRApply extends ASTOp {
  static final String VARS[] = new String[]{ "", "ary", "dbl1.2", "fun"};
  static final Type   TYPES[]= new Type  []{ Type.ARY, Type.ARY, Type.DBL, Type.fcn(0,new Type[]{Type.ARY,Type.ARY}) };
  ASTRApply( ) { super(VARS,TYPES); }
  @Override String opStr(){ return "apply";}
  @Override ASTOp make() {return this;}
  @Override void apply(Env env, int argcnt) {
    ASTOp op = (ASTOp)env.popFun();    // ary->ary but better be ary[,1]->ary[,1]
    double d = env.popDbl();
    Frame fr = env.popFrame();  // The Frame to work on
    if( d==2 || d== -1 ) {      // Work on columns

      throw H2O.unimpl();
    } else if( d==1 || d == -2 ) { // Work on rows
      throw H2O.unimpl();
    } else throw new IllegalArgumentException("MARGIN limited to 1 (rows) or 2 (cols)");
  }
}

