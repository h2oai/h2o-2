package water.exec;

import java.text.*;
import java.util.*;
import water.*;
import water.fvec.*;

/** Parse a generic R string and build an AST, in the context of an H2O Cloud
 *  @author cliffc@0xdata.com
 */

// --------------------------------------------------------------------------
abstract public class AST extends Iced {
  final transient Type _t;
  AST( Type t ) { assert t != null; _t = t; }
  static AST parseCXExpr(Exec2 E ) {
    AST ast2, ast = ASTSlice.parse(E);
    if( ast == null ) return ASTAssign.parseNew(E);
    // Can find an infix: {op expr}*
    if( (ast2 = ASTApply.parseInfix(E,ast)) != null ) return ast2;
    // Can find '=' between expressions
    if( (ast2 = ASTAssign.parse    (E,ast)) != null ) return ast2;
    // Infix trinay
    if( (ast2 = ASTIfElse.parse    (E,ast)) != null ) return ast2;
    return ast;                 // Else a simple slice/expr
  }

  static AST parseVal(Exec2 E ) {
    AST ast;
    // Simple paren expression
    if( E.peek('(') )  return E.xpeek(')',E._x,parseCXExpr(E));
    if( (ast = ASTId   .parse(E)) != null ) return ast;
    if( (ast = ASTNum  .parse(E)) != null ) return ast;
    if( (ast = ASTOp   .parse(E)) != null ) return ast;
    return null;
  }
  abstract void exec(Env env);
  boolean isPosConstant() { return false; }
  protected StringBuilder indent( StringBuilder sb, int d ) { 
    for( int i=0; i<d; i++ ) sb.append("  "); 
    return sb.append(_t).append(' ');
  }
  public StringBuilder toString( StringBuilder sb, int d ) { return indent(sb,d).append(this); }
}

// --------------------------------------------------------------------------
class ASTStatement extends AST {
  final AST[] _asts;
  ASTStatement( AST[] asts ) { super(asts[asts.length-1]._t); _asts = asts; }

  static ASTStatement parse( Exec2 E ) {
    ArrayList<AST> asts = new ArrayList<AST>();
    while( true ) {
      AST ast = parseCXExpr(E);
      if( ast == null ) break;
      asts.add(ast);
      if( !E.peek(';') ) break;
    }
    if( asts.size()==0 ) return null;
    return new ASTStatement(asts.toArray(new AST[asts.size()]));
  }
  void exec(Env env) { 
    for( int i=0; i<_asts.length-1; i++ ) {
      _asts[i].exec(env);       // Exec all statements
      env.pop();                // Pop all intermediate results
    } 
    _asts[_asts.length-1].exec(env); // Return final statement as result
  }
  @Override public String toString() { return ";;;"; }
  public StringBuilder toString( StringBuilder sb, int d ) {
    for( int i=0; i<_asts.length-1; i++ )
      _asts[i].toString(sb,d+1).append(";\n");
    return _asts[_asts.length-1].toString(sb,d+1);
  }
}

// --------------------------------------------------------------------------
class ASTApply extends AST {
  final AST _args[];
  private ASTApply( AST args[], int x ) { super(args[0]._t.ret());  _args = args;  }

  // Wrap compatible but different-sized ops in reduce/bulk ops.
  static ASTApply make(AST args[],Exec2 E, int x) {
    // Make a type variable for this application
    Type ts[] = new Type[args.length];
    ts[0] = Type.unbound(x);
    for( int i=1; i<ts.length; i++ )
      ts[i] = args[i]._t.find();
    Type ft1 = Type.fcn(x,ts);
    AST fast = args[0];
    Type ft2 = fast._t.find();  // Should be a function type
    if( ft1.union(ft2) )        // Union 'em
      return new ASTApply(args,x);
    // Error handling
    if( ft2.isNotFun() )      // Oops, failed basic sanity
      E.throwErr("Function-parens following a "+ft2,x);
    if( ft2._ts.length != ts.length )
      E.throwErr("Passed "+(ts.length-1)+" args but expected "+(ft2._ts.length-1),x);
    String vars[] = (fast instanceof ASTOp) ? ((ASTOp)fast)._vars : null;
    for( int i=1; i<ts.length; i++ )
      if( !ft2._ts[i].union(args[i]._t) )
        E.throwErr("Arg "+(vars==null?("#"+i):("'"+vars[i]+"'"))+" typed as "+ft2._ts[i]+" but passed "+args[i]._t.find(),x);
    throw H2O.fail();
  }

  // Parse a prefix operator
  static AST parsePrefix(Exec2 E) { 
    AST pre = parseVal(E);
    if( pre == null ) return null;
    while( true ) {
      if( pre._t.isNotFun() ) return pre; // Bail now if clearly not a function
      int x = E._x;
      if( !E.peek('(') ) return pre; // Plain op, no prefix application
      AST args[] = new AST[] { pre, null };
      int i=1;
      if( !E.peek(')') ) {
        while( true ) {
          args[i++] = parseCXExpr(E);
          if( E.peek(')') ) break;
          E.xpeek(',',E._x,null);
          if( i==args.length ) args = Arrays.copyOf(args,args.length<<1);
        }
      }
      args = Arrays.copyOf(args,i);
      pre = make(args,E,x);
    }
  }

  // Parse an infix boolean operator
  static AST parseInfix(Exec2 E, AST ast) {
    AST inf = null;
    while( true ) {
      ASTOp op = ASTOp.parse(E);
      if( op == null || op._vars.length != 3 ) return inf;
      int x = E._x;
      AST rite = ASTSlice.parse(E);
      if( rite==null ) E.throwErr("Missing expr or unknown ID",x);
      ast = inf = make(new AST[]{op,ast,rite},E,x);
    }
  }

  @Override public String toString() { return _args[0].toString()+"()"; }
  @Override public StringBuilder toString( StringBuilder sb, int d ) {
    _args[0].toString(sb,d).append("\n");
    for( int i=1; i<_args.length-1; i++ )
      _args[i].toString(sb,d+1).append('\n');
    return _args[_args.length-1].toString(sb,d+1);
  }
  // Apply: execute all arguments (including the function argument) yielding
  // the function itself, plus all normal arguments on the stack.  Then execute
  // the function, which is responsible for popping all arguments and pushing
  // the result.
  @Override void exec(Env env) {
    int sp = env._sp;
    for( AST arg : _args ) arg.exec(env);
    assert sp+_args.length==env._sp;
    env.fun(-_args.length).apply(env,_args.length);
  }
}

// --------------------------------------------------------------------------
class ASTSlice extends AST {
  final AST _ast, _cols, _rows; // 2-D slice of an expression
  ASTSlice( Type t, AST ast, AST cols, AST rows ) { 
    super(t); _ast = ast; _cols = cols; _rows = rows; 
  }
  static AST parse(Exec2 E ) {
    int x = E._x;
    AST ast = ASTApply.parsePrefix(E);
    if( ast == null ) return null;
    if( !E.peek('[') ) return ast; // No slice
    if( !Type.ARY.union(ast._t) ) E.throwErr("Not an ary",x);
    if(  E.peek(']') ) return ast; // [] ===> same as no slice
    AST rows=E.xpeek(',',(x=E._x),parseCXExpr(E));
    if( rows != null && !rows._t.union(Type.dblary()) ) E.throwErr("Must be scalar or array",x);
    AST cols=E.xpeek(']',(x=E._x),parseCXExpr(E));
    if( cols != null && !cols._t.union(Type.dblary()) ) E.throwErr("Must be scalar or array",x);
    Type t =                    // Provable scalars will type as a scalar
      rows != null && rows.isPosConstant() && 
      cols != null && cols.isPosConstant() ? Type.DBL : Type.ARY;
    return new ASTSlice(t,ast,cols,rows);
  }

  @Override void exec(Env env) {
    int sp = env._sp;  _ast.exec(env);  assert sp+1==env._sp;
    Frame fr=env.popFrame();

    // Scalar load?  Throws AIIOOB if out-of-bounds
    if( _t.isDbl() ) {
      // Known that rows & cols are simple positive constants.
      // Use them directly, throwing a runtime error if OOB.
      long row = (long)((ASTNum)_rows)._d;
      int  col = (int )((ASTNum)_cols)._d;
      double d = fr.vecs()[col].at(row);
      env.push(d);
    } else {
      // Else It's A Big Copy.  Some Day look at proper memory sharing,
      // disallowing unless an active-temp is available, etc.
      // Eval cols before rows (R's eval order).
      long cols[] = select(fr.numCols(),_cols,env);
      long rows[] = select(fr.numRows(),_rows,env);
      Frame fr2 = fr.deepSlice(rows,cols);
      env.push(fr2);
    }
  }
  @Override public String toString() { return "[,]"; }
  public StringBuilder toString( StringBuilder sb, int d ) {
    indent(sb,d).append(this).append('\n');
    _ast.toString(sb,d+1).append("\n");
    if( _cols==null ) indent(sb,d+1).append("all\n");
    else      _cols.toString(sb,d+1).append("\n");
    if( _rows==null ) indent(sb,d+1).append("all");
    else      _rows.toString(sb,d+1);
    return sb;
  }

  // Execute a col/row selection & return the selection.  NULL means "all".
  // Error to mix negatives & positive.  Negative list is sorted, with dups
  // removed.  Positive list can have dups (which replicates cols) and is
  // ordered.  numbers.  1-based numbering; 0 is ignored & removed.
  static long[] select( long len, AST ast, Env env ) {
    if( ast == null ) return null; // Trivial "all"
    int sp = env._sp;  ast.exec(env);  assert sp+1==env._sp;
    long cols[];
    if( !env.isFrame() ) {
      int col = (int)env.popDbl(); // Silent truncation
      if( col > 0 && col >  len ) throw new IllegalArgumentException("Trying to select column "+col+" but only "+len+" present.");
      if( col < 0 && col < -len ) col=0; // Ignore a non-existent column
      if( col == 0 ) return new long[0];
      return new long[]{col};
    }
    // Got a frame/list of results.
    // Decide if we're a toss-out or toss-in list
    Frame fr = env.popFrame();
    try {
      if( fr.numCols() > 1 ) throw new IllegalArgumentException("Selector must be a single column: "+fr);
      throw H2O.unimpl();
    } finally { env.subRef(fr); }
  }
}

// --------------------------------------------------------------------------
class ASTId extends AST {
  final String _id;
  final int _depth;             // *Relative* lexical depth of definition
  final int _num;               // Number/slot in the lexical scope
  ASTId( Type t, String id, int d, int n ) { super(t); _id=id; _depth=d; _num=n; }
  // Parse a valid ID, or return null;
  static ASTId parse(Exec2 E) { 
    int x = E._x;
    String var = E.isID();
    if( var == null ) return null;
    // Built-in ops parse as ops, not vars
    if( ASTOp.OPS.containsKey(var) ) { E._x=x; return null; }
    // See if pre-existing
    for( int d=E.lexical_depth(); d >=0; d-- )
      for( ASTId id : E._env.get(d) ) 
        if( var.equals(id._id) )
          // Return an ID with a relative lexical depth and same slot#
          return new ASTId(id._t,id._id,E.lexical_depth()-d,id._num);
    // Never see-before ID?  Treat as a bad parse
    E._x=x;
    return null;
  }
  // Parse a NEW valid ID, or return null;
  static String parseNew(Exec2 E) { 
    int x = E._x;
    String id = E.isID();
    if( id == null ) return null;
    // Built-in ops parse as ops, not vars
    if( ASTOp.OPS.containsKey(id) ) { E._x=x; return null; }
    return id;
  }
  @Override void exec(Env env) { env.push_slot(_depth,_num);  }
  @Override public String toString() { return _id; }
}

// --------------------------------------------------------------------------
class ASTAssign extends AST {
  final AST _lhs;
  final AST _eval;
  ASTAssign( AST lhs, AST eval ) { super(lhs._t); _lhs=lhs; _eval=eval; }
  // Parse a valid LHS= or return null
  static ASTAssign parse(Exec2 E, AST ast) { 
    int x = E._x;
    if( !E.peek('=') ) return null;
    AST ast2=ast;
    if( (ast instanceof ASTSlice) ) // Peek thru slice op
      ast2 = ((ASTSlice)ast)._ast;
    // Must be a simple in-scope ID
    if( !(ast2 instanceof ASTId) ) E.throwErr("Can only assign to ID (or slice)",x);
    AST eval = parseCXExpr(E);
    if( eval == null ) E.throwErr("Missing RHS",x);
    ASTId id = (ASTId)ast2;
    if( id._depth > 0 ) {       // Shadowing an outer scope?
      id = extend_local(E,eval._t,id._id);
      if( ast2 != ast ) throw H2O.unimpl(); // Must copy whole array locally, before updating the local copy
      else ast = id;
    } else if( !ast._t.union(eval._t) ) // Disallow type changes in local scope
      E.throwErr("Assigning a "+eval._t+" into '"+id._id+"' which is a "+id._t,x);
    return new ASTAssign(ast,eval);
  }
  // Parse a valid LHS= or return null - for a new variable
  static ASTAssign parseNew(Exec2 E) { 
    int x = E._x;
    String var = ASTId.parseNew(E);
    if( var == null ) return null;
    if( !E.peek('=') ) {        // Not an assignment
      if( Exec2.isLetter(var.charAt(0) ) ) E.throwErr("Unknown var "+var,x);
      E._x=x; return null;      // Let higher parse levels sort it out
    }
    x = E._x;
    AST eval = parseCXExpr(E);
    if( eval == null ) E.throwErr("Missing RHS",x);
    // Extend the local environment by the new name
    return new ASTAssign(extend_local(E,eval._t,var),eval);
  }
  static ASTId extend_local( Exec2 E, Type t, String var ) {
    ArrayList<ASTId> vars = E._env.get(E.lexical_depth());
    ASTId id = new ASTId(t,var,0,vars.size());
    vars.add(id);
    return id;
  }

  @Override void exec(Env env) {
    _eval.exec(env);            // RHS before LHS (R eval order)
    if( _lhs instanceof ASTId ) {
      ASTId id = (ASTId)_lhs;
      env.tos_into_slot(id._depth,id._num);
      return;
    }
    // Peel apart a slice assignment
    ASTSlice slice = (ASTSlice)_lhs;
    ASTId id = (ASTId)slice._ast;
    // Typed as a double ==> the row & col selectors are simple constants
    if( slice._t == Type.DBL ) { // Typed as a double?
      double d = env.popDbl();   // Only allows double into a double
      long row = (long)((ASTNum)slice._rows)._d;
      int  col = (int )((ASTNum)slice._cols)._d;
      env.frId(id._depth,id._num).vecs()[col].set(row,d);
      env.push(d);
      return;
    }

    // Slice assignment
    throw H2O.unimpl(); 
  }
  @Override public String toString() { return "="; }
  @Override public StringBuilder toString( StringBuilder sb, int d ) { 
    indent(sb,d).append(this).append('\n');
    _lhs.toString(sb,d+1).append('\n');
    _eval.toString(sb,d+1);
    return sb;
  }
}

// --------------------------------------------------------------------------
class ASTNum extends AST {
  static final NumberFormat NF = NumberFormat.getInstance();
  static { NF.setGroupingUsed(false); }
  final double _d;
  ASTNum(double d) { super(Type.DBL); _d=d; }
  // Parse a number, or throw a parse error
  static ASTNum parse(Exec2 E) { 
    ParsePosition pp = new ParsePosition(E._x);
    Number N = NF.parse(E._str,pp);
    if( pp.getIndex()==E._x ) return null;
    assert N instanceof Double || N instanceof Long;
    E._x = pp.getIndex();
    double d = (N instanceof Double) ? (double)(Double)N : (double)(Long)N;
    return new ASTNum(d);
  }
  boolean isPosConstant() { return _d >= 0; }
  @Override void exec(Env env) { env.push(_d); }
  @Override public String toString() { return Double.toString(_d); }
}

// --------------------------------------------------------------------------
abstract class ASTOp extends AST {
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
    String s = opStr()+"(";
    for( int i=1; i<_vars.length; i++ )
      s += _vars[i]+",";
    s += ')';
    return s;
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

  @Override void exec(Env env) { 
    env.push(this); 
  }
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
    env.pop();                  // Pop self
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
      }.doAll(fr.numCols(),fr)._outputFrame;
    env.push(fr.copyHeaders(fr2,null));
  }
}

class ASTIsNA extends ASTUniOp { String opStr(){ return "is.na"; } ASTOp make() {return new ASTIsNA();} double op(double d) { return Double.isNaN(d)?1:0;}}
class ASTSgn  extends ASTUniOp { String opStr(){ return "sgn" ; } ASTOp make() {return new ASTSgn ();} double op(double d) { return Math.signum(d);}}
class ASTNrow extends ASTUniOp { 
  ASTNrow() { super(VARS,new Type[]{Type.DBL,Type.ARY}); }
  @Override String opStr() { return "nrow"; }  
  @Override ASTOp make() {return this;} 
  @Override void apply(Env env, int argcnt) { env.poppush(env.popFrame().numRows());  }
}
class ASTNcol extends ASTUniOp { 
  ASTNcol() { super(VARS,new Type[]{Type.DBL,Type.ARY}); }
  @Override String opStr() { return "ncol"; }  
  @Override ASTOp make() {return this;} 
  @Override void apply(Env env, int argcnt) { env.poppush(env.popFrame().numCols());  }
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
    env.pop();                  // Pop self
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
      }.doAll(ncols,fr)._outputFrame;
    env.push(fr.copyHeaders(fr2,null));
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

// --------------------------------------------------------------------------
class ASTFunc extends ASTOp {
  final AST _body;
  final int _tmps;
  ASTFunc( String vars[], Type vtypes[], AST body, int tmps ) { super(vars,vtypes); _body = body; _tmps=tmps; }
  @Override String opStr() { return "fun"; }
  @Override ASTOp make() { throw H2O.fail();} 
  static ASTOp parseFcn(Exec2 E ) {
    int x = E._x;
    String var = E.isID();
    if( var == null ) return null;
    if( !"function".equals(var) ) { E._x = x; return null; }
    E.xpeek('(',E._x,null);
    ArrayList<ASTId> vars = new ArrayList<ASTId>();
    if( !E.peek(')') ) {
      while( true ) {
        x = E._x;
        var = E.isID();
        if( var == null ) E.throwErr("Invalid var",x);
        for( ASTId id : vars ) if( var.equals(id._id) ) E.throwErr("Repeated argument",x);
        // Add unknown-type variable to new vars list
        vars.add(new ASTId(Type.unbound(x),var,0,vars.size()));
        if( E.peek(')') ) break;
        E.xpeek(',',E._x,null);
      }
    }
    int argcnt = vars.size();   // Record current size, as body may extend
    // Parse the body
    E.xpeek('{',(x=E._x),null);
    E._env.push(vars);
    AST body = E.xpeek('}',E._x,ASTStatement.parse(E));
    if( body == null ) E.throwErr("Missing function body",x);
    E._env.pop();

    // The body should force the types.  Build a type signature.
    String xvars[] = new String[argcnt+1];
    Type   types[] = new Type  [argcnt+1];
    xvars[0] = "fun";
    types[0] = body._t;         // Return type of body
    for( int i=0; i<argcnt; i++ ) {
      ASTId id = vars.get(i);
      xvars[i+1] = id._id;
      types[i+1] = id._t;
    }
    return new ASTFunc(xvars,types,body,vars.size()-argcnt);
  }  
  @Override void apply(Env env, int argcnt) { 
    int res_idx = env.pushScope(argcnt-1);
    env.push(_tmps);
    _body.exec(env);
    env.tos_into_slot(1,res_idx-1);
    env.popScope();
  }
  @Override public StringBuilder toString( StringBuilder sb, int d ) {
    indent(sb,d).append(this).append(") {\n");
    _body.toString(sb,d+1).append("\n");
    return indent(sb,d).append("}");
  }
}
