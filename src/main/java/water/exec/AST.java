package water.exec;

import java.text.*;
import java.util.*;
import water.*;
import water.fvec.*;

/** Parse a generic R string and build an AST, in the context of an H2O Cloud
 *  @author cliffc@0xdata.com
 */

// --------------------------------------------------------------------------
abstract public class AST {
  final Type _t;
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

  protected StringBuilder indent( StringBuilder sb, int d ) { 
    for( int i=0; i<d; i++ ) sb.append("  "); 
    return sb.append(_t).append(' ');
  }
  void exec(Env env) { 
    System.out.println("Exec not impl for: "+getClass());
    throw H2O.unimpl(); 
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
      ts[i] = args[i]._t;
    Type ft1 = Type.fcn(x,ts);
    AST fast = args[0];
    Type ft2 = fast._t;         // Should be a function type
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
        E.throwErr("Arg "+(vars==null?("#"+i):("'"+vars[i]+"'"))+" typed as "+ft2._ts[i]+" but passed "+args[i]._t,x);
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
    assert env.isFun(-_args.length);
    env.fun(-_args.length).apply(env);
  }
}

// --------------------------------------------------------------------------
class ASTSlice extends AST {
  final AST _ast, _cols, _rows; // 2-D slice of an expression
  ASTSlice( AST ast, AST cols, AST rows ) { 
    super(Type.ARY); _ast = ast; _cols = cols; _rows = rows; 
  }
  static AST parse(Exec2 E ) {
    int x = E._x;
    AST ast = ASTApply.parsePrefix(E);
    if( ast == null ) return null;
    if( !E.peek('[') ) return ast; // No slice
    if( !Type.ARY.union(ast._t) ) E.throwErr("Not an ary",x);
    if(  E.peek(']') ) return ast; // [] ===> same as no slice
    AST rows=E.xpeek(',',E._x,parseCXExpr(E));
    AST cols=E.xpeek(']',E._x,parseCXExpr(E));
    return new ASTSlice(ast,cols,rows);
  }

  @Override void exec(Env env) {
    int sp = env._sp;
    _ast.exec(env);
    assert sp+1==env._sp;
    Frame fr=env.popFrame();
  
    // Column subselection?
    int cols[];
    if( _cols != null ) {
      _cols.exec(env);
      assert sp+1==env._sp;
      if( !env.isFrame() ) {
        int col = (int)env.popDbl(); // Silent truncation
        cols = new int[]{col};
      } else {
        throw H2O.unimpl();
      }

      // Decide if we're a toss-out or toss-in list
      int mode=0;
      for( int c : cols ) {
        if( c==0 ) continue;
        if( mode==0 ) mode=c;
        if( (mode^c) < 0 ) 
          throw new IllegalArgumentException("Cannot mix selection signs: "+mode+" vs "+c);
        if( c < 0 ) throw H2O.unimpl();
      }

    } else {
      cols = new int[fr.numCols()];
      for( int i=0; i<cols.length; i++ ) cols[i]=i;
    }
    throw H2O.unimpl();

    //// Shallow copy all requested columns
    //// ...really: if allowed, we can make as many Vec copies as
    //// requested... and can make more later even in the same
    //
    //
    //Frame fr2 = new Frame();
    //for( int i=0; i<cols.length; i++ )
    //  if( cols[i]>0 && cols[i] < fr.numCols() )
    //    fr2.add(fr._names[cols[i]],fr.vecs()[cols[i]]);
    //
    //// Row subselection? 
    //long rows[] = null;
    //if( _rows != null ) throw H2O.unimpl();
    //
    //Frame tmp = env.tmp(fr2);   // My Handy Temp
    //
    //// Bulk (expensive) copy into the temp
    //new DeepSlice(cols,rows).doAll(new Frame(tmp).add(fr));
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
  
  //// Bulk (expensive) copy from 2nd cols into 1st cols.
  //// Sliced by the given cols & rows
  //private static class DeepSlice extends MRTask2<DeepSlice> {
  //  final int  _cols[];
  //  final long _rows[];
  //  DeepSlice( int cols[], long rows[] ) { _cols=cols; _rows=rows; }
  //  @Override public void map( Chunk chks[] ) {
  //    if( _rows != null ) throw H2O.unimpl();
  //    for( int i=0; i<_cols.length; i++ ) {
  //      throw H2O.unimpl();
  //    }
  //  }
  //}
}

// --------------------------------------------------------------------------
class ASTId extends AST {
  final String _id;
  final int _depth;             // Lexical depth of definition
  ASTId( Type t, String id, int d ) { super(t); _id=id; _depth=d; }
  // Parse a valid ID, or return null;
  static ASTId parse(Exec2 E) { 
    int x = E._x;
    String id = E.isID();
    if( id == null ) return null;
    // Built-in ops parse as ops, not vars
    if( ASTOp.OPS.containsKey(id) ) { E._x=x; return null; }
    // See if pre-existing
    for( int d=E._env.size()-1; d >=0; d-- ) {
      HashMap<String,Type> vars = E._env.get(d);
      if( !vars.containsKey(id) ) continue;
      return new ASTId(vars.get(id),id,d);
    }
    // Never see-before ID?  Treat as a bad parse
    E._x=x;
    return null;
  }
  // Parse a NEW valid ID, or return null;
  static ASTId parseNew(Exec2 E) { 
    int x = E._x;
    String id = E.isID();
    if( id == null ) return null;
    // Built-in ops parse as ops, not vars
    if( ASTOp.OPS.containsKey(id) ) { E._x=x; return null; }
    return new ASTId(Type.unbound(x)/*untyped as of yet*/,id,E.lexical_depth());
  }
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
    ASTId id = (ASTId)ast2;
    if( id._depth < E.lexical_depth() ) { // Shadowing an outer scope?
      id = new ASTId(Type.unbound(x),id._id,E.lexical_depth());
      // Extend the local environment by the new name
      E._env.get(id._depth).put(id._id,id._t); 
    }
    x = E._x;
    AST eval = parseCXExpr(E);
    if( !id._t.union(eval._t) ) // Disallow type changes in local scope
      E.throwErr("Assigning a "+eval._t+" into '"+id._id+"' which is a "+id._t,x);
    return new ASTAssign(ast,eval);
  }
  // Parse a valid LHS= or return null - for a new variable
  static ASTAssign parseNew(Exec2 E) { 
    int x = E._x;
    ASTId id = ASTId.parseNew(E);
    if( id == null ) return null;
    if( !E.peek('=') ) {        // Not an assignment
      if( E.isLetter(id._id.charAt(0) ) ) E.throwErr("Unknown id "+id._id,x);
      E._x=x; return null;      // Let higher parse levels sort it out
    }
    assert id._t._t==0;         // It is a new var so untyped
    assert id._depth == E.lexical_depth(); // And will be set in the current scope
    x = E._x;
    AST eval = parseCXExpr(E);
    id = new ASTId(eval._t,id._id,id._depth);
    // Extend the local environment by the new name
    E._env.get(id._depth).put(id._id,id._t);
    return new ASTAssign(id,eval);
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

  @Override void exec(Env env) { env.push(this); }
  void apply(Env env) {
    System.out.println("Apply not impl for: "+getClass());
  }
}

abstract class ASTUniOp extends ASTOp {
  static final String VARS[] = new String[]{ "", "x"};
  static Type[] newsig() {
    Type t1 = Type.dblary();
    return new Type[]{Type.anyary(new Type[]{t1}),t1};
  }
  ASTUniOp( ) { super(VARS,newsig()); }
  protected ASTUniOp( String[] vars, Type[] types ) { super(vars,types); }
}
class ASTIsNA extends ASTUniOp { String opStr(){ return "isNA"; } ASTOp make() {return new ASTIsNA();} }
class ASTSgn  extends ASTUniOp { String opStr(){ return "sgn" ; } ASTOp make() {return new ASTSgn ();} }
class ASTNrow extends ASTUniOp { 
  ASTNrow() { super(VARS,new Type[]{Type.DBL,Type.ARY}); }
  @Override String opStr() { return "nrow"; }  
  @Override ASTOp make() {return this;} 
}
class ASTNcol extends ASTUniOp { 
  ASTNcol() { super(VARS,new Type[]{Type.DBL,Type.ARY}); }
  @Override String opStr() { return "ncol"; }  
  @Override ASTOp make() {return this;} 
}

abstract class ASTBinOp extends ASTOp {
  static final String VARS[] = new String[]{ "", "x","y"};
  static Type[] newsig() {
    Type t1 = Type.dblary(), t2 = Type.dblary();
    return new Type[]{Type.anyary(new Type[]{t1,t2}),t1,t2};
  }
  ASTBinOp( ) { super(VARS, newsig()); }
  abstract double op( double d0, double d1 );
  @Override void apply(Env env) {
    double d1 = env.popDbl();
    double d0 = env.popDbl();
    env.poppush(op(d0,d1));
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
}

// Variable length; instances will be created of required length
class ASTCat extends ASTOp {
  @Override String opStr() { return "c"; }
  ASTCat( ) { super(new String[]{"cat","dbls"},
                    new Type[]{Type.ARY,Type.varargs(Type.DBL)}); }
  @Override ASTOp make() {return this;} 
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
}


// --------------------------------------------------------------------------
class ASTFunc extends ASTOp {
  AST _body;
  ASTFunc( String vars[], Type vtypes[], AST body ) { super(vars,vtypes); _body = body; }
  @Override String opStr() { return "fun"; }
  @Override ASTOp make() { throw H2O.fail();} 
  static ASTOp parseFcn(Exec2 E ) {
    int x = E._x;
    String id = E.isID();
    if( id == null ) return null;
    if( !"function".equals(id) ) { E._x = x; return null; }
    E.xpeek('(',E._x,null);
    LinkedHashMap<String,Type> vars = new LinkedHashMap<String,Type>();
    if( !E.peek(')') ) {
      while( true ) {
        x = E._x;
        id = E.isID();
        if( id == null ) E.throwErr("Invalid id",x);
        if( vars.containsKey(id) ) E.throwErr("Repeated argument",x);
        vars.put(id,Type.unbound(x)); // Add unknown-type variable to new vars list
        if( E.peek(')') ) break;
        E.xpeek(',',E._x,null);
      }
    }
    // Build a signature.
    String xvars[] = new String[vars.size()+1];
    int i=0;   xvars[i++] = "fun";
    for( String id2 : vars.keySet() )  xvars[i++] = id2;

    E.xpeek('{',(x=E._x),null);
    E._env.push(vars);
    AST body = E.xpeek('}',E._x,ASTStatement.parse(E));
    if( body == null ) E.throwErr("Missing function body",x);
    E._env.pop();

    // The body should force the types.  Build a type signature.
    Type types[] = new Type[xvars.length];
    types[0] = body._t;         // Return type of body
    for( int j=1; j<xvars.length; j++ )
      types[j] = vars.get(xvars[j]);
    return new ASTFunc(xvars,types,body);
  }  
  @Override public StringBuilder toString( StringBuilder sb, int d ) {
    indent(sb,d).append(this).append(") {\n");
    _body.toString(sb,d+1).append("\n");
    return indent(sb,d).append("}");
  }
}
