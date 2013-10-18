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
  enum Type { fun, ary, dbl }
  final Type _t;
  AST( Type t ) { _t = t; }
  static AST parseCXExpr(Exec2 E ) {
    AST ast2, ast = ASTSlice.parse(E);
    if( ast == null ) return ASTAssign.parseNew(E);
    // Can find '=' between expressions
    if( (ast2 = ASTAssign.parse     (E,ast)) != null ) return ast2;
    // Op followed by ()
    if( (ast2 = ASTApply.parsePrefix(E,ast)) != null ) return ast2;
    // Can find an infix op between expressions
    if( (ast2 = ASTApply.parseInfix (E,ast)) != null ) return ast2;
    if( E.peek('?') ) { throw H2O.unimpl(); } // infix trinary
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
  boolean isPure() { return true; } // Side-effect free
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
  @Override boolean isPure() { throw H2O.unimpl(); }

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
class ASTSlice extends AST {
  final AST _ast, _cols, _rows; // 2-D slice of an expression
  ASTSlice( AST ast, AST cols, AST rows ) { 
    super(Type.ary); _ast = ast; _cols = cols; _rows = rows; 
  }
  static AST parse(Exec2 E ) {
    int x = E._x;
    AST ast = parseVal(E);
    if( ast == null ) return null;
    if( !E.peek('[') ) return ast; // No slice
    if( ast._t == null ) ast = ((ASTId)ast).setOnUse(E,Type.ary);
    if( ast._t != Type.ary ) E.throwErr("Not an ary",x);
    if(  E.peek(']') ) return ast; // [] ===> same as no slice
    AST rows=E.xpeek(',',E._x,parseCXExpr(E));
    AST cols=E.xpeek(']',E._x,parseCXExpr(E));
    return new ASTSlice(ast,cols,rows);
  }

  @Override boolean isPure() {
    return _ast.isPure() && 
      (_cols==null || _cols.isPure()) &&
      (_rows==null || _rows.isPure());
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
    return new ASTId(null,id,E.lexical_depth()-1);
  }
  ASTId setOnUse( Exec2 E, Type t ) {
    assert _t == null;          // Currently an untyped variable
    HashMap<String,Type> vars = E._env.get(_depth);
    assert vars.containsKey(_id) && vars.get(_id)==null;
    vars.put(_id,t);            // Set type on 1st use
    return new ASTId(t,_id,_depth);
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
    AST eval = parseCXExpr(E);
    return new ASTAssign(ast,eval);
  }
  // Parse a valid LHS= or return null - for a new variable
  static ASTAssign parseNew(Exec2 E) { 
    int x = E._x;
    ASTId id = ASTId.parseNew(E);
    if( id == null ) return null;
    if( !E.peek('=') ) { E._x=x; return null; }
    assert id._t==null;         // It is a new var so untyped
    assert id._depth == E.lexical_depth()-1; // And will be set in the current scope
    x = E._x;
    AST eval = parseCXExpr(E);
    id = new ASTId(eval._t,id._id,id._depth);
    // Extend the local environment by the new name
    E._env.get(id._depth).put(id._id,id._t);
    return new ASTAssign(id,eval);
  }
  boolean isPure() { return false; }
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
  ASTNum(double d) { super(Type.dbl); _d=d; }
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
class ASTApply extends AST {
  final AST _args[];
  private ASTApply( AST args[] ) { super(((ASTOp)args[0])._vtypes[0]);  _args = args;  }

  // Wrap compatible but different-sized ops in reduce/bulk ops.
  static ASTApply make(AST args[],Exec2 E, int x) {
    ASTOp op = (ASTOp)args[0]; // Checked before I get here
    assert op._vtypes.length == args.length;

    // Check that all arguments match, or can be auto-expanded.  Any op taking
    // a scalar and passed an array will be auto-expanded.
    for( int i=1; i<args.length; i++ ) {
      Type ot = op._vtypes[i];
      if( ot == Type.fun ) {
        System.out.println(op._vars[i]+" isa? "+args[i]);
        throw H2O.unimpl();     // Deep function type checking
      }
      if( args[i]._t == null )
        args[i] = ((ASTId)args[i]).setOnUse(E,ot);
      if( ot == args[i]._t ) continue; // both scalar or both array
      if( ot != Type.dbl )
        E.throwErr("Arg '"+op._vars[i]+"' "+(ot==null?"untyped":" typed as "+ot)+" but passed "+args[i]._t,x);
      // Expansion needed, will insert in a later pass
    }
    return new ASTApply(args);
  }

  // Parse a prefix operator
  static AST parsePrefix(Exec2 E, AST ast) { 
    if( !(ast instanceof ASTOp) ) return null;
    ASTOp op = (ASTOp)ast;
    int x = E._x;
    if( !E.peek('(') ) return null; // Plain op, no prefix application
    AST args[] = new AST[] { op, null };
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
    if( op._vars!=null ) {      // Check fixed-arg length
      if( i != op._vars.length ) 
        E.throwErr("Expected "+(op._vars.length-1)+" args but found "+(i-1),x);
    } else op.set_varargs_types(args);
    return make(args,E,x);
  }

  // Parse an infix boolean operator
  static AST parseInfix(Exec2 E, AST ast) { 
    ASTOp op = ASTOp.parse(E);
    if( op == null ) return null;
    if( op._vars.length != 3 ) return null;
    int x = E._x;
    AST rite = parseCXExpr(E);
    if( rite==null ) E.throwErr("Missing expr or unknown ID",x);
    return make(new AST[]{op,ast,rite},E,x);
  }

  boolean isPure() {
    for( AST arg : _args )
      if( !arg.isPure() ) return false;
    return true;
  }
  @Override public String toString() { return _args[0].toString()+"()"; }
  @Override public StringBuilder toString( StringBuilder sb, int d ) {
    if( _args[0] instanceof ASTFunc ) _args[0].toString(sb,d).append("()\n");
    else indent(sb,d).append(this).append("\n");
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

    // Variable argcnt
    put(new ASTCat ());
    put(new ASTByCol());
  }
  static private void put(ASTOp ast) { OPS.put(ast.opStr(),ast); }

  // All fields are final, because functions are immutable
  final String _vars[];         // Variable names
  final Type   _vtypes[];       // Every arg is typed
  ASTOp( String vars[], Type vtypes[] ) { 
    super(Type.fun);  _vars = vars;  _vtypes = vtypes;
  }

  abstract String opStr();
  @Override public String toString() { 
    String s = _vtypes[0]+" "+opStr()+"(";
    for( int i=1; i<_vars.length; i++ )
      s += _vtypes[i]+" "+_vars[i]+",";
    s += ')';
    return s;
  }

  // Parse an OP or return null.
  static ASTOp parse(Exec2 E) {
    int x = E._x;
    String id = E.isID();
    if( id == null ) return null;
    ASTOp op = OPS.get(id);
    if( op != null ) return op;
    E._x = x;                 // Roll back, no parse happened
    // Attempt a user-mode function parse
    return ASTFunc.parseFcn(E);
  }
  AST[] set_varargs_types(AST args[]) { throw H2O.fail(); }

  @Override void exec(Env env) { env.push(this); }
  void apply(Env env) {
    System.out.println("Apply not impl for: "+getClass());
  }
}

abstract class ASTUniOp extends ASTOp {
  static final String VARS[] = new String[]{ "", "x"};
  static final Type   TYPES[]= new Type  []{ Type.dbl, Type.dbl };
  ASTUniOp( ) { super(VARS,TYPES); }
}
class ASTIsNA extends ASTUniOp { @Override String opStr() { return "isNA"; }  }
class ASTSgn  extends ASTUniOp { @Override String opStr() { return "sgn" ; }  }
class ASTNrow extends ASTUniOp { @Override String opStr() { return "nrow"; }  }
class ASTNcol extends ASTUniOp { @Override String opStr() { return "ncol"; }  }

abstract class ASTBinOp extends ASTOp {
  static final String VARS[] = new String[]{ "", "x","y"};
  static final Type   TYPES[]= new Type  []{ Type.dbl, Type.dbl, Type.dbl };
  ASTBinOp( ) { super(VARS,TYPES); }
  abstract double op( double d0, double d1 );
  @Override void apply(Env env) {
    double d1 = env.popDbl();
    double d0 = env.popDbl();
    env.poppush(op(d0,d1));
  }
}
class ASTPlus extends ASTBinOp { @Override String opStr(){ return "+"  ;} double op(double d0, double d1) { return d0+d1;}}
class ASTSub  extends ASTBinOp { @Override String opStr(){ return "-"  ;} double op(double d0, double d1) { return d0-d1;}}
class ASTMul  extends ASTBinOp { @Override String opStr(){ return "*"  ;} double op(double d0, double d1) { return d0*d1;}}
class ASTDiv  extends ASTBinOp { @Override String opStr(){ return "/"  ;} double op(double d0, double d1) { return d0/d1;}}
class ASTMin  extends ASTBinOp { @Override String opStr(){ return "min";} double op(double d0, double d1) { return Math.min(d0,d1);}}

class ASTByCol extends ASTOp {
  static final String VARS[] = new String[]{ "", "ary","op2"};
  static final Type   TYPES[]= new Type  []{ Type.ary, Type.ary, Type.fun };
  ASTByCol( ) { super(VARS,TYPES); }
  @Override String opStr(){ return "byCol";}
}


// Variable length; instances will be created of required length
class ASTCat extends ASTOp {
  @Override String opStr() { return "c"; }
  ASTCat( ) { super(null,null); }
  private ASTCat( String[] vars, Type[] types ) { super(vars,types); }
  // Make a custom-typed function with explicit types for the varargs
  @Override AST[] set_varargs_types(AST args[]) {
    String[] vars  = new String[args.length];
    Type  [] types = new Type  [args.length];
    Arrays.fill(vars,"x");
    Arrays.fill(types,Type.dbl);
    vars [0] = opStr();
    types[0] = Type.ary;        // Always array-type result
    args[0] = new ASTCat(vars,types);
    return args;
  }
}

// --------------------------------------------------------------------------
class ASTFunc extends ASTOp {
  AST _body;
  ASTFunc( String vars[], Type vtypes[], AST body ) { super(vars,vtypes); _body = body; }
  @Override String opStr() { return "fun"; }
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
        vars.put(id,null);        // Add unknown-type variable to new vars list
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
    for( int j=1; j<xvars.length; j++ ) {
      types[j] = vars.get(xvars[j]);
      if( types[j]==null ) System.out.println("Warning: var "+xvars[j]+" failed to get typed.");
    }
    return new ASTFunc(xvars,types,body);
  }  
  @Override public StringBuilder toString( StringBuilder sb, int d ) {
    indent(sb,d).append(this).append(") {\n");
    _body.toString(sb,d+1).append("\n");
    return indent(sb,d).append("}");
  }
}
