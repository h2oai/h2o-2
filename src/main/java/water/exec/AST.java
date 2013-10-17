package water.exec;

import java.text.*;
import java.util.Arrays;
import java.util.HashMap;
import water.*;
import water.fvec.*;

/** Parse a generic R string and build an AST, in the context of an H2O Cloud
 *  @author cliffc@0xdata.com
 */

// --------------------------------------------------------------------------
abstract public class AST {
  enum Type { fun, ary, dbl };
  final Type _t;
  AST( Type t ) { _t = t; }
  static AST parseCXExpr(Exec2 E ) {
    AST ast2, ast = ASTSlice.parse(E);
    if( ast == null ) return null;
    // Can find '=' between expressions
    if( (ast2 = ASTAssign.parse  (E,ast)) != null ) return ast2;
    // Can find an infix op between expressions
    if( (ast2 = ASTApply.parseInfix(E,ast)) != null ) return ast2;
    if( E.peek('?') ) { throw H2O.unimpl(); } // infix trinary
    return ast;
  }

  static AST parseExpr(Exec2 E ) {
    AST ast;
    // Simple paren expression
    if( E.peek('(') )  return E.xpeek(')',E._x,parseCXExpr(E));
    if( (ast = ASTKey.parse(E)) != null ) return ast;
    if( (ast = ASTNum.parse(E)) != null ) return ast;
    if( (ast = ASTApply.parsePrefix(E)) != null ) return ast;
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
class ASTSlice extends AST {
  final AST _ast, _cols, _rows; // 2-D slice of an expression
  ASTSlice( AST ast, AST cols, AST rows ) { 
    super(Type.ary); _ast = ast; _cols = cols; _rows = rows; 
  }
  static AST parse(Exec2 E ) {
    AST ast = parseExpr(E);
    if( ast == null ) return null;
    if( ast._t != Type.ary ) return ast; // No slice allowed unless known frame
    if( !E.peek('[') ) return ast; // No slice
    if(  E.peek(']') ) return ast; // [] ===> same as no slice
    int x;
    AST rows=E.xpeek(',',(x=E._x),parseCXExpr(E));
    AST cols=E.xpeek(']',(x=E._x),parseCXExpr(E));
    return new ASTSlice(ast,cols,rows);
  }

  @Override boolean isPure() {
    return _ast.isPure() && 
      (_cols==null ? true : _cols.isPure()) &&
      (_rows==null ? true : _rows.isPure());
  }
  @Override void exec(Env env) {
    int sp = env._sp;
    _ast.exec(env);
    assert sp+1==env._sp;
    Frame fr=env.popFrame();
  
    // Column subselection?
    int cols[] = null;
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
      for( int i=0; i<cols.length; i++ ) {
        int c = cols[i];
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
class ASTKey extends AST {
  final Key _key;
  ASTKey( Key key ) { super(Type.ary); _key=key; }
  // Parse a valid H2O Frame Key, or return null;
  static ASTKey parse(Exec2 E) { 
    int x = E._x;
    String id = E.isID();
    if( id == null ) return null;
    Key key = Key.make(id);
    Iced ice = UKV.get(key);
    if( ice==null || !(ice instanceof Frame) ) { E._x = x; return null; }
    return new ASTKey(key);
  }
  @Override public String toString() { return _key.toString(); }
  @Override void exec(Env env) { env.push((Frame)(DKV.get(_key).get())); }
}

// --------------------------------------------------------------------------
class ASTAssign extends AST {
  final AST _lhs;
  final AST _eval;
  ASTAssign( AST lhs, AST eval ) { super(Type.ary); _lhs=lhs; _eval=eval; }
  // Parse a valid LHS= or return null
  static ASTAssign parse(Exec2 E, AST ast) { 
    if( !E.peek('=') ) return null;
    AST ast2=ast;
    if( (ast instanceof ASTSlice) ) // Peek thru slice op
      ast2 = ((ASTSlice)ast)._ast;
    // Must be a simple in-scope ID
    if( !(ast2 instanceof ASTKey) )  return null;
    int x = E._x;
    AST eval = parseCXExpr(E);
    return new ASTAssign(ast,eval);
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
      if( op._vtypes[i] == Type.fun ) {
        System.out.println(op._vars[i]+" isa? "+args[i]);
        
        throw H2O.unimpl();     // Deep function type checking
      }
      if( op._vtypes[i] == args[i]._t ) continue; // both scalar or both array
      if( op._vtypes[i] != Type.dbl )
        E.throwErr("Mismatched arg: '"+op._vars[i]+"': "+op._vtypes[i]+" vs "+args[i]._t,x);
      // Expansion needed, will insert in a later pass
    }

    return new ASTApply(args);
  }

  // Parse a prefix operator
  static AST parsePrefix(Exec2 E) { 
    int x = E._x;
    ASTOp op = ASTOp.parse(E);
    if( op == null ) return null;
    if( !E.peek('(') ) return null; // Plain op, no prefix application
    // Fixed arg count
    if( op._vars!=null ) {
      AST args[] = new AST[op._vars.length];
      args[0] = op;
      for( int i=1; i<args.length-1; i++ )
        args[i] = E.xpeek(',',E._x,parseCXExpr(E));
      args[args.length-1]=parseCXExpr(E);
      return E.xpeek(')',E._x,ASTApply.make(args,E,x));
    }
    // Variable arg cnt
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
    return make(op.set_varargs_types(Arrays.copyOf(args,i)),E,x);
  }

  // Parse an infix boolean operator
  static AST parseInfix(Exec2 E, AST ast) { 
    ASTOp op = ASTOp.parse(E);
    if( op == null ) return null;
    if( op._vars.length != 3 ) return null;
    int x = E._x;
    AST rite = parseCXExpr(E);
    return make(new AST[]{op,ast,rite},E,x);
  }

  boolean isPure() {
    for( AST arg : _args )
      if( !arg.isPure() ) return false;
    return true;
  }
  @Override public String toString() { return _args[0].toString()+"()"; }
  @Override public StringBuilder toString( StringBuilder sb, int d ) { 
    indent(sb,d).append("apply(").append(_args[0]).append(")\n");
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
    return null;
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
