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
  // Size, for compatible-shape checking.
  final int  _cols;
  final long _rows;
  AST( int cols, long rows ) { _cols=cols; _rows=rows; }
  static AST parseCXExpr(Exec2 E ) {
    AST ast2, ast = ASTSlice.parse(E);
    if( ast == null ) return null;
    // Can find '=' between expressions
    if( (ast2 = ASTAssign.parse  (E,ast)) != null ) return ast2;
    // Can find an infix op between expressions
    if( (ast2 = ASTOp.parseInfix(E,ast)) != null ) return ast2;
    if( E.peek('?') ) { throw H2O.unimpl(); } // infix trinary
    return ast;
  }

  static AST parseExpr(Exec2 E ) {
    AST ast;
    // Simple paren expression
    if( E.peek('(') )  return E.xpeek(')',E._x,parseCXExpr(E));
    if( (ast = ASTKey.parse(E)) != null ) return ast;
    if( (ast = ASTNum.parse(E)) != null ) return ast;
    if( (ast = ASTOp .parsePrefix(E)) != null ) return ast;
    return null;
  }
  protected StringBuilder indent( StringBuilder sb, int d ) { 
    for( int i=0; i<d; i++ ) sb.append("  "); 
    return sb.append(dimStr()).append(' ');
  }
  boolean isScalar() { return _cols==1 && _rows==1; }
  boolean isPure() { return true; } // Side-effect free
  void exec(Env env) { 
    System.out.println("Exec not impl for: "+getClass());
    throw H2O.unimpl(); 
  }
  public StringBuilder toString( StringBuilder sb, int d ) { return indent(sb,d).append(this); }
  public String dimStr() { return dimStr(_cols,_rows); }
  public static String dimStr(int col, long row) {
    if( col==0 && row==0 ) return "fun";
    return (col>=1?""+col:"?")+"x"+(row>=1?""+row:"?");
  }
}

// --------------------------------------------------------------------------
class ASTSlice extends AST {
  final AST _ast, _colsel, _rowsel; // 2-D slice of an expression
  ASTSlice( int cols, long rows, AST ast, AST colsel, AST rowsel ) { 
    super(cols,rows); 
    _ast = ast; _colsel = colsel; _rowsel = rowsel; 
  }
  static AST parse(Exec2 E ) {
    AST ast = parseExpr(E);
    if( ast == null ) return null;
    if( !E.peek('[') ) return ast; // No slice
    if(  E.peek(']') ) return ast; // [] ===> same as no slice
    int x;
    AST rows=E.xpeek(',',(x=E._x),parseExpr(E));
    long nrows = rows == null ? ast._rows :      peek(rows,ast._rows,E,x);
    AST cols=E.xpeek(']',(x=E._x),parseExpr(E));
    int  ncols = cols == null ? ast._cols : (int)peek(cols,ast._cols,E,x);
    return new ASTSlice(ncols,nrows,ast,cols,rows);
  }

  // Peek into constant expressions & return count of selected items
  private static long peek( AST ast, long len, Exec2 E, int x ) {
    if( ast._cols != 1 ) E.throwErr("Slice selector can only be a single column",x);
    if( ast instanceof ASTNum ) {
      long d = (long)((ASTNum)ast)._d;
      if( d < -len ) return len;  // Exclude nothing
      if( d <   0  ) return len-1;// Exclude one thing
      if( d ==  0  ) return  0;   // Select nothing
      if( d <  len ) return  1;   // Select 1 thing
      E.throwErr("Select off end: "+d,x);
    }
    // Else check on group selections
    throw H2O.unimpl();
  }

  @Override boolean isPure() {
    return _ast.isPure() && 
      (_colsel==null ? true : _colsel.isPure()) &&
      (_rowsel==null ? true : _rowsel.isPure());
  }
  @Override void exec(Env env) {
    int sp = env._sp;
    _ast.exec(env);
    assert sp+1==env._sp;
    Frame fr=null; double d;
    if( env.isFrame() ) fr=env.popFrame(); else d = env.popDbl();

    // Column subselection?
    if( _colsel != null ) {
      assert _colsel._cols == 1 && _colsel._rows > 0; // parser only allows 1-col results
      _colsel.exec(env);
      assert sp+1==env._sp;
      Frame cfr=null; int cd;
      if( env.isFrame() ) cfr=env.popFrame(); else cd = (int)env.popDbl();
      if( cfr==null ) {
        throw H2O.unimpl();
        
      } else {
        throw H2O.unimpl();
      }
    }
    if( _rowsel != null ) throw H2O.unimpl();
    throw H2O.unimpl();
  }
  @Override public String toString() { return "[,]"; }
  public StringBuilder toString( StringBuilder sb, int d ) {
    indent(sb,d).append(this).append('\n');
    _ast.toString(sb,d+1).append("\n");
    if( _colsel==null ) indent(sb,d+1).append("all\n");
    else      _colsel.toString(sb,d+1).append("\n");
    if( _rowsel==null ) indent(sb,d+1).append("all");
    else      _rowsel.toString(sb,d+1);
    return sb;
  }
}

// --------------------------------------------------------------------------
class ASTKey extends AST {
  final Key _key;
  ASTKey( int cols, long rows, Key key ) { super(cols,rows); _key=key; }
  // Parse a valid H2O Frame Key, or return null;
  static ASTKey parse(Exec2 E) { 
    int x = E._x;
    String id = E.isID();
    if( id == null ) return null;
    Key key = Key.make(id);
    Iced ice = UKV.get(key);
    if( ice==null || !(ice instanceof Frame) ) { E._x = x; return null; }
    Frame fr = (Frame)ice;
    return new ASTKey(fr.numCols(),fr.numRows(),key);
  }
  @Override public String toString() { return _key.toString(); }
  @Override void exec(Env env) { env.push((Frame)(DKV.get(_key).get())); }
}

// --------------------------------------------------------------------------
class ASTAssign extends AST {
  final ASTKey _key;
  final AST _eval;
  ASTAssign( ASTKey key, AST eval ) { 
    super(key._cols,key._rows);
    _key=key; _eval=eval;
  }
  // Parse a valid H2O Frame Key, or return null;
  static ASTAssign parse(Exec2 E, AST ast) { 
    if( !(ast instanceof ASTKey) ) return null;
    if( !E.peek('=') ) return null;
    int x = E._x;
    AST eval = parseCXExpr(E);
    E.throwIfNotCompat(ast,eval,x);
    return new ASTAssign((ASTKey)ast,eval);
  }
  boolean isPure() { return false; }
  @Override public String toString() { return "="; }
  @Override public StringBuilder toString( StringBuilder sb, int d ) { 
    indent(sb,d).append(this).append('\n');
    _key.toString(sb,d+1).append('\n');
    _eval.toString(sb,d+1);
    return sb;
  }
}

// --------------------------------------------------------------------------
class ASTNum extends AST {
  static final NumberFormat NF = NumberFormat.getInstance();
  static { NF.setGroupingUsed(false); }
  final double _d;
  ASTNum(double d ) { super(1,1); _d=d; }
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
  private ASTApply( AST args[], int cols, long rows ) { super(cols,rows);  _args = args;  }

  // Wrap compatible but different-sized ops in reduce/bulk ops.
  static ASTApply make(AST args[],Exec2 E, int x) {
    ASTOp op = (ASTOp)args[0]; // Checked before I get here
    assert op._vcols.length+1 == args.length;

    // Check that all column arguments match, or can be auto-expanded.  Any op
    // taking a single column and passed multiple columns will be auto-expanded.
    int col = -1;               // Expansion size, if needed
    for( int i=1; i<args.length; i++ ) {
      if( args[i]._cols == op._vcols[i-1] ) continue;
      if( col == -1 ) col = args[i]._cols;
      if( op._vcols[i-1] != 1 || col != args[i]._cols )
        E.throwErr("Mismatched cols in '"+op._vars[i]+"': "+op._vcols[i-1]+" vs "+args[i].dimStr(),x);
    }

    // Check that all row arguments match, or can be auto-expanded.  Any op
    // taking a single row and passed multiple rows will be auto-expanded.
    long row = -1;              // Expansion size, if needed
    for( int i=1; i<args.length; i++ ) {
      if( args[i]._rows == op._vrows[i-1] ) continue;
      if( row == -1 ) row = args[i]._rows;
      if( op._vrows[i-1] != 1 || row != args[i]._rows )
        E.throwErr("Mismatched rows in '"+op._vars[i]+"': "+op._vrows[i-1]+" vs "+args[i].dimStr(),x);
    }

    // Auto-expand simple scalar ops across columns.  Replace {op,args...}
    // with {byCol,col,op,args...}
    if( col != -1 || row != -1 ) {
      throw H2O.unimpl();
    }

    return new ASTApply(args,args[0]._cols,args[0]._rows);
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
  final String _vars[];       // Variable names
  final int   _vcols[];       // Every var is size-typed
  final long  _vrows[];
  ASTOp( int cols, long rows, String vars[], int vcols[], long vrows[] ) { 
    super(cols,rows);           // Result size
    // The input args
    _vars = vars;  _vcols = vcols;  _vrows = vrows;
  }

  abstract String opStr();
  @Override public String toString() { 
    String s = opStr()+"(";
    for( int i=0; i<_vars.length; i++ )
      s += dimStr(_vcols[i],_vrows[i])+" "+_vars[i]+",";
    s += ')';
    return s;
  }

  // Parse an OP or return null.
  private static ASTOp parse(Exec2 E) {
    int x = E._x;
    String id = E.isID();
    if( id == null ) return null;
    ASTOp op = OPS.get(id);
    if( op != null ) return op;
    E._x = x;                 // Roll back, no parse happened
    return null;
  }

  // Parse a prefix operator
  static AST parsePrefix(Exec2 E) { 
    int x = E._x;
    ASTOp op = parse(E);
    if( op == null ) return null;
    // Fixed arg count
    if( op._vars!=null ) {
      AST args[] = new AST[op._vars.length+1];
      E.xpeek('(',x,null);  
      args[0] = op;
      for( int i=1; i<args.length-1; i++ )
        args[i] = E.xpeek(',',E._x,parseCXExpr(E));
      args[args.length-1]=parseCXExpr(E);
      return E.xpeek(')',E._x,ASTApply.make(args,E,x));
    }
    // Variable arg cnt
    throw H2O.unimpl();
    //E.xpeek('(',x,null);  
    //AST args[] = new AST[2];
    //int i=0;
    //while( true ) {
    //  args[i++] = parseCXExpr(E);
    //  if( E.peek(')') ) break;
    //  E.xpeek(',',E._x,null);
    //  if( i==args.length ) args = Arrays.copyOf(args,args.length<<1);
    //}
    //return ASTApply.make(Arrays.copyOf(args,i),1,i);
  }

  // Parse an infix boolean operator
  static ASTApply parseInfix(Exec2 E, AST ast) { 
    ASTOp op = parse(E);
    if( op == null ) return null;
    if( op._vars.length != 2 ) return null;
    int x = E._x;
    AST rite = parseCXExpr(E);
    return ASTApply.make(new AST[]{op,ast,rite},E,x);
  }

  @Override void exec(Env env) { env.push(this); }
  void apply(Env env) {
    System.out.println("Apply not impl for: "+getClass());
  }
}


abstract class ASTUniOp extends ASTOp {
  static final String VARS[] = new String[]{"x"};
  static final int    COLS[] = new int   []{ 1 };
  static final long   ROWS[] = new long  []{ 1 };
  ASTUniOp( ) { super(1,1,VARS,COLS,ROWS); }
}
class ASTIsNA extends ASTUniOp { @Override String opStr() { return "isNA"; }  }
class ASTSgn  extends ASTUniOp { @Override String opStr() { return "sgn" ; }  }
class ASTNrow extends ASTUniOp { @Override String opStr() { return "nrow"; }  }
class ASTNcol extends ASTUniOp { @Override String opStr() { return "ncol"; }  }

abstract class ASTBinOp extends ASTOp {
  static final String VARS[] = new String[]{"x","y"};
  static final int    COLS[] = new int   []{ 1 , 1 };
  static final long   ROWS[] = new long  []{ 1 , 1 };
  ASTBinOp( ) { super(1,1,VARS,COLS,ROWS); }
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
  ASTCat( ) { super(-1,-1,null,null,null); }
}

// Iterate a function across columns
class ASTByCol extends ASTOp {
  static final String VARS[] = new String[]{"op","fr","x"};
  // Each ByCol is type-size matched to it's parsed frame expression.
  ASTByCol( ASTOp op, AST fr ) {
    super(fr._cols,fr._rows,VARS, new int []{0,fr._cols,1}, new long[]{0,fr._rows,1});
  }
  @Override String opStr() { return "byCol"; }
  @Override void apply(Env env) {
    throw H2O.unimpl();
  }
}

// Iterate a function down rows
class ASTByRow extends ASTOp {
  static final String VARS[] = new String[]{"op","col","x"};
  // Each ByRow is type-size matched to it's parsed frame expression.
  ASTByRow( ASTOp op, AST fr ) {
    super(1,fr._rows,VARS, new int []{0,1,1}, new long[]{0,fr._rows,1});
  }
  @Override String opStr() { return "byRow"; }
  @Override void apply(Env env) {
    throw H2O.unimpl();
  }
}
