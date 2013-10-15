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
abstract class AST implements Cloneable {
  // Size, for compatible-shape checking.
  final int  _cols;
  final long _rows;
  AST( int cols, long rows ) { _cols=cols; _rows=rows; }
  static AST parseCXExpr(Exec2 E ) {
    AST ast2, ast = parseExpr(E);
    if( ast == null ) return null;
    // Can find an infix op between expressions
    if( (ast2 = ASTOp.parseInfix(E,ast)) != null ) return ast2;
    // Can find '=' between expressions
    if( (ast2 = ASTAssign.parse  (E,ast)) != null ) return ast2;
    if( ast.isScalar() && E.peek('?') ) { throw H2O.unimpl(); } // infix trinary
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
  abstract boolean isPure();  // Side-effect free
  void exec(Env env) { 
    System.out.println("Exec not impl for: "+getClass());
    throw H2O.unimpl(); 
  }
  public StringBuilder toString( StringBuilder sb, int d ) { return indent(sb,d).append(this); }
  public String dimStr() { return dimStr(_cols,_rows); }
  public static String dimStr(int col, long row) {
    if( col==0 && row==0 ) return "fun";
    assert col >=1 && row >= 1;
    return col+"x"+row;
  }
}

// --------------------------------------------------------------------------
class ASTKey extends AST {
  final Key _key;
  final AST _colsel, _rowsel; // Row-selection, Col-selection
  ASTKey( int cols, long rows, Key key, AST colsel, AST rowsel ) { 
    super(cols,rows); 
    _key=key;
    _colsel = colsel;  _rowsel = rowsel;
  }
  // Parse a valid H2O Frame Key, or return null;
  static ASTKey parse(Exec2 E) { 
    int x = E._x;
    String id = E.isID();
    if( id == null ) return null;
    Key key = Key.make(id);
    Iced ice = UKV.get(key);
    if( ice==null || !(ice instanceof Frame) ) { E._x = x; return null; }
    Frame fr = (Frame)ice;
    AST rows=null, cols=null;
    if( E.peek('[') ) {       // Subsets?
      rows= E.xpeek(',',(x=E._x),parseCXExpr(E));
      if( rows != null && rows._cols != 1 ) E.throwErr("Row select only a single column only",x);
      cols= E.xpeek(']',(x=E._x),parseCXExpr(E));
      if( cols != null && cols._cols != 1 ) E.throwErr("Col select only a single column only",x);
    } 
    return new ASTKey(cols==null?fr.numCols():(int)cols._rows,
                      rows==null?fr.numRows():     rows._rows,
                      key,cols,rows);
  }
  boolean isPure() { return true; }
  @Override public String toString() { return _key.toString(); }
  @Override public StringBuilder toString( StringBuilder sb, int d ) { 
    indent(sb,d).append(this);
    if( _colsel != null || _rowsel != null ) {
      sb.append("[,]\n");
      if( _rowsel == null ) indent(sb,d+1).append("all rows\n");
      else _rowsel.toString(sb,d+1).append('\n');
      if( _colsel == null ) indent(sb,d+1).append("all cols");
      else _colsel.toString(sb,d+1);
    }
    return sb;
  }
  //@Override Exec2 exec(Vec.VectorGroup vg) {
  //  if( _colsel != null ) _ecol = _colsel.exec(vg);
  //  if( _rowsel != null ) _erow = _rowsel.exec(vg);
  //  if( _ecol == null && _erow == null )
  //    return new Exec2((Frame)(DKV.get(_key).get()));
  //  if( _ecol._fr==null && _erow._fr==null )
  //    throw H2O.unimpl();  
  //  throw H2O.unimpl();  
  //}
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
  boolean isPure() { return true; }
  @Override void exec(Env env) { env.push(_d); }
  @Override public String toString() { return Double.toString(_d); }
  @Override public StringBuilder toString( StringBuilder sb, int d ) { return indent(sb,d).append(this); }
}

// --------------------------------------------------------------------------
class ASTApply extends AST {
  final AST _args[];
  private ASTApply( AST args[], int cols, long rows ) { super(cols,rows);  _args = args;  }

  // Wrap compatible but different-sized ops in reduce/bulk ops.
  static ASTApply make(AST args[],Exec2 E, int x) {
    //// 1-arg case; check for size-type operators
    //if( args.length==1 ) {
    //  if( this instanceof ASTNrow || this instanceof ASTNcol ) {
    //    ASTOp op = make(args,1,1); // Result is always a scalar
    //    if( !op.isPure() ) E.throwErr("nrow and ncol expressions cannot have side effects",x);
    //    return op;
    //  }
    //}
    // 2-arg case; check for compatible row counts.
    // Insert expansion operators as needed.
    if( args.length==2 ) {
      //  if( args[0]._rows != args[1]._rows ) {
      //    if( args[0]._rows==1 )      args[0] = new ASTByRow(args[0],args[1]._rows);
      //    else if( args[1]._rows==1 ) args[1] = new ASTByRow(args[1],args[0]._rows);
      //    else E.throwErr("Mismatch rows: "+args[0]._rows+" and "+args[1]._rows,x);
      //  }
      //  if( args[0]._cols != args[1]._cols ) {
      //    if( args[0]._cols==1 )      args[0] = new ASTByCol(args[0],args[1]._cols);
      //    else if( args[1]._cols==1 ) args[1] = new ASTByCol(args[1],args[0]._cols);
      //    else E.throwErr("Mismatch cols: "+args[0]._cols+" and "+args[1]._cols,x);
      //  }
      E.throwIfNotCompat(args[1],args[2],x);
    }

    ASTOp op = (ASTOp)args[0]; // Checked before I get here
    for( int i=0; i<op._vcols.length; i++ )
      if( op._vcols[i] != args[i+1]._cols ||
          op._vrows[i] != args[i+1]._rows )
        E.throwErr("Actual argument to '"+op._vars[i]+"' expected to be "+dimStr(op._vcols[i],op._vrows[i])+" but was "+args[i+1].dimStr(),x);

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
  //@Override Exec2 exec(Vec.VectorGroup vg) {
  //  _e2s = new Exec2[_args.length];
  //  for( int i=0; i<_args.length; i++ )
  //    _e2s[i] = _args[i].exec(vg);
  //  return null;
  //}
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
    put(new ASTCat  ());
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
  @Override boolean isPure() { return true; }
  @Override public String toString() { 
    String s = opStr()+"(";
    for( int i=0; i<_vars.length; i++ )
      s += dimStr(_vcols[i],_vrows[i])+" "+_vars[i]+",";
    s += ')';
    return s;
  }
  @Override public StringBuilder toString( StringBuilder sb, int d ) { 
    return indent(sb,d).append(this);
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
    throw H2O.unimpl();
    //int x = E._x;
    //ASTOp op = parse(E);
    //if( op == null ) return null;
    //// Fixed arg count
    //if( op._args!=null ) {
    //  AST args[] = new AST[op._args.length];
    //  E.xpeek('(',x,null);  
    //  for( int i=0; i<args.length-1; i++ )
    //    args[i] = E.xpeek(',',E._x,parseCXExpr(E));
    //  args[args.length-1]=parseCXExpr(E);
    //  return E.xpeek(')',E._x,op.make_rows(args,E,x));
    //}
    //// Variable arg cnt
    //E.xpeek('(',x,null);  
    //AST args[] = new AST[2];
    //int i=0;
    //while( true ) {
    //  args[i++] = parseCXExpr(E);
    //  if( E.peek(')') ) break;
    //  E.xpeek(',',E._x,null);
    //  if( i==args.length ) args = Arrays.copyOf(args,args.length<<1);
    //}
    //return op.make(Arrays.copyOf(args,i),1,i);
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
}
class ASTPlus extends ASTBinOp { @Override String opStr() { return "+"  ; }  }
class ASTSub  extends ASTBinOp { @Override String opStr() { return "-"  ; }  }
class ASTMul  extends ASTBinOp { @Override String opStr() { return "*"  ; }  }
class ASTDiv  extends ASTBinOp { @Override String opStr() { return "/"  ; }  }
class ASTMin  extends ASTBinOp { @Override String opStr() { return "min"; }  }

// Variable length; instances will be created of required length
class ASTCat extends ASTOp {
  @Override String opStr() { return "c"; }
  ASTCat( ) { super(-1,-1,null,null,null); }
}

