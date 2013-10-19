package water.exec;

import java.text.*;
import java.util.Arrays;
import java.util.HashMap;
import water.*;
import water.fvec.*;

/** Execute a generic R string, in the context of an H2O Cloud
 *  @author cliffc@0xdata.com
 */
public class Exec2 {
  // Parse a string, execute it & return a Frame.
  // Grammer:
  //   statements := cxexpr ; statements
  //   cxexpr :=                   // COMPLEX expr 
  //           key_slice = cxexpr  // subset assignment; must be equal shapes 
  //           id = cxexpr         // temp typed assignment; dropped when scope exits
  //           expr
  //           expr op2 cxexpr     // apply(op2,expr,cxexpr); ....optional INFIX notation
  //           expr0 ? expr : expr // exprs must have *compatible* shapes
  //   expr :=                     // expr is a Frame, a 2-d table
  //           num                 // Scalars, treated as 1x1
  //           id                  // any visible var; will be typed
  //           key_slice           // Rectangular R-value
  //           function(v0,v1,v2) { statements; ...v0,v1,v2... } // 1st-class lexically scoped functions
  //           ( cxexpr )          // Ordering evaluation
  //           ifelse(expr0,cxexpr,cxexpr)  // exprs must have *compatible* shapes
  //           apply(op,cxexpr,...)// Apply function op to args
  //   key_slice :=
  //           key                 // A Frame, dimensions stored in K/V already
  //           key [expr1,expr1]   // slice rows & cols by index
  //           key [expr1,expr1]   // subset assignment of *same* shape
  //           key [,expr1]        // subset assignment of *same* shape
  //           key [expr1,]        // subset assignment of *same* shape
  //   key  := any Key mapping to a Frame.

  //   func1:= {id -> expr0}     // user function; id will be a scalar in expr0
  //   op1  := func1 sgn sin cos ...any unary op...
  //   func2:= {id,id -> expr0}  // user reduction function; id will be a scalar in expr0
  //   op2  := func2 min max + - * / % & |    ...any boolean op...
  //   func3:= {id -> expr1}     // id will be an expr1
  //
  //   same shape == same rows, same cols
  //   compatible shape == same shape, or (1 row x same cols), (same rows x 1 cols), or 1x1
  //
  // Example: Compute mean for each col:
  //    means = apply1(+,fr)/nrows(fr)
  // Example: Replace NA's with 0:
  //    {x -> isna(x) ? 0 : x}(fr)
  // Example: Replace NA's with mean:
  //    apply1({col -> mean=apply1(+,col)/nrows(col); apply1({x->isna(x)?mean:x},col) },fr)

  public static Frame exec( String str ) throws IllegalArgumentException {
    AST ast = new Exec2(str).parse();
    System.out.println(ast.toString(new StringBuilder(),0).toString());
    Vec.VectorGroup vg = new Vec.VectorGroup();
    Exec2 res = ast.exec(vg);
    if( res._fr != null ) return res._fr;
    return new Frame(new String[]{"result"},
                     new Vec[]{new Vec(vg.addVecs(1)[0],res._d)});
  }

  private Exec2( String str ) { _str = str; _buf = str.toCharArray(); }
  private Exec2( double d ) { _str=null; _buf=null; _d=d; }
  private Exec2( Frame fr ) { _str=null; _buf=null; _fr=fr; }
  // Simple parser state
  final String _str;
  final char _buf[];
  int _x;
  // Simple execution state
  Frame _fr;                    // For  Big   Data
  double _d;                    // For little Data
  AST _fun;                     // 1st-class Function-as-data
  
  private AST parse() { 
    AST ast = AST.parseCXExpr(this); 
    skipWS();                   // No trailing crud
    return _x == _buf.length ? ast : throwErr("Junk at end of line",_buf.length-1);
  }

  // --------------------------------------------------------------------------
  private void skipWS() {
    while( _x < _buf.length && _buf[_x] <= ' ' )  _x++;
  }
  // Skip whitespace.
  // If c is the next char, eat it & return true
  // Else return false.
  private boolean peek(char c) {
    if( _x ==_buf.length ) return false;
    while( _buf[_x] <= ' ' )
      if( ++_x ==_buf.length ) return false;
    if( _buf[_x]!=c ) return false;
    _x++;
    return true;
  }
  // Same as peek, but throw if char not found  
  private AST xpeek(char c, int x, AST ast) { return peek(c) ? ast : throwErr("Missing '"+c+"'",x); }

  // Return an ID string, or null if we get weird stuff or numbers.  Valid IDs
  // include all the operators, except parens (function application) and assignment.
  // Valid IDs: +   ++   <=  > ! [ ] joe123 ABC 
  // Invalid  : +++ 0joe ( = ) 123.45 1e3
  private String isID() {
    skipWS();
    if( _x>=_buf.length ) return null; // No characters to parse
    char c = _buf[_x];
    // Fail on special chars in the grammer
    if( isReserved(c) ) return null;
    // Fail on leading numeric
    if( isDigit(c) ) return null;
    _x++;                       // Accept parse of 1 char

    // If first char is letter, standard ID
    if( isLetter(c) ) {
      int x=_x-1;               // start of ID
      while( _x < _buf.length && isLetter2(_buf[_x]) )
        _x++;
      return _str.substring(x,_x);
    }

    // If first char is special, accept 1 or 2 special chars
    if( _x>=_buf.length ) return _str.substring(_x-1,_x);
    char c2=_buf[_x];
    if( isDigit(c2) || isLetter(c2) || isWS(c2) || isReserved(c2) ) return _str.substring(_x-1,_x);
    _x++;
    return _str.substring(_x-2,_x);
  }

  private static boolean isDigit(char c) { return c>='0' && c<= '9'; }
  private static boolean isWS(char c) { return c<=' '; }
  private static boolean isReserved(char c) { return c=='(' || c==')' || c=='='; }
  private static boolean isLetter(char c) { return (c>='a'&&c<='z') || (c>='A' && c<='Z');  }
  private static boolean isLetter2(char c) { 
    if( c=='.' || c==':' || c=='\\' || c=='/' ) return true;
    if( isDigit(c) ) return true;
    return isLetter(c);
  }

  // --------------------------------------------------------------------------
  abstract static private class AST implements Cloneable {
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
    Exec2 exec(Vec.VectorGroup vg) { 
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
  static private class ASTKey extends AST {
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
  static private class ASTAssign extends AST {
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
  static private class ASTNum extends AST {
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
    @Override Exec2 exec(Vec.VectorGroup vg) { return new Exec2(_d); }
    @Override public String toString() { return Double.toString(_d); }
    @Override public StringBuilder toString( StringBuilder sb, int d ) { return indent(sb,d).append(this); }
  }

  // --------------------------------------------------------------------------
  static private class ASTApply extends AST {
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
  abstract static private class ASTOp extends AST {
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


  abstract static private class ASTUniOp extends ASTOp {
    static final String VARS[] = new String[]{"x"};
    static final int    COLS[] = new int   []{ 1 };
    static final long   ROWS[] = new long  []{ 1 };
    ASTUniOp( ) { super(1,1,VARS,COLS,ROWS); }
  }
  static private class ASTIsNA extends ASTUniOp { @Override String opStr() { return "isNA"; }  }
  static private class ASTSgn  extends ASTUniOp { @Override String opStr() { return "sgn" ; }  }
  static private class ASTNrow extends ASTUniOp { @Override String opStr() { return "nrow"; }  }
  static private class ASTNcol extends ASTUniOp { @Override String opStr() { return "ncol"; }  }

  abstract static private class ASTBinOp extends ASTOp {
    static final String VARS[] = new String[]{"x","y"};
    static final int    COLS[] = new int   []{ 1 , 1 };
    static final long   ROWS[] = new long  []{ 1 , 1 };
    ASTBinOp( ) { super(1,1,VARS,COLS,ROWS); }
  }
  static private class ASTPlus extends ASTBinOp { @Override String opStr() { return "+"  ; }  }
  static private class ASTSub  extends ASTBinOp { @Override String opStr() { return "-"  ; }  }
  static private class ASTMul  extends ASTBinOp { @Override String opStr() { return "*"  ; }  }
  static private class ASTDiv  extends ASTBinOp { @Override String opStr() { return "/"  ; }  }
  static private class ASTMin  extends ASTBinOp { @Override String opStr() { return "min"; }  }

  // Variable length; instances will be created of required length
  static private class ASTCat extends ASTOp {
    @Override String opStr() { return "c"; }
    ASTCat( ) { super(-1,-1,null,null,null); }
  }

  // --------------------------------------------------------------------------
  private boolean throwIfNotCompat(AST l, AST r, int idx ) {
    assert l._rows != -1 && r._rows != -1 && l._cols != -1 && r._cols != -1;
    if( !(l._rows==1 || r._rows==1 || l._rows==r._rows) ||
        !(l._cols==1 || r._cols==1 || l._cols==r._cols) )  
      throwErr("Frames not compatible: "+l.dimStr()+" vs "+r.dimStr(),idx);
    return true;
  }

  // Nicely report a syntax error
  private AST throwErr( String msg, int idx ) {
    int lo = _x, hi=idx;
    if( idx < _x ) { lo = idx; hi=_x; }
    String s = msg+" @ "+lo;
    if( lo != hi ) s += "-"+hi;
    s += '\n'+_str+'\n';
    int i;
    for( i=0; i<lo; i++ ) s+= ' ';
    s+='^'; i++;
    for( ; i<hi; i++ ) s+= '-';
    if( i<=hi ) s+= '^';
    s += '\n';
    throw new IllegalArgumentException(s);
  }
}
