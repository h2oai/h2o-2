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
  //   cxexpr :=                   // COMPLEX expr 
  //           key_slice = cxexpr  // subset assignment; must be equal shapes 
  //           expr
  //           expr op2 cxexpr     // apply all; ....optional INFIX notation
  //           expr0 ? expr : expr // exprs must have *compatible* shapes
  //   expr :=                     // expr is a Frame, a 2-d table
  //           num | id            // Scalars, treated as 1x1
  //           ( cxexpr )          // Ordering evaluation
  //           op1(cxexpr)         // apply op1 to all elements
  //           op2(cxexpr,cxexpr)  // apply op2 to all; exprs must have *compatible* shapes
  //           apply1(op,cxexpr)   // for-all-cols in expr, apply op to col
  //           apply1(op2,cxexpr)  // reduce cols; result is size 1xN
  //           apply2(op2,cxexpr)  // reduce rows; result is size NX1
  //           apply (op2,cxexpr)  // reduce all; result is size 1x1
  //           op2(cxexpr)         // reduce all; ....optional notation
  //           ifelse(expr0,cxexpr,cxexpr)  // exprs must have *compatible* shapes
  //           key_slice           // R-value
  //   key_slice:
  //           key                 // A Frame, dimensions stored in K/V already
  //           key [expr1,expr1]   // slice rows & cols by index
  //           key [expr1,expr1]   // subset assignment of *same* shape
  //           key [,expr1]        // subset assignment of *same* shape
  //           key [expr1,]        // subset assignment of *same* shape

  //   key  := any Key mapping to a Frame.
  //   
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
    return null;
  }

  private Exec2( String str ) { _str = str; _buf = str.toCharArray(); }
  final String _str;
  final char _buf[];
  int _x;
  
  private AST parse() { 
    AST ast = AST.parseCXExpr(this); 
    // Now EOL
    skipWS();
    if( _x < _buf.length ) 
      throwErr("Junk at end of line",_buf.length-1);
    return ast;
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
  private AST xpeek(char c, int x, AST ast) {
    if( !peek(c) ) throwErr("Missing close-paren",x);
    return ast;
  }

  // Return an ID string, or null if we get weird stuff or numbers.
  // Valid IDs: + ++ <= > ! [ ] joe123 ABC 
  // Invalid: +++ 0joe ( =
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

    // If first char is special, accept 1 or 2 specials
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
      sb.append(_rows).append('x').append(_cols).append(' ');
      return sb;
    }
    boolean isScalar() { return _cols==1 && _rows==1; }
    abstract boolean isPure();  // Side-effect free
    public StringBuilder toString( StringBuilder sb, int d ) { return indent(sb,d).append(this); }
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
    @Override public String toString() { return Double.toString(_d); }
    @Override public StringBuilder toString( StringBuilder sb, int d ) { return indent(sb,d).append(this); }
  }

  // --------------------------------------------------------------------------
  abstract static private class ASTOp extends AST {
    static final HashMap<String,ASTOp> OPS = new HashMap();
    static {
      // Unary ops
      put(new ASTIsNA (new AST[1],-1,-1));
      put(new ASTSgn  (new AST[1],-1,-1));
      put(new ASTNrows(new AST[1],-1,-1));
      put(new ASTNcols(new AST[1],-1,-1));

      // Binary ops
      put(new ASTPlus (new AST[2],-1,-1));
      put(new ASTSub  (new AST[2],-1,-1));
      put(new ASTMul  (new AST[2],-1,-1));
      put(new ASTDiv  (new AST[2],-1,-1));
      put(new ASTMin  (new AST[2],-1,-1));

      // Variable argcnt
      put(new ASTCat  (   null   ,-1,-1));
    }
    static private void put(ASTOp ast) { OPS.put(ast.opStr(),ast); }
    final AST _args[];
    ASTOp( AST args[], int cols, long rows ) { super(cols,rows);   _args = args;  }
    abstract String opStr();
    abstract ASTOp make(AST args[],int cols, long rows);

    // Wrap compatible but different-sized ops in reduce/bulk ops.
    ASTOp make_rows(AST args[],Exec2 E, int x) {
      if( args.length > 2 ) throw H2O.unimpl();
      // 1-arg case; check for size-type operators
      if( args.length==1 ) {
        if( this instanceof ASTNrows || this instanceof ASTNcols ) {
          ASTOp op = make(args,1,1); // Result is always a scalar
          if( !op.isPure() ) E.throwErr("nrows and ncols expressions cannot have side effects",x);
          return op;
        }
      }
      // 2-arg case; check for compatible row counts.
      // Insert expansion operators as needed.
      if( args.length==2 ) {
        if( args[0]._rows != args[1]._rows ) {
          if( args[0]._rows==1 )      args[0] = new ASTByRow(args[0],args[1]._rows);
          else if( args[1]._rows==1 ) args[1] = new ASTByRow(args[1],args[0]._rows);
          else E.throwErr("Mismatch rows: "+args[0]._rows+" and "+args[1]._rows,x);
        }
        if( args[0]._cols != args[1]._cols ) {
          if( args[0]._cols==1 )      args[0] = new ASTByCol(args[0],args[1]._cols);
          else if( args[1]._cols==1 ) args[1] = new ASTByCol(args[1],args[0]._cols);
          else E.throwErr("Mismatch cols: "+args[0]._cols+" and "+args[1]._cols,x);
        }
        E.throwIfNotCompat(args[0],args[1],x);
      }
      return make(args,args[0]._cols,args[0]._rows);
    }

    boolean isPure() {
      for( AST arg : _args )
        if( !arg.isPure() ) return false;
      return true;
    }
    @Override public String toString() { return opStr(); }
    @Override public StringBuilder toString( StringBuilder sb, int d ) { 
      indent(sb,d).append(this).append('\n');
      for( int i=0; i<_args.length-1; i++ )
        _args[i].toString(sb,d+1).append('\n');
      return _args[_args.length-1].toString(sb,d+1);
    }

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
      if( op._args!=null ) {
        AST args[] = new AST[op._args.length];
        E.xpeek('(',x,null);  
        for( int i=0; i<args.length-1; i++ )
          args[i] = E.xpeek(',',E._x,parseCXExpr(E));
        args[args.length-1]=parseCXExpr(E);
        return E.xpeek(')',E._x,op.make_rows(args,E,x));
      }
      // Variable arg cnt
      E.xpeek('(',x,null);  
      AST args[] = new AST[2];
      int i=0;
      while( true ) {
        args[i++] = parseCXExpr(E);
        if( E.peek(')') ) break;
        E.xpeek(',',E._x,null);
        if( i==args.length ) args = Arrays.copyOf(args,args.length<<1);
      }
      return op.make(Arrays.copyOf(args,i),1,i);
    }

    // Parse an infix boolean operator
    static AST parseInfix(Exec2 E, AST ast) { 
      ASTOp op = parse(E);
      if( op == null ) return null;
      if( op._args==null || op._args.length != 2 ) return null;
      int x = E._x;
      AST rite = parseCXExpr(E);
      return op.make_rows(new AST[]{ast,rite},E,x);
    }
  }


  static private class ASTByRow extends ASTOp {
    @Override String opStr() { return "byRow"; }
    ASTByRow( AST arg, long rows ) { super(new AST[]{arg},arg._cols,rows); }
    @Override ASTOp make( AST args[], int cols, long rows ) { throw H2O.fail(); }
  }
  static private class ASTByCol extends ASTOp {
    @Override String opStr() { return "byCol"; }
    ASTByCol( AST arg, int cols ) { super(new AST[]{arg},cols,arg._rows); }
    @Override ASTOp make( AST args[], int cols, long rows ) { throw H2O.fail(); }
  }

  static private class ASTIsNA extends ASTOp {
    @Override String opStr() { return "isNA"; }
    ASTIsNA( AST args[], int cols, long rows ) { super(args,cols,rows); }
    @Override ASTOp make( AST args[], int cols, long rows ) { return new ASTIsNA(args,cols,rows); }
  }
  static private class ASTSgn extends ASTOp {
    @Override String opStr() { return "sgn"; }
    ASTSgn( AST args[], int cols, long rows ) { super(args,cols,rows); }
    @Override ASTOp make( AST args[], int cols, long rows ) { return new ASTSgn(args,cols,rows); }
  }
  static private class ASTNrows extends ASTOp {
    @Override String opStr() { return "nrows"; }
    ASTNrows( AST args[], int cols, long rows ) { super(args,cols,rows); }
    @Override ASTOp make( AST args[], int cols, long rows ) { return new ASTNrows(args,cols,rows); }
  }
  static private class ASTNcols extends ASTOp {
    @Override String opStr() { return "ncols"; }
    ASTNcols( AST args[], int cols, long rows ) { super(args,cols,rows); }
    @Override ASTOp make( AST args[], int cols, long rows ) { return new ASTNcols(args,cols,rows); }
  }
  static private class ASTPlus extends ASTOp {
    @Override String opStr() { return "+"; }
    ASTPlus( AST args[], int cols, long rows ) { super(args,cols,rows); }
    @Override ASTOp make( AST args[], int cols, long rows ) { return new ASTPlus(args,cols,rows); }
  }
  static private class ASTSub extends ASTOp {
    @Override String opStr() { return "-"; }
    ASTSub( AST args[], int cols, long rows ) { super(args,cols,rows); }
    @Override ASTOp make( AST args[], int cols, long rows ) { return new ASTSub(args,cols,rows); }
  }
  static private class ASTMul extends ASTOp {
    @Override String opStr() { return "*"; }
    ASTMul( AST args[], int cols, long rows ) { super(args,cols,rows); }
    @Override ASTOp make( AST args[], int cols, long rows ) { return new ASTMul(args,cols,rows); }
  }
  static private class ASTDiv extends ASTOp {
    @Override String opStr() { return "/"; }
    ASTDiv( AST args[], int cols, long rows ) { super(args,cols,rows); }
    @Override ASTOp make( AST args[], int cols, long rows ) { return new ASTDiv(args,cols,rows); }
  }
  static private class ASTMin extends ASTOp {
    @Override String opStr() { return "min"; }
    ASTMin( AST args[], int cols, long rows ) { super(args,cols,rows); }
    @Override ASTOp make( AST args[], int cols, long rows ) { return new ASTMin(args,cols,rows); }
  }
  static private class ASTCat extends ASTOp {
    @Override String opStr() { return "c"; }
    ASTCat( AST args[], int cols, long rows ) { super(args,cols,rows); }
    @Override ASTOp make( AST args[], int cols, long rows ) { return new ASTCat(args,cols,rows); }
  }


  // --------------------------------------------------------------------------
  private boolean throwIfNotCompat(AST l, AST r, int idx ) {
    assert l._rows != -1 && r._rows != -1 && l._cols != -1 && r._cols != -1;
    if( !(l._rows==1 || r._rows==1 || l._rows==r._rows) ||
        !(l._cols==1 || r._cols==1 || l._cols==r._cols) )  
      throwErr("Frames not compatible: "+l._rows+"x"+l._cols+" vs "+r._rows+"x"+r._cols ,idx);
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
