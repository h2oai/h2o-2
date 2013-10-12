package water.exec;

import water.fvec.*;
import water.*;
import java.text.*;
import java.util.HashMap;

/** Execute a generic R string, in the context of an H2O Cloud
 * @author cliffc@0xdata.com
 */
public class Exec2 {
  // Parse a string, execute it & return a Frame.
  // Grammer:
  //   expr :=                   // expr is a Frame, a 2-d table
  //           num | id          // Scalars, treated as 1x1
  //           key               // A Frame, dimensions stored in K/V already
  //           ( expr )          // Ordering evaluation
  //           op1(expr)         // apply op1 to all elements
  //           op2(expr,expr)    // apply op2 to all; exprs must have *compatible* shapes
  //           expr op2 expr     // apply all; ....optional INFIX notation
  //           apply1(op,expr)   // for-all-cols in expr, apply op to col

  //           apply1(op2,expr)  // reduce cols; result is size 1xN
  //           apply2(op2,expr)  // reduce rows; result is size NX1
  //           apply (op2,expr)  // reduce all; result is size 1x1
  //           op2(expr)         // reduce all; ....optional notation
  //           expr0             // any 1x1 expr
  //           expr0 ? expr : expr      // exprs must have *compatible* shapes
  //           ifelse(expr0,expr,expr)  // exprs must have *compatible* shapes
  //           expr1             // any 1xN expr (exactly one col, N rows)
  //           expr[expr1,expr1] // slice rows & cols by index
  //           key = expr        // key & expr must have *same* shape
  //           key [expr1,expr1] = expr  // subset assignment of *same* shape
  //           key [,expr1]      = expr  // subset assignment of *same* shape
  //           key [expr1,]      = expr  // subset assignment of *same* shape
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
  
  private AST parse() { return AST.parseExpr(this); }


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

  // Return an ID string, or null if we get weird stuff or numbers.
  // Valid IDs: + ++ <= > ! [ ] joe123 ABC 
  // Invalid: +++ 0joe ( =
  private String isID() {
    skipWS();
    if( _x>=_buf.length ) return null; // No characters to parse
    char c = _buf[_x];
    // Fail on special chars in the grammer
    if( c=='(' || c==')' || c=='=' ) return null;
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
    if( isDigit(c2) || isLetter(c2) || isWS(c2) ) return _str.substring(_x-1,_x);
    _x++;
    return _str.substring(_x-2,_x);
  }

  private static boolean isDigit(char c) { return c>='0' && c<= '9'; }
  private static boolean isWS(char c) { return c<=' '; }
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
    static AST parseExpr(Exec2 E ) {
      if( E.peek('(') ) { throw H2O.unimpl(); } // op_pre or expr
      AST ast = ASTKey.parse(E);
      if( ast != null && E.peek('=') ) { throw H2O.unimpl(); } // assignment
      if( ast == null )         // Key parse optionally returns
        ast = ASTNum.parse(E);  // Number parse either throws or valid returns
      ast = ASTOp2.parseInfix(E,ast); // Infix op, or not?
      E.skipWS();
      if( E._x < E._buf.length ) 
        E.throwErr("Junk at end of line",E._buf.length-1);
      return ast;
    }
    protected void indent( StringBuilder sb, int d ) { 
      for( int i=0; i<d; i++ ) sb.append("  "); 
      sb.append(_rows).append('x').append(_cols).append(' ');
    }
    public StringBuilder toString( StringBuilder sb, int d ) { indent(sb,d); return sb.append(this); }
  }
  static private class ASTKey extends AST {
    final Key _key;
    ASTKey( int cols, long rows, Key key) { super(cols,rows); _key=key; }
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
    @Override public StringBuilder toString( StringBuilder sb, int d ) { indent(sb,d); return sb.append(this); }
  }
  static private class ASTNum extends AST {
    static final NumberFormat NF = NumberFormat.getInstance();
    final double _d;
    ASTNum(double d ) { super(1,1); _d=d; }
    // Parse a number, or throw a parse error
    static ASTNum parse(Exec2 E) { 
      ParsePosition pp = new ParsePosition(E._x);
      Number N = NF.parse(E._str,pp);
      if( pp.getIndex()==E._x ) E.throwErr("Number parse",pp.getErrorIndex());
      assert N instanceof Double || N instanceof Long;
      E._x = pp.getIndex();
      double d = (N instanceof Double) ? (double)(Double)N : (double)(Long)N;
      return new ASTNum(d);
    }
    @Override public String toString() { return Double.toString(_d); }
    @Override public StringBuilder toString( StringBuilder sb, int d ) { indent(sb,d); return sb.append(this); }
  }

  abstract static private class ASTOp2 extends AST {
    static final HashMap<String,ASTOp2> OP2S = new HashMap();
    static {
      put(new ASTPlus());
      put(new ASTSub());
    }
    static private void put(ASTOp2 ast) { OP2S.put(ast.opStr(),ast); }
    final AST _left, _rite;
    ASTOp2( ) { super(-1,-1); _left=_rite=null; }
    ASTOp2( AST left, AST rite ) { 
      super(Math.max(left._cols,rite._cols),
            Math.max(left._rows,rite._rows));
      _left = left;  _rite=rite;
    }
    abstract String opStr();
    abstract ASTOp2 make(AST left, AST rite);
    @Override public String toString() { return opStr(); }
    @Override public StringBuilder toString( StringBuilder sb, int d ) { 
      indent(sb,d); sb.append(this).append('\n');
      _left.toString(sb,d+1).append('\n');
      _rite.toString(sb,d+1);
      return sb;
    }

    // Parse an infix operator, or return the original AST
    static AST parseInfix(Exec2 E, AST ast) { 
      int x = E._x;
      String id = E.isID();
      if( id == null ) return ast;
      ASTOp2 op2 = OP2S.get(id);
      if( op2==null ) {         // No ops match
        E._x = x;               // Roll back, no parse happened
        return ast; 
      }
      // Parsed an Op2 - so now parse right side of infix
      AST rite = parseExpr(E);
      E.throwIfNotCompat(ast,rite,x);
      return op2.make(ast,rite);
    }
  }

  static private class ASTPlus extends ASTOp2 {
    @Override String opStr() { return "+"; }
    ASTPlus( ) { super(); }
    ASTPlus( AST left, AST rite ) { super(left,rite); }
    @Override ASTOp2 make( AST left, AST rite ) { return new ASTPlus(left,rite); }
  }
  static private class ASTSub extends ASTOp2 {
    @Override String opStr() { return "-"; }
    ASTSub( ) { super(); }
    ASTSub( AST left, AST rite ) { super(left,rite); }
    @Override ASTOp2 make( AST left, AST rite ) { return new ASTSub(left,rite); }
  }

  private void throwIfNotCompat(AST l, AST r, int idx ) {
    assert l._rows != -1 && r._rows != -1 && l._cols != -1 && r._cols != -1;
    if( !(l._rows==1 || r._rows==1 || l._rows==r._rows) )  throwErr("Frames not compatible: ",idx);
    if( !(l._cols==1 || r._cols==1 || l._cols==r._cols) )  throwErr("Frames not compatible: ",idx);
  }

  // Nicely report a syntax error
  private void throwErr( String msg, int idx ) {
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
