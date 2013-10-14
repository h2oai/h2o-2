package water.exec;

import water.fvec.*;
import water.*;
import java.text.*;
import java.util.HashMap;

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
      AST ast = parseExpr(E);
      if( ast == null ) return null;
      // Can find an infix op between expressions
      AST ast2 = ASTOp2.parseInfix(E,ast); // Infix op, or not?
      if( ast2 != null ) return ast2;
      if( ast instanceof ASTKey && E.peek('=') ) { throw H2O.unimpl(); } // assignment
      if( ast instanceof ASTKey && E.peek('[') ) { throw H2O.unimpl(); } // subset assignment
      if( ast.isScalar() && E.peek('?') ) { throw H2O.unimpl(); } // infix trinary
      return ast;
    }

    static AST parseExpr(Exec2 E ) {
      AST ast;
      // Simple paren expression
      if( E.peek('(') )  return E.xpeek(')',E._x,parseCXExpr(E));
      if( (ast = ASTKey.parse(E)) != null ) return ast;
      if( (ast = ASTNum.parse(E)) != null ) return ast;
      if( (ast = ASTOp1.parsePrefix(E)) != null ) return ast;
      if( (ast = ASTOp2.parsePrefix(E)) != null ) return ast;
      return null;
    }
    protected void indent( StringBuilder sb, int d ) { 
      for( int i=0; i<d; i++ ) sb.append("  "); 
      sb.append(_rows).append('x').append(_cols).append(' ');
    }
    boolean isScalar() { return _cols==1 && _rows==1; }
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
      if( pp.getIndex()==E._x ) return null;
      assert N instanceof Double || N instanceof Long;
      E._x = pp.getIndex();
      double d = (N instanceof Double) ? (double)(Double)N : (double)(Long)N;
      return new ASTNum(d);
    }
    @Override public String toString() { return Double.toString(_d); }
    @Override public StringBuilder toString( StringBuilder sb, int d ) { indent(sb,d); return sb.append(this); }
  }

  abstract static private class ASTOp1 extends AST {
    static final HashMap<String,ASTOp1> OP1S = new HashMap();
    static {
      put(new ASTIsNA());
      put(new ASTSgn());
    }
    static private void put(ASTOp1 ast) { OP1S.put(ast.opStr(),ast); }
    final AST _left;
    ASTOp1( ) { super(-1,-1); _left=null; }
    ASTOp1( AST left ) { 
      super(left._cols,left._rows);
      _left = left;
    }
    abstract String opStr();
    abstract ASTOp1 make(AST left);
    @Override public String toString() { return opStr(); }
    @Override public StringBuilder toString( StringBuilder sb, int d ) { 
      indent(sb,d); sb.append(this).append('\n');
      _left.toString(sb,d+1);
      return sb;
    }

    // Parse an prefix operator
    static AST parsePrefix(Exec2 E) { 
      int x = E._x;
      String id = E.isID();
      if( id == null ) return null;
      ASTOp1 op1 = OP1S.get(id);
      if( op1==null ) {         // No ops match
        E._x = x;               // Roll back, no parse happened
        return null;
      }
      E.xpeek('(',x,null);  
      return E.xpeek(')',E._x,op1.make(parseCXExpr(E)));
    }
  }

  static private class ASTIsNA extends ASTOp1 {
    @Override String opStr() { return "isNA"; }
    ASTIsNA( ) { super(); }
    ASTIsNA( AST left ) { super(left); }
    @Override ASTOp1 make( AST left ) { return new ASTIsNA(left); }
  }
  static private class ASTSgn extends ASTOp1 {
    @Override String opStr() { return "Sgn"; }
    ASTSgn( ) { super(); }
    ASTSgn( AST left ) { super(left); }
    @Override ASTOp1 make( AST left ) { return new ASTSgn(left); }
  }


  abstract static private class ASTOp2 extends AST {
    static final HashMap<String,ASTOp2> OP2S = new HashMap();
    static {
      put(new ASTPlus());
      put(new ASTSub());
      put(new ASTMul());
      put(new ASTDiv());
      put(new ASTMin());
    }
    static private void put(ASTOp2 ast) { OP2S.put(ast.opStr(),ast); }
    final AST _left, _rite;
    ASTOp2( ) { super(-1,-1); _left=_rite=null; }
    ASTOp2( AST left, AST rite ) { 
      // Compatibility rules:
      // RxC meets RxC ==> element-wise op
      // RxC meets Rx1 ==> row-wide op
      // RxC meets 1xC ==> col-wide op
      // RxC meets 1x1 ==> scalar op
      super(Math.max(left._cols,rite._cols),
            Math.max(left._rows,rite._rows));
      _left = left;  _rite=rite;
      assert left._rows==1 || rite._rows==1 || left._rows==rite._rows;
      assert left._cols==1 || rite._cols==1 || left._cols==rite._cols;
    }
    abstract String opStr();
    abstract ASTOp2 make(AST left, AST rite);
    ASTOp2 parseRite(AST left,Exec2 E) {
      int x = E._x;
      AST rite = parseCXExpr(E);
      E.throwIfNotCompat(left,rite,x);
      return make(left,rite);
    }

    @Override public String toString() { return opStr(); }
    @Override public StringBuilder toString( StringBuilder sb, int d ) { 
      indent(sb,d); sb.append(this).append('\n');
      _left.toString(sb,d+1).append('\n');
      _rite.toString(sb,d+1);
      return sb;
    }

    // Parse an infix operator
    static AST parseInfix(Exec2 E, AST ast) { 
      int x = E._x;
      String id = E.isID();
      if( id == null ) return null;
      ASTOp2 op2 = OP2S.get(id);
      if( op2==null ) {         // No ops match
        E._x = x;               // Roll back, no parse happened
        return null;
      }
      return op2.parseRite(ast,E); // Parsed an Op2 - so now parse right side of infix
    }
    static AST parsePrefix(Exec2 E) { 
      int x = E._x;
      String id = E.isID();
      if( id == null ) return null;
      ASTOp2 op2 = OP2S.get(id);
      if( op2==null ) {         // No ops match
        E._x = x;               // Roll back, no parse happened
        return null;
      }
      E.xpeek('(',x,null);  
      AST left = E.xpeek(',',E._x,parseCXExpr(E));
      return     E.xpeek(')',E._x,op2.parseRite(left,E));
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
  static private class ASTMul extends ASTOp2 {
    @Override String opStr() { return "*"; }
    ASTMul( ) { super(); }
    ASTMul( AST left, AST rite ) { super(left,rite); }
    @Override ASTOp2 make( AST left, AST rite ) { return new ASTMul(left,rite); }
  }
  static private class ASTDiv extends ASTOp2 {
    @Override String opStr() { return "/"; }
    ASTDiv( ) { super(); }
    ASTDiv( AST left, AST rite ) { super(left,rite); }
    @Override ASTOp2 make( AST left, AST rite ) { return new ASTDiv(left,rite); }
  }
  static private class ASTMin extends ASTOp2 {
    @Override String opStr() { return "min"; }
    ASTMin( ) { super(); }
    ASTMin( AST left, AST rite ) { super(left,rite); }
    @Override ASTOp2 make( AST left, AST rite ) { return new ASTMin(left,rite); }
  }

  private boolean throwIfNotCompat(AST l, AST r, int idx ) {
    assert l._rows != -1 && r._rows != -1 && l._cols != -1 && r._cols != -1;
    if( !(l._rows==1 || r._rows==1 || l._rows==r._rows) )  throwErr("Frames not compatible: ",idx);
    if( !(l._cols==1 || r._cols==1 || l._cols==r._cols) )  throwErr("Frames not compatible: ",idx);
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
