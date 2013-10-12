package water.exec;

import water.fvec.*;
import water.*;

/** Execute a generic R string, in the context of an H2O Cloud
 * @author cliffc@0xdata.com
 */
public class Exec2 {
  byte _buf[];
  int _x;
  
  // Parse a string, execute it & return a Frame.
  // Grammer:
  //   expr := ( expr )
  //           ( op_pre expr expr ... )
  //           key = expr
  //           key/num
  //           key/num op_in expr
  //   val  := key | num
  //   key  := any Key mapping to a Frame.
  //   op_in:= + - * / % & | ...etc...
  //   op_pre := min max ...etc...

  public static Frame exec( String str ) throws ParserException, EvaluationException {
    AST ast = new Exec2().parse(str);
    System.out.println(ast);
    return null;
  }
  private AST parse(String str) { 
    _buf = str.getBytes();
    return AST.parseExpr(this);
  }

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

  abstract static private class AST {
    abstract String opStr();
    static AST parseExpr(Exec2 E ) {
      if( E.peek('(') ) { throw H2O.unimpl(); } // op_pre or expr
      AST ast = ASTKey.parse(E);
      if( ast != null && E.peek('=') ) { throw H2O.unimpl(); } // assignment
      if( ast == null )          // Key parse optionally returns
        ast = ASTNum.parse(E);   // Number parse either throws or valid returns
      return ASTInfix.parse(E,ast); // Infix op, or not?
    }
  }
  static private class ASTKey extends AST {
    Key _key;
    @Override String opStr() { return _key.toString(); }
    // Parse a valid H2O Frame Key, or return null;
    static ASTKey parse(Exec2 E) { throw H2O.unimpl(); }
  }
  static private class ASTNum extends AST {
    double _d;
    @Override String opStr() { return Double.toString(_d); }
    // Parse a number, or throw a parse error
    static ASTNum parse(Exec2 E) { throw H2O.unimpl(); }
  }
  static private class ASTInfix extends AST {
    @Override String opStr() { return "+"; }
    // Parse an infix operator, or return the original AST
    static ASTInfix parse(Exec2 E, AST ast) { throw H2O.unimpl(); }
  }

}