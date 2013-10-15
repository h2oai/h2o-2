package water.exec;

import java.text.*;
import java.util.Arrays;
import java.util.HashMap;
import water.*;
import water.fvec.*;

/** Parse & execute a generic R-like string, in the context of an H2O Cloud
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

  public static Env exec( String str ) throws IllegalArgumentException {
    AST ast = new Exec2(str).parse();
    System.out.println(ast.toString(new StringBuilder(),0).toString());
    Env env = new Env();
    ast.exec(env);
    return env;
  }

  private Exec2( String str ) { _str = str; _buf = str.toCharArray(); }
  // Simple parser state
  final String _str;
  final char _buf[];
  int _x;
  
  AST parse() { 
    AST ast = AST.parseCXExpr(this); 
    skipWS();                   // No trailing crud
    return _x == _buf.length ? ast : throwErr("Junk at end of line",_buf.length-1);
  }

  // --------------------------------------------------------------------------
  // Generic parsing functions
  // --------------------------------------------------------------------------

  void skipWS() {
    while( _x < _buf.length && _buf[_x] <= ' ' )  _x++;
  }
  // Skip whitespace.
  // If c is the next char, eat it & return true
  // Else return false.
  boolean peek(char c) {
    if( _x ==_buf.length ) return false;
    while( _buf[_x] <= ' ' )
      if( ++_x ==_buf.length ) return false;
    if( _buf[_x]!=c ) return false;
    _x++;
    return true;
  }
  // Same as peek, but throw if char not found  
  AST xpeek(char c, int x, AST ast) { return peek(c) ? ast : throwErr("Missing '"+c+"'",x); }

  static boolean isDigit(char c) { return c>='0' && c<= '9'; }
  static boolean isWS(char c) { return c<=' '; }
  static boolean isReserved(char c) { return c=='(' || c==')' || c=='='; }
  static boolean isLetter(char c) { return (c>='a'&&c<='z') || (c>='A' && c<='Z');  }
  static boolean isLetter2(char c) { 
    if( c=='.' || c==':' || c=='\\' || c=='/' ) return true;
    if( isDigit(c) ) return true;
    return isLetter(c);
  }

  // Return an ID string, or null if we get weird stuff or numbers.  Valid IDs
  // include all the operators, except parens (function application) and assignment.
  // Valid IDs: +   ++   <=  > ! [ ] joe123 ABC 
  // Invalid  : +++ 0joe ( = ) 123.45 1e3
  String isID() {
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

  // --------------------------------------------------------------------------
  boolean throwIfNotCompat(AST l, AST r, int idx ) {
    assert l._rows != -1 && r._rows != -1 && l._cols != -1 && r._cols != -1;
    if( !(l._rows==1 || r._rows==1 || l._rows==r._rows) ||
        !(l._cols==1 || r._cols==1 || l._cols==r._cols) )  
      throwErr("Frames not compatible: "+l.dimStr()+" vs "+r.dimStr(),idx);
    return true;
  }

  // Nicely report a syntax error
  AST throwErr( String msg, int idx ) {
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
