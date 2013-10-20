package water.exec;

import java.util.*;
import water.*;

/** Parse & execute a generic R-like string, in the context of an H2O Cloud
 *  @author cliffc@0xdata.com
 */
public class Exec2 {
  // Parse a string, execute it & return a Frame.
  // Basic types: Frame, Scalar, Function
  // Functions are 1st class; every argument typed one of the above.
  // Assignment is always to in-scope variables only.
  // Initial environment has all Frame Keys mapped to Frame-typed variables

  // Big Allocation: all expressions are eval'd in a context where a large temp
  // is available, and all allocations are compatible with that temp.  Linear-
  // logic style execution is guaranteed inside the big temp.  Parse error if
  // an expression which is not provably small does not have an active temp.

  // Grammar:
  //   statements := cxexpr ; statements
  //   cxexpr :=                   // COMPLEX expr 
  //           iexpr               // Simple RHS-expr
  //           id = cxexpr         // Shadows outer var with a ptr assignment; no copy, read-only
  //                               // Overwrites inner var; types must match.
  //           id := cxexpr        // Deep-copy writable temp; otherwise same as above
  //           id[] = cxexpr       // Only for writable temps; slice assignment
  //           id[] := cxexpr      // Deep-copy, then slice assignment
  //           iexpr ? cxexpr : cxexpr  // exprs must have equal types
  //   iexpr := 
  //           slice {op2 slice}*  // Infix notation, evals LEFT TO RIGHT
  //   slice := 
  //           expr                // Can be a dbl or fcn or ary
  //           expr[]              // whole ary val
  //           expr[,]             // whole ary val
  //           expr[cxexpr,cxexpr] // row & col ary slice (row FIRST, col SECOND)
  //           expr[,cxexpr]       // col-only ary slice
  //           expr[cxexpr,]       // row-only ary slice
  //   expr :=
  //           val
  //           val(cxexpr,...)*    // Prefix function application, evals LEFT TO RIGHT
  //   val :=
  //           ( cxexpr )          // Ordering evaluation
  //           id                  // any visible var; will be typed
  //           num                 // Scalars, treated as 1x1
  //           op                  // Built-in functions
  //           function(v0,v1,v2) { statements; ...v0,v1,v2... } // 1st-class lexically scoped functions
  //   op  := sgn sin cos ...any unary op...
  //   op  := min max + - * / % & |    ...any boolean op...
  //   op  := c // R's "c" operator, returns a Nx1 array
  //   op  := ary byCol(ary,dbl op2(dbl,dbl))

  public static Env exec( String str ) throws IllegalArgumentException {
    System.out.println(str);
    AST ast = new Exec2(str).parse();
    System.out.println(ast.toString(new StringBuilder(),0).toString());
    Env env = new Env();
    try {
      //ast.exec(env); 
    } catch( RuntimeException t ) {
      env.remove();
      throw t;
    }
    return env;
  }

  // Simple parser state
  final String _str;
  final char _buf[];            // Chars from the string
  int _x;                       // Parse pointer
  Stack<LinkedHashMap<String,Type>> _env;
  private Exec2( String str ) {
    _str = str;
    _buf = str.toCharArray();
    _env = new Stack();
    // Preload the global environment from existing Frames
    LinkedHashMap<String,Type> global = new LinkedHashMap();
    for( Value v : H2O.values() )
      if( v.type()==TypeMap.FRAME )
        global.put(v._key.toString(),Type.ARY);
    _env.push(global);
  }
  int lexical_depth() { return _env.size(); }
  
  AST parse() { 
    AST ast = ASTStatement.parse(this); 
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
  static boolean isReserved(char c) { return c=='(' || c==')' || c=='=' || c=='[' || c==']' || c==','; }
  static boolean isLetter(char c) { return (c>='a'&&c<='z') || (c>='A' && c<='Z') || c=='_';  }
  static boolean isLetter2(char c) { 
    if( c=='.' || c==':' || c=='\\' || c=='/' ) return true;
    return isDigit(c) || isLetter(c);
  }

  // Return an ID string, or null if we get weird stuff or numbers.  Valid IDs
  // include all the operators, except parens (function application) and assignment.
  // Valid IDs: +   ++   <=  > ! [ ] joe123 ABC 
  // Invalid  : +++ 0joe ( = ) 123.45 1e3
  String isID() {
    skipWS();
    if( _x>=_buf.length ) return null; // No characters to parse
    char c = _buf[_x];
    // Fail on special chars in the grammar
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
  // Nicely report a syntax error
  AST throwErr( String msg, int idx ) {
    int lo = _x, hi=idx;
    if( idx < _x ) { lo = idx; hi=_x; }
    String s = msg+ '\n'+_str+'\n';
    int i;
    for( i=0; i<lo; i++ ) s+= ' ';
    s+='^'; i++;
    for( ; i<hi; i++ ) s+= '-';
    if( i<=hi ) s+= '^';
    s += '\n';
    throw new IllegalArgumentException(s);
  }
}
