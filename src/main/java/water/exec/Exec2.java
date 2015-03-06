package water.exec;

import water.*;
import water.fvec.Frame;
import water.util.Log;

import java.util.ArrayList;
import java.util.Stack;

/** Parse and execute a generic R-like string, in the context of an H2O Cloud
 *  @author cliffc@0xdata.com
 */
public class Exec2 {
  // Parse a string, execute it & return a Frame.
  // Basic types: ary (Frame), dbl (scalar double), fcn (function)
  // Functions are 1st class; every argument typed one of the above.
  // Assignment is always to in-scope variables only.
  // Initial environment has all Frame Keys mapped to Frame-typed variables

  // Big Allocation: all expressions are eval'd in a context where a large temp
  // is available, and all allocations are compatible with that temp.  Linear-
  // logic style execution is guaranteed inside the big temp.  Parse error if
  // an expression which is not provably small does not have an active temp.

  // Grammar:
  //   statements := cxexpr ; statements
  //   cxexpr :=                    // COMPLEX expr
  //           infix_expr           // Simple RHS-expr
  //           id = cxexpr          // Shadows outer var with a ptr assignment; no copy
  //                                // Overwrites inner var; types must match.
  //           id <- cxexpr         // Alternative R syntax for assignment
  //           id[] = cxexpr        // Slice/partial assignment; id already exists
  //           iexpr ? cxexpr : cxexpr  // exprs must have equal types
  //   infix_expr :=                // Leading infix expression
  //           op1 infix_expr term* // +x but also e.g. ++--!+-!-++!3
  //           op1?  slice term*    // e.g. cos() or -sin(foo) or -+-fun()[1,2]
  //   term : =                     // Infix expression
  //           op2 infix_expr       // Standard R operator prec ordering
  //   slice :=
  //           prefix_expr          // No slicing
  //           prefix_expr[]        // Whole slice
  //           prefix_expr[cxexpr?,cxexpr?] // optional row & col slicing
  //           prefix_expr$col      // named column
  //   prefix_expr :=
  //           val
  //           val(cxexpr,...)*     // Prefix function application, evals LEFT TO RIGHT
  //   val :=
  //           ( cxexpr )           // Ordering evaluation
  //           id                   // any visible var; will be typed
  //           num                  // Scalars, treated as 1x1
  //           op                   // Built-in functions
  //           function(v0,v1,v2) { statements; ...v0,v1,v2... } // 1st-class lexically scoped functions
  //           function(v0,v1,v2) statement // Single statement variant
  //   op1 := + - !                 // Unary operators allowed w/out parens prefix location
  //   op2 := + - * / % & | <= > >= !=  ... // Binary operators allowed w/out parens infix location
  //   op  := sgn sin cos nrow ncol isNA sqrt isTRUE year month day ...
  //   op  := min max sum sdev mean  ...
  //   op  := c cbind seq quantile table ...  // Various R operators

  public static Env exec( String str ) throws IllegalArgumentException {
    cluster_init();
    // Preload the global environment from existing Frames
    ArrayList<ASTId> global = new ArrayList<ASTId>();
    ArrayList<Key>   locked = new ArrayList<Key>  ();
    Env env = new Env(locked);
    final Key [] frameKeys = H2O.KeySnapshot.globalSnapshot().filter(new H2O.KVFilter() {
        @Override public boolean filter(H2O.KeyInfo k) { return k._type == TypeMap.FRAME; }
      }).keys();
    Log.info("Locking " + frameKeys.length +"keys for Exec2.");
    for( Key k : frameKeys ) {      // Convert all VAs to Frames
      Value val = DKV.get(k);
      if( val == null || !val.isFrame()) continue;
      // Bad if it's already locked by 'null', because lock by 'null' is removed when you leave Exec.
      // Before was adding all frames with read-shared lock here.
      // Should be illegal to add any keys locked by "null' to exec? (is it only unparsed keys?)
      // undoing. this doesn't always work (gets stack trace)
      Frame fr = val.get();
      String kstr = k.toString();
      try {
        env.push(fr,kstr);
        global.add(new ASTId(Type.ARY,kstr,0,global.size()));
        fr.read_lock(null);
        locked.add(fr._key);
      } catch( Exception e ) {
        Log.err("Exception while adding frame "+k+" to Exec env");
      }
    }

    // Some global constants
    global.add(new ASTId(Type.DBL,"TRUE",0,global.size())); env.push(1.0);
    global.add(new ASTId(Type.DBL,"FALSE",0,global.size())); env.push(0.0);
    global.add(new ASTId(Type.DBL,"T",0,global.size()));  env.push(1.0);
    global.add(new ASTId(Type.DBL,"F",0,global.size()));  env.push(0.0);
    global.add(new ASTId(Type.DBL,"NA",0,global.size()));  env.push(Double.NaN);
    global.add(new ASTId(Type.DBL,"Inf",0,global.size())); env.push(Double.POSITIVE_INFINITY);

    // Parse.  Type-errors get caught here and throw IAE
    try {
      int argcnt = global.size();
      Exec2 ex = new Exec2(str, global);
      AST ast = ex.parse();

      env.push(global.size()-argcnt);   // Push space for temps
      ast.exec(env);
      env.postWrite();
    } catch( RuntimeException t ) {
      env.remove_and_unlock();
      throw t;
    }
    return env;
  }

  // Simple parser state
  final String _str;
  final char _buf[];            // Chars from the string
  int _x;                       // Parse pointer
  Stack<ArrayList<ASTId>> _env;
  private Exec2( String str, ArrayList<ASTId> global ) {
    _str = str;
    _buf = str.toCharArray();
    _env = new Stack<ArrayList<ASTId>>();
    _env.push(global);
  }
  int lexical_depth() { return _env.size()-1; }

  AST parse() {
    AST ast = ASTStatement.parse(this);
    skipWS();                   // No trailing crud
    return _x == _buf.length ? ast : throwErr("Junk at end of line",_buf.length-1);
  }

  // --------------------------------------------------------------------------
  // Generic parsing functions
  // --------------------------------------------------------------------------

  void skipWS() { skipWS(false); }
  void skipWS( boolean EOS) {
    while( _x < _buf.length && isWS(_buf[_x]) && (!EOS || _buf[_x]!='\n') )  _x++;
  }
  // Skip whitespace.
  // If c is the next char, eat it & return true
  // Else return false.
  boolean peek(char c) { return peek(c,false); }
  // Peek for 'c' past whitespace but not past a newline if EOS is set
  // (basically treat newline as the statement-end character ';' which does not
  // match c)
  boolean peek(char c, boolean EOS) {
    char d;
    while( _x < _buf.length && isWS(_buf[_x]) && _buf[_x]!='\n' ) _x++;
    int nx=_x;
    if( !EOS ) while( nx < _buf.length && isWS(_buf[nx]) ) nx++;
    if( nx==_buf.length || _buf[nx]!=c ) return false;
    _x=nx+1;
    return true;
  }
  // Same as peek, but throw if char not found.  Always newlines are treated as whitespace
  AST xpeek(char c, int x, AST ast) { return peek(c,false) ? ast : throwErr("Missing '"+c+"'",x); }

  // True if end-of-statement (';' or '\n' or no-more-data)
  boolean peekEOS() {
    while( _x < _buf.length ) {
      char d = _buf[_x++];
      if( d==';' || d=='\n' ) return true;
      if( !isWS(d) ) { _x--; return false; }
    }
    return false;
  }

  static boolean isDigit(char c) { return c>='0' && c<= '9'; }
  static boolean isWS(char c) { return c<=' '; }
  static boolean isReserved(char c) { return c=='(' || c==')' || c=='[' || c==']' || c==',' || c==':' || c==';' || c=='$'; }
  static boolean isLetter(char c) { return (c>='a'&&c<='z') || (c>='A' && c<='Z') || c=='_';  }
  static boolean isLetter2(char c) {
    return c=='.' || c==':' || c=='\\' || isDigit(c) || isLetter(c);
  }
  static boolean isQuote(char c) { return c=='"' || c=='\''; }

  // Return an ID string, or null if we get weird stuff or numbers.  Valid IDs
  // include all the operators, except parens (function application) and assignment.
  // Valid IDs: + - <=  > ! [ ] joe123 ABC
  // Invalid  : +++ 0joe ( = ) 123.45 1e3
  String isID() {
    if( _x>=_buf.length ) return null; // No characters to parse
    char c = _buf[_x];
    // Fail on special chars in the grammar
    if( isReserved(c) && c != ':') return null;
    if (c == ':') {
      if (!isDigit(_buf[_x+1])) return null;
    }
    // Fail on leading numeric
    if( isDigit(c) ) return null;
    if (c == '^' && _buf[_x+1] == '-') return _str.substring(++_x -1, _x);
    _x++;                       // Accept parse of 1 char

    // If first char is letter, standard ID
    if( isLetter(c) ) {
      int x=_x-1;               // start of ID
      while( _x < _buf.length && isLetter2(_buf[_x]) )
        _x++;
      return _str.substring(x,_x);
    }

    // Check for super-special operators that are three chars of the form %*%.
    // These are calls to R's matrix operators.
    if( _x+2 <= _buf.length && c == '%' && _buf[_x+1] == '%' ) {
      if( _buf[_x] == '*' ) { _x+=2; return "%*%"; }
      if (_buf[_x] == '/' ) { _x+=2; return "%/%"; }
    }

    if (_x+2 <= _buf.length && c == '%' && _buf[_x] == '%') { _x++; return "%%"; }

    // If first char is special, accept 1 or 2 special chars.
    // i.e. allow != >= == <= but not = alone
    if( _x>=_buf.length ) return _str.substring(_x-1,_x);
    char c2=_buf[_x];
    if( isDigit(c2) || isLetter(c2) || isWS(c2) || isReserved(c2) ) {
      if( c=='=' ) { _x--; return null; } // Equals alone is not an ID
      return _str.substring(_x-1,_x);
    }
    if( c=='<' && c2=='-' ) { _x--; return null; } // The other assignment operator
    // Must accept as single letters to avoid ambiguity
    if( c=='+' || c=='-' || c=='*' || c=='/' ) return _str.substring(_x-1,_x);
    // One letter look ahead to decide on what to accept
    if( c=='=' || c=='!' || c=='<' || c =='>' )
      if ( c2 =='=' ) return _str.substring(++_x-2,_x);
      else return _str.substring(_x-1,_x);
    _x++;                       // Else accept e.g. <= >= ++ != == etc...
    return _str.substring(_x-2,_x);
  }

  String isString() {  // returns string value without enclosing quotes
    if( _x>=_buf.length ) return null; // No characters to parse
    char c = _buf[_x];

    if( isQuote(c) ) {
      int x=_x+1;
      while( x < _buf.length && _buf[x] != c )x++;
      return _str.substring(_x+1,x);
    }
    return null;
  }

  // isID specifically does not parse "=" or "<-".  This guy does.
  boolean isAssign(boolean EOS) {
    if( peek('<',EOS) ) {
      if( _buf[_x]=='-' ) { _x++; return true; }
      else return false;
    }
    if( !peek('=',EOS) ) return false;
    char c = _buf[_x];
    if( c != '=' ) return true; // what valid 2-letter tokens start with "="?  ==  but not =+ =>
    _x--;
    return false;
  }

  // Yet-to-be-parsed 
  private String debug() { return new String(_buf,_x,_buf.length-_x); }

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

  // To avoid a class-circularity hang, we need to force other members of the
  // cluster to load the Exec & AST classes BEFORE trying to execute code
  // remotely, because e.g. ddply runs functions on all nodes.
  private static boolean _inited;       // One-shot init
  private static void cluster_init() {
    if( _inited ) return;
    new DRemoteTask() {
      @Override public void lcompute() {
        new ASTPlus();          // Touch a common class to force loading
        tryComplete();
      }
      @Override public void reduce( DRemoteTask dt ) { }
    }.invokeOnAllNodes();
    _inited = true;
  }
}
