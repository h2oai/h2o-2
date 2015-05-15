package water.exec;

import hex.FrameTask.DataInfo;
import hex.Quantiles;
import hex.gram.Gram.GramTask;
import hex.la.DMatrix;
import hex.la.Matrix;
import jsr166y.CountedCompleter;
import org.apache.commons.math3.util.ArithmeticUtils;
import org.joda.time.DateTime;
import org.joda.time.MutableDateTime;
import org.joda.time.format.DateTimeFormatter;
import water.*;
import water.api.QuantilesPage;
import water.fvec.*;
import water.util.Utils;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/** Parse a generic R string and build an AST, in the context of an H2O Cloud
 *  @author cliffc@0xdata.com
 */

// --------------------------------------------------------------------------
public abstract class ASTOp extends AST {
  // The order of operator precedence follows R rules.
  // Highest the first
  static final public int OPP_PREFIX   = 100; /* abc() */
  static final public int OPP_POWER    = 13;  /* ^ */
  static final public int OPP_UPLUS    = 12;  /* + */
  static final public int OPP_UMINUS   = 12;  /* - */
  static final public int OPP_INTDIV   = 11;  /* %/% */
  static final public int OPP_MOD      = 11;  /* %xyz% */
  static final public int OPP_MUL      = 10;  /* * */
  static final public int OPP_DIV      = 10;  /* / */
  static final public int OPP_PLUS     = 9;   /* + */
  static final public int OPP_MINUS    = 9;   /* - */
  static final public int OPP_GT       = 8;   /* > */
  static final public int OPP_GE       = 8;   /* >= */
  static final public int OPP_LT       = 8;   /* < */
  static final public int OPP_LE       = 8;   /* <= */
  static final public int OPP_EQ       = 8;   /* == */
  static final public int OPP_NE       = 8;   /* != */
  static final public int OPP_NOT      = 7;   /* ! */
  static final public int OPP_AND      = 6;   /* &, && */
  static final public int OPP_OR       = 5;   /* |, || */
  static final public int OPP_DILDA    = 4;   /* ~ */
  static final public int OPP_RARROW   = 3;   /* ->, ->> */
  static final public int OPP_ASSN     = 2;   /* = */
  static final public int OPP_LARROW   = 1;   /* <-, <<- */
  // Operator assocation order
  static final public int OPA_LEFT     = 0;
  static final public int OPA_RIGHT    = 1;
  // Operation formula notations
  static final public int OPF_INFIX    = 0;
  static final public int OPF_PREFIX   = 1;
  // Tables of operators by arity
  static final public HashMap<String,ASTOp> UNI_INFIX_OPS = new HashMap();
  static final public HashMap<String,ASTOp> BIN_INFIX_OPS = new HashMap();
  static final public HashMap<String,ASTOp> PREFIX_OPS    = new HashMap();
  static final public HashMap<String,ASTOp> UDF_OPS       = new HashMap();
  // Too avoid a cyclic class-loading dependency, these are init'd before subclasses.
  static final String VARS1[] = new String[]{ "", "x"};
  static final String VARS2[] = new String[]{ "", "x","y"};
  static {
    // Unary infix ops
    putUniInfix(new ASTUniPlus());
    putUniInfix(new ASTUniMinus());
    putUniInfix(new ASTNot());
    // Binary infix ops
    putBinInfix(new ASTPlus());
    putBinInfix(new ASTSub());
    putBinInfix(new ASTMul());
    putBinInfix(new ASTDiv());
    putBinInfix(new ASTPow());
    putBinInfix(new ASTPow2());
    putBinInfix(new ASTMod());
    putBinInfix(new ASTMod2());
    putBinInfix(new ASTAND());
    putBinInfix(new ASTOR());
    putBinInfix(new ASTLT());
    putBinInfix(new ASTLE());
    putBinInfix(new ASTGT());
    putBinInfix(new ASTGE());
    putBinInfix(new ASTEQ());
    putBinInfix(new ASTNE());
    putBinInfix(new ASTLA());
    putBinInfix(new ASTLO());
    putBinInfix(new ASTMMult());
    putBinInfix(new ASTIntDiv());
    putBinInfix(new ASTColSeq());

    // Unary prefix ops
    putPrefix(new ASTIsNA());
    putPrefix(new ASTNrow());
    putPrefix(new ASTNcol());
    putPrefix(new ASTLength());
    putPrefix(new ASTAbs ());
    putPrefix(new ASTSgn ());
    putPrefix(new ASTSqrt());
    putPrefix(new ASTCeil());
    putPrefix(new ASTFlr ());
    putPrefix(new ASTTrun());
    putPrefix(new ASTRound());
    putPrefix(new ASTSignif());
    putPrefix(new ASTLog ());
    putPrefix(new ASTExp ());
    putPrefix(new ASTScale());
    putPrefix(new ASTFactor());
    putPrefix(new ASTNumeric());
    putPrefix(new ASTIsFactor());
    putPrefix(new ASTAnyFactor());   // For Runit testing
    putPrefix(new ASTCanBeCoercedToLogical());
    putPrefix(new ASTAnyNA());
    putPrefix(new ASTIsTRUE());
    putPrefix(new ASTMTrans());

    // Trigonometric functions
    putPrefix(new ASTCos());
    putPrefix(new ASTSin());
    putPrefix(new ASTTan());
    putPrefix(new ASTACos());
    putPrefix(new ASTASin());
    putPrefix(new ASTATan());
    putPrefix(new ASTCosh());
    putPrefix(new ASTSinh());
    putPrefix(new ASTTanh());

    // Time extractions, to and from msec since the Unix Epoch
    putPrefix(new ASTYear  ());
    putPrefix(new ASTMonth ());
    putPrefix(new ASTDay   ());
    putPrefix(new ASTHour  ());
    putPrefix(new ASTMinute());
    putPrefix(new ASTSecond());
    putPrefix(new ASTMillis());
    putPrefix(new ASTasDate());

    // Time series operations
    putPrefix(new ASTDiff  ());

    // More generic reducers
    putPrefix(new ASTMin ());
    putPrefix(new ASTMax ());
    putPrefix(new ASTSum ());
    putPrefix(new ASTSdev());
    putPrefix(new ASTVar());
    putPrefix(new ASTMean());
    putPrefix(new ASTMedian());
    putPrefix(new ASTMostCommon());
    putPrefix(new ASTMinNaRm());
    putPrefix(new ASTMaxNaRm());
    putPrefix(new ASTSumNaRm());
    putPrefix(new ASTXorSum ());

    // Misc
    putPrefix(new ASTSeq   ());
    putPrefix(new ASTSeqLen());
    putPrefix(new ASTRepLen());
    putPrefix(new ASTQtile ());
    putPrefix(new ASTCat   ());
    putPrefix(new ASTCbind ());
    putPrefix(new ASTRbind ());
    putPrefix(new ASTTable ());
    putPrefix(new ASTReduce());
    putPrefix(new ASTIfElse());
    putPrefix(new ASTRApply());
    putPrefix(new ASTSApply());
    putPrefix(new ASTddply ());
    putPrefix(new ASTUnique());
    putPrefix(new ASTRunif ());
    putPrefix(new ASTCut   ());
    putPrefix(new ASTfindInterval());
    putPrefix(new ASTPrint ());
    putPrefix(new ASTLs    ());
    putPrefix(new ASTStrSplit());
    putPrefix(new ASTToLower());
    putPrefix(new ASTToUpper());
    putPrefix(new ASTGSub());
    putPrefix(new ASTSetLevel());
    putPrefix(new ASTStrSub());
    putPrefix(new ASTRevalue());
    putPrefix(new ASTWhich());
    putPrefix(new ASTTrim());
    putPrefix(new ASTSample());
  }
  static private boolean isReserved(String fn) {
    return UNI_INFIX_OPS.containsKey(fn) || BIN_INFIX_OPS.containsKey(fn) || PREFIX_OPS.containsKey(fn);
  }
  static private void putUniInfix(ASTOp ast) { UNI_INFIX_OPS.put(ast.opStr(),ast); }
  static private void putBinInfix(ASTOp ast) { BIN_INFIX_OPS.put(ast.opStr(),ast); }
  static private void putPrefix  (ASTOp ast) {    PREFIX_OPS.put(ast.opStr(),ast); }
  static         void putUDF     (ASTOp ast, String fn) {
    if (isReserved(fn)) throw new IllegalArgumentException("Trying to overload a reserved method: "+fn+". Must not overload a reserved method with a user-defined function.");
    if (UDF_OPS.containsKey(fn)) removeUDF(fn);
    UDF_OPS.put(fn,ast);
  }
  static         void removeUDF  (String fn) { UDF_OPS.remove(fn); }
  static public ASTOp isOp(String id) {
    // This order matters. If used as a prefix OP, `+` and `-` are binary only.
    ASTOp op4 =       UDF_OPS.get(id); if( op4 != null ) return op4;
    return isBuiltinOp(id);
  }
  static public ASTOp isBuiltinOp(String id) {
    ASTOp op3 =    PREFIX_OPS.get(id); if( op3 != null ) return op3;
    ASTOp op2 = BIN_INFIX_OPS.get(id); if( op2 != null ) return op2;
    ASTOp op1 = UNI_INFIX_OPS.get(id);                   return op1;
  }
  static public boolean isInfixOp(String id) {
    return BIN_INFIX_OPS.containsKey(id) || UNI_INFIX_OPS.containsKey(id);
  }
  static public boolean isUDF(String id) {
    return UDF_OPS.containsKey(id);
  }
  static public boolean isUDF(ASTOp op) { return isUDF(op.opStr()); }
  static public Set<String> opStrs() {
    Set<String> all = UNI_INFIX_OPS.keySet();
    all.addAll(BIN_INFIX_OPS.keySet());
    all.addAll(PREFIX_OPS.keySet());
    all.addAll(UDF_OPS.keySet());
    return all;
  }

  final int _form;          // formula notation, 0 - infix, 1 - prefix
  final int _precedence;    // operator precedence number
  final int _association;   // 0 - left associated, 1 - right associated
  // All fields are final, because functions are immutable
  final String _vars[];     // Variable names
  ASTOp( String vars[], Type ts[], int form, int prec, int asso) {
    super(Type.fcn(ts));
    _form = form;
    _precedence = prec;
    _association = asso;
    _vars = vars;
    assert ts.length==vars.length : "No vars?" + this;
  }
  ASTOp( String vars[], Type t, int form, int prec, int asso) {
    super(t);
    _form = form;
    _precedence = prec;
    _association = asso;
    _vars = vars;
    assert t._ts.length==vars.length : "No vars?" + this;
  }
  abstract String opStr();
  abstract ASTOp  make();

  public boolean leftAssociate( ) {
    return _association == OPA_LEFT;
  }

  @Override public String toString() {
    String s = _t._ts[0]+" "+opStr()+"(";
    int len=_t._ts.length;
    for( int i=1; i<len-1; i++ )
      s += _t._ts[i]+" "+(_vars==null?"":_vars[i])+", ";
    return s + (len > 1 ? _t._ts[len-1]+" "+(_vars==null?"":_vars[len-1]) : "")+")";
  }
  public String toString(boolean verbose) {
    if( !verbose ) return toString(); // Just the fun name& arg names
    return toString();
  }

  static ASTOp parse(Exec2 E) {
    int x = E._x;
    String id = E.isID();
    if( id == null ) return null;
    ASTOp op = isOp(id);  // The order matters. If used as a prefix OP, `+` and `-` are binary only.
    // Also, if assigning to a built-in function then do not parse-as-a-fcn.
    // Instead it will default to parsing as an ID in ASTAssign.parse
    if( op != null ) {
      int x1 = E._x;
      if (!E.peek('=') && !(E.peek('<') && E.peek('-'))) {
        E._x = x1; return op.make();
      }
    }
    E._x = x;
    return ASTFunc.parseFcn(E);
  }

  // Parse a unary infix OP or return null.
  static ASTOp parseUniInfixOp(Exec2 E) {
    int x = E._x;
    String id = E.isID();
    if( id == null ) return null;
    ASTOp op = UNI_INFIX_OPS.get(id);
    if( op != null) return op.make();
    E._x = x;                 // Roll back, no parse happened
    return null;
  }

  // Parse a binary infix OP or return null.
  static ASTOp parseBinInfixOp(Exec2 E) {
    int x = E._x;
    String id = E.isID();
    if( id == null ) return null;
    ASTOp op = BIN_INFIX_OPS.get(id);
    if( op != null) return op.make();
    E._x = x;                 // Roll back, no parse happened
    return null;
  }

  @Override void exec(Env env) { env.push(this); }
  // Standard column-wise function application
  abstract void apply(Env env, int argcnt, ASTApply apply);
  // Special row-wise 'apply'
  double[] map(Env env, double[] in, double[] out) { throw H2O.unimpl(); }
}

abstract class ASTUniOp extends ASTOp {
  static Type[] newsig() {
    Type t1 = Type.dblary();
    return new Type[]{t1,t1};
  }
  ASTUniOp( int form, int precedence, int association ) {
    super(VARS1,newsig(),form,precedence,association);
  }
  double op( double d ) { throw H2O.fail(); }
  protected ASTUniOp( String[] vars, Type[] types, int form, int precedence, int association ) {
    super(vars,types,form,precedence,association);
  }
  @Override void apply(Env env, int argcnt, ASTApply apply) {
    // Expect we can broadcast across all functions as needed.
    if( !env.isAry() ) { env.poppush(op(env.popDbl())); return; }
    Frame fr = env.popAry();
    String skey = env.key();
    final ASTUniOp uni = this;  // Final 'this' so can use in closure
    Frame fr2 = new MRTask2() {
        @Override public void map( Chunk chks[], NewChunk nchks[] ) {
          for( int i=0; i<nchks.length; i++ ) {
            NewChunk n =nchks[i];
            Chunk c = chks[i];
            int rlen = c._len;
            for( int r=0; r<rlen; r++ )
              n.addNum(uni.op(c.at0(r)));
          }
        }
      }.doAll(fr.numCols(),fr).outputFrame(fr._names, null);
    env.subRef(fr,skey);
    env.pop();                  // Pop self
    env.push(fr2);
  }
}

abstract class ASTUniPrefixOp extends ASTUniOp {
  ASTUniPrefixOp( ) { super(OPF_PREFIX,OPP_PREFIX,OPA_RIGHT); }
  ASTUniPrefixOp( String[] vars, Type[] types ) { super(vars,types,OPF_PREFIX,OPP_PREFIX,OPA_RIGHT); }
}

class ASTCos  extends ASTUniPrefixOp { @Override String opStr(){ return "cos";   } @Override ASTOp make() {return new ASTCos ();} @Override double op(double d) { return Math.cos(d);}}
class ASTSin  extends ASTUniPrefixOp { @Override String opStr(){ return "sin";   } @Override ASTOp make() {return new ASTSin ();} @Override double op(double d) { return Math.sin(d);}}
class ASTTan  extends ASTUniPrefixOp { @Override String opStr(){ return "tan";   } @Override ASTOp make() {return new ASTTan ();} @Override double op(double d) { return Math.tan(d);}}
class ASTACos extends ASTUniPrefixOp { @Override String opStr(){ return "acos";  } @Override ASTOp make() {return new ASTACos();} @Override double op(double d) { return Math.acos(d);}}
class ASTASin extends ASTUniPrefixOp { @Override String opStr(){ return "asin";  } @Override ASTOp make() {return new ASTASin();} @Override double op(double d) { return Math.asin(d);}}
class ASTATan extends ASTUniPrefixOp { @Override String opStr(){ return "atan";  } @Override ASTOp make() {return new ASTATan();} @Override double op(double d) { return Math.atan(d);}}
class ASTCosh extends ASTUniPrefixOp { @Override String opStr(){ return "cosh";  } @Override ASTOp make() {return new ASTCosh ();} @Override double op(double d) { return Math.cosh(d);}}
class ASTSinh extends ASTUniPrefixOp { @Override String opStr(){ return "sinh";  } @Override ASTOp make() {return new ASTSinh ();} @Override double op(double d) { return Math.sinh(d);}}
class ASTTanh extends ASTUniPrefixOp { @Override String opStr(){ return "tanh";  } @Override ASTOp make() {return new ASTTanh ();} @Override double op(double d) { return Math.tanh(d);}}

class ASTAbs  extends ASTUniPrefixOp { @Override String opStr(){ return "abs";   } @Override ASTOp make() {return new ASTAbs ();} @Override double op(double d) { return Math.abs(d);}}
class ASTSgn  extends ASTUniPrefixOp { @Override String opStr(){ return "sgn" ;  } @Override ASTOp make() {return new ASTSgn ();} @Override double op(double d) { return Math.signum(d);}}
class ASTSqrt extends ASTUniPrefixOp { @Override String opStr(){ return "sqrt";  } @Override ASTOp make() {return new ASTSqrt();} @Override double op(double d) { return Math.sqrt(d);}}
class ASTCeil extends ASTUniPrefixOp { @Override String opStr(){ return "ceil";  } @Override ASTOp make() {return new ASTCeil();} @Override double op(double d) { return Math.ceil(d);}}
class ASTFlr  extends ASTUniPrefixOp { @Override String opStr(){ return "floor"; } @Override ASTOp make() {return new ASTFlr ();} @Override double op(double d) { return Math.floor(d);}}
class ASTTrun extends ASTUniPrefixOp { @Override String opStr(){ return "trunc"; } @Override ASTOp make() {return new ASTTrun();} @Override double op(double d) { return d>=0?Math.floor(d):Math.ceil(d);}}
class ASTLog  extends ASTUniPrefixOp { @Override String opStr(){ return "log";   } @Override ASTOp make() {return new ASTLog ();} @Override double op(double d) { return Math.log(d);}}
class ASTExp  extends ASTUniPrefixOp { @Override String opStr(){ return "exp";   } @Override ASTOp make() {return new ASTExp ();} @Override double op(double d) { return Math.exp(d);}}
//class ASTIsNA extends ASTUniPrefixOp { @Override String opStr(){ return "is.na"; } @Override ASTOp make() {return new ASTIsNA();} @Override double op(double d) { return Double.isNaN(d)?1:0;}}
class ASTIsNA extends ASTUniPrefixOp { @Override String opStr(){ return "is.na";} @Override ASTOp make() { return new ASTIsNA();} @Override double op(double d) { return Double.isNaN(d)?1:0;}
  @Override void apply(Env env, int argcnt, ASTApply apply) {
    // Expect we can broadcast across all functions as needed.
    if( !env.isAry() ) { env.poppush(op(env.popDbl())); return; }
    Frame fr = env.popAry();
    String skey = env.key();
    final ASTUniOp uni = this;  // Final 'this' so can use in closure
    Frame fr2 = new MRTask2() {
      @Override public void map( Chunk chks[], NewChunk nchks[] ) {
        for( int i=0; i<nchks.length; i++ ) {
          NewChunk n = nchks[i];
          Chunk c = chks[i];
          int rlen = c._len;
          for( int r=0; r<rlen; r++ )
            n.addNum( ( c.isNA0(r) || isNA0(c, r)) ? 1 : 0);
        }
      }
    }.doAll(fr.numCols(),fr).outputFrame(fr._names, null);
    env.subRef(fr,skey);
    env.pop();                  // Pop self
    env.push(fr2);
  }

  private boolean isNA0(Chunk c, int row0) {
    if (c._vec.isEnum()) {
      if (c._vec.domain()[(int) c.at0(row0)].equals("NA")) return true;
    }
    return false;
  }
}

class ASTWhich extends ASTOp {

  ASTWhich() { super(new String[]{"which", "x"},
                     new Type[]{Type.dblary(), Type.dblary()},
                     OPF_PREFIX, OPP_PREFIX, OPA_RIGHT);}

  @Override String opStr() { return "which"; }
  @Override ASTOp  make()  { return new ASTWhich(); }

  @Override void apply(Env env, int argcnt, ASTApply apply) {
    if(env.isAry()) {
      Frame fr = env.popAry();
      if (fr.numCols() != 1) throw new IllegalArgumentException("`which` accepts at exactly 1 column!");
      String skey = env.key();
      Frame fr2 = new MRTask2() {
        @Override public void map(Chunk chk, NewChunk nchk) {
          for (int r = 0; r < chk._len; ++r)
            if (chk.at0(r) == 1) nchk.addNum(chk._start + r + 1);
        }
      }.doAll(1,fr).outputFrame(new String[]{"which"},null);
      env.subRef(fr,skey);
      env.pop();                  // Pop self
      env.push(fr2);
    }
  }
}



class ASTRound extends ASTOp {
  @Override String opStr() { return "round"; }
  ASTRound() { super(new String[]{"round", "x", "digits"},
                   new Type[]{Type.dblary(), Type.dblary(), Type.DBL},
                   OPF_PREFIX,
                   OPP_PREFIX,
                   OPA_RIGHT);
  }
  @Override ASTOp make() { return this; }
  @Override void apply(Env env, int argcnt, ASTApply apply) {
    final int digits = (int)env.popDbl();
    if(env.isAry()) {
      Frame fr = env.popAry();
      for(int i = 0; i < fr.vecs().length; i++) {
        if(fr.vecs()[i].isEnum())
          throw new IllegalArgumentException("Non-numeric column " + String.valueOf(i+1) + " in data frame");
      }
      String skey = env.key();
      Frame fr2 = new MRTask2() {
        @Override public void map(Chunk chks[], NewChunk nchks[]) {
          for(int i = 0; i < nchks.length; i++) {
            NewChunk n = nchks[i];
            Chunk c = chks[i];
            int rlen = c._len;
            for(int r = 0; r < rlen; r++)
              n.addNum(roundDigits(c.at0(r),digits));
          }
        }
      }.doAll(fr.numCols(),fr).outputFrame(fr.names(),fr.domains());
      env.subRef(fr,skey);
      env.pop();                  // Pop self
      env.push(fr2);
    }
    else
      env.poppush(roundDigits(env.popDbl(),digits));
  }
  static double roundDigits(double x, int digits) {
    if(Double.isNaN(x)) return x;
    BigDecimal bd = new BigDecimal(x);
    bd = bd.setScale(digits, RoundingMode.HALF_EVEN);
    return bd.doubleValue();
  }
}

class ASTSignif extends ASTOp {
  @Override String opStr() { return "signif"; }
  ASTSignif() { super(new String[]{"signif", "x", "digits"},
                   new Type[]{Type.dblary(), Type.dblary(), Type.DBL},
                   OPF_PREFIX,
                   OPP_PREFIX,
                   OPA_RIGHT);
  }
  @Override ASTOp make() { return this; }
  @Override void apply(Env env, int argcnt, ASTApply apply) {
    final int digits = (int)env.popDbl();
    if(digits < 0)
      throw new IllegalArgumentException("Error in signif: argument digits must be a non-negative integer");

    if(env.isAry()) {
      Frame fr = env.popAry();
      for(int i = 0; i < fr.vecs().length; i++) {
        if(fr.vecs()[i].isEnum())
          throw new IllegalArgumentException("Non-numeric column " + String.valueOf(i+1) + " in data frame");
      }
      String skey = env.key();
      Frame fr2 = new MRTask2() {
        @Override public void map(Chunk chks[], NewChunk nchks[]) {
          for(int i = 0; i < nchks.length; i++) {
            NewChunk n = nchks[i];
            Chunk c = chks[i];
            int rlen = c._len;
            for(int r = 0; r < rlen; r++)
              n.addNum(signifDigits(c.at0(r),digits));
          }
        }
      }.doAll(fr.numCols(),fr).outputFrame(fr.names(),fr.domains());
      env.subRef(fr,skey);
      env.pop();                  // Pop self
      env.push(fr2);
    }
    else
      env.poppush(signifDigits(env.popDbl(),digits));
  }
  static double signifDigits(double x, int digits) {
    if(Double.isNaN(x)) return x;
    BigDecimal bd = new BigDecimal(x);
    bd = bd.round(new MathContext(digits, RoundingMode.HALF_EVEN));
    return bd.doubleValue();
  }
}

class ASTNrow extends ASTUniPrefixOp {
  ASTNrow() { super(VARS1,new Type[]{Type.DBL,Type.ARY}); }
  @Override String opStr() { return "nrow"; }
  @Override ASTOp make() {return this;}
  @Override void apply(Env env, int argcnt, ASTApply apply) {
    Frame fr = env.popAry();
    String skey = env.key();
    double d = fr.numRows();
    env.subRef(fr,skey);
    env.poppush(d);
  }
}

class ASTNcol extends ASTUniPrefixOp {
  ASTNcol() { super(VARS1,new Type[]{Type.DBL,Type.ARY}); }
  @Override String opStr() { return "ncol"; }
  @Override ASTOp make() {return this;}
  @Override void apply(Env env, int argcnt, ASTApply apply) {
    Frame fr = env.popAry();
    String skey = env.key();
    double d = fr.numCols();
    env.subRef(fr,skey);
    env.poppush(d);
  }
}

class ASTLength extends ASTUniPrefixOp {
  ASTLength() { super(VARS1, new Type[]{Type.DBL,Type.ARY}); }
  @Override String opStr() { return "length"; }
  @Override ASTOp make() { return this; }
  @Override void apply(Env env, int argcnt, ASTApply apply) {
    Frame fr = env.popAry();
    String skey = env.key();
    double d = fr.numCols() == 1 ? fr.numRows() : fr.numCols();
    env.subRef(fr,skey);
    env.poppush(d);
  }
}

class ASTIsFactor extends ASTUniPrefixOp {
  ASTIsFactor() { super(VARS1,new Type[]{Type.DBL,Type.ARY}); }
  @Override String opStr() { return "is.factor"; }
  @Override ASTOp make() {return this;}
  @Override void apply(Env env, int argcnt, ASTApply apply) {
    if(!env.isAry()) { env.poppush(0); return; }
    Frame fr = env.popAry();
    String skey = env.key();
    double d = 1;
    Vec[] v = fr.vecs();
    for(int i = 0; i < v.length; i++) {
      if(!v[i].isEnum()) { d = 0; break; }
    }
    env.subRef(fr,skey);
    env.poppush(d);
  }
}

// Added to facilitate Runit testing
class ASTAnyFactor extends ASTUniPrefixOp {
  ASTAnyFactor() { super(VARS1,new Type[]{Type.DBL,Type.ARY}); }
  @Override String opStr() { return "any.factor"; }
  @Override ASTOp make() {return this;}
  @Override void apply(Env env, int argcnt, ASTApply apply) {
    if(!env.isAry()) { env.poppush(0); return; }
    Frame fr = env.popAry();
    String skey = env.key();
    double d = 0;
    Vec[] v = fr.vecs();
    for(int i = 0; i < v.length; i++) {
      if(v[i].isEnum()) { d = 1; break; }
    }
    env.subRef(fr,skey);
    env.poppush(d);
  }
}

class ASTCanBeCoercedToLogical extends ASTUniPrefixOp {
  ASTCanBeCoercedToLogical() { super(VARS1,new Type[]{Type.DBL,Type.ARY}); }
  @Override String opStr() { return "canBeCoercedToLogical"; }
  @Override ASTOp make() {return this;}
  @Override void apply(Env env, int argcnt, ASTApply apply) {
    if(!env.isAry()) { env.poppush(0); return; }
    Frame fr = env.popAry();
    String skey = env.key();
    double d = 0;
    Vec[] v = fr.vecs();
    for (Vec aV : v) {
      if (aV.isInt()) {
        if ((aV.min() == 0 && aV.max() == 1) || (aV.min() == 0 && aV.min() == aV.max()) || (aV.min() == 1 && aV.min() == aV.max())) {
          d = 1;
          break;
        }
      }
    }
    env.subRef(fr,skey);
    env.poppush(d);
  }
}

class ASTAnyNA extends ASTUniPrefixOp {
  ASTAnyNA() { super(VARS1,new Type[]{Type.DBL,Type.ARY}); }
  @Override String opStr() { return "any.na"; }
  @Override ASTOp make() {return this;}
  @Override void apply(Env env, int argcnt, ASTApply apply) {
    if(!env.isAry()) { env.poppush(0); return; }
    Frame fr = env.popAry();
    String skey = env.key();
    double d = 0;
    Vec[] v = fr.vecs();
    for(int i = 0; i < v.length; i++) {
      if(v[i].naCnt() > 0) { d = 1; break; }
    }
    env.subRef(fr, skey);
    env.poppush(d);
  }
}

class ASTIsTRUE extends ASTUniPrefixOp {
  ASTIsTRUE() {super(VARS1,new Type[]{Type.DBL,Type.unbound()});}
  @Override String opStr() { return "isTRUE"; }
  @Override ASTOp make() {return new ASTIsTRUE();}  // to make sure fcn get bound at each new context
  @Override void apply(Env env, int argcnt, ASTApply apply) {
    double res = env.isDbl() && env.popDbl()==1.0 ? 1:0;
    env.pop();
    env.poppush(res);
  }
}

class ASTScale extends ASTUniPrefixOp {
  ASTScale() { super(VARS1,new Type[]{Type.ARY,Type.ARY}); }
  @Override String opStr() { return "scale"; }
  @Override ASTOp make() {return this;}
  @Override void apply(Env env, int argcnt, ASTApply apply) {
    if(!env.isAry()) { env.poppush(Double.NaN); return; }
    Frame fr = env.popAry();
    String skey = env.key();
    Frame fr2 = new Scale().doIt(fr.numCols(), fr).outputFrame(fr._names, fr.domains());
    env.subRef(fr,skey);
    env.pop();                  // Pop self
    env.push(fr2);
  }

  private static class Scale extends MRTask2<Scale> {
    protected int _nums = 0;
    protected int[] _ind;    // Saves indices of numeric cols first, followed by enums
    protected double[] _normSub;
    protected double[] _normMul;

    @Override public void map(Chunk chks[], NewChunk nchks[]) {
      // Normalize numeric cols only
      for(int k = 0; k < _nums; k++) {
        int i = _ind[k];
        NewChunk n = nchks[i];
        Chunk c = chks[i];
        int rlen = c._len;
        for(int r = 0; r < rlen; r++)
          n.addNum((c.at0(r)-_normSub[i])*_normMul[i]);
      }

      for(int k = _nums; k < chks.length; k++) {
        int i = _ind[k];
        NewChunk n = nchks[i];
        Chunk c = chks[i];
        int rlen = c._len;
        for(int r = 0; r < rlen; r++)
          n.addNum(c.at0(r));
      }
    }

    public Scale doIt(int outputs, Frame fr) { return dfork2(outputs, fr).getResult(); }
    public Scale dfork2(int outputs, Frame fr) {
      final Vec [] vecs = fr.vecs();
      for(int i = 0; i < vecs.length; i++) {
        if(!vecs[i].isEnum()) _nums++;
      }
      if(_normSub == null) _normSub = MemoryManager.malloc8d(_nums);
      if(_normMul == null) { _normMul = MemoryManager.malloc8d(_nums); Arrays.fill(_normMul,1); }
      if(_ind == null) _ind = MemoryManager.malloc4(vecs.length);

      int ncnt = 0; int ccnt = 0;
      for(int i = 0; i < vecs.length; i++){
        if(!vecs[i].isEnum()) {
          _normSub[ncnt] = vecs[i].mean();
          _normMul[ncnt] = 1.0/vecs[i].sigma();
          _ind[ncnt++] = i;
        } else
          _ind[_nums+(ccnt++)] = i;
      }
      assert ncnt == _nums && (ncnt + ccnt == vecs.length);
      return dfork(outputs, fr, false);
    }
  }
}

// ----
abstract class ASTTimeOp extends ASTOp {
  static Type[] newsig() {
    Type t1 = Type.dblary();
    return new Type[]{t1,t1};
  }
  ASTTimeOp() { super(VARS1,newsig(),OPF_PREFIX,OPP_PREFIX,OPA_RIGHT); }
  abstract long op( MutableDateTime dt );
  @Override void apply(Env env, int argcnt, ASTApply apply) {
    // Single instance of MDT for the single call
    if( !env.isAry() ) {        // Single point
      double d = env.popDbl();
      if( !Double.isNaN(d) ) d = op(new MutableDateTime((long)d));
      env.poppush(d);
      return;
    }
    // Whole column call
    Frame fr = env.popAry();
    String skey = env.key();
    final ASTTimeOp uni = this;  // Final 'this' so can use in closure
    Frame fr2 = new MRTask2() {
        @Override public void map( Chunk chks[], NewChunk nchks[] ) {
          MutableDateTime dt = new MutableDateTime(0);
          for( int i=0; i<nchks.length; i++ ) {
            NewChunk n =nchks[i];
            Chunk c = chks[i];
            int rlen = c._len;
            for( int r=0; r<rlen; r++ ) {
              double d = c.at0(r);
              if( !Double.isNaN(d) ) {
                dt.setMillis((long)d);
                d = uni.op(dt);
              }
              n.addNum(d);
            }
          }
        }
      }.doAll(fr.numCols(),fr).outputFrame(fr._names, null);
    env.subRef(fr,skey);
    env.pop();                  // Pop self
    env.push(fr2);
  }
}

class ASTYear  extends ASTTimeOp { @Override String opStr(){ return "year" ; } @Override ASTOp make() {return new ASTYear  ();} @Override long op(MutableDateTime dt) { return dt.getYear();}}
class ASTMonth extends ASTTimeOp { @Override String opStr(){ return "month"; } @Override ASTOp make() {return new ASTMonth ();} @Override long op(MutableDateTime dt) { return dt.getMonthOfYear()-1;}}
class ASTDay   extends ASTTimeOp { @Override String opStr(){ return "day"  ; } @Override ASTOp make() {return new ASTDay   ();} @Override long op(MutableDateTime dt) { return dt.getDayOfMonth();}}
class ASTHour  extends ASTTimeOp { @Override String opStr(){ return "hour" ; } @Override ASTOp make() {return new ASTHour  ();} @Override long op(MutableDateTime dt) { return dt.getHourOfDay();}}
class ASTMinute extends ASTTimeOp { @Override String opStr(){return "minute";} @Override ASTOp make() {return new ASTMinute();} @Override long op(MutableDateTime dt) { return dt.getMinuteOfHour();}}
class ASTSecond extends ASTTimeOp { @Override String opStr(){return "second";} @Override ASTOp make() {return new ASTSecond();} @Override long op(MutableDateTime dt) { return dt.getSecondOfMinute();}}
class ASTMillis extends ASTTimeOp { @Override String opStr(){return "millis";} @Override ASTOp make() {return new ASTMillis();} @Override long op(MutableDateTime dt) { return dt.getMillisOfSecond();}}

class ASTasDate extends ASTOp {
  ASTasDate() { super(new String[]{"as.Date", "x", "format"},
          new Type[]{Type.ARY, Type.ARY, Type.STR},
          OPF_PREFIX,
          OPP_PREFIX,OPA_RIGHT); }
  @Override String opStr() { return "as.Date"; }
  @Override ASTOp make() {return new ASTasDate();}
  @Override void apply(Env env, int argcnt, ASTApply apply) {
    final String format = env.popStr();
    if (format.isEmpty()) throw new IllegalArgumentException("as.Date requires a non-empty format string");
    // check the format string more?

    Frame fr = env.ary(-1);

    if( fr.vecs().length != 1 || !fr.vecs()[0].isEnum() )
      throw new IllegalArgumentException("as.Date requires a single column of factors");

    Frame fr2 = new MRTask2() {
      @Override public void map( Chunk chks[], NewChunk nchks[] ) {
        //done on each node in lieu of rewriting DateTimeFormatter as Iced
        DateTimeFormatter dtf = ParseTime.forStrptimePattern(format).withZone(ParseTime.getTimezone());
        for( int i=0; i<nchks.length; i++ ) {
          NewChunk n =nchks[i];
          Chunk c = chks[i];
          int rlen = c._len;
          for( int r=0; r<rlen; r++ ) {
            if (!c.isNA0(r)) {
              String date = c._vec.domain((long)c.at0(r));
              n.addNum(DateTime.parse(date, dtf).getMillis(), 0);
            } else n.addNA();
          }
        }
      }
    }.doAll(fr.numCols(),fr).outputFrame(fr._names, null);
    env.poppush(2, fr2, null);
  }
}

class ASTStrSplit extends ASTOp {
  ASTStrSplit() { super(new String[]{"strsplit", "x", "split"},
          new Type[]{Type.ARY, Type.ARY, Type.STR},
          OPF_PREFIX,
          OPP_PREFIX, OPA_RIGHT); }
  @Override String opStr() { return "strsplit"; }
  @Override ASTOp make() { return new ASTStrSplit(); }
  @Override void apply(Env env, int argcnt, ASTApply apply) {
    String split = env.popStr();
    Frame fr = env.ary(-1);
    if (fr.numCols() != 1) throw new IllegalArgumentException("strsplit requires a single column.");
    split = split.isEmpty() ? "" : split;
    final String[]   old_domains = fr.anyVec().domain();
    final String[][] new_domains = newDomains(old_domains, split);
    final String[]   col_names   = new String[new_domains.length];
    for (int i = 1; i <= col_names.length; ++i)
      col_names[i-1] = "C"+i;

    final String regex = split;
    Frame fr2 = new MRTask2() {
      @Override public void map(Chunk[] cs, NewChunk[] ncs) {
        Chunk c = cs[0];
        for (int i = 0; i < c._len; ++i) {
          int idx = (int)c.at0(i);
          String s = old_domains[idx];
          String[] ss = s.split(regex);
          int cnt = 0;
          for (int j = 0; j < ss.length; ++j) {
            int n_idx = Arrays.asList(new_domains[cnt]).indexOf(ss[j]);
            if (n_idx == -1) ncs[cnt++].addNA();
            else ncs[cnt++].addNum(n_idx);
          }
          if (cnt < ncs.length)
            for (; cnt < ncs.length; ++cnt) ncs[cnt].addNA();
        }
      }
    }.doAll(col_names.length, fr).outputFrame(col_names, new_domains);

    env.poppush(2, fr2, null);
  }

  private String[][] newDomains(String[] domains, String regex) {
    ArrayList<HashSet<String>> strs = new ArrayList<HashSet<String>>();
    for (String domain : domains) {
      String[] news = domain.split(regex);
      for (int i = 0; i < news.length; ++i) {
        if (strs.size() == i) {
          HashSet<String> x = new HashSet<String>();
          x.add(news[i]);
          strs.add(x);
        } else {
          HashSet<String> x = strs.get(i);
          x.add(news[i]);
          strs.set(i, x);
        }
      }
    }
    String[][] doms = new String[strs.size()][];
    for (int i = 0; i < strs.size(); ++i) {
      HashSet<String> x = strs.get(i);
      doms[i] = new String[x.size()];
      for (int j = 0; j < x.size(); ++j)
        doms[i][j] = (String)x.toArray()[j];
    }
    return doms;
  }
}

class ASTToLower extends ASTUniPrefixOp {

  @Override String opStr() { return "tolower"; }
  @Override ASTOp make() { return new ASTToLower(); }

  @Override void apply(Env env, int argcnt, ASTApply apply) {
    if( !env.isAry() ) { throw new IllegalArgumentException("tolower only operates on a single vector!"); }
    Frame fr = env.popAry();
    if (fr.numCols() != 1) throw new IllegalArgumentException("tolower only takes a single column of data. Got "+ fr.numCols()+" columns.");
    String skey = env.key();
    String[] new_dom = fr.anyVec().domain().clone();
    for (int i = 0; i < new_dom.length; ++i)
      new_dom[i] = new_dom[i].toLowerCase(Locale.ENGLISH);

    Frame fr2 = new Frame(fr._names, fr.vecs());
    fr2.anyVec()._domain = new_dom;
    env.subRef(fr,skey);
    env.pop();
    env.push(fr2);
  }
}

class ASTToUpper extends ASTUniPrefixOp {

  @Override String opStr() { return "toupper"; }
  @Override ASTOp make() { return new ASTToUpper(); }

  @Override void apply(Env env, int argcnt, ASTApply apply) {
    if( !env.isAry() ) { throw new IllegalArgumentException("toupper only operates on a single vector!"); }
    Frame fr = env.popAry();
    if (fr.numCols() != 1) throw new IllegalArgumentException("toupper only takes a single column of data. Got "+ fr.numCols()+" columns.");
    String skey = env.key();
    String[] new_dom = fr.anyVec().domain().clone();
    for (int i = 0; i < new_dom.length; ++i)
      new_dom[i] = new_dom[i].toUpperCase(Locale.ENGLISH);

    Frame fr2 = new Frame(fr._names, fr.vecs());
    fr2.anyVec()._domain = new_dom;
    env.subRef(fr,skey);
    env.pop();
    env.push(fr2);
  }
}

class ASTRevalue extends ASTOp {

  ASTRevalue(){ super(new String[]{"revalue", "x", "replace", "warn_missing"},
          new Type[]{Type.ARY, Type.ARY, Type.STR, Type.DBL},
          OPF_PREFIX,
          OPP_PREFIX, OPA_RIGHT); }

  @Override String opStr() { return "revalue"; }
  @Override ASTOp  make()  { return new ASTRevalue(); }

  @Override void apply(Env env, int argcnt, ASTApply apply) {
    final boolean warn_missing = env.popDbl() == 1;
    final String replace = env.popStr();
    String skey = env.key();
    Frame fr = env.popAry();
    if (fr.numCols() != 1) throw new IllegalArgumentException("revalue works on a single column at a time.");
    String[] old_dom = fr.anyVec()._domain;
    if (old_dom == null) throw new IllegalArgumentException("Column is not a factor column. Can only revalue a factor column.");

    HashMap<String, String> dom_map = hashMap(replace);

    for (int i = 0; i < old_dom.length; ++i) {
      if (dom_map.containsKey(old_dom[i])) {
        old_dom[i] = dom_map.get(old_dom[i]);
        dom_map.remove(old_dom[i]);
      }
    }
    if (dom_map.size() > 0 && warn_missing) {
      for (String k : dom_map.keySet()) {
        env._warnings = Arrays.copyOf(env._warnings, env._warnings.length + 1);
        env._warnings[env._warnings.length - 1] = "Warning: old value " + k + " not a factor level.";
      }
    }
  }

  private HashMap<String, String> hashMap(String replace) {
    HashMap<String, String> map = new HashMap<String, String>();
    //replace is a ';' separated string. Each piece after splitting is a key:value pair.
    String[] maps = replace.split(";");
    for (String s : maps) {
      String[] pair = s.split(":");
      String key   = pair[0];
      String value = pair[1];
      map.put(key, value);
    }
    return map;
  }
}


class ASTGSub extends ASTOp {
  ASTGSub() { super(new String[]{"gsub", "pattern", "replacement", "x", "ignore.case"},
          new Type[]{Type.ARY, Type.STR, Type.STR, Type.ARY, Type.DBL},
          OPF_PREFIX,
          OPP_PREFIX, OPA_RIGHT); }
  @Override String opStr() { return "gsub"; }
  @Override ASTOp make() { return new ASTGSub(); }
  @Override void apply(Env env, int argcnt, ASTApply apply) {
    final boolean ignore_case = env.popDbl() == 1;
    String skey = env.key();
    Frame fr = env.popAry();
    if (fr.numCols() != 1) throw new IllegalArgumentException("gsub works on a single column at a time.");
    final String replacement = env.popStr();
    final String pattern = env.popStr();
    String[] doms = fr.anyVec().domain().clone();
    for (int i = 0; i < doms.length; ++i)
      doms[i] = ignore_case ? doms[i].toLowerCase(Locale.ENGLISH).replaceAll(pattern, replacement)
                            : doms[i].replaceAll(pattern, replacement);

    Frame fr2 = new Frame(fr.names(), fr.vecs());
    fr2.anyVec()._domain = doms;
    env.subRef(fr, skey);
    env.poppush(1, fr2, null);
  }
}

class ASTSetLevel extends ASTOp {
  ASTSetLevel() { super(new String[]{"setLevel", "x", "level"},
          new Type[]{Type.ARY, Type.ARY, Type.STR},
          OPF_PREFIX,
          OPP_PREFIX, OPA_RIGHT); }
  @Override String opStr() { return "setLevel"; }
  @Override ASTOp make() { return new ASTSetLevel(); }
  @Override void apply(Env env, int argcnt, ASTApply apply) {
    final String level = env.popStr();
    String skey = env.key();
    Frame fr = env.popAry();
    if (fr.numCols() != 1) throw new IllegalArgumentException("setLevel works on a single column at a time.");
    String[] doms = fr.anyVec().domain().clone();
    if( doms == null )
      throw new IllegalArgumentException("Cannot set the level on a non-factor column!");
    final int idx = Arrays.asList(doms).indexOf(level);
    if (idx == -1)
      throw new IllegalArgumentException("Did not find level `" + level + "` in the column.");

    Frame fr2 = new MRTask2() {
      @Override public void map(Chunk c, NewChunk nc) {
        for (int i=0;i<c._len;++i)
          nc.addNum(idx);
      }
    }.doAll(1, fr.anyVec()).outputFrame(null, fr.names(), fr.domains());
    env.subRef(fr, skey);
    env.poppush(1, fr2, null);
  }
}

class ASTTrim extends ASTOp {
  ASTTrim() { super(new String[]{"trim","x"},
          new Type[]{Type.dblary(), Type.dblary()},
          OPF_PREFIX,
          OPP_PREFIX, OPA_RIGHT); }
  @Override String opStr() { return "trim"; }
  @Override ASTOp make() { return new ASTTrim(); }
  @Override void apply(Env env, int argcnt, ASTApply apply) {
    String skey = env.key();
    Frame fr = env.popAry();
    if (fr.numCols() != 1) throw new IllegalArgumentException("trim works on a single column at a time.");
    String[] doms = fr.anyVec().domain().clone();
    for (int i = 0; i < doms.length; ++i) doms[i] = doms[i].trim();
    Frame fr2 = new Frame(fr.names(), fr.vecs());
    fr2.anyVec()._domain = doms;
    env.subRef(fr, skey);
    env.poppush(1, fr2, null);
  }
}

//FIXME: Create new chunks that overlay the frame to avoid ragged chunk issue
class ASTSample extends ASTOp {
  ASTSample() { super(new String[]{"sample", "ary", "nobs", "seed"},
                      new Type[]{Type.ARY, Type.ARY, Type.DBL, Type.DBL},
                      OPF_PREFIX, OPP_PREFIX, OPA_RIGHT); }
  @Override String opStr() { return "sample"; }
  @Override ASTOp make() { return new ASTSample(); }
  @Override void apply(Env env, int argcnt, ASTApply apply) {
    final double seed = env.popDbl();
    final double nobs = env.popDbl();
    String skey = env.key();
    Frame fr = env.popAry();
    long[] espc = fr.anyVec()._espc;
    long[] chk_sizes = new long[espc.length];
    final long[] css = new long[espc.length];
    for (int i = 0; i < espc.length-1; ++i)
      chk_sizes[i] = espc[i+1] - espc[i];
    chk_sizes[chk_sizes.length-1] = fr.numRows() - espc[espc.length-1];
    long per_chunk_sample = (long) Math.floor(nobs / (double)espc.length);
    long defecit = (long) (nobs - per_chunk_sample*espc.length) ;
    // idxs is an array list of chunk indexes for adding to the sample size. Chunks with no defecit can not be "sampled" as candidates.
    ArrayList<Integer> idxs = new ArrayList<Integer>();
    for (int i = 0; i < css.length; ++i) {
      // get the max allowed rows to sample from the chunk
      css[i] = Math.min(per_chunk_sample, chk_sizes[i]);
      // if per_chunk_sample > css[i] => spread around the defecit to meet number of rows requirement.
      long def = per_chunk_sample - css[i];
      // no more "room" in chunk `i`
      if (def >= 0) {
        defecit += def;
      // else `i` has "room"
      }
      if (chk_sizes[i] > per_chunk_sample) idxs.add(i);
    }
    if (defecit > 0) {
      Random rng = new Random(seed != -1 ? (long)seed : System.currentTimeMillis());
      while (defecit > 0) {
        if (idxs.size() <= 0) break;
        // select chunks at random and add to the number of rows they should sample,
        // up to the number of rows in the chunk.
        int rand = rng.nextInt(idxs.size());
        if (css[idxs.get(rand)] == chk_sizes[idxs.get(rand)]) {
          idxs.remove(rand);
          continue;
        }
        css[idxs.get(rand)]++;
        defecit--;
      }
    }

    Frame fr2 = new MRTask2() {
      @Override public void map(Chunk[] chks, NewChunk[] nchks) {
        int N = chks[0]._len;
        int m = 0;
        long n = css[chks[0].cidx()];
        int row = 0;
        Random rng = new Random(seed != -1 ? (long)seed : System.currentTimeMillis());
        while( m  < n) {
          double u = rng.nextDouble();
          if ( (N - row)* u >= (n - m)) {
            row++;
          } else {
            for (int i = 0; i < chks.length; ++i) nchks[i].addNum(chks[i].at0(row));
            row++; m++;
          }
        }
      }
    }.doAll(fr.numCols(), fr).outputFrame(fr.names(), fr.domains());
    env.subRef(fr, skey);
    env.poppush(1, fr2, null);
  }
}

class ASTStrSub extends ASTOp {
  ASTStrSub() { super(new String[]{"sub", "pattern", "replacement", "x", "ignore.case"},
          new Type[]{Type.ARY, Type.STR, Type.STR, Type.ARY, Type.DBL},
          OPF_PREFIX,
          OPP_PREFIX, OPA_RIGHT); }
  @Override String opStr() { return "sub"; }
  @Override ASTOp make() { return new ASTStrSub(); }
  @Override void apply(Env env, int argcnt, ASTApply apply) {
    final boolean ignore_case = env.popDbl() == 1;
    String skey = env.key();
    Frame fr = env.popAry();
    if (fr.numCols() != 1) throw new IllegalArgumentException("sub works on a single column at a time.");
    final String replacement = env.popStr();
    final String pattern = env.popStr();
    String[] doms = fr.anyVec().domain().clone();
    for (int i = 0; i < doms.length; ++i)
      doms[i] = ignore_case ? doms[i].toLowerCase(Locale.ENGLISH).replaceFirst(pattern, replacement)
              : doms[i].replaceFirst(pattern, replacement);

    Frame fr2 = new Frame(fr.names(), fr.vecs());
    fr2.anyVec()._domain = doms;
    env.subRef(fr, skey);
    env.poppush(1, fr2, null);
  }
}

// Finite backward difference for user-specified lag
// http://en.wikipedia.org/wiki/Finite_difference
class ASTDiff extends ASTOp {
  ASTDiff() { super(new String[]{"diff", "x", "lag", "differences"},
                      new Type[]{Type.ARY, Type.ARY, Type.DBL, Type.DBL},
                      OPF_PREFIX,
                      OPP_PREFIX,
                      OPA_RIGHT); }
  @Override String opStr() { return "diff"; }
  @Override ASTOp make() {return new ASTDiff();}
  @Override void apply(Env env, int argcnt, ASTApply apply) {
    final int diffs = (int)env.popDbl();
    if(diffs < 0) throw new IllegalArgumentException("differences must be an integer >= 1");
    final int lag = (int)env.popDbl();
    if(lag < 0) throw new IllegalArgumentException("lag must be an integer >= 1");

    Frame fr = env.popAry();
    String skey = env.key();
    if(fr.vecs().length != 1 || fr.vecs()[0].isEnum())
      throw new IllegalArgumentException("diff takes a single numeric column vector");

    Frame fr2 = new MRTask2() {
      @Override public void map(Chunk chk, NewChunk nchk) {
        int rstart = (int)(diffs*lag - chk._start);
        if(rstart > chk._len) return;
        rstart = Math.max(0, rstart);

        // Formula: \Delta_h^n x_t = \sum_{i=0}^n (-1)^i*\binom{n}{k}*x_{t-i*h}
        for(int r = rstart; r < chk._len; r++) {
          double x = chk.at0(r);
          long row = chk._start + r;

          for(int i = 1; i <= diffs; i++) {
            double x_lag = chk.at_slow(row - i*lag);
            double coef = ArithmeticUtils.binomialCoefficient(diffs, i);
            x += (i % 2 == 0) ? coef*x_lag : -coef*x_lag;
          }
          nchk.addNum(x);
        }
      }
    }.doAll(1,fr).outputFrame(fr.names(), fr.domains());
    env.subRef(fr, skey);
    env.pop();
    env.push(fr2);
  }
}

// ----
// Class of things that will auto-expand across arrays in a 2-to-1 way:
// applying 2 things (from an array or scalar to array or scalar) producing an
// array or scalar result.
abstract class ASTBinOp extends ASTOp {
  static Type[] newsig() {
    Type t1 = Type.dblary(), t2 = Type.dblary();
    return new Type[]{Type.anyary(new Type[]{t1,t2}),t1,t2};
  }
  ASTBinOp( int form, int precedence, int association ) {
    super(VARS2, newsig(), form, precedence, association); // binary ops are infix ops
  }
  abstract double op( double d0, double d1 );
  @Override void apply(Env env, int argcnt, ASTApply apply) {
    // Expect we can broadcast across all functions as needed.
    Frame fr0 = null, fr1 = null;
    double d0=0, d1=0;
    if( env.isAry() ) fr1 = env.popAry(); else d1 = env.popDbl();  String k0 = env.key();
    if( env.isAry() ) fr0 = env.popAry(); else d0 = env.popDbl();  String k1 = env.key();
    if( fr0==null && fr1==null ) {
      env.poppush(op(d0,d1));
      return;
    }
    final boolean lf = fr0 != null;
    final boolean rf = fr1 != null;
    final double df0 = d0, df1 = d1;
    Frame fr  = null;           // Do-All frame
    int ncols = 0;              // Result column count
    if( fr0 !=null ) {          // Left?
      ncols = fr0.numCols();
      if( fr1 != null ) {
        if( fr0.numCols() != fr1.numCols() ||
            fr0.numRows() != fr1.numRows() )
          throw new IllegalArgumentException("Arrays must be same size: LHS FRAME NUM ROWS/COLS: "+fr0.numRows()+"/"+fr0.numCols() +" vs RHS FRAME NUM ROWS/COLS: "+fr1.numRows()+"/"+fr1.numCols());
        fr = new Frame(fr0).add(fr1,true);
      } else {
        fr = fr0;
      }
    } else {
      ncols = fr1.numCols();
      fr = fr1;
    }
    final ASTBinOp bin = this;  // Final 'this' so can use in closure

    // Run an arbitrary binary op on one or two frames & scalars
    Frame fr2 = new MRTask2() {
        @Override public void map( Chunk chks[], NewChunk nchks[] ) {
          for( int i=0; i<nchks.length; i++ ) {
            NewChunk n =nchks[i];
            int rlen = chks[0]._len;
            Chunk c0 = chks[i];
            if( (!c0._vec.isEnum() &&
                 !(lf && rf && chks[i+nchks.length]._vec.isEnum())) ||
                bin instanceof ASTEQ ||
                bin instanceof ASTNE ) {
              for( int r=0; r<rlen; r++ ) {
                double lv; double rv;
                if (lf) {
                  if(vecs(i).isUUID() || (chks[i].isNA0(r) && !bin.opStr().equals("|"))) { n.addNum(Double.NaN); continue; }
                  lv = chks[i].at0(r);
                } else {
                  if (Double.isNaN(df0) && !bin.opStr().equals("|")) { n.addNum(Double.NaN); continue; }
                  lv = df0;
                }
                if (rf) {
                  if(vecs(i+(lf ? nchks.length:0)).isUUID() || chks[i].isNA0(r) && !bin.opStr().equals("|")) { n.addNum(Double.NaN); continue; }
                  rv = chks[i+(lf ? nchks.length:0)].at0(r);
                } else {
                  if (Double.isNaN(df1) && !bin.opStr().equals("|")) { n.addNum(Double.NaN); continue; }
                  rv = df1;
                }
                n.addNum(bin.op(lv, rv));
              }
            } else {
              for( int r=0; r<rlen; r++ )  n.addNA();
            }
          }
        }
      }.doAll(ncols,fr).outputFrame((lf ? fr0 : fr1)._names,null);
    if( fr0 != null ) env.subRef(fr0,k0);
    if( fr1 != null ) env.subRef(fr1,k1);
    env.pop();
    env.push(fr2);
  }
}

class ASTUniPlus  extends ASTUniOp { ASTUniPlus()  { super(OPF_INFIX, OPP_UPLUS,  OPA_RIGHT); } @Override String opStr(){ return "+"  ;} @Override ASTOp make() {return new ASTUniPlus(); } @Override double op(double d) { return d;}}
class ASTUniMinus extends ASTUniOp { ASTUniMinus() { super(OPF_INFIX, OPP_UMINUS, OPA_RIGHT); } @Override String opStr(){ return "-"  ;} @Override ASTOp make() {return new ASTUniMinus();} @Override double op(double d) { return -d;}}
class ASTNot      extends ASTUniOp { ASTNot()      { super(OPF_INFIX, OPP_NOT,    OPA_RIGHT); } @Override String opStr(){ return "!"  ;} @Override ASTOp make() {return new ASTNot();     } @Override double op(double d) { return d==0?1:0; }}
class ASTPlus     extends ASTBinOp { ASTPlus()     { super(OPF_INFIX, OPP_PLUS,   OPA_LEFT ); } @Override String opStr(){ return "+"  ;} @Override ASTOp make() {return new ASTPlus();} @Override double op(double d0, double d1) { return d0+d1;}}
class ASTSub      extends ASTBinOp { ASTSub()      { super(OPF_INFIX, OPP_MINUS,  OPA_LEFT); }  @Override String opStr(){ return "-"  ;} @Override ASTOp make() {return new ASTSub ();} @Override double op(double d0, double d1) { return d0-d1;}}
class ASTMul      extends ASTBinOp { ASTMul()      { super(OPF_INFIX, OPP_MUL,    OPA_LEFT); }  @Override String opStr(){ return "*"  ;} @Override ASTOp make() {return new ASTMul ();} @Override double op(double d0, double d1) { return d0*d1;}}
class ASTDiv      extends ASTBinOp { ASTDiv()      { super(OPF_INFIX, OPP_DIV,    OPA_LEFT); }  @Override String opStr(){ return "/"  ;} @Override ASTOp make() {return new ASTDiv ();} @Override double op(double d0, double d1) { return d0/d1;}}
class ASTPow      extends ASTBinOp { ASTPow()      { super(OPF_INFIX, OPP_POWER,  OPA_RIGHT);}  @Override String opStr(){ return "^"  ;} @Override ASTOp make() {return new ASTPow ();} @Override double op(double d0, double d1) { return Math.pow(d0,d1);}}
class ASTPow2     extends ASTBinOp { ASTPow2()     { super(OPF_INFIX, OPP_POWER,  OPA_RIGHT);}  @Override String opStr(){ return "**" ;} @Override ASTOp make() {return new ASTPow2();} @Override double op(double d0, double d1) { return Math.pow(d0,d1);}}
class ASTMod      extends ASTBinOp { ASTMod()      { super(OPF_INFIX, OPP_MOD,    OPA_LEFT); }  @Override String opStr(){ return "%"  ;} @Override ASTOp make() {return new ASTMod ();} @Override double op(double d0, double d1) { return d0%d1;}}
class ASTMod2      extends ASTBinOp { ASTMod2()    { super(OPF_INFIX, OPP_MOD,    OPA_LEFT); }  @Override String opStr(){ return "%%"  ;} @Override ASTOp make() {return new ASTMod2 ();} @Override double op(double d0, double d1) { return d0%d1;}}
class ASTLT       extends ASTBinOp { ASTLT()       { super(OPF_INFIX, OPP_LT,     OPA_LEFT); }  @Override String opStr(){ return "<"  ;} @Override ASTOp make() {return new ASTLT  ();} @Override double op(double d0, double d1) { return d0<d1 && !Utils.equalsWithinOneSmallUlp(d0,d1)?1:0;}}
class ASTLE       extends ASTBinOp { ASTLE()       { super(OPF_INFIX, OPP_LE,     OPA_LEFT); }  @Override String opStr(){ return "<=" ;} @Override ASTOp make() {return new ASTLE  ();} @Override double op(double d0, double d1) { return d0<d1 ||  Utils.equalsWithinOneSmallUlp(d0,d1)?1:0;}}
class ASTGT       extends ASTBinOp { ASTGT()       { super(OPF_INFIX, OPP_GT,     OPA_LEFT); }  @Override String opStr(){ return ">"  ;} @Override ASTOp make() {return new ASTGT  ();} @Override double op(double d0, double d1) { return d0>d1 && !Utils.equalsWithinOneSmallUlp(d0,d1)?1:0;}}
class ASTGE       extends ASTBinOp { ASTGE()       { super(OPF_INFIX, OPP_GE,     OPA_LEFT); }  @Override String opStr(){ return ">=" ;} @Override ASTOp make() {return new ASTGE  ();} @Override double op(double d0, double d1) { return d0>d1 ||  Utils.equalsWithinOneSmallUlp(d0,d1)?1:0;}}
class ASTEQ       extends ASTBinOp { ASTEQ()       { super(OPF_INFIX, OPP_EQ,     OPA_LEFT); }  @Override String opStr(){ return "==" ;} @Override ASTOp make() {return new ASTEQ  ();} @Override double op(double d0, double d1) { return Utils.equalsWithinOneSmallUlp(d0,d1)?1:0;}}
class ASTNE       extends ASTBinOp { ASTNE()       { super(OPF_INFIX, OPP_NE,     OPA_LEFT); }  @Override String opStr(){ return "!=" ;} @Override ASTOp make() {return new ASTNE  ();} @Override double op(double d0, double d1) { return Utils.equalsWithinOneSmallUlp(d0,d1)?0:1;}}
class ASTLA       extends ASTBinOp { ASTLA()       { super(OPF_INFIX, OPP_AND,    OPA_LEFT); }  @Override String opStr(){ return "&"  ;} @Override ASTOp make() {return new ASTLA  ();} @Override double op(double d0, double d1) { return (d0!=0 && d1!=0) ? (Double.isNaN(d0) || Double.isNaN(d1)?Double.NaN:1) :0;}}
class ASTLO       extends ASTBinOp { ASTLO()       { super(OPF_INFIX, OPP_OR,     OPA_LEFT); }  @Override String opStr(){ return "|"  ;} @Override ASTOp make() {return new ASTLO  ();} @Override double op(double d0, double d1) {
  if (d0 == 0 && Double.isNaN(d1)) { return Double.NaN; }
  if (d1 == 0 && Double.isNaN(d0)) { return Double.NaN; }
  if (Double.isNaN(d0) && Double.isNaN(d1)) { return Double.NaN; }
  if (d0 == 0 && d1 == 0) { return 0; }
  return 1;
}}

class ASTIntDiv   extends ASTBinOp { ASTIntDiv()   { super(OPF_INFIX, OPP_INTDIV, OPA_LEFT); }  @Override String opStr(){ return "%/%";} @Override ASTOp make() {return new ASTIntDiv();} @Override double op(double d0, double d1) { return Math.floor(d0/d1); }}
// Variable length; instances will be created of required length
abstract class ASTReducerOp extends ASTOp {
  final double _init;
  boolean _narm;        // na.rm in R
  ASTReducerOp( double init, boolean narm ) {
    super(new String[]{"","dbls"},
          new Type[]{Type.DBL,Type.varargs(Type.dblary())},
          OPF_PREFIX,
          OPP_PREFIX,
          OPA_RIGHT);
    _init = init;
    _narm = narm;
  }
  @Override double[] map(Env env, double[] in, double[] out) {
    double s = _init;
    for (double v : in) if (!_narm || !Double.isNaN(v)) s = op(s,v);
    if (out == null || out.length < 1) out = new double[1];
    out[0] = s;
    return out;
  }
  abstract double op( double d0, double d1 );
  @Override void apply(Env env, int argcnt, ASTApply apply) {
    double sum=_init;
    for( int i=0; i<argcnt-1; i++ )
      if( env.isDbl() ) sum = op(sum,env.popDbl());
      else {
        Frame fr = env.popAry();
        String skey = env.key();
        sum = op(sum,_narm?new NaRmRedOp(this).doAll(fr)._d:new RedOp(this).doAll(fr)._d);
        env.subRef(fr,skey);
      }
    env.poppush(sum);
  }

  private static class RedOp extends MRTask2<RedOp> {
    final ASTReducerOp _bin;
    RedOp( ASTReducerOp bin ) { _bin = bin; _d = bin._init; }
    double _d;
    @Override public void map( Chunk chks[] ) {
      for( int i=0; i<chks.length; i++ ) {
        Chunk C = chks[i];
        for( int r=0; r<C._len; r++ )
          _d = _bin.op(_d,C.at0(r));
        if( Double.isNaN(_d) ) break;
      }
    }
    @Override public void reduce( RedOp s ) { _d = _bin.op(_d,s._d); }
  }

  private static class NaRmRedOp extends MRTask2<NaRmRedOp> {
    final ASTReducerOp _bin;
    NaRmRedOp( ASTReducerOp bin ) { _bin = bin; _d = bin._init; }
    double _d;
    @Override public void map( Chunk chks[] ) {
      for( int i=0; i<chks.length; i++ ) {
        Chunk C = chks[i];
        for( int r=0; r<C._len; r++ )
          if (!Double.isNaN(C.at0(r)))
            _d = _bin.op(_d,C.at0(r));
        if( Double.isNaN(_d) ) break;
      }
    }
    @Override public void reduce( NaRmRedOp s ) { _d = _bin.op(_d,s._d); }
  }
}

class ASTSum     extends ASTReducerOp { ASTSum( )     {super(0,false);} @Override String opStr(){ return "sum"      ;} @Override ASTOp make() {return new ASTSum();    } @Override double op(double d0, double d1) { return d0+d1;}}
class ASTSumNaRm extends ASTReducerOp { ASTSumNaRm( ) {super(0,true) ;} @Override String opStr(){ return "sum.na.rm";} @Override ASTOp make() {return new ASTSumNaRm();} @Override double op(double d0, double d1) { return d0+d1;}}

class ASTReduce extends ASTOp {
  static final String VARS[] = new String[]{ "", "op2", "ary"};
  static final Type   TYPES[]= new Type  []{ Type.ARY, Type.fcn(new Type[]{Type.DBL,Type.DBL,Type.DBL}), Type.ARY };
  ASTReduce( ) { super(VARS,TYPES,OPF_PREFIX,OPP_PREFIX,OPA_RIGHT); }
  @Override String opStr(){ return "Reduce";}
  @Override ASTOp make() {return this;}
  @Override void apply(Env env, int argcnt, ASTApply apply) { throw H2O.unimpl(); }
}

// TODO: Check refcnt mismatch issue: tmp = cbind(h.hex,3.5) results in different refcnts per col
class ASTCbind extends ASTOp {
  @Override String opStr() { return "cbind"; }
  ASTCbind( ) { super(new String[]{"cbind","ary"},
                      new Type[]{Type.ARY,Type.varargs(Type.dblary())},
                      OPF_PREFIX,
                      OPP_PREFIX,OPA_RIGHT); }
  @Override ASTOp make() {return new ASTCbind(); }
  @Override void apply(Env env, int argcnt, ASTApply apply) {
    Vec vmax = null;
    for(int i = 0; i < argcnt-1; i++) {
      if(env.isAry(-argcnt+1+i)) {
        Frame tmp = env.ary(-argcnt+1+i);
        if(vmax == null) vmax = tmp.vecs()[0];
        else if(tmp.numRows() != vmax.length())
          // R pads shorter cols to match max rows by cycling/repeating, but we won't support that
          throw new IllegalArgumentException("Row mismatch! Expected " + String.valueOf(vmax.length()) + " but frame has " + String.valueOf(tmp.numRows()));
      }
    }

    Frame fr = new Frame(new String[0],new Vec[0]);
    for(int i = 0; i < argcnt-1; i++) {
      if( env.isAry(-argcnt+1+i) ) {
        String name;
        Frame fr2 = env.ary(-argcnt+1+i);
        Frame fr3 = fr.makeCompatible(fr2);
        if( fr3 != fr2 ) {      // If copied into a new Frame, need to adjust refs
          env.addRef(fr3);
          env.subRef(fr2,null);
        }
        // Take name from an embedded assign: "cbind(colNameX = some_frame, ...)"
        if( fr2.numCols()==1 && apply != null && (name = apply._args[i+1].argName()) != null ) {
          if (name.equals(fr3._key.toString())) fr.add(fr3,true);
          else fr.add(name, fr3.anyVec());
        } else fr.add(fr3,true);
      } else {
        double d = env.dbl(-argcnt+1+i);
        Vec v = vmax == null ? Vec.make1Elem(d) : vmax.makeCon(d);
        fr.add("C" + String.valueOf(i+1), v);
        env.addRef(v);
      }
    }
    env._ary[env._sp-argcnt] = fr;  env._fcn[env._sp-argcnt] = null;
    env._sp -= argcnt-1;
    Arrays.fill(env._ary,env._sp,env._sp+(argcnt-1),null);
    assert env.check_refcnt(fr.anyVec());
  }
}

class ASTRbind extends ASTOp {
  @Override String opStr() { return "rbind"; }
  ASTRbind( ) { super(new String[]{"rbind","ary"},
          new Type[]{Type.ARY,Type.varargs(Type.dblary())},
          OPF_PREFIX,
          OPP_PREFIX,OPA_RIGHT); }
  @Override ASTOp make() {return new ASTRbind(); }

  private static String get_type(Vec v) {
    if (v.isUUID()) return "UUID";
    if (v.isEnum()) return "factor";
    if (v.isTime()) return "time";
    if (v.isFloat() || v.isInt()) return "numeric";
    return "bad";
  }


  private static class RbindMRTask extends MRTask2<RbindMRTask> {
    private final int[] _emap;
    private final int _chunkOffset;
    private final Vec _v;
    RbindMRTask(H2O.H2OCountedCompleter hc, int[] emap, Vec v, int offset) { super(hc); _emap = emap; _v = v; _chunkOffset = offset;}

    @Override public void map(Chunk cs) {
      int idx = _chunkOffset+cs.cidx();
      Key ckey = Vec.chunkKey(_v._key, idx);
      if (_emap != null) {
        NewChunk nc = new NewChunk(_v, idx);
        // loop over rows and update ints for new domain mapping according to vecs[c].domain()
        for (int r=0;r < cs._len;++r) {
          if (cs.isNA0(r)) nc.addNA();
          else nc.addNum(_emap[(int)cs.at80(r)], 0);
        }
        nc.close(_fs);
      } else {
        Chunk oc = cs.clone();
        oc._start = -1;
        oc._vec = null;
        oc._mem = cs.getBytes().clone(); // needless replication of the data, can do ref counting on byte[] _mem
        DKV.put(ckey, oc, _fs, true);
      }
    }
  }

  private static class RbindTask extends H2O.H2OCountedCompleter<RbindTask> {
    final transient Vec[] _vecs;
    final Vec _v;
    final long[] _espc;
    String[] _dom;

    RbindTask(H2O.H2OCountedCompleter cc, Vec[] vecs, Vec v, long[] espc) { super(cc); _vecs = vecs; _v = v; _espc = espc; }

    private static Map<Integer, String> invert(Map<String, Integer> map) {
      Map<Integer, String> inv = new HashMap<Integer, String>();
      for (Map.Entry<String, Integer> e : map.entrySet()) {
        inv.put(e.getValue(), e.getKey());
      }
      return inv;
    }

    @Override public void compute2() {
      addToPendingCount(_vecs.length-1);
      boolean isEnum = _vecs[0].domain() != null;
      int[][] emaps  = new int[_vecs.length][];

      if (isEnum) {
        // loop to create BIG domain
        HashMap<String, Integer> dmap = new HashMap<String, Integer>(); // probably should allocate something that's big enough (i.e. 2*biggest_domain)
        int c = 0;
        for (int i = 0; i < _vecs.length; ++i) {
          emaps[i] = new int[_vecs[i].domain().length];
          for (int j = 0; j < emaps[i].length; ++j)
            if (!dmap.containsKey(_vecs[i].domain()[j]))
              dmap.put(_vecs[i].domain()[j], emaps[i][j]=c++);
            else emaps[i][j] = dmap.get(_vecs[i].domain()[j]);
        }
        _dom = new String[dmap.size()];
        HashMap<Integer, String> inv = (HashMap<Integer, String>) invert(dmap);
        for (int s = 0; s < _dom.length; ++s) _dom[s] = inv.get(s);
      }
      int offset=0;
      for (int i=0; i<_vecs.length; ++i) {
        new RbindMRTask(this, emaps[i], _v, offset).asyncExec(_vecs[i]);
        offset += _vecs[i].nChunks();
      }
    }

    @Override public void onCompletion(CountedCompleter cc) {
      _v._domain = _dom;
      UKV.put(_v._key,_v);
    }
  }

  private static class ParallelRbinds extends H2O.H2OCountedCompleter{

    private final Frame[] _f;
    private final int _argcnt;
    private final AtomicInteger _ctr;
    private int _maxP = 100;

    private long[] _espc;
    private Vec[] _vecs;
    ParallelRbinds(Frame[] f, int argcnt) { _f = f; _argcnt = argcnt; _ctr = new AtomicInteger(_maxP-1); }  //TODO pass maxP to constructor

    @Override public void compute2() {
      addToPendingCount(_f[0].numCols()-1);
      int nchks=0;
      for (int i =0; i < _argcnt; ++i)
        nchks+=_f[i].anyVec().nChunks();

      _espc = new long[nchks+1];
      int coffset = _f[0].anyVec().nChunks();
      long[] first_espc = _f[0].anyVec()._espc;
      System.arraycopy(first_espc, 0, _espc, 0, first_espc.length);
      for (int i=1; i < _argcnt; ++i) {
        long roffset = _espc[coffset];
        long[] espc = _f[i].anyVec()._espc;
        int j = 1;
        for (; j < espc.length; j++)
          _espc[coffset + j] = roffset+ espc[j];
        coffset += _f[i].anyVec().nChunks();
      }

      Key[] keys = _f[0].anyVec().group().addVecs(_f[0].numCols());
      _vecs = new Vec[keys.length];
      String type;
      for (int i=0; i<_vecs.length; ++i) {
        _vecs[i] = new Vec(keys[i], _espc, null, (type = get_type(_f[0].vec(i))).equals("UUID"), type.equals("time") ? (byte) 3 : (byte) -1);
      }

      for (int i=0; i < Math.min(_maxP, _vecs.length); ++i) forkVecTask(i);
    }

    private void forkVecTask(final int i) {
      Vec[] vecs = new Vec[_argcnt];
      for (int j= 0; j < _argcnt; ++j) {
        Vec vm, v = _f[j].vec(i);
        vecs[j] = ((vm=v.masterVec())==null) ? v : vm;
      }
      new RbindTask(new Callback(), vecs, _vecs[i], _espc).fork();
    }

    private class Callback extends H2O.H2OCallback {
      public Callback(){super(ParallelRbinds.this);}
      @Override public void callback(H2O.H2OCountedCompleter h2OCountedCompleter) {
        int i = _ctr.incrementAndGet();
        if(i < _vecs.length)
          forkVecTask(i);
      }
    }
  }


  @Override void apply(Env env, int argcnt, ASTApply apply) {
    // quick check to make sure rbind is feasible
    if (argcnt-1 == 1) { return; } // leave stack as is

    Frame[] fs = new Frame[argcnt-1];
    Frame f1 = env.peekAry();
    int j = fs.length-1;
    boolean[] wrapped = new boolean[f1.numCols()];
    for (int c = 0; c<f1.numCols(); ++c) wrapped[c] = f1.vec(c).masterVec() != null;
    fs[j--] = f1;
    // do error checking and compute new offsets in tandem
    for (int i = 1; i < argcnt-1; ++i) {
      Frame t = env.ary(-(i+1));
      fs[j--]=t;
      // check columns match
      if (t.numCols() != f1.numCols())
        throw new IllegalArgumentException("Column mismatch! Expected " + f1.numCols() + " but frame has " + t.numCols());

      // check column types
      for (int c = 0; c < f1.numCols(); ++c) {
        wrapped[c] |= t.vec(c).masterVec() != null;
        if (!get_type(f1.vec(c)).equals(get_type(t.vec(c))))
          throw new IllegalArgumentException("Column type mismatch! Expected type " + get_type(f1.vec(c)) + " but vec has type " + get_type(t.vec(c)));
      }
    }

    ParallelRbinds t;
    H2O.submitTask(t = new ParallelRbinds(fs, argcnt-1)).join();
    for (int i = 0; i < wrapped.length; ++i)
      if (wrapped[i]) t._vecs[i] = t._vecs[i].toEnum();
    Key m = Key.make();
    env.poppush(argcnt, new Frame(m, f1.names(), t._vecs), m.toString());
  }
}

class ASTMinNaRm extends ASTReducerOp {
  ASTMinNaRm( ) { super( Double.POSITIVE_INFINITY, true ); }
  @Override
  String opStr(){ return "min.na.rm";}
  @Override
  ASTOp make() {return new ASTMinNaRm();}
  @Override double op(double d0, double d1) { return Math.min(d0, d1); }
  @Override void apply(Env env, int argcnt, ASTApply apply) {
    double min = Double.POSITIVE_INFINITY;
    int nacnt = 0;
    for( int i=0; i<argcnt-1; i++ )
      if( env.isDbl() ) {
        double a = env.popDbl();
        if (Double.isNaN(a)) nacnt++;
        else min = Math.min(min, a);
      }
      else {
        Frame fr = env.peekAry();
        for (Vec v : fr.vecs())
          min = Math.min(min, v.min());
        env.pop();
      }
    if (nacnt > 0 && min == Double.POSITIVE_INFINITY)
      min = Double.NaN;
    env.poppush(min);
  }
}

class ASTMaxNaRm extends ASTReducerOp {
  ASTMaxNaRm( ) { super( Double.NEGATIVE_INFINITY, true ); }
  @Override
  String opStr(){ return "max.na.rm";}
  @Override
  ASTOp make() {return new ASTMaxNaRm();}
  @Override double op(double d0, double d1) { return Math.max(d0,d1); }
  @Override void apply(Env env, int argcnt, ASTApply apply) {
    double max = Double.NEGATIVE_INFINITY;
    int nacnt = 0;
    for( int i=0; i<argcnt-1; i++ )
      if( env.isDbl() ) {
        double a = env.popDbl();
        if (Double.isNaN(a)) nacnt++;
        else max = Math.max(max, a);
      }
      else {
        Frame fr = env.peekAry();
        for (Vec v : fr.vecs())
          max = Math.max(max, v.max());
        env.pop();
      }
    if (nacnt > 0 && max == Double.NEGATIVE_INFINITY)
      max = Double.NaN;
    env.poppush(max);
  }
}

class ASTMin extends ASTReducerOp {
  ASTMin( ) { super( Double.POSITIVE_INFINITY, false); }
  @Override
  String opStr(){ return "min";}
  @Override
  ASTOp make() {return new ASTMin();}
  @Override double op(double d0, double d1) { return Math.min(d0, d1); }
  @Override void apply(Env env, int argcnt, ASTApply apply) {
    double min = Double.POSITIVE_INFINITY;
    for( int i=0; i<argcnt-1; i++ )
      if( env.isDbl() ) min = Math.min(min, env.popDbl());
      else {
        Frame fr = env.peekAry();
        for (Vec v : fr.vecs())
          if (v.naCnt() > 0) { min = Double.NaN; break; }
          else min = Math.min(min, v.min());
        env.pop();
      }
    env.poppush(min);
  }
}

class ASTMax extends ASTReducerOp {
  ASTMax( ) { super( Double.NEGATIVE_INFINITY, false ); }
  @Override
  String opStr(){ return "max";}
  @Override
  ASTOp make() {return new ASTMax();}
  @Override double op(double d0, double d1) { return Math.max(d0,d1); }
  @Override void apply(Env env, int argcnt, ASTApply apply) {
    double max = Double.NEGATIVE_INFINITY;
    for( int i=0; i<argcnt-1; i++ )
      if( env.isDbl() ) max = Math.max(max, env.popDbl());
      else {
        Frame fr = env.peekAry();
        for (Vec v : fr.vecs())
          if (v.naCnt() > 0) { max = Double.NaN; break; }
          else max = Math.max(max, v.max());
        env.pop();
      }
    env.poppush(max);
  }
}

// R like binary operator &&
class ASTAND extends ASTOp {
  @Override String opStr() { return "&&"; }
  ASTAND( ) {
    super(new String[]{"", "x", "y"},
          new Type[]{Type.DBL,Type.dblary(),Type.dblary()},
          OPF_PREFIX,
          OPP_AND,
          OPA_RIGHT);
  }
  @Override ASTOp make() { return new ASTAND(); }
  @Override void apply(Env env, int argcnt, ASTApply apply) {
    double op1 = env.isAry(-2) ? env.ary(-2).vecs()[0].at(0) : env.dbl(-2);
    double op2 = op1==0 ? 0 :
           Double.isNaN(op1) ? Double.NaN :
           env.isAry(-1) ? env.ary(-1).vecs()[0].at(0) : env.dbl(-1);
    env.pop(3);
    if (!Double.isNaN(op2)) op2 = op2==0?0:1;
    env.push(op2);
  }
}

// R like binary operator ||
class ASTOR extends ASTOp {
  @Override String opStr() { return "||"; }
  ASTOR( ) {
    super(new String[]{"", "x", "y"},
          new Type[]{Type.DBL,Type.dblary(),Type.dblary()},
          OPF_PREFIX,
          OPP_OR,
          OPA_RIGHT);
  }
  @Override ASTOp make() { return new ASTOR(); }
  @Override void apply(Env env, int argcnt, ASTApply apply) {
    double op1 = env.isAry(-2) ? env.ary(-2).vecs()[0].at(0) : env.dbl(-2);
    double op2 = !Double.isNaN(op1) && op1!=0 ? 1 :
            env.isAry(-1) ? env.ary(-1).vecs()[0].at(0) : env.dbl(-1);
    if (!Double.isNaN(op2) && op2 != 0)
      op2 = 1;
    else if (op2 == 0 && Double.isNaN(op1))
      op2 = Double.NaN;
    env.push(op2);
  }
}

// Brute force implementation of matrix multiply
class ASTMMult extends ASTOp {
  @Override String opStr() { return "%*%"; }
  ASTMMult( ) {
    super(new String[]{"", "x", "y"},
          new Type[]{Type.ARY,Type.ARY,Type.ARY},
          OPF_PREFIX,
          OPP_MUL,
          OPA_RIGHT);
  }
  @Override ASTOp make() { return new ASTMMult(); }
  @Override void apply(Env env, int argcnt, ASTApply apply) {
    env.poppush(3, DMatrix.mmul(env.ary(-2),env.ary(-1)),null);
  }
}

// Brute force implementation of matrix transpose
class ASTMTrans extends ASTOp {
  @Override String opStr() { return "t"; }
  ASTMTrans( ) {
   super(new String[]{"", "x"},
         new Type[]{Type.ARY,Type.dblary()},
         OPF_PREFIX,
         OPP_PREFIX,
         OPA_RIGHT);
  }
  @Override ASTOp make() { return new ASTMTrans(); }
  @Override void apply(Env env, int argcnt, ASTApply apply) {
    if(!env.isAry(-1)) {
      Key k = new Vec.VectorGroup().addVec();
      Futures fs = new Futures();
      AppendableVec avec = new AppendableVec(k);
      NewChunk chunk = new NewChunk(avec, 0);
      chunk.addNum(env.dbl(-1));
      chunk.close(0, fs);
      Vec vec = avec.close(fs);
      fs.blockForPending();
      vec._domain = null;
      Frame fr = new Frame(new String[] {"C1"}, new Vec[] {vec});
      env.poppush(2,new Matrix(fr).trans(),null);
    } else
      env.poppush(2,DMatrix.transpose(env.ary(-1)),null);
  }
}

// Similar to R's seq_len
class ASTSeqLen extends ASTOp {
  @Override String opStr() { return "seq_len"; }
  ASTSeqLen( ) {
    super(new String[]{"seq_len", "n"},
            new Type[]{Type.ARY,Type.DBL},
            OPF_PREFIX,
            OPP_PREFIX,
            OPA_RIGHT);
  }
  @Override ASTOp make() { return this; }
  @Override void apply(Env env, int argcnt, ASTApply apply) {
    long len = (long)env.popDbl();
    if (len <= 0)
      throw new IllegalArgumentException("Error in seq_len(" +len+"): argument must be coercible to positive integer");
    env.poppush(1,new Frame(new String[]{"c"}, new Vec[]{Vec.makeSeq(len)}),null);
  }
}
class ASTColSeq extends ASTOp {
  @Override String opStr() { return ":"; }
  ASTColSeq() { super(new String[]{":", "from", "to"},
          new Type[]{Type.dblary(), Type.DBL, Type.DBL},
          OPF_PREFIX,
          OPP_PREFIX,
          OPA_RIGHT);
  }
  @Override ASTOp make() { return this; }
  @Override void apply(Env env, int argcnt, ASTApply apply) {
    double by = 1.0;
    double to = env.popDbl();
    double from = env.popDbl();

    double delta = to - from;
    if(delta == 0 && to == 0)
      env.poppush(to);
    else {
      double n = delta/by;
      if(n < 0)
        throw new IllegalArgumentException("wrong sign in 'by' argument");
      else if(n > Double.MAX_VALUE)
        throw new IllegalArgumentException("'by' argument is much too small");

      double dd = Math.abs(delta)/Math.max(Math.abs(from), Math.abs(to));
      if(dd < 100*Double.MIN_VALUE)
        env.poppush(from);
      else {
        Key k = new Vec.VectorGroup().addVec();
        Futures fs = new Futures();
        AppendableVec av = new AppendableVec(k);
        NewChunk nc = new NewChunk(av, 0);
        int len = (int)n + 1;
        for (int r = 0; r < len; r++) nc.addNum(from + r*by);
        // May need to adjust values = by > 0 ? min(values, to) : max(values, to)
        nc.close(0, fs);
        Vec vec = av.close(fs);
        fs.blockForPending();
        vec._domain = null;
        env.poppush(1, new Frame(new String[] {"C1"}, new Vec[] {vec}), null);
      }
    }
  }
}

// Same logic as R's generic seq method
class ASTSeq extends ASTOp {
  @Override String opStr() { return "seq"; }
  ASTSeq() { super(new String[]{"seq", "from", "to", "by"},
                   new Type[]{Type.dblary(), Type.DBL, Type.DBL, Type.DBL},
                   OPF_PREFIX,
                   OPP_PREFIX,
                   OPA_RIGHT);
  }
  @Override ASTOp make() { return this; }
  @Override void apply(Env env, int argcnt, ASTApply apply) {
    double by = env.popDbl();
    double to = env.popDbl();
    double from = env.popDbl();

    double delta = to - from;
    if(delta == 0 && to == 0)
      env.poppush(to);
    else {
      double n = delta/by;
      if(n < 0)
        throw new IllegalArgumentException("wrong sign in 'by' argument");
      else if(n > Double.MAX_VALUE)
        throw new IllegalArgumentException("'by' argument is much too small");

      double dd = Math.abs(delta)/Math.max(Math.abs(from), Math.abs(to));
      if(dd < 100*Double.MIN_VALUE)
        env.poppush(from);
      else {
        Key k = new Vec.VectorGroup().addVec();
        Futures fs = new Futures();
        AppendableVec av = new AppendableVec(k);
        NewChunk nc = new NewChunk(av, 0);
        int len = (int)n + 1;
        for (int r = 0; r < len; r++) nc.addNum(from + r*by);
        // May need to adjust values = by > 0 ? min(values, to) : max(values, to)
        nc.close(0, fs);
        Vec vec = av.close(fs);
        fs.blockForPending();
        vec._domain = null;
        env.poppush(1, new Frame(new String[] {"C1"}, new Vec[] {vec}), null);
      }
    }
  }
}

class ASTRepLen extends ASTOp {
  @Override String opStr() { return "rep_len"; }
  ASTRepLen() { super(new String[]{"rep_len", "x", "length.out"},
                   new Type[]{Type.dblary(), Type.DBL, Type.DBL},
                   OPF_PREFIX,
                   OPP_PREFIX,
                   OPA_RIGHT);
  }
  @Override ASTOp make() { return this; }
  @Override void apply(Env env, int argcnt, ASTApply apply) {
    if(env.isAry(-2)) H2O.unimpl();
    else {
      long len = (long)env.popDbl();
      if(len <= 0)
        throw new IllegalArgumentException("Error in rep_len: argument length.out must be coercible to a positive integer");
      double x = env.popDbl();
      env.poppush(1,new Frame(new String[]{"C1"}, new Vec[]{Vec.makeConSeq(x, len)}),null);
    }
  }
}

// Compute exact quantiles given a set of cutoffs, using multipass binning algo.
class ASTQtile extends ASTOp {
  @Override String opStr() { return "quantile"; }

  ASTQtile( ) {
    super(new String[]{"quantile","x","probs"},
          new Type[]{Type.ARY, Type.ARY, Type.ARY},
          OPF_PREFIX,
          OPP_PREFIX,
          OPA_RIGHT);
  }
  @Override ASTQtile make() { return new ASTQtile(); }

  @Override void apply(Env env, int argcnt, ASTApply apply) {
    Frame x = env.ary(-2);
    Vec xv  = x          .theVec("Argument #1 in Quantile contains more than 1 column.");
    Vec pv  = env.ary(-1).theVec("Argument #2 in Quantile contains more than 1 column.");
    double p[] = new double[(int)pv.length()];

    for (int i = 0; i < pv.length(); i++) {
      if ((p[i]=pv.at((long)i)) < 0 || p[i] > 1)
        throw new  IllegalArgumentException("Quantile: probs must be in the range of [0, 1].");
    }
    if ( xv.isEnum() ) {
        throw new  IllegalArgumentException("Quantile: column type cannot be Enum.");
    }

    // create output vec
    Vec res = pv.makeCon(Double.NaN);

    final int MAX_ITERATIONS = 16;
    final int MAX_QBINS = 1000; // less uses less memory, can take more passes
    final boolean MULTIPASS = true; // approx in 1 pass if false
    // Type 7 matches R default
    final int INTERPOLATION = 7; // linear if quantile not exact on row. 2 uses mean.

    // a little obtuse because reusing first pass object, if p has multiple thresholds
    // since it's always the same (always had same valStart/End seed = vec min/max
    // some MULTIPASS conditionals needed if we were going to make this work for approx or exact
    final Quantiles[] qbins1 = new Quantiles.BinTask2(MAX_QBINS, xv.min(), xv.max()).doAll(xv)._qbins;
    for( int i=0; i<p.length; i++ ) {
      double quantile = p[i];
      // need to pass a different threshold now for each finishUp!
      qbins1[0].finishUp(xv, new double[]{quantile}, INTERPOLATION, MULTIPASS);
      if( qbins1[0]._done ) {
        res.set(i,qbins1[0]._pctile[0]);
      } else {
        // the 2-N map/reduces are here (with new start/ends. MULTIPASS is implied
        Quantiles[] qbinsM = new Quantiles.BinTask2(MAX_QBINS, qbins1[0]._newValStart, qbins1[0]._newValEnd).doAll(xv)._qbins;
        for( int iteration = 2; iteration <= MAX_ITERATIONS; iteration++ ) {
          qbinsM[0].finishUp(xv, new double[]{quantile}, INTERPOLATION, MULTIPASS);
          if( qbinsM[0]._done ) {
            res.set(i,qbinsM[0]._pctile[0]);
            break;
          }
          // the 2-N map/reduces are here (with new start/ends. MULTIPASS is implied
          qbinsM = new Quantiles.BinTask2(MAX_QBINS, qbinsM[0]._newValStart, qbinsM[0]._newValEnd).doAll(xv)._qbins;
        }
      }
    }

    res.chunkForChunkIdx(0).close(0,null);
    res.postWrite();
    env.poppush(argcnt, new Frame(new String[]{"Quantile"}, new Vec[]{res}), null);
  }
}

// Variable length; flatten all the component arys
class ASTCat extends ASTOp {
  @Override String opStr() { return "c"; }
  ASTCat( ) { super(new String[]{"cat","dbls"},
          new Type[]{Type.ARY,Type.varargs(Type.dblary())},
          OPF_PREFIX,
          OPP_PREFIX,
          OPA_RIGHT); }
  @Override ASTOp make() {return new ASTCat();}
  @Override double[] map(Env env, double[] in, double[] out) {
    if (out == null || out.length < in.length) out = new double[in.length];
    for (int i = 0; i < in.length; i++) out[i] = in[i];
    return out;
  }
  @Override void apply(Env env, int argcnt, ASTApply apply) {
    Key key = Vec.VectorGroup.VG_LEN1.addVecs(1)[0];
    AppendableVec av = new AppendableVec(key);
    NewChunk nc = new NewChunk(av,0);
    for( int i=0; i<argcnt-1; i++ ) {
      if (env.isAry(i-argcnt+1)) for (Vec vec : env.ary(i-argcnt+1).vecs()) {
        if (vec.nChunks() > 1) H2O.unimpl();
        for (int r = 0; r < vec.length(); r++) nc.addNum(vec.at(r));
      }
      else nc.addNum(env.dbl(i-argcnt+1));
    }
    nc.close(0,null);
    Vec v = av.close(null);
    env.pop(argcnt);
    env.push(new Frame(new String[]{"C1"}, new Vec[]{v}));
  }
}

class ASTRunif extends ASTOp {
  @Override String opStr() { return "runif"; }
  ASTRunif() { super(new String[]{"runif","dbls","seed"},
                     new Type[]{Type.ARY,Type.ARY,Type.DBL},
                     OPF_PREFIX,
                     OPP_PREFIX,
                     OPA_RIGHT); }
  @Override ASTOp make() {return new ASTRunif();}
  @Override void apply(Env env, int argcnt, ASTApply apply) {
    double temp = env.popDbl();
    final long seed = (temp == -1) ? System.currentTimeMillis() : (long)temp;
    Frame fr = env.popAry();
    String skey = env.key();
    long [] espc = fr.anyVec()._espc;
    long rem = fr.numRows();
    if(rem > espc[espc.length-1]) throw H2O.unimpl();
    for(int i = 0; i < espc.length; ++i){
      if(rem <= espc[i]){
        espc = Arrays.copyOf(espc, i+1);
        break;
      }
    }
    espc[espc.length-1] = rem;
    Vec randVec = new Vec(fr.anyVec().group().addVecs(1)[0],espc);
    Futures fs = new Futures();
    DKV.put(randVec._key,randVec, fs);
    for(int i = 0; i < espc.length-1; ++i)
      DKV.put(randVec.chunkKey(i),new C0DChunk(0,(int)(espc[i+1]-espc[i])),fs);
    fs.blockForPending();
    new MRTask2() {
      @Override public void map(Chunk c){
        Random rng = new Random(seed*c.cidx());
        for(int i = 0; i < c._len; ++i)
          c.set0(i, (float)rng.nextDouble());
      }
    }.doAll(randVec);
    env.subRef(fr,skey);
    env.pop();
    env.push(new Frame(new String[]{"rnd"},new Vec[]{randVec}));
  }
}

class ASTSdev extends ASTOp {
  ASTSdev() { super(new String[]{"sd", "ary"}, new Type[]{Type.DBL,Type.ARY},
                    OPF_PREFIX,
                    OPP_PREFIX,
                    OPA_RIGHT); }
  @Override String opStr() { return "sd"; }
  @Override ASTOp make() { return new ASTSdev(); }
  @Override void apply(Env env, int argcnt, ASTApply apply) {
    Frame fr = env.peekAry();
    if (fr.vecs().length > 1)
      throw new IllegalArgumentException("sd does not apply to multiple cols.");
    if (fr.vecs()[0].isEnum())
      throw new IllegalArgumentException("sd only applies to numeric vector.");
    double sig = fr.vecs()[0].sigma();
    env.pop();
    env.poppush(sig);
  }
}

class ASTVar extends ASTOp {
  ASTVar() { super(new String[]{"var", "ary"}, new Type[]{Type.dblary(),Type.dblary()},
                   OPF_PREFIX,
                   OPP_PREFIX,
                   OPA_RIGHT); }
  @Override String opStr() { return "var"; }
  @Override ASTOp make() { return new ASTVar(); }
  @Override void apply(Env env, int argcnt, ASTApply apply) {
    if(env.isDbl()) {
      env.pop(2); env.push(Double.NaN);
    } else {
      Frame fr = env.ary(-1);
      String[] colnames = fr.names();

      // Save standard deviations for later use
      double[] sdev = new double[fr.numCols()];
      for(int i = 0; i < fr.numCols(); i++)
        sdev[i] = fr.vecs()[i].sigma();

      // TODO: Might be more efficient to modify DataInfo to allow for separate standardization of mean and std dev
      DataInfo dinfo = new DataInfo(fr, 0, true, false, DataInfo.TransformType.STANDARDIZE);
      GramTask tsk = new GramTask(null, dinfo, false, false).doAll(dinfo._adaptedFrame);
      double[][] var = tsk._gram.getXX();
      long nobs = tsk._nobs;

      assert sdev.length == var.length;
      assert sdev.length == var[0].length;

      // Just push the scalar if input is a single col
      if(var.length == 1 && var[0].length == 1) {
        env.pop(2);
        double x = var[0][0]*sdev[0]*sdev[0];   // Undo normalization of each col's standard deviation
        x = x*nobs/(nobs-1);   // Divide by n-1 rather than n so unbiased
        env.push(x);
      } else {
        // Build output vecs for var-cov matrix
        Key keys[] = Vec.VectorGroup.VG_LEN1.addVecs(var.length);
        Vec[] vecs = new Vec[var.length];
        for(int i = 0; i < var.length; i++) {
          AppendableVec v = new AppendableVec(keys[i]);
          NewChunk c = new NewChunk(v,0);
          v._domain = null;
          for (int j = 0; j < var[0].length; j++) {
            double x = var[i][j]*sdev[i]*sdev[j];   // Undo normalization of each col's standard deviation
            x = x*nobs/(nobs-1);   // Divide by n-1 rather than n so unbiased
            c.addNum(x);
          }
          c.close(0, null);
          vecs[i] = v.close(null);
        }
        env.pop(2); env.push(new Frame(colnames, vecs));
      }
    }
  }
}

class ASTMean extends ASTOp {
  ASTMean() { super(new String[]{"mean", "ary"}, new Type[]{Type.DBL,Type.ARY},
                    OPF_PREFIX,
                    OPP_PREFIX,
                    OPA_RIGHT); }
  @Override String opStr() { return "mean"; }
  @Override ASTOp make() { return new ASTMean(); }
  @Override void apply(Env env, int argcnt, ASTApply apply) {
    Frame fr = env.peekAry();
    if (fr.vecs().length > 1)
      throw new IllegalArgumentException("mean does not apply to multiple cols.");
    if (fr.vecs()[0].isEnum())
      throw new IllegalArgumentException("mean only applies to numeric vector.");
    double ave = fr.vecs()[0].mean();
    env.pop();
    env.poppush(ave);
  }
  @Override double[] map(Env env, double[] in, double[] out) {
    if (out == null || out.length < 1) out = new double[1];
    double s = 0;  int cnt=0;
    for (double v : in) if( !Double.isNaN(v) ) { s+=v; cnt++; }
    out[0] = s/cnt;
    return out;
  }
}

class ASTMedian extends ASTOp {
  ASTMedian() { super(new String[]{"median", "ary"}, new Type[]{Type.DBL,Type.ARY},
          OPF_PREFIX,
          OPP_PREFIX,
          OPA_RIGHT); }
  @Override String opStr() { return "median"; }
  @Override ASTOp make() { return new ASTMedian(); }
  @Override void apply(Env env, int argcnt, ASTApply apply) {
    Frame fr = env.peekAry();
    if (fr.vecs().length > 1)
      throw new IllegalArgumentException("median does not apply to multiple cols.");
    if (fr.vecs()[0].isEnum())
      throw new IllegalArgumentException("median only applies to numeric vector.");
    QuantilesPage qp = new QuantilesPage();
    qp.source_key = fr;
    qp.column = fr.anyVec();
    qp.invoke();
    double median =  qp.result;
    env.pop();
    env.poppush(median);
  }
}

class ASTMostCommon extends ASTOp {
  ASTMostCommon() { super(new String[]{"mode", "ary"}, new Type[]{Type.DBL,Type.ARY},
          OPF_PREFIX,
          OPP_PREFIX,
          OPA_RIGHT); }
  @Override String opStr() { return "mode"; }
  @Override ASTOp make() { return new ASTMostCommon(); }
  @Override void apply(Env env, int argcnt, ASTApply apply) {
    Frame fr = env.peekAry();
    if (fr.vecs().length > 1)
      throw new IllegalArgumentException("mode does not apply to multiple cols.");
    if (!fr.vecs()[0].isEnum())
      throw new IllegalArgumentException("mode only applies to factor columns.");
    Vec column = fr.anyVec();
    String dom[] = column.domain();
    long[][] levels = new long[1][];
    levels[0] = new Vec.CollectDomain(column).doAll(new Frame(column)).domain();
    long[][] counts = new ASTTable.Tabularize(levels).doAll(column)._counts;
    long maxCounts = -1;
    int mode = -1;
    for (int i = 0; i < counts[0].length; ++i) {
      if (counts[0][i] > maxCounts && !dom[i].equals("NA")) {
        maxCounts = counts[0][i];
        mode = i;
      }
    }
    double mc = mode != -1 ? (double)mode : (double)Arrays.asList(dom).indexOf("NA");
    if (mc == -1) mc = Double.NaN;
    env.pop();
    env.poppush(mc);
  }
}

class ASTXorSum extends ASTReducerOp { ASTXorSum() {super(0,false); }
  @Override String opStr(){ return "xorsum";}
  @Override ASTOp make() {return new ASTXorSum();}
  @Override double op(double d0, double d1) {
    long d0Bits = Double.doubleToLongBits(d0);
    long d1Bits = Double.doubleToLongBits(d1);
    long xorsumBits = d0Bits ^ d1Bits;
    // just need to not get inf or nan. If we zero the upper 4 bits, we won't
    final long ZERO_SOME_SIGN_EXP = 0x0fffffffffffffffL;
    xorsumBits = xorsumBits & ZERO_SOME_SIGN_EXP;
    double xorsum = Double.longBitsToDouble(xorsumBits);
    return xorsum;
  }
  @Override double[] map(Env env, double[] in, double[] out) {
    if (out == null || out.length < 1) out = new double[1];
    long xorsumBits = 0;
    long vBits;
    // for dp ieee 754 , sign and exp are the high 12 bits
    // We don't want infinity or nan, because h2o will return a string.
    double xorsum = 0;
    for (double v : in) {
      vBits = Double.doubleToLongBits(v);
      xorsumBits = xorsumBits ^ vBits;
    }
    // just need to not get inf or nan. If we zero the upper 4 bits, we won't
    final long ZERO_SOME_SIGN_EXP = 0x0fffffffffffffffL;
    xorsumBits = xorsumBits & ZERO_SOME_SIGN_EXP;
    xorsum = Double.longBitsToDouble(xorsumBits);
    out[0] = xorsum;
    return out;
  }
}

// Selective return.  If the selector is a double, just eval both args and
// return the selected one.  If the selector is an array, then it must be
// compatible with argument arrays (if any), and the selection is done
// element-by-element.
class ASTIfElse extends ASTOp {
  static final String VARS[] = new String[]{"ifelse","tst","true","false"};
  static Type[] newsig() {
    Type t1 = Type.unbound(), t2 = Type.unbound(), t3=Type.unbound();
    return new Type[]{Type.anyary(new Type[]{t1,t2,t3}),t1,t2,t3};
  }
  ASTIfElse( ) { super(VARS, newsig(),OPF_INFIX,OPP_PREFIX,OPA_RIGHT); }
  @Override ASTOp make() {return new ASTIfElse();}
  @Override String opStr() { return "ifelse"; }
  // Parse an infix trinary ?: operator
  static AST parse(Exec2 E, AST tst, boolean EOS) {
    if( !E.peek('?',true) ) return null;
    int x=E._x;
    AST tru=E.xpeek(':',E._x,parseCXExpr(E,false));
    if( tru == null ) E.throwErr("Missing expression in trinary",x);
    x = E._x;
    AST fal=parseCXExpr(E,EOS);
    if( fal == null ) E.throwErr("Missing expression in trinary",x);
    return ASTApply.make(new AST[]{new ASTIfElse(),tst,tru,fal},E,x);
  }
  @Override void apply(Env env, int argcnt, ASTApply apply) {
    // All or none are functions
    assert ( env.isFcn(-1) &&  env.isFcn(-2) &&  _t.ret().isFcn())
      ||   (!env.isFcn(-1) && !env.isFcn(-2) && !_t.ret().isFcn());
    // If the result is an array, then one of the other of the two must be an
    // array.  , and this is a broadcast op.
    assert !_t.isAry() || env.isAry(-1) || env.isAry(-2);

    // Single selection?  Then just pick slots
    if( !env.isAry(-3) ) {
      if( env.dbl(-3)==0 ) env.pop_into_stk(-4);
      else {  env.pop();   env.pop_into_stk(-3); }
      return;
    }

    Frame  frtst=null, frtru= null, frfal= null;
    double  dtst=  0 ,  dtru=   0 ,  dfal=   0 ;
    String kf, kt, kq;

    boolean bothStr=false; // are both yes and no a string? ok that's easy to deal with...
    String stru=null, sfal=null;

    if( env.isAry() ) frfal= env.popAry();
    else if( env.isDbl() && !env.isStr() ) { dfal = env.popDbl(); }
    else if( env.isStr() ) { sfal=env.popStr(); dfal=0.0; }
    kf = env.key();

    if( env.isAry() ) frtru= env.popAry();
    else if( env.isDbl() && !env.isStr() ) { dtru = env.popDbl(); }
    else if( env.isStr() ) { stru=env.popStr(); dtru=1.0; }
    kt = env.key();

    if( env.isAry() ) frtst= env.popAry(); else dtst = env.popDbl();
    kq = env.key();

    bothStr= stru!=null&&sfal!=null; // bothStr==true => make domain [stru, sfal]

    // Multi-selection
    // Build a doAll frame
    Frame fr  = new Frame(frtst); // Do-All frame
    final int  ncols = frtst.numCols(); // Result column count
    final long nrows = frtst.numRows(); // Result row count
    String names[]=null;
    if( frtru !=null ) {          // True is a Frame?
      if( frtru.numCols() != ncols ||  frtru.numRows() != nrows )
        throw new IllegalArgumentException("Arrays must be same size: "+frtst+" vs "+frtru);
      fr.add(frtru,true);
      names = frtru._names;
    }
    if( frfal !=null ) {          // False is a Frame?
      if( frfal.numCols() != ncols ||  frfal.numRows() != nrows )
        throw new IllegalArgumentException("Arrays must be same size: "+frtst+" vs "+frfal);
      fr.add(frfal,true);
      names = frfal._names;
    }
    if( names==null && frtst!=null ) names = frtst._names;
    final boolean t = frtru != null;
    final boolean f = frfal != null;
    final double fdtru = dtru;
    final double fdfal = dfal;
    String[][] domains=fr.domains();
    if( bothStr ) domains[0] = new String[]{sfal,stru};
    // Run a selection picking true/false across the frame
    Frame fr2 = new MRTask2() {
        @Override public void map( Chunk chks[], NewChunk nchks[] ) {
          for( int i=0; i<nchks.length; i++ ) {
            NewChunk n =nchks[i];
            int off=i;
            Chunk ctst=     chks[off];
            Chunk ctru= t ? chks[off+=ncols] : null;
            Chunk cfal= f ? chks[off+=ncols] : null;
            int rlen = ctst._len;
            for( int r=0; r<rlen; r++ )
              if( ctst.isNA0(r) ) n.addNA();
              else n.addNum(ctst.at0(r)!=0 ? (t ? ctru.at0(r) : fdtru) : (f ? cfal.at0(r) : fdfal));
          }
        }
      }.doAll(ncols,fr).outputFrame(names,domains);
    env.subRef(frtst,kq);
    if( frtru != null ) env.subRef(frtru,kt);
    if( frfal != null ) env.subRef(frfal,kf);
    env.pop();
    env.push(fr2);
  }
}

class ASTCut extends ASTOp {
  ASTCut() { super(new String[]{"cut", "ary", "dbls"},
                   new Type[]{Type.ARY, Type.ARY, Type.dblary()},
                   OPF_PREFIX,
                   OPP_PREFIX,
                   OPA_RIGHT); }
  @Override String opStr() { return "cut"; }
  @Override ASTOp make() {return new ASTCut();}
  @Override void apply(Env env, int argcnt, ASTApply apply) {
    if(env.isDbl()) {
      final int nbins = (int) Math.floor(env.popDbl());
      if(nbins < 2)
        throw new IllegalArgumentException("Number of intervals must be at least 2");

      Frame fr = env.popAry();
      String skey = env.key();
      if(fr.vecs().length != 1 || fr.vecs()[0].isEnum())
        throw new IllegalArgumentException("First argument must be a numeric column vector");

      final double fmax = fr.vecs()[0].max();
      final double fmin = fr.vecs()[0].min();
      final double width = (fmax - fmin)/nbins;
      if(width == 0) throw new IllegalArgumentException("Data vector is constant!");
      // Note: I think R perturbs constant vecs slightly so it can still bin values

      // Construct domain names from bins intervals
      String[][] domains = new String[1][nbins];
      domains[0][0] = "(" + String.valueOf(fmin - 0.001*(fmax-fmin)) + "," + String.valueOf(fmin + width) + "]";
      for(int i = 1; i < nbins; i++)
        domains[0][i] = "(" + String.valueOf(fmin + i*width) + "," + String.valueOf(fmin + (i+1)*width) + "]";

      Frame fr2 = new MRTask2() {
        @Override public void map(Chunk chk, NewChunk nchk) {
          for(int r = 0; r < chk._len; r++) {
            double x = chk.at0(r);
            double n = x == fmax ? nbins-1 : Math.floor((x - fmin)/width);
            nchk.addNum(n);
          }
        }
      }.doAll(1,fr).outputFrame(fr._names, domains);
      env.subRef(fr, skey);
      env.pop();
      env.push(fr2);
    } else if(env.isAry()) {
      Frame ary = env.popAry();
      String skey1 = env.key();
      if(ary.vecs().length != 1 || ary.vecs()[0].isEnum())
        throw new IllegalArgumentException("Second argument must be a numeric column vector");
      Vec brks = ary.vecs()[0];
      // TODO: Check that num rows below some cutoff, else this will likely crash

      // Remove duplicates and sort vector of breaks in ascending order
      SortedSet<Double> temp = new TreeSet<Double>();
      for(int i = 0; i < brks.length(); i++) temp.add(brks.at(i));
      int cnt = 0; final double[] cutoffs = new double[temp.size()];
      for(Double x : temp) { cutoffs[cnt] = x; cnt++; }

      if(cutoffs.length < 2)
        throw new IllegalArgumentException("Vector of breaks must have at least 2 unique values");
      Frame fr = env.popAry();
      String skey2 = env.key();
      if(fr.vecs().length != 1 || fr.vecs()[0].isEnum())
        throw new IllegalArgumentException("First argument must be a numeric column vector");

      // Construct domain names from bin intervals
      final int nbins = cutoffs.length-1;
      String[][] domains = new String[1][nbins];
      for(int i = 0; i < nbins; i++)
        domains[0][i] = "(" + cutoffs[i] + "," + cutoffs[i+1] + "]";

      Frame fr2 = new MRTask2() {
        @Override public void map(Chunk chk, NewChunk nchk) {
          for(int r = 0; r < chk._len; r++) {
            double x = chk.at0(r);
            if(Double.isNaN(x) || x <= cutoffs[0] || x > cutoffs[cutoffs.length-1])
              nchk.addNum(Double.NaN);
            else {
              for(int i = 1; i < cutoffs.length; i++) {
                if(x <= cutoffs[i]) { nchk.addNum(i-1); break; }
              }
            }
          }
        }
      }.doAll(1,fr).outputFrame(fr._names, domains);
      env.subRef(ary, skey1);
      env.subRef(fr, skey2);
      env.pop();
      env.push(fr2);
    } else throw H2O.unimpl();
  }
}

class ASTfindInterval extends ASTOp {
  ASTfindInterval() { super(new String[]{"findInterval", "ary", "vec", "rightmost.closed"},
                          new Type[]{Type.ARY, Type.ARY, Type.dblary(), Type.DBL},
                          OPF_PREFIX,
                          OPP_PREFIX,
                          OPA_RIGHT); }
  @Override String opStr() { return "findInterval"; }
  @Override ASTOp make() { return new ASTfindInterval(); }
  @Override void apply(Env env, int argcnt, ASTApply apply) {
    final boolean rclosed = env.popDbl() == 0 ? false : true;

    if(env.isDbl()) {
      final double cutoff = env.popDbl();

      Frame fr = env.popAry();
      String skey = env.key();
      if(fr.vecs().length != 1 || fr.vecs()[0].isEnum())
        throw new IllegalArgumentException("First argument must be a numeric column vector");

      Frame fr2 = new MRTask2() {
        @Override public void map(Chunk chk, NewChunk nchk) {
          for(int r = 0; r < chk._len; r++) {
            double x = chk.at0(r);
            if(Double.isNaN(x))
              nchk.addNum(Double.NaN);
            else {
              if(rclosed)
                nchk.addNum(x > cutoff ? 1 : 0);   // For rightmost.closed = TRUE
              else
                nchk.addNum(x >= cutoff ? 1 : 0);
            }
          }
        }
      }.doAll(1,fr).outputFrame(fr._names, fr.domains());
      env.subRef(fr, skey);
      env.pop();
      env.push(fr2);
    } else if(env.isAry()) {
      Frame ary = env.popAry();
      String skey1 = env.key();
      if(ary.vecs().length != 1 || ary.vecs()[0].isEnum())
        throw new IllegalArgumentException("Second argument must be a numeric column vector");
      Vec brks = ary.vecs()[0];
      // TODO: Check that num rows below some cutoff, else this will likely crash

      // Check if vector of cutoffs is sorted in weakly ascending order
      final int len = (int)brks.length();
      final double[] cutoffs = new double[len];
      for(int i = 0; i < len-1; i++) {
        if(brks.at(i) > brks.at(i+1))
          throw new IllegalArgumentException("Second argument must be sorted in non-decreasing order");
        cutoffs[i] = brks.at(i);
      }
      cutoffs[len-1] = brks.at(len-1);

      Frame fr = env.popAry();
      String skey2 = env.key();
      if(fr.vecs().length != 1 || fr.vecs()[0].isEnum())
        throw new IllegalArgumentException("First argument must be a numeric column vector");

      Frame fr2 = new MRTask2() {
        @Override public void map(Chunk chk, NewChunk nchk) {
          for(int r = 0; r < chk._len; r++) {
            double x = chk.at0(r);
            if(Double.isNaN(x))
              nchk.addNum(Double.NaN);
            else {
              double n = Arrays.binarySearch(cutoffs, x);
              if(n < 0) nchk.addNum(-n-1);
              else if(rclosed && n == len-1) nchk.addNum(n);   // For rightmost.closed = TRUE
              else nchk.addNum(n+1);
            }
          }
        }
      }.doAll(1,fr).outputFrame(fr._names, fr.domains());
      env.subRef(ary, skey1);
      env.subRef(fr, skey2);
      env.pop();
      env.push(fr2);
    }
  }
}

class ASTFactor extends ASTOp {
  ASTFactor() { super(new String[]{"factor", "ary"},
                      new Type[]{Type.ARY, Type.ARY},
                      OPF_PREFIX,
                      OPP_PREFIX,OPA_RIGHT); }
  @Override String opStr() { return "factor"; }
  @Override ASTOp make() {return new ASTFactor();}
  @Override void apply(Env env, int argcnt, ASTApply apply) {
    Frame ary = env.peekAry();   // Ary on top of stack, keeps +1 refcnt
    String skey = env.peekKey();
    if( ary.numCols() != 1 )
      throw new IllegalArgumentException("factor requires a single column");
    Vec v0 = ary.vecs()[0];
    Vec v1 = v0.isEnum() ? null : v0.toEnum();
    if (v1 != null) {
      ary = new Frame(ary._names,new Vec[]{v1});
      skey = null;
    }
    env.poppush(2, ary, skey);
  }
}

class ASTNumeric extends ASTOp {
  ASTNumeric() { super(new String[]{"as.numeric", "ary"},
          new Type[]{Type.ARY, Type.ARY},
          OPF_PREFIX,
          OPP_PREFIX,OPA_RIGHT); }
  @Override String opStr() { return "as.numeric"; }
  @Override ASTOp make() {return new ASTNumeric();}
  @Override void apply(Env env, int argcnt, ASTApply apply) {
    Frame ary = env.peekAry();   // Ary on top of stack, keeps +1 refcnt
    String skey = env.peekKey();
    Vec[] nvecs = new Vec[ary.numCols()];
    for (int c = 0; c < ary.numCols(); ++c) {
      Vec v = ary.vecs()[c];
      Vec nv = v.isEnum() ? v.masterVec() : null;
      (nvecs[c] = nv == null ? v : nv)._domain = null;
    }
    ary = new Frame(ary._names, nvecs);
    env.poppush(2, ary, skey);
  }
}

class ASTPrint extends ASTOp {
  static Type[] newsig() {
    Type t1 = Type.unbound();
    return new Type[]{t1, t1, Type.varargs(Type.unbound())};
  }
  ASTPrint() { super(new String[]{"print", "x", "y..."},
                     newsig(),
                     OPF_PREFIX,
                     OPP_PREFIX,OPA_RIGHT); }
  @Override String opStr() { return "print"; }
  @Override ASTOp make() {return new ASTPrint();}
  @Override void apply(Env env, int argcnt, ASTApply apply) {
    for( int i=1; i<argcnt; i++ ) {
      if( env.isAry(i-argcnt) ) {
        env._sb.append(env.ary(i-argcnt).toStringAll());
      } else {
        env._sb.append(env.toString(env._sp+i-argcnt,true));
      }
    }
    env.pop(argcnt-2);          // Pop most args
    env.pop_into_stk(-2);       // Pop off fcn, returning 1st arg
  }
}

/**
 * R 'ls' command.
 *
 * This method is purely for the console right now.  Print stuff into the string buffer.
 * JSON response is not configured at all.
 */
class ASTLs extends ASTOp {
  ASTLs() { super(new String[]{"ls"},
                  new Type[]{Type.DBL},
                  OPF_PREFIX,
                  OPP_PREFIX,
                  OPA_RIGHT); }
  @Override String opStr() { return "ls"; }
  @Override ASTOp make() {return new ASTLs();}
  @Override void apply(Env env, int argcnt, ASTApply apply) {
    for( Key key : H2O.KeySnapshot.globalSnapshot().keys())
      if( key.user_allowed() && H2O.get(key) != null )
        env._sb.append(key.toString());
    // Pop the self-function and push a zero.
    env.pop();
    env.push(0.0);
  }
}
