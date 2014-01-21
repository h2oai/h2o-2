package water.exec;

import java.util.*;

import water.*;
import water.fvec.*;
import water.util.Log;
import water.util.Utils;

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

    // Unary prefix ops
    putPrefix(new ASTIsNA());
    putPrefix(new ASTNrow());
    putPrefix(new ASTNcol());
    putPrefix(new ASTAbs ());
    putPrefix(new ASTSgn ());
    putPrefix(new ASTSqrt());
    putPrefix(new ASTCeil());
    putPrefix(new ASTFlr ());
    putPrefix(new ASTLog ());
    putPrefix(new ASTExp ());
    putPrefix(new ASTScale());
    putPrefix(new ASTFactor());
    putPrefix(new ASTIsFactor());
    putPrefix(new ASTAnyFactor());   // For Runit testing
    putPrefix(new ASTAnyNA());
    putPrefix(new ASTIsTRUE());

    putPrefix(new ASTCos());  // Trigonometric functions
    putPrefix(new ASTSin());
    putPrefix(new ASTTan());
    putPrefix(new ASTACos());
    putPrefix(new ASTASin());
    putPrefix(new ASTATan());
    putPrefix(new ASTCosh());
    putPrefix(new ASTSinh());
    putPrefix(new ASTTanh());

    // More generic reducers
    putPrefix(new ASTMin ());
    putPrefix(new ASTMax ());
    putPrefix(new ASTSum ());
    putPrefix(new ASTSdev());
    putPrefix(new ASTMean());
    putPrefix(new ASTMinNaRm());
    putPrefix(new ASTMaxNaRm());
    putPrefix(new ASTSumNaRm());
    // Misc
    putPrefix(new ASTSeq   ());
    putPrefix(new ASTCat   ());
    putPrefix(new ASTCbind ());
    putPrefix(new ASTTable ());
    putPrefix(new ASTReduce());
    putPrefix(new ASTIfElse());
    putPrefix(new ASTRApply());
    putPrefix(new ASTRunif ());
    putPrefix(new ASTCut   ());
    putPrefix(new ASTPrint ());
    putPrefix(new ASTLs    ());
  }
  static private void putUniInfix(ASTOp ast) { UNI_INFIX_OPS.put(ast.opStr(),ast); }
  static private void putBinInfix(ASTOp ast) { BIN_INFIX_OPS.put(ast.opStr(),ast); }
  static private void putPrefix  (ASTOp ast) {    PREFIX_OPS.put(ast.opStr(),ast); }
  static public ASTOp isOp(String id) {
    // This order matters. If used as a prefix OP, `+` and `-` are binary only.
    ASTOp op3 =    PREFIX_OPS.get(id); if( op3 != null ) return op3;
    ASTOp op2 = BIN_INFIX_OPS.get(id); if( op2 != null ) return op2;
    ASTOp op1 = UNI_INFIX_OPS.get(id);                   return op1;
  }
  static public boolean isInfixOp(String id) {
    return BIN_INFIX_OPS.containsKey(id) || UNI_INFIX_OPS.containsKey(id);
  }
  static public Set<String> opStrs() {
    Set<String> all = UNI_INFIX_OPS.keySet();
    all.addAll(BIN_INFIX_OPS.keySet());
    all.addAll(PREFIX_OPS.keySet());
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
    if( op != null && !E.peek('=') ) return op.make();
    E._x = x;
    return ASTFunc.parseFcn(E);
  }

  // Parse a prefix OP or return null.
  static ASTOp parsePrefixOp(Exec2 E) {
    int x = E._x;
    String id = E.isID();
    if( id == null ) return null;
    ASTOp op = PREFIX_OPS.get(id);
    if( op != null ) return op.make();
    E._x = x;                 // Roll back, no parse happened
    // Attempt a user-mode function parse
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

  double[] map(Env env, double[] in, double[] out) { H2O.unimpl(); return null; }

  @Override void exec(Env env) { env.push(this); }
  abstract void apply(Env env, int argcnt);
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
  @Override void apply(Env env, int argcnt) {
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

class ASTCos  extends ASTUniPrefixOp { String opStr(){ return "cos";   } ASTOp make() {return new ASTCos ();} double op(double d) { return Math.cos(d);}}
class ASTSin  extends ASTUniPrefixOp { String opStr(){ return "sin";   } ASTOp make() {return new ASTSin ();} double op(double d) { return Math.sin(d);}}
class ASTTan  extends ASTUniPrefixOp { String opStr(){ return "tan";   } ASTOp make() {return new ASTTan ();} double op(double d) { return Math.tan(d);}}
class ASTACos extends ASTUniPrefixOp { String opStr(){ return "acos";  } ASTOp make() {return new ASTACos();} double op(double d) { return Math.acos(d);}}
class ASTASin extends ASTUniPrefixOp { String opStr(){ return "asin";  } ASTOp make() {return new ASTASin();} double op(double d) { return Math.asin(d);}}
class ASTATan extends ASTUniPrefixOp { String opStr(){ return "atan";  } ASTOp make() {return new ASTATan();} double op(double d) { return Math.atan(d);}}
class ASTCosh extends ASTUniPrefixOp { String opStr(){ return "cosh";  } ASTOp make() {return new ASTCosh ();} double op(double d) { return Math.cosh(d);}}
class ASTSinh extends ASTUniPrefixOp { String opStr(){ return "sinh";  } ASTOp make() {return new ASTSinh ();} double op(double d) { return Math.sinh(d);}}
class ASTTanh extends ASTUniPrefixOp { String opStr(){ return "tanh";  } ASTOp make() {return new ASTTanh ();} double op(double d) { return Math.tanh(d);}}

class ASTAbs  extends ASTUniPrefixOp { String opStr(){ return "abs";   } ASTOp make() {return new ASTAbs ();} double op(double d) { return Math.abs(d);}}
class ASTSgn  extends ASTUniPrefixOp { String opStr(){ return "sgn" ;  } ASTOp make() {return new ASTSgn ();} double op(double d) { return Math.signum(d);}}
class ASTSqrt extends ASTUniPrefixOp { String opStr(){ return "sqrt";  } ASTOp make() {return new ASTSqrt();} double op(double d) { return Math.sqrt(d);}}
class ASTCeil extends ASTUniPrefixOp { String opStr(){ return "ceil";  } ASTOp make() {return new ASTCeil();} double op(double d) { return Math.ceil(d);}}
class ASTFlr  extends ASTUniPrefixOp { String opStr(){ return "floor"; } ASTOp make() {return new ASTFlr ();} double op(double d) { return Math.floor(d);}}
class ASTLog  extends ASTUniPrefixOp { String opStr(){ return "log";   } ASTOp make() {return new ASTLog ();} double op(double d) { return Math.log(d);}}
class ASTExp  extends ASTUniPrefixOp { String opStr(){ return "exp";   } ASTOp make() {return new ASTExp ();} double op(double d) { return Math.exp(d);}}
class ASTIsNA extends ASTUniPrefixOp { String opStr(){ return "is.na"; } ASTOp make() {return new ASTIsNA();} double op(double d) { return Double.isNaN(d)?1:0;}}

class ASTNrow extends ASTUniPrefixOp {
  ASTNrow() { super(VARS1,new Type[]{Type.DBL,Type.ARY}); }
  @Override String opStr() { return "nrow"; }
  @Override ASTOp make() {return this;}
  @Override void apply(Env env, int argcnt) {
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
  @Override void apply(Env env, int argcnt) {
    Frame fr = env.popAry();
    String skey = env.key();
    double d = fr.numCols();
    env.subRef(fr,skey);
    env.poppush(d);
  }
}

class ASTIsFactor extends ASTUniPrefixOp {
  ASTIsFactor() { super(VARS1,new Type[]{Type.DBL,Type.ARY}); }
  @Override String opStr() { return "is.factor"; }
  @Override ASTOp make() {return this;}
  @Override void apply(Env env, int argcnt) {
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
  @Override void apply(Env env, int argcnt) {
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

class ASTAnyNA extends ASTUniPrefixOp {
  ASTAnyNA() { super(VARS1,new Type[]{Type.DBL,Type.ARY}); }
  @Override String opStr() { return "any.na"; }
  @Override ASTOp make() {return this;}
  @Override void apply(Env env, int argcnt) {
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
  @Override void apply(Env env, int argcnt) {
    double res = env.isDbl() && env.popDbl()==1.0 ? 1:0;
    env.pop();
    env.poppush(res);
  }
}

class ASTScale extends ASTUniPrefixOp {
  ASTScale() { super(VARS1,new Type[]{Type.ARY,Type.ARY}); }
  @Override String opStr() { return "scale"; }
  @Override ASTOp make() {return this;}
  @Override void apply(Env env, int argcnt) {
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
  @Override void apply(Env env, int argcnt) {
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
    final double fd0 = d0;
    final double fd1 = d1;
    Frame fr  = null;           // Do-All frame
    int ncols = 0;              // Result column count
    if( fr0 !=null ) {          // Left?
      ncols = fr0.numCols();
      if( fr1 != null ) {
        if( fr0.numCols() != fr1.numCols() ||
            fr0.numRows() != fr1.numRows() )
          throw new IllegalArgumentException("Arrays must be same size: "+fr0+" vs "+fr1);
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
            Chunk c0= !lf ? null : chks[i];
            Chunk c1= !rf ? null : chks[i+(lf?nchks.length:0)];
            int rlen = (lf ? c0 : c1)._len;
            for( int r=0; r<rlen; r++ ) {
              if(chks[i]._vec.isEnum())
                n.addNA(); //slam in NA if op on enum; same as R
              else
                n.addNum(bin.op(lf ? c0.at0(r) : fd0, rf ? c1.at0(r) : fd1));
            }
          }
        }
      }.doAll(ncols,fr).outputFrame((lf ? fr0 : fr1)._names,(lf ? fr0 : fr1).domains());
    if( fr0 != null ) env.subRef(fr0,k0);
    if( fr1 != null ) env.subRef(fr1,k1);
    env.pop();
    env.push(fr2);
  }
}

class ASTUniPlus  extends ASTUniOp { ASTUniPlus()  { super(OPF_INFIX, OPP_UPLUS,  OPA_RIGHT); } String opStr(){ return "+"  ;} ASTOp make() {return new ASTUniPlus(); } double op(double d) { return d;}}
class ASTUniMinus extends ASTUniOp { ASTUniMinus() { super(OPF_INFIX, OPP_UMINUS, OPA_RIGHT); } String opStr(){ return "-"  ;} ASTOp make() {return new ASTUniMinus();} double op(double d) { return -d;}}
class ASTNot      extends ASTUniOp { ASTNot()      { super(OPF_INFIX, OPP_NOT,    OPA_RIGHT); } String opStr(){ return "!"  ;} ASTOp make() {return new ASTNot();     } double op(double d) { return d==0?1:0; }}
class ASTPlus     extends ASTBinOp { ASTPlus()     { super(OPF_INFIX, OPP_PLUS,   OPA_LEFT ); } String opStr(){ return "+"  ;} ASTOp make() {return new ASTPlus();} double op(double d0, double d1) { return d0+d1;}}
class ASTSub      extends ASTBinOp { ASTSub()      { super(OPF_INFIX, OPP_MINUS,  OPA_LEFT); }  String opStr(){ return "-"  ;} ASTOp make() {return new ASTSub ();} double op(double d0, double d1) { return d0-d1;}}
class ASTMul      extends ASTBinOp { ASTMul()      { super(OPF_INFIX, OPP_MUL,    OPA_LEFT); }  String opStr(){ return "*"  ;} ASTOp make() {return new ASTMul ();} double op(double d0, double d1) { return d0*d1;}}
class ASTDiv      extends ASTBinOp { ASTDiv()      { super(OPF_INFIX, OPP_DIV,    OPA_LEFT); }  String opStr(){ return "/"  ;} ASTOp make() {return new ASTDiv ();} double op(double d0, double d1) { return d0/d1;}}
class ASTPow      extends ASTBinOp { ASTPow()      { super(OPF_INFIX, OPP_POWER,  OPA_RIGHT);}  String opStr(){ return "^"  ;} ASTOp make() {return new ASTPow ();} double op(double d0, double d1) { return Math.pow(d0,d1);}}
class ASTPow2     extends ASTBinOp { ASTPow2()     { super(OPF_INFIX, OPP_POWER,  OPA_RIGHT);}  String opStr(){ return "**" ;} ASTOp make() {return new ASTPow2();} double op(double d0, double d1) { return Math.pow(d0,d1);}}
class ASTMod      extends ASTBinOp { ASTMod()      { super(OPF_INFIX, OPP_MOD,    OPA_LEFT); }  String opStr(){ return "%"  ;} ASTOp make() {return new ASTMod ();} double op(double d0, double d1) { return d0%d1;}}
class ASTLT       extends ASTBinOp { ASTLT()       { super(OPF_INFIX, OPP_LT,     OPA_LEFT); }  String opStr(){ return "<"  ;} ASTOp make() {return new ASTLT  ();} double op(double d0, double d1) { return d0<d1 && !Utils.equalsWithinOneSmallUlp(d0,d1)?1:0;}}
class ASTLE       extends ASTBinOp { ASTLE()       { super(OPF_INFIX, OPP_LE,     OPA_LEFT); }  String opStr(){ return "<=" ;} ASTOp make() {return new ASTLE  ();} double op(double d0, double d1) { return d0<d1 ||  Utils.equalsWithinOneSmallUlp(d0,d1)?1:0;}}
class ASTGT       extends ASTBinOp { ASTGT()       { super(OPF_INFIX, OPP_GT,     OPA_LEFT); }  String opStr(){ return ">"  ;} ASTOp make() {return new ASTGT  ();} double op(double d0, double d1) { return d0>d1 && !Utils.equalsWithinOneSmallUlp(d0,d1)?1:0;}}
class ASTGE       extends ASTBinOp { ASTGE()       { super(OPF_INFIX, OPP_GE,     OPA_LEFT); }  String opStr(){ return ">=" ;} ASTOp make() {return new ASTGE  ();} double op(double d0, double d1) { return d0>d1 ||  Utils.equalsWithinOneSmallUlp(d0,d1)?1:0;}}
class ASTEQ       extends ASTBinOp { ASTEQ()       { super(OPF_INFIX, OPP_EQ,     OPA_LEFT); }  String opStr(){ return "==" ;} ASTOp make() {return new ASTEQ  ();} double op(double d0, double d1) { return Utils.equalsWithinOneSmallUlp(d0,d1)?1:0;}}
class ASTNE       extends ASTBinOp { ASTNE()       { super(OPF_INFIX, OPP_NE,     OPA_LEFT); }  String opStr(){ return "!=" ;} ASTOp make() {return new ASTNE  ();} double op(double d0, double d1) { return Utils.equalsWithinOneSmallUlp(d0,d1)?0:1;}}
class ASTLA       extends ASTBinOp { ASTLA()       { super(OPF_INFIX, OPP_AND,    OPA_LEFT); }  String opStr(){ return "&"  ;} ASTOp make() {return new ASTLA  ();} double op(double d0, double d1) { return (d0!=0 && d1!=0) ? (Double.isNaN(d0) || Double.isNaN(d1)?Double.NaN:1) :0;}}
class ASTLO       extends ASTBinOp { ASTLO()       { super(OPF_INFIX, OPP_OR,     OPA_LEFT); }  String opStr(){ return "|"  ;} ASTOp make() {return new ASTLO  ();} double op(double d0, double d1) { return (d0==0 && d1==0) ? (Double.isNaN(d0) || Double.isNaN(d1)?Double.NaN:0) :1;}}

// Variable length; instances will be created of required length
abstract class ASTReducerOp extends ASTOp {
  final double _init;
  final boolean _narm;        // na.rm in R
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
  @Override void apply(Env env, int argcnt) {
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

class ASTSum     extends ASTReducerOp { ASTSum( )     {super(0,false);} String opStr(){ return "sum"      ;} ASTOp make() {return new ASTSum();    } double op(double d0, double d1) { return d0+d1;}}
class ASTSumNaRm extends ASTReducerOp { ASTSumNaRm( ) {super(0,true) ;} String opStr(){ return "sum.na.rm";} ASTOp make() {return new ASTSumNaRm();} double op(double d0, double d1) { return d0+d1;}}

class ASTReduce extends ASTOp {
  static final String VARS[] = new String[]{ "", "op2", "ary"};
  static final Type   TYPES[]= new Type  []{ Type.ARY, Type.fcn(new Type[]{Type.DBL,Type.DBL,Type.DBL}), Type.ARY };
  ASTReduce( ) { super(VARS,TYPES,OPF_PREFIX,OPP_PREFIX,OPA_RIGHT); }
  @Override String opStr(){ return "Reduce";}
  @Override ASTOp make() {return this;}
  @Override void apply(Env env, int argcnt) { throw H2O.unimpl(); }
}

// TODO: Check refcnt mismatch issue: tmp = cbind(h.hex,3.5) results in different refcnts per col
class ASTCbind extends ASTOp {
  @Override String opStr() { return "cbind"; }
  ASTCbind( ) { super(new String[]{"cbind","ary"},
                      new Type[]{Type.ARY,Type.varargs(Type.dblary())},
                      OPF_PREFIX,
                      OPP_PREFIX,OPA_RIGHT); }
  @Override ASTOp make() {return this;}
  @Override void apply(Env env, int argcnt) {
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

    Frame fr = null;
    if(env.isAry(-argcnt+1))
      fr = new Frame(env.ary(-argcnt+1));
    else {
      // Vec v = new Vec(Key.make(), env.dbl(-argcnt+1));
      double d = env.dbl(-argcnt+1);
      Vec v = vmax == null ? new Vec(Key.make(), d) : vmax.makeCon(d);
      fr = new Frame(new String[] {"c0"}, new Vec[] {v});
      env.addRef(v);
    }

    for(int i = 1; i < argcnt-1; i++) {
      if(env.isAry(-argcnt+1+i))
        fr.add(env.ary(-argcnt+1+i),true);
      else {
        double d = env.dbl(-argcnt+1+i);
        // Vec v = fr.vecs()[0].makeCon(d);
        Vec v = vmax == null ? new Vec(Key.make(), d) : vmax.makeCon(d);
        fr.add("c" + String.valueOf(i), v);
        env.addRef(v);
      }
    }
    env._ary[env._sp-argcnt] = fr;
    env._sp -= argcnt-1;
    assert env.check_refcnt(fr.anyVec());
  }
}

class ASTMinNaRm extends ASTReducerOp {
  ASTMinNaRm( ) { super( Double.POSITIVE_INFINITY, true ); }
  String opStr(){ return "min.na.rm";}
  ASTOp make() {return new ASTMinNaRm();}
  @Override double op(double d0, double d1) { return Math.min(d0, d1); }
  @Override void apply(Env env, int argcnt) {
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
  String opStr(){ return "max.na.rm";}
  ASTOp make() {return new ASTMaxNaRm();}
  @Override double op(double d0, double d1) { return Math.max(d0,d1); }
  @Override void apply(Env env, int argcnt) {
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
  String opStr(){ return "min";}
  ASTOp make() {return new ASTMin();}
  @Override double op(double d0, double d1) { return Math.min(d0, d1); }
  @Override void apply(Env env, int argcnt) {
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
  String opStr(){ return "max";}
  ASTOp make() {return new ASTMax();}
  @Override double op(double d0, double d1) { return Math.max(d0,d1); }
  @Override void apply(Env env, int argcnt) {
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
  @Override void apply(Env env, int argcnt) {
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
  @Override void apply(Env env, int argcnt) {
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

// Similar to R's seq_len
class ASTSeq extends ASTOp {
  @Override String opStr() { return "seq_len"; }
  ASTSeq( ) {
    super(new String[]{"seq_len", "n"},
            new Type[]{Type.ARY,Type.DBL},
            OPF_PREFIX,
            OPP_PREFIX,
            OPA_RIGHT);
  }
  @Override ASTOp make() { return this; }
  @Override void apply(Env env, int argcnt) {
    int len = (int)env.popDbl();
    if (len <= 0)
      throw new IllegalArgumentException("Error in seq_len(" +len+"): argument must be coercible to positive integer");
    Key key = Vec.VectorGroup.VG_LEN1.addVecs(1)[0];
    AppendableVec av = new AppendableVec(key);
    NewChunk nc = new NewChunk(av,0);
    for (int r = 0; r < len; r++) nc.addNum(r+1);
    nc.close(0,null);
    Vec v = av.close(null);
    env.pop();
    env.push(new Frame(new String[]{"c"}, new Vec[]{v}));
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
  @Override void apply(Env env, int argcnt) {
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
    env.push(new Frame(new String[]{"c"}, new Vec[]{v}));
  }
}

class ASTRunif extends ASTOp {
  @Override String opStr() { return "runif"; }
  ASTRunif() { super(new String[]{"runif","dbls"},
                     new Type[]{Type.ARY,Type.ARY},
                     OPF_PREFIX,
                     OPP_PREFIX,
                     OPA_RIGHT); }
  @Override ASTOp make() {return new ASTRunif();}
  @Override void apply(Env env, int argcnt) {
    Frame fr = env.popAry();
    String skey = env.key();
    long [] espc = fr.anyVec()._espc;
    long rem = fr.numRows();
    if(rem > espc[espc.length-1])throw H2O.unimpl();
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
    final long seed = System.currentTimeMillis();
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
  @Override void apply(Env env, int argcnt) {
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

class ASTMean extends ASTOp {
  ASTMean() { super(new String[]{"mean", "ary"}, new Type[]{Type.DBL,Type.ARY},
                    OPF_PREFIX,
                    OPP_PREFIX,
                    OPA_RIGHT); }
  @Override String opStr() { return "mean"; }
  @Override ASTOp make() { return new ASTMean(); }
  @Override void apply(Env env, int argcnt) {
    Frame fr = env.peekAry();
    if (fr.vecs().length > 1)
      throw new IllegalArgumentException("sd does not apply to multiple cols.");
    if (fr.vecs()[0].isEnum())
      throw new IllegalArgumentException("sd only applies to numeric vector.");
    double ave = fr.vecs()[0].mean();
    env.pop();
    env.poppush(ave);
  }
}

class ASTTable extends ASTOp {
  ASTTable() { super(new String[]{"table", "ary"}, new Type[]{Type.ARY,Type.ARY},
                     OPF_PREFIX,
                     OPP_PREFIX,
                     OPA_RIGHT); }
  @Override String opStr() { return "table"; }
  @Override ASTOp make() { return new ASTTable(); }
  @Override void apply(Env env, int argcnt) {
    int ncol;
    Frame fr = env.ary(-1);
    if ((ncol = fr.vecs().length) > 2)
      throw new IllegalArgumentException("table does not apply to more than two cols.");
    for (int i = 0; i < ncol; i++) if (!fr.vecs()[i].isInt())
      throw new IllegalArgumentException("table only applies to integer vectors.");
    String[][] domains = new String[ncol][];  // the domain names to display as row and col names
                                              // if vec does not have original domain, use levels returned by CoolectDomain
    int[][] levels = new int[ncol][];
    for (int i = 0; i < ncol; i++) {
      Vec v = fr.vecs()[i];
      levels[i] = new Vec.CollectDomain(v).doAll(new Frame(v)).domain();
      domains[i] = v.domain();
    }
    long[][] counts = new Tabularize(levels).doAll(fr)._counts;
    // Build output vecs
    Key keys[] = Vec.VectorGroup.VG_LEN1.addVecs(counts.length+1);
    Vec[] vecs = new Vec[counts.length+1];
    String[] colnames = new String[counts.length+1];
    AppendableVec v0 = new AppendableVec(keys[0]);
    v0._domain = fr.vecs()[0].domain() == null ? null : fr.vecs()[0].domain().clone();
    NewChunk c0 = new NewChunk(v0,0);
    for( int i=0; i<levels[0].length; i++ ) c0.addNum((double) levels[0][i]);
    c0.close(0,null);
    vecs[0] = v0.close(null);
    colnames[0] = "row.names";
    if (ncol==1) colnames[1] = "Count";
    for (int level1=0; level1 < counts.length; level1++) {
      AppendableVec v = new AppendableVec(keys[level1+1]);
      NewChunk c = new NewChunk(v,0);
      v._domain = null;
      for (int level0=0; level0 < counts[level1].length; level0++)
        c.addNum((double) counts[level1][level0]);
      c.close(0, null);
      vecs[level1+1] = v.close(null);
      if (ncol>1) {
        colnames[level1+1] = domains[1]==null? Integer.toString(levels[1][level1]) : domains[1][levels[1][level1]];
      }
    }
    env.pop(2);
    env.push(new Frame(colnames, vecs));
  }
  private static class Tabularize extends MRTask2<Tabularize> {
    public final int[][]  _domains;
    public long[][] _counts;

    public Tabularize(int[][] dom) { super(); _domains=dom; }
    @Override public void map(Chunk[] cs) {
      assert cs.length == _domains.length;
      _counts = _domains.length==1? new long[1][] : new long[_domains[1].length][];
      for (int i=0; i < _counts.length; i++) _counts[i] = new long[_domains[0].length];
      for (int i=0; i < cs[0]._len; i++) {
        int level0, level1;
        if (cs[0].isNA0(i)) continue; else level0 = Arrays.binarySearch(_domains[0],(int)cs[0].at80(i));
        assert 0 <= level0 && level0 < _domains[0].length;
        if (cs.length>1) {
          if (cs[1].isNA0(i)) continue; else level1 = Arrays.binarySearch(_domains[1],(int)cs[1].at80(i));
          assert 0 <= level1 && level1 < _domains[1].length;
        } else {
          level1 = 0;
        }
        _counts[level1][level0]++;
      }
    }
    @Override public void reduce(Tabularize that) { Utils.add(_counts,that._counts); }
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
  static AST parse(Exec2 E, AST tst) {
    if( !E.peek('?') ) return null;
    int x=E._x;
    AST tru=E.xpeek(':',E._x,parseCXExpr(E));
    if( tru == null ) E.throwErr("Missing expression in trinary",x);
    x = E._x;
    AST fal=parseCXExpr(E);
    if( fal == null ) E.throwErr("Missing expression in trinary",x);
    return ASTApply.make(new AST[]{new ASTIfElse(),tst,tru,fal},E,x);
  }
  @Override void apply(Env env, int argcnt) {
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
    if( env.isAry() ) frfal= env.popAry(); else dfal = env.popDbl(); String kf = env.key();
    if( env.isAry() ) frtru= env.popAry(); else dtru = env.popDbl(); String kt = env.key();
    if( env.isAry() ) frtst= env.popAry(); else dtst = env.popDbl(); String kq = env.key();

    // Multi-selection
    // Build a doAll frame
    Frame fr  = new Frame(frtst); // Do-All frame
    final int  ncols = frtst.numCols(); // Result column count
    final long nrows = frtst.numRows(); // Result row count
    if( frtru !=null ) {          // True is a Frame?
      if( frtru.numCols() != ncols ||  frtru.numRows() != nrows )
        throw new IllegalArgumentException("Arrays must be same size: "+frtst+" vs "+frtru);
      fr.add(frtru,true);
    }
    if( frfal !=null ) {          // False is a Frame?
      if( frfal.numCols() != ncols ||  frfal.numRows() != nrows )
        throw new IllegalArgumentException("Arrays must be same size: "+frtst+" vs "+frfal);
      fr.add(frfal,true);
    }
    final boolean t = frtru != null;
    final boolean f = frfal != null;
    final double fdtru = dtru;
    final double fdfal = dfal;

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
      }.doAll(ncols,fr).outputFrame(fr._names,fr.domains());
    env.subRef(frtst,kq);
    if( frtru != null ) env.subRef(frtru,kt);
    if( frfal != null ) env.subRef(frfal,kf);
    env.pop();
    env.push(fr2);
  }
}

// --------------------------------------------------------------------------
// R's Apply.  Function is limited to taking a single column and returning
// a single column.  Double is limited to 1 or 2, statically determined.
class ASTRApply extends ASTOp {
  static final String VARS[] = new String[]{ "", "ary", "dbl1.2", "fcn"};
  ASTRApply( ) { super(VARS,
                       new Type[]{ Type.ARY, Type.ARY, Type.DBL, Type.fcn(new Type[]{Type.dblary(),Type.ARY}) },
                       OPF_PREFIX,
                       OPP_PREFIX,
                       OPA_RIGHT); }
  @Override String opStr(){ return "apply";}
  @Override ASTOp make() {return new ASTRApply();}
  @Override void apply(Env env, int argcnt) {
    int oldsp = env._sp;
    // Peek everything from the stack
    final ASTOp op = env.fcn(-1);    // ary->dblary but better be ary[,1]->dblary[,1]
    double d = env.dbl(-2);    // MARGIN: ROW=1, COLUMN=2 selector
    Frame fr = env.ary(-3);    // The Frame to work on
    if( d==2 || d== -1 ) {     // Work on columns?
      int ncols = fr.numCols();

      // If results are doubles, make vectors-of-length-1 for them all
      Key keys[] = null;
      if( op._t.ret().isDbl() ) {
        keys = Vec.VectorGroup.VG_LEN1.addVecs(ncols);
      } else assert op._t.ret().isAry();

      // Apply the function across columns
      Frame fr2 = new Frame(new String[0],new Vec[0]);
      Vec vecs[] = fr.vecs();
      for( int i=0; i<ncols; i++ ) {
        env.push(op);
        env.push(new Frame(new String[]{fr._names[i]},new Vec[]{vecs[i]}));
        env.fcn(-2).apply(env, 2);
        Vec v;
        if( keys != null ) {    // Doubles or Frame results?
          // Jam the double into a Vec of its own
          AppendableVec av = new AppendableVec(keys[i]);
          NewChunk nc = new NewChunk(av,0);
          nc.addNum(env.popDbl());
          nc.close(0, null);
          env.push(new Frame(v = av.close(null)));
        } else {                      // Frame results
          if( env.ary(-1).numCols() != 1 )
            throw new IllegalArgumentException("apply requires that "+op+" return 1 column");
          // Leave the ary on stack
          //v = env.popAry().anyVec();// Remove without lowering refcnt
        }
        //fr2.add(fr._names[i],v); // Add, with refcnt already +1
      }
      for( int i=0; i<ncols; i++ )
        fr2.add(fr._names[i], env.ary(-ncols+i).anyVec());

      int narg = env._sp - oldsp + 4;
      env.poppush(narg, fr2, null);
      assert env.isAry();
      assert env._sp == oldsp-4+1;
      return;
    }
    if( d==1 || d==-2) {      // Work on rows
      // apply on rows is essentially a map function
      Type ts[] = new Type[2];
      ts[0] = Type.unbound();
      ts[1] = Type.ARY;
      Type ft1 = Type.fcn(ts);
      Type ft2 = op._t.find();  // Should be a function type
      if( !ft1.union(ft2) ) {
        if( ft2._ts.length != 2 )
          throw new IllegalArgumentException("FCN " + op.toString() + " cannot accept one argument.");
        if( !ft2._ts[1].union(ts[1]) )
          throw new IllegalArgumentException("Arg " + op._vars[1] + " typed " + ft2._ts[1].find() + " but passed as " + ts[1]);
        assert false;
      }
      // find out return type
      final double[] rowin = new double[fr.vecs().length];
      for (int c = 0; c < rowin.length; c++) rowin[c] = fr.vecs()[c].at(0);
      final double[] rowout = op.map(env,rowin,null);
      final Env env0 = env;
      MRTask2 mrt = new MRTask2() {
        @Override
        public void map(Chunk[] cs, NewChunk[] ncs) {
          for (int i = 0; i < cs[0]._len; i++) {
            for (int c = 0; c < cs.length; c++) rowin[c] = cs[c].at0(i);
            op.map(env0, rowin, rowout);
            for (int c = 0; c < ncs.length; c++) ncs[c].addNum(rowout[c]);
          }
        }
      };
      String[] names = new String[rowout.length];
      for (int i = 0; i < names.length; i++) names[i] = "C"+(i+1);
      Frame res = mrt.doAll(rowout.length,fr).outputFrame(names, null);
      env.poppush(4,res,null);
      return;
    }
    throw new IllegalArgumentException("MARGIN limited to 1 (rows) or 2 (cols)");
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
  @Override void apply(Env env, int argcnt) {
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
            if(x <= cutoffs[0] || x > cutoffs[cutoffs.length-1])
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

class ASTFactor extends ASTOp {
  ASTFactor() { super(new String[]{"factor", "ary"},
                      new Type[]{Type.ARY, Type.ARY},
                      OPF_PREFIX,
                      OPP_PREFIX,OPA_RIGHT); }
  @Override String opStr() { return "factor"; }
  @Override ASTOp make() {return new ASTFactor();}
  @Override void apply(Env env, int argcnt) {
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
  @Override void apply(Env env, int argcnt) {
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
  @Override void apply(Env env, int argcnt) {
    for( Key key : H2O.keySet() )
      if( key.user_allowed() && H2O.get(key) != null )
        env._sb.append(key.toString());
    // Pop the self-function and push a zero.
    env.pop();
    env.push(0.0);
  }
}
