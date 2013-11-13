package water.exec;

import java.util.*;

import org.apache.commons.lang.ArrayUtils;

import water.*;
import water.fvec.*;

/** Parse a generic R string and build an AST, in the context of an H2O Cloud
 *  @author cliffc@0xdata.com
 */

// --------------------------------------------------------------------------
public abstract class ASTOp extends AST {
  static final HashMap<String,ASTOp> OPS = new HashMap();
  static {
    // Unary ops
    put(new ASTIsNA());
    put(new ASTNrow());
    put(new ASTNcol());
    put(new ASTAbs());
    put(new ASTSgn ());
    put(new ASTSqrt());
    put(new ASTCeil());
    put(new ASTFlr());
    put(new ASTLog());
    put(new ASTExp());
    put(new ASTNot());

    // Binary ops
    put(new ASTPlus());
    put(new ASTSub ());
    put(new ASTMul ());
    put(new ASTDiv ());
    put(new ASTPow ());
    put(new ASTMod ());
    put(new ASTMin ());
    put(new ASTMax ());
    put(new ASTLT  ());
    put(new ASTLE  ());
    put(new ASTGT  ());
    put(new ASTGE  ());
    put(new ASTEQ  ());
    put(new ASTNE  ());
    put(new ASTLA  ());
    put(new ASTLO  ());

    // Misc
    put(new ASTCat ());
    put(new ASTSum ());
    put(new ASTTable ());
    put(new ASTReduce());
    put(new ASTIfElse());
    put(new ASTRApply());
    put(new ASTRunif());
    put(new ASTCut());
  }
  static private void put(ASTOp ast) { OPS.put(ast.opStr(),ast); }

  // All fields are final, because functions are immutable
  final String _vars[];         // Variable names
  ASTOp( String vars[], Type ts[] ) { super(Type.fcn(ts)); _vars=vars; }

  abstract String opStr();
  abstract ASTOp make();

  @Override public String toString() {
    String s = _t._ts[0]+" "+opStr()+"(";
    int len=_vars.length;
    for( int i=1; i<len-1; i++ )
      s += _t._ts[i]+" "+_vars[i]+", ";
    return s + _t._ts[len-1]+" "+_vars[len-1]+")";
  }
  public String toString(boolean verbose) {
    if( !verbose ) return toString(); // Just the fun name& arg names
    return toString();
  }

  // Parse an OP or return null.
  static ASTOp parse(Exec2 E) {
    int x = E._x;
    String id = E.isID();
    if( id == null ) return null;
    ASTOp op = OPS.get(id);
    if( op != null ) return op.make();
    E._x = x;                 // Roll back, no parse happened
    // Attempt a user-mode function parse
    return ASTFunc.parseFcn(E);
  }

  @Override void exec(Env env) { env.push(this); }
  abstract void apply(Env env, int argcnt);
}

abstract class ASTUniOp extends ASTOp {
  static final String VARS[] = new String[]{ "", "x"};
  static Type[] newsig() {
    Type t1 = Type.dblary();
    return new Type[]{Type.anyary(new Type[]{t1}),t1};
  }
  ASTUniOp( ) { super(VARS,newsig()); }
  double op( double d ) { throw H2O.fail(); }
  protected ASTUniOp( String[] vars, Type[] types ) { super(vars,types); }
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
      }.doAll(fr.numCols(),fr).outputFrame(fr._names, fr.domains());
    env.subRef(fr,skey);
    env.pop();                  // Pop self
    env.push(fr2);
  }
}

class ASTAbs  extends ASTUniOp { String opStr(){ return "abs";   } ASTOp make() {return new ASTAbs ();} double op(double d) { return Math.abs(d);}}
class ASTSgn  extends ASTUniOp { String opStr(){ return "sgn" ;  } ASTOp make() {return new ASTSgn ();} double op(double d) { return Math.signum(d);}}
class ASTSqrt extends ASTUniOp { String opStr(){ return "sqrt";  } ASTOp make() {return new ASTSqrt();} double op(double d) { return Math.sqrt(d);}}
class ASTCeil extends ASTUniOp { String opStr(){ return "ceil";  } ASTOp make() {return new ASTCeil();} double op(double d) { return Math.ceil(d);}}
class ASTFlr  extends ASTUniOp { String opStr(){ return "floor"; } ASTOp make() {return new ASTFlr(); } double op(double d) { return Math.floor(d);}}
class ASTLog  extends ASTUniOp { String opStr(){ return "log";   } ASTOp make() {return new ASTLog ();} double op(double d) { return Math.log(d);}}
class ASTExp  extends ASTUniOp { String opStr(){ return "exp";   } ASTOp make() {return new ASTExp ();} double op(double d) { return Math.exp(d);}}
class ASTIsNA extends ASTUniOp { String opStr(){ return "is.na"; } ASTOp make() {return new ASTIsNA();} double op(double d) { return Double.isNaN(d)?1:0;}}
class ASTNot  extends ASTUniOp { String opStr(){ return "!";     } ASTOp make() {return new ASTNot(); } double op(double d) { return d==0?1:0; }}
class ASTNrow extends ASTUniOp {
  ASTNrow() { super(VARS,new Type[]{Type.DBL,Type.ARY}); }
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
class ASTNcol extends ASTUniOp {
  ASTNcol() { super(VARS,new Type[]{Type.DBL,Type.ARY}); }
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

abstract class ASTBinOp extends ASTOp {
  static final String VARS[] = new String[]{ "", "x","y"};
  static Type[] newsig() {
    Type t1 = Type.dblary(), t2 = Type.dblary();
    return new Type[]{Type.anyary(new Type[]{t1,t2}),t1,t2};
  }
  ASTBinOp( ) { super(VARS, newsig()); }
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
        fr = new Frame(fr0).add(fr1);
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
            for( int r=0; r<rlen; r++ )
              n.addNum(bin.op(lf ? c0.at0(r) : fd0, rf ? c1.at0(r) : fd1));
          }
        }
      }.doAll(ncols,fr).outputFrame(fr._names,fr.domains());
    if( fr0 != null ) env.subRef(fr0,k0);
    if( fr1 != null ) env.subRef(fr1,k1);
    env.pop();
    env.push(fr2);
  }
}
class ASTPlus extends ASTBinOp { String opStr(){ return "+"  ;} ASTOp make() {return new ASTPlus();} double op(double d0, double d1) { return d0+d1;}}
class ASTSub  extends ASTBinOp { String opStr(){ return "-"  ;} ASTOp make() {return new ASTSub ();} double op(double d0, double d1) { return d0-d1;}}
class ASTMul  extends ASTBinOp { String opStr(){ return "*"  ;} ASTOp make() {return new ASTMul ();} double op(double d0, double d1) { return d0*d1;}}
class ASTDiv  extends ASTBinOp { String opStr(){ return "/"  ;} ASTOp make() {return new ASTDiv ();} double op(double d0, double d1) { return d0/d1;}}
class ASTPow  extends ASTBinOp { String opStr(){ return "^"  ;} ASTOp make() {return new ASTPow ();} double op(double d0, double d1) { return Math.pow(d0,d1);}}
class ASTMod  extends ASTBinOp { String opStr(){ return "%"  ;} ASTOp make() {return new ASTMod ();} double op(double d0, double d1) { return d0%d1;}}
class ASTMin  extends ASTBinOp { String opStr(){ return "min";} ASTOp make() {return new ASTMin ();} double op(double d0, double d1) { return Math.min(d0,d1);}}
class ASTMax  extends ASTBinOp { String opStr(){ return "max";} ASTOp make() {return new ASTMax ();} double op(double d0, double d1) { return Math.max(d0,d1);}}
class ASTLT   extends ASTBinOp { String opStr(){ return "<"  ;} ASTOp make() {return new ASTLT  ();} double op(double d0, double d1) { return d0< d1?1:0;}}
class ASTLE   extends ASTBinOp { String opStr(){ return "<=" ;} ASTOp make() {return new ASTLE  ();} double op(double d0, double d1) { return d0<=d1?1:0;}}
class ASTGT   extends ASTBinOp { String opStr(){ return ">"  ;} ASTOp make() {return new ASTGT  ();} double op(double d0, double d1) { return d0> d1?1:0;}}
class ASTGE   extends ASTBinOp { String opStr(){ return ">=" ;} ASTOp make() {return new ASTGE  ();} double op(double d0, double d1) { return d0>=d1?1:0;}}
class ASTEQ   extends ASTBinOp { String opStr(){ return "==" ;} ASTOp make() {return new ASTEQ  ();} double op(double d0, double d1) { return d0==d1?1:0;}}
class ASTNE   extends ASTBinOp { String opStr(){ return "!=" ;} ASTOp make() {return new ASTNE  ();} double op(double d0, double d1) { return d0!=d1?1:0;}}
class ASTLA   extends ASTBinOp { String opStr(){ return "&&" ;} ASTOp make() {return new ASTLA  ();} double op(double d0, double d1) { return (d0!=0 && d1!=0)?1:0;}}
class ASTLO   extends ASTBinOp { String opStr(){ return "||" ;} ASTOp make() {return new ASTLO  ();} double op(double d0, double d1) { return (d0==0 && d1==0)?0:1;}}

class ASTReduce extends ASTOp {
  static final String VARS[] = new String[]{ "", "op2", "ary"};
  static final Type   TYPES[]= new Type  []{ Type.ARY, Type.fcn(new Type[]{Type.DBL,Type.DBL,Type.DBL}), Type.ARY };
  ASTReduce( ) { super(VARS,TYPES); }
  @Override String opStr(){ return "Reduce";}
  @Override ASTOp make() {return this;}
  @Override void apply(Env env, int argcnt) { throw H2O.unimpl(); }
}

// Variable length; instances will be created of required length
class ASTCat extends ASTOp {
  @Override String opStr() { return "c"; }
  ASTCat( ) { super(new String[]{"cat","dbls"},
                    new Type[]{Type.ARY,Type.varargs(Type.DBL)}); }
  @Override ASTOp make() {return this;}
  @Override void apply(Env env, int argcnt) {
    AppendableVec av = new AppendableVec(Vec.newKey());
    NewChunk nc = new NewChunk(av,0);
    for( int i=0; i<argcnt-1; i++ )
      nc.addNum(env.dbl(-argcnt+1+i));
    nc.close(0,null);
    Vec v = av.close(null);
    env.pop(argcnt);
    env.push(new Frame(new String[]{"c"}, new Vec[]{v}));
  }
}

class ASTRunif extends ASTOp {
  @Override String opStr() { return "runif"; }
  ASTRunif() { super(new String[]{"runif","dbls"},
      new Type[]{Type.ARY,Type.ARY}); }
  @Override ASTOp make() {return new ASTRunif();}
  @Override void apply(Env env, int argcnt) {
    Frame fr = env.popAry();
    String skey = env.key();
    long [] espc = fr.anyVec().espc();
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

class ASTTable extends ASTOp {
  ASTTable() { super(new String[]{"table", "ary"}, new Type[]{Type.ARY,Type.ARY}); }
  @Override String opStr() { return "table"; }
  @Override ASTOp make() { return new ASTTable(); }
  @Override void apply(Env env, int argcnt) {
    Frame fr = env.popAry();
    String skey = env.key();
    if (fr.vecs().length > 1)
      throw new IllegalArgumentException("table does not apply to multiple cols.");
    if (! fr.vecs()[0].isInt())
      throw new IllegalArgumentException("table only applies to integer vector.");
    int[]  domain = new Vec.CollectDomain(fr.vecs()[0]).doAll(fr).domain();
    long[] counts = new Tabularize(domain).doAll(fr)._counts;
    // Build output vecs
    Key keys[] = Vec.VectorGroup.VG_LEN1.addVecs(2);
    AppendableVec v0 = new AppendableVec(keys[0]);
    NewChunk c0 = new NewChunk(v0,0);
    for( int i=0; i<domain.length; i++ ) c0.addNum((double) domain[i]);
    c0.close(0,null);
    AppendableVec v1 = new AppendableVec(keys[1]);
    NewChunk c1 = new NewChunk(v1,0);
    for( int i=0; i<domain.length; i++ ) c1.addNum((double) counts[i]);
    c1.close(0,null);
    env.subRef(fr,skey);
    env.pop();
    env.push(new Frame(new String[]{fr._names[0],"count"}, new Vec[]{v0.close(null), v1.close(null)}));
  }
  private static class Tabularize extends MRTask2<Tabularize> {
    public final int[]  _domain;
    public long[] _counts;

    public Tabularize(int[] dom) { super(); _domain=dom; _counts=new long[dom.length];}
    @Override public void map(Chunk chk) {
      for (int i = 0; i < chk._len; i++)
        if (! chk.isNA0(i)) {
          int cls = Arrays.binarySearch(_domain,(int)chk.at80(i));
          assert 0 <= cls && cls < _domain.length;
          _counts[cls] ++;
        }
    }
    @Override public void reduce(Tabularize other) {
      for (int i = 0; i < _counts.length; i++) _counts[i] += other._counts[i];
    }
  }
}
// Variable length; instances will be created of required length
class ASTSum extends ASTOp {
  @Override String opStr() { return "sum"; }
  ASTSum( ) { super(new String[]{"sum","dbls"},
                    new Type[]{Type.DBL,Type.varargs(Type.dblary())}); }
  @Override ASTOp make() {return new ASTSum();}
  @Override void apply(Env env, int argcnt) {
    double sum=0;
    for( int i=0; i<argcnt-1; i++ )
      if( env.isDbl() ) sum += env.popDbl();
      else {
        Frame fr = env.popAry();
        String skey = env.key();
        sum += new Sum().doAll(fr)._d;
        env.subRef(fr,skey);
      }
    env.poppush(sum);
  }

  private static class Sum extends MRTask2<Sum> {
    double _d;
    @Override public void map( Chunk chks[] ) {
      for( int i=0; i<chks.length; i++ ) {
        Chunk C = chks[i];
        for( int r=0; r<C._len; r++ )
          _d += C.at0(r);
        if( Double.isNaN(_d) ) break;
      }
    }
    @Override public void reduce( Sum s ) { _d+=s._d; }
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
  ASTIfElse( ) { super(VARS, newsig()); }
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
      fr.add(frtru);
    }
    if( frfal !=null ) {          // False is a Frame?
      if( frfal.numCols() != ncols ||  frfal.numRows() != nrows )
        throw new IllegalArgumentException("Arrays must be same size: "+frtst+" vs "+frfal);
      fr.add(frfal);
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
  ASTRApply( ) { super(VARS,new Type[]{ Type.ARY, Type.ARY, Type.DBL, Type.fcn(new Type[]{Type.dblary(),Type.ARY}) }); }
  @Override String opStr(){ return "apply";}
  @Override ASTOp make() {return new ASTRApply();}
  @Override void apply(Env env, int argcnt) {
    int oldsp = env._sp;
    // Peek everything from the stack
    ASTOp op = env.fcn(-1);    // ary->dblary but better be ary[,1]->dblary[,1]
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
        env.fcn(-2).apply(env,2);
        Vec v;
        if( keys != null ) {    // Doubles or Frame results?
          // Jam the double into a Vec of its own
          AppendableVec av = new AppendableVec(keys[i]);
          NewChunk nc = new NewChunk(av,0);
          nc.addNum(env.popDbl());
          nc.close(0,null);
          env.addRef(v = av.close(null));
        } else {                      // Frame results
          if( env.ary(-1).numCols() != 1 )
            throw new IllegalArgumentException("apply requires that "+op+" return 1 column");
          v = env.popAry().anyVec();// Remove without lowering refcnt
        }
        fr2.add(fr._names[i],v); // Add, with refcnt already +1
      }
      // At this point, fr2 has refcnt++ already, and the stack is still full.
      env.pop(4);
      env.push(1);
      env._ary[env._sp-1] = fr2;
      assert env.isAry();
      assert env._sp == oldsp-4+1;
      return;
    }
    if( d==1 || d == -2 )       // Work on rows
      throw H2O.unimpl();
    throw new IllegalArgumentException("MARGIN limited to 1 (rows) or 2 (cols)");
  }
}

class ASTCut extends ASTOp {
  ASTCut() { super(new String[]{"cut", "ary", "dbls"}, new Type[]{Type.ARY, Type.ARY, Type.dblary()}); }
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
      String skey = env.key();
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
      env.subRef(fr, skey);
      env.pop();
      env.push(fr2);
    } else throw H2O.unimpl();
  }
}