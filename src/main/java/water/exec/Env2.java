package water.exec;

import water.Iced;

/**
 * R-like environment used to support row based computation.
 * @author bai@0xdata.com
 */
public class Env2 extends Iced {
// An environment maintains the current execution state along with all its transitive enclosures.
// Function and its environment form a closure when the function is created. For sake of thread safety,
// the environment should be copied on write before the function is to be invoked on a new thread.
// For now COW has to be done manually.
  Env2     _encl;    // the enclosing environment
  // names and values in the current scope, following the order of return variable, formal parameters and local variables.
  String   _sym[];   // symbols in this scope,
  double   _d  [];   // double value; null if array or function.
  double   _ary[][]; // array; null if double or function.
  ASTOp    _fcn[];   // function; closure if user defined.
  // Following place holders for computing only
  transient double _d0;
  transient double _ary0[];
  transient ASTOp  _fcn0;

  private Env2() {}

  public Env2( Env2 encl, ASTFunc fcn ) {
    int nsym = fcn._locals.length;
    _encl = encl;
    _sym  = fcn._locals;
    _d    = new double[nsym];
    _ary  = new double[nsym][];
    _fcn  = new ASTOp [nsym];
  }

  public Env2 copy() {
    Env2 copy = new Env2();
    copy._encl = _encl==null?null:_encl.copy();
    copy._sym  = _sym;
    copy._d    = _d.clone();
    copy._ary  = _ary.clone();
    copy._fcn  = _fcn.clone();
    for (int i = 0; i < _ary.length; i++) if (_ary[i] != null) copy._ary[i] = _ary[i].clone();
    return copy;
  }

  public static Env2 makeDummy() {
    Env2 dummy = new Env2();
    dummy._encl = null;
    dummy._sym  = new String[0];
    dummy._d    = new double[0];
    dummy._ary  = new double[0][];
    dummy._fcn  = new ASTOp [0];
    return dummy;
  }

  private int findSym( String sym ) {
    for (int i = 0; i < _sym.length; i++) if (sym.equals(_sym[i])) return i;
    return -1;
  }

  private int symIdx( String sym ) {
    int ix = findSym(sym);
    if (ix < 0)
      throw new IllegalArgumentException("Symbol does not exist in this scope.");
    return ix;
  }

  private boolean fetch(String id, Env2 env) {
    int ix = findSym(id);
    if (ix >= 0) { copyover(ix,env); return true; }
    return _encl!=null && _encl.fetch(id, env);
  }

  private void copyover(int i, Env2 env) {
    env._d0   = _d  [i];
    env._ary0 = _ary[i];
    env._fcn0 = _fcn[i];
  }
  public void     setDbl(int i, double   d  ) { _d  [i] = d; _ary[i] = null; _fcn[i] = null; }
  public void     setAry(int i, double[] ary) { _d  [i] = 0; _ary[i] = ary;  _fcn[i] = null; }
  public void     setFcn(int i, ASTOp    fcn) { _d  [i] = 0; _ary[i] = null; _fcn[i] = fcn;  }
  public void     setDbl0(double   d  ) { _d0 = d; _ary0 = null; _fcn0 = null; }
  public void     setAry0(double[] ary) { _d0 = 0; _ary0 = ary;  _fcn0 = null; }
  public void     setFcn0(ASTOp    fcn) { _d0 = 0; _ary0 = null; _fcn0 = fcn;  }
  public double[] getAry0() { return _ary0; }
  public ASTOp    getFcn0() { return _fcn0; }
  public double   getDbl0() { if (_ary0!=null || _fcn0!=null ) return Double.NaN; else return _d0; }
  public boolean  isAry () { return _fcn0 == null && _ary0 != null; }
  public boolean  isFcn () { return _fcn0 != null; }
  public boolean  isDbl () { return _fcn0 == null && _ary0 == null; }
  public void     asnAry( String id, double ary[] ) { setAry(symIdx(id), ary); }
  public void     asnFcn( String id, ASTOp  fcn   ) { setFcn(symIdx(id), fcn); }
  public void     asnDbl( String id, double d     ) { setDbl(symIdx(id), d  ); }
  public void     fetch ( String id ) { fetch(id, this); }
}
