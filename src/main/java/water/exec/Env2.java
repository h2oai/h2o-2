package water.exec;

import org.omg.DynamicAny._DynAnyFactoryStub;
import water.Iced;
import water.util.Utils.IcedHashMap;

import java.util.HashMap;

/**
 * R-like environment used to support row based computation.
 * @author bai@0xdata.com
 */
public class Env2 extends Iced {
// An environment maintains the current execution state. Function along with
// the environment when it's created form a closure.
  Env2     _encl;    // the enclosing environment
  // names and values in the current scope, following the order of return variable, formal parameters and local variables.
  String   _sym[];   // symbols in this scope,
  double   _d  [];   // double value; null if array or function.
  double   _ary[][]; // array; null if double or function.
  ASTOp    _fcn[];   // function; closure if user defined.

  public Env2( Env2 encl, ASTFunc fcn ) {
    int nsym = fcn._vars.length+1;
    _encl = encl;
    _sym  = fcn._vars;
    _d    = new double[nsym];
    _ary  = new double[nsym][];
    _fcn  = new ASTOp [nsym];
  }

  private int findSym( String sym ) {
    for (int i = 1; i < _sym.length; i++) if (sym.equals(_sym[i])) return i;
    return -1;
  }

  private boolean fetch(String id, Env2 env) {
    int ix = findSym(id);
    if (ix > 0) { copyover(ix,env); return true; }
    return _encl!=null && _encl.fetch(id, env);
  }

  private void copyover(int i, Env2 env) {
    env._d  [0] = _d  [i];
    env._ary[0] = _ary[i];
    env._fcn[0] = _fcn[i];
  }
  public void setDbl(int i, double d) {
    _d  [i] = d;
    _ary[i] = null;
    _fcn[i] = null;
  }
  public void setAry(int i, double[] ary) {
    _d  [i] = 0;
    _ary[i] = ary;
    _fcn[i] = null;
  }
  public void setFcn(int i, ASTOp fcn) {
    _d  [i] = 0;
    _ary[i] = null;
    _fcn[i] = fcn;
  }
  public double[] retAry() { return _ary[0]; }
  public ASTOp    retFcn() { return _fcn[0]; }
  public double   retDbl() { if (_ary[0]!=null || _fcn[0]!=null ) return Double.NaN; else return _d[0]; }

  public void     asnAry( String id, double ary[] ) { setAry(findSym(id), ary); }
  public void     asnFcn( String id, ASTOp  fcn   ) { setFcn(findSym(id), fcn); }
  public void     asnDbl( String id, double d     ) { setDbl(findSym(id), d  ); }

  public void     fetch ( String id ) { fetch(id, this); }
}
