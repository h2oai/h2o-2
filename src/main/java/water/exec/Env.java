package water.exec;

import java.text.*;
import java.util.Arrays;
import java.util.HashMap;
import water.*;
import water.fvec.*;

/** Execute a R-like AST, in the context of an H2O Cloud
 *  @author cliffc@0xdata.com
 */
public class Env {
  // An environment is a classic stack of values, passed into AST's as the
  // execution state.  The 3 types we support are Frames (2-d tables of data),
  // doubles (which are an optimized form of a 1x1 Frame), and ASTs (which are
  // 1st class functions).
  Frame  _fr [];                // Frame (or null if not a frame)
  double _d  [];                // Double (only if frame & func are null)
  ASTOp  _fun[];                // Functions (or null if not a function)
  int    _sp;                   // Stack pointer

  // Also a Pascal-style display, one display entry per lexical scope.  Slot
  // zero is the start of the global scope (which contains all global vars like
  // hex Keys) and always starts at offset 0.
  int _display[];
  int _tod;

  boolean _allow_tmp;           // Deep-copy allowed to tmp
  boolean _busy_tmp;            // Assert temp is available for use
  Frame  _tmp;                  // The One Big Active Tmp

  Env( ) { _fr=new Frame[2]; _d=new double[2]; _fun=new ASTOp[2]; _display=new int[4]; }
  public int sp() { return _sp; }
  public boolean isFrame() { return _fr [_sp-1] != null; }
  public boolean isFun  () { return _fun[_sp-1] != null; }
  public boolean isDbl  () { return !isFrame() && !isFun(); }
  public boolean isFun  (int i) { return _fun[_sp+i] != null; }
  public ASTOp fun(int i) { ASTOp op = _fun[_sp+i]; assert op != null; return op; }

  // Push k empty slots
  void push( int slots ) {
    assert 0 <= slots && slots < 1000;
    int len = _d.length;
    _sp += slots;
    while( _sp > len ) {
      _fr = Arrays.copyOf(_fr ,len<<1);
      _d  = Arrays.copyOf(_d  ,len<<1);
      _fun= Arrays.copyOf(_fun,len<<=1);
    }
  }
  void push( Frame fr ) { push(1); _fr [_sp-1] = fr ; }
  void push( double d ) { push(1); _d  [_sp-1] = d  ; }
  void push( ASTOp fun) { push(1); _fun[_sp-1] = fun; }

  // Copy from display offset d, nth slot
  void push_slot( int d, int n ) {
    int idx = _display[_tod-d]+n;
    push(1);
    _fr [_sp-1] = _fr [idx];
    _d  [_sp-1] = _d  [idx];
    _fun[_sp-1] = _fun[idx];
  }

  // Pop a slot
  void pop( ) {
    _fun[--_sp]=null;
    removeRef(_fr[_sp]);
    _fr[_sp] = null;
  }
  static void removeRef( Frame fr ) {
    if( fr != null ) {
      Futures fs = new Futures();
      Vec[] vecs = fr.vecs();
      for( Vec vec : vecs ) 
        if( vec instanceof TmpVec )
          UKV.remove(vec._key,fs);
      fs.blockForPending();
    }
  }

  // Return a Frame; ref-cnt of all things remains unchanged.  Caller is
  // responsible for tracking lifetime.
  public Frame  popFrame() { Frame fr = _fr[--_sp];  _fr[_sp] = null; assert allAlive(fr); return fr; }
  public double popDbl  () { assert _fr[_sp-1]==null && _fun[_sp-1] == null; pop(); return _d  [_sp]; }
  public AST    popFun  () { assert _fr[_sp-1]==null && _fun[_sp-1] != null; pop(); return _fun[_sp]; }
  public void poppush(double d) {
    assert isFun();
    _fun[_sp-1] = null;
    _d  [_sp-1] = d;
    assert isDbl();
  }

  // Nice assert
  boolean allAlive(Frame fr) {
    for( Vec vec : fr.vecs() ) 
      if( vec instanceof TmpVec && ((TmpVec)vec)._refcnt <= 0 ) 
        return false;
    return true;
  }

  // Get The Temp; must be compatible with 'x'
  Frame tmp(Frame x) {
    if( _tmp == null ) {
      assert _allow_tmp;
      _allow_tmp = false;
      _tmp = TmpVec.deepAllocTmp(x);
    }
    if( _tmp.numCols() < x.numCols() ||
        _tmp.numRows() < x.numRows() )
      throw new IllegalArgumentException("Working storage: "+_tmp+" too small for "+x);
    assert !_busy_tmp;          // tmp not currently in use
    _busy_tmp = true;
    return _tmp;
  }

  // Remove all embedded frames.  Pop the stack.
  public void remove() {
    while( _sp > 0 ) pop();
    removeRef(_tmp);  _tmp=null;
  }

  public String result( ) {
    assert _tod==0 : "Still have lexical scopes past the global";
    return toString(_sp-1);
  }

  public String toString(int i) {
    if( _fr[i] != null ) return _fr[i].numCols()+"x"+_fr[i].numRows();
    else if( _fun[i] != null ) return _fun[i].toString();
    return Double.toString(_d[i]);
  }
  @Override public String toString() {
    String s="{";
    for( int i=0; i<_sp; i++ ) {
      if( _fr[i] != null ) s += _fr[i].numCols()+"x"+_fr[i].numRows();
      else if( _fun[i] != null ) s += _fun[i];
      else s += _d[i];
      s += ',';
    }
    return s+"}";
  }
}
