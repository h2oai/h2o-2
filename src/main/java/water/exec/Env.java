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

  Env( ) { _fr=new Frame[2]; _d=new double[2]; _fun=new ASTOp[2]; }
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

  // Pop a slot
  void pop( ) {
    _fun[--_sp]=null;
    Frame fr = _fr[_sp];
    if( fr != null ) {
      Vec[] vecs = fr.vecs();
      for( Vec vec : vecs ) 
        if( vec instanceof TmpVec )
          throw H2O.unimpl();
      _fr[_sp] = null;
    }
  }

  public Frame  popFrame() { Frame fr = _fr[--_sp];  assert fr != null;  pop(); return fr; }
  public double popDbl  () { assert _fr[_sp-1]==null && _fun[_sp-1] == null; pop(); return _d[_sp]; }
  public AST    popFun  () { assert _fr[_sp-1]==null && _fun[_sp-1] != null; pop(); return _fun[_sp]; }
  public void poppush(double d) {
    assert isFun();
    _fun[_sp-1] = null;
    _d  [_sp-1] = d;
    assert isDbl();
  }

  // Remove all embedded frames.  Pop the stack.
  public void remove() {
    while( _sp > 0 ) pop();
  }

  @Override public String toString() {
    String s="{";
    for( int i=0; i<_sp; i++ ) {
      if( _fr[i] != null ) s += AST.dimStr(_fr[i].numCols(),_fr[i].numRows());
      else if( _fun[i] != null ) s += _fun[i];
      else s += _d[i];
      s += ',';
    }
    return s+"}";
  }
}
