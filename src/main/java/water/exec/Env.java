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
  Frame  _fr [] = new Frame [4]; // Frame (or null if not a frame)
  double _d  [] = new double[4]; // Double (only if frame & func are null)
  ASTOp  _fun[] = new ASTOp [4]; // Functions (or null if not a function)
  int    _sp;                    // Stack pointer

  // Also a Pascal-style display, one display entry per lexical scope.  Slot
  // zero is the start of the global scope (which contains all global vars like
  // hex Keys) and always starts at offset 0.
  int _display[] = new int[4];
  int _tod;

  // Ref Counts for each vector
  HashMap<Vec,Integer> _refcnt = new HashMap<Vec,Integer>();

  boolean _allow_tmp;           // Deep-copy allowed to tmp
  boolean _busy_tmp;            // Assert temp is available for use
  Frame  _tmp;                  // The One Big Active Tmp

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
  void push( Frame fr ) { push(1); _fr [_sp-1] = addRef(fr) ; }
  void push( double d ) { push(1); _d  [_sp-1] = d  ; }
  void push( ASTOp fun) { push(1); _fun[_sp-1] = fun; }

  // Copy from display offset d, nth slot
  void push_slot( int d, int n ) {
    int idx = _display[_tod-d]+n;
    push(1);
    _fr [_sp-1] = addRef(_fr[idx]);
    _d  [_sp-1] = _d  [idx];
    _fun[_sp-1] = _fun[idx];
  }

  // Pop a slot.  Lowers refcnts on vectors.  Always leaves stack null behind
  // (to avoid dangling pointers stretching lifetimes).
  void pop( ) {
    assert _sp > _display[_tod]; // Do not over-pop current scope
    _fun[--_sp]=null;
    _fr [  _sp]=subRef(_fr[_sp]);
  }

  void popScope() {
    assert _tod > 0;            // Something to pop?
    assert _sp >= _display[_tod]; // Did not over-pop already?
    while( _sp >= _display[_tod] ) pop();
    _tod--;
  }

  public double popDbl  () { assert isDbl(); pop(); return _d  [_sp]; }
  public AST    popFun  () { assert isFun(); pop(); return _fun[_sp]; }
  // Pop & return a Frame; ref-cnt of all things remains unchanged.  
  // Caller is responsible for tracking lifetime.
  public Frame  popFrame() { assert isFrame(); Frame fr = _fr[--_sp]; _fr[_sp]=null; assert allAlive(fr); return fr; }
  // Replace a function invocation with it's result
  public void poppush(double d) {
    assert isFun();
    _fun[_sp-1] = null;
    _d  [_sp-1] = d;
    assert isDbl();
  }

  // Nice assert
  boolean allAlive(Frame fr) {
    for( Vec vec : fr.vecs() ) 
      assert _refcnt.get(vec) > 0;
    return true;
  }

  // Lower the refcnt on all vecs in this frame.
  // Immediately free all vecs with zero count.
  // Always return a null.
  Frame subRef( Frame fr ) {
    if( fr == null ) return null;
    Futures fs = null;
    for( Vec vec : fr.vecs() ) {
      int cnt = _refcnt.get(vec)-1;
      if( cnt > 0 ) _refcnt.put(vec,cnt);
      else {
        if( fs == null ) fs = new Futures();
        UKV.remove(vec._key,fs);
        _refcnt.remove(vec);
      }
    }
    if( fs != null )
      fs.blockForPending();
    return null;
  }

  // Add a refcnt to all vecs in this frame
  Frame addRef( Frame fr ) {
    if( fr == null ) return null;
    for( Vec vec : fr.vecs() ) {
      Integer I = _refcnt.get(vec);
      assert I==null || I>0;
      _refcnt.put(vec,I==null?1:I+1);
    }
    return fr;
  }

  // Remove all embedded frames, but not things in the global scope.
  public void remove() {  while( _tod > 0 ) popScope();  }

  // Pop and return the result as a string
  public String resultString( ) {
    assert _tod==0 : "Still have lexical scopes past the global";
    String s = toString(_sp-1);
    pop();
    return s;
  }

  public String toString(int i) {
    if( _fr[i] != null ) return _fr[i].numRows()+"x"+_fr[i].numCols();
    else if( _fun[i] != null ) return _fun[i].toString();
    return Double.toString(_d[i]);
  }
  @Override public String toString() {
    String s="{";
    for( int i=0; i<_sp; i++ )   s += toString(i)+",";
    return s+"}";
  }
}
