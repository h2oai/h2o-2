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
  AST    _fun[];                // Functions (or null if not a function)
  int    _sp;                   // Stack pointer

  Env( ) { _fr=new Frame[2]; _d=new double[2]; _fun=new AST[2]; }

  // Push k empty slots
  void push( int slots ) {
    assert 0 <= slots && slots < 1000;
    int len = _d.length;
    _sp += slots;
    while( _sp > len ) {
      _fr = Arrays.copyOf(_fr ,len<<1);
      _d  = Arrays.copyOf(_d  ,len<<1);
      _fun= Arrays.copyOf(_fun,len<<1);
    }
  }
  // Pop a slot
  void pop( ) {
    _fun=null;
    Frame fr = _fr[_sp];
    if( fr != null ) { 
      System.out.println("missing lifetime checking");
      _fr[_sp] = null;
    }
    _sp--;
  }

  void push( Frame fr ) { push(1); _fr [_sp-1] = fr ; }
  void push( double d ) { push(1); _d  [_sp-1] = d  ; }
  void push( AST fun  ) { push(1); _fun[_sp-1] = fun; }

  // Remove all embedded frames.  Pop the stack.
  void remove() {
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
