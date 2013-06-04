package water.fvec;

import water.*;

// The empty-compression function, where data is in 'long's.
public class C8Vector extends BigVector {
  @Override long   at_impl ( int    i ) { return UDP.get8 (_mem,i<<3); }
  @Override double atd_impl( int    i ) { return UDP.get8d(_mem,i<<3); }
  @Override void   append2 ( long   l ) { throw H2O.fail(); }
  @Override void   append2 ( double d ) { throw H2O.fail(); }
  @Override public AutoBuffer write(AutoBuffer bb) { throw H2O.fail(); }
  @Override public C8Vector read(AutoBuffer bb) { 
    _mem = bb.bufClose();
    _start = -1;
    _len = _mem.length>>3;
    assert _mem.length == _len<<3;
    return this; 
  }
}
