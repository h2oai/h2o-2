package water.fvec;

import water.*;

// The empty-compression function, where data is in shorts
public class C2Vector extends BigVector {
  static final int OFF=0;
  C2Vector( byte[] bs ) { _mem=bs; _start = -1; _len = _mem.length>>1; }
  @Override long   at_impl ( int    i ) { return UDP.get2(_mem,(i<<1)+OFF); }
  @Override double atd_impl( int    i ) { return UDP.get2(_mem,(i<<1)+OFF); }
  @Override void   append2 ( long l, int exp ) { throw H2O.fail(); }
  @Override public AutoBuffer write(AutoBuffer bb) { return bb.putA1(_mem,_mem.length); }
  @Override public C2Vector read(AutoBuffer bb) { 
    _mem = bb.bufClose(); 
    _start = -1;
    _len = _mem.length>>1;
    assert _mem.length == _len<<1;
    return this; 
  }
}
