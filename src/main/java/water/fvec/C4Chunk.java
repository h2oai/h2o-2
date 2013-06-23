package water.fvec;

import water.*;

// The empty-compression function, where data is in 'int's.
public class C4Chunk extends Chunk {
  static protected final long _NA = Integer.MIN_VALUE;
  C4Chunk( byte[] bs ) { _mem=bs; _start = -1; _len = _mem.length>>2; }
  @Override protected final long at8_impl( int i ) {
    long res = UDP.get4(_mem,i<<2);
    return res == _NA?_vec._iNA:res;
  }
  @Override protected final double atd_impl( int i ) {
    long res = UDP.get4(_mem,i<<2);
    return res == _NA?_vec._fNA:res;
  }
  @Override void   append2 ( long l, int exp ) { throw H2O.fail(); }
  @Override boolean hasFloat() { return false; }
  @Override public AutoBuffer write(AutoBuffer bb) { return bb.putA1(_mem,_mem.length); }
  @Override public C4Chunk read(AutoBuffer bb) {
    _mem = bb.bufClose();
    _start = -1;
    _len = _mem.length>>2;
    assert _mem.length == _len<<2;
    return this;
  }
}
