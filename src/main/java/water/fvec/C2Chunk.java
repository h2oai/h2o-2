package water.fvec;

import water.*;

// The empty-compression function, where data is in shorts
public class C2Chunk extends Chunk {
  static protected final long _NA = Short.MIN_VALUE;
  static final int OFF=0;
  C2Chunk( byte[] bs ) { _mem=bs; _start = -1; _len = _mem.length>>1; }
  @Override public long   get ( int    i ) {
    int res = UDP.get2(_mem,(i<<1)+OFF);
    return res == _NA?_vec._iNA:res;
  }
  @Override public double getd( int    i ) {
    int res = UDP.get2(_mem,(i<<1)+OFF);
    return res == _NA?_vec._fNA:res;
  }
  @Override void   append2 ( long l, int exp ) { throw H2O.fail(); }
  @Override public AutoBuffer write(AutoBuffer bb) { return bb.putA1(_mem,_mem.length); }
  @Override public C2Chunk read(AutoBuffer bb) {
    _mem = bb.bufClose();
    _start = -1;
    _len = _mem.length>>1;
    assert _mem.length == _len<<1;
    return this;
  }
}
