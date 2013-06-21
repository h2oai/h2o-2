package water.fvec;

import water.*;

// The empty-compression function, where data is in 'long's.
public class C8Chunk extends Chunk {
  protected static final long _NA = Long.MIN_VALUE;
  C8Chunk( byte[] bs ) { _mem=bs; _start = -1; _len = _mem.length>>3; }
  @Override public long   get ( int i ) {
    long res = UDP.get8(_mem,i<<3);
    return  res == _NA?_vec._iNA:res;
  }
  @Override public double getd( int    i ) {
    long res = UDP.get8(_mem,i<<3);
    return res == _NA?_vec._fNA:res;
  }
  @Override void   append2 ( long l, int exp ) { throw H2O.fail(); }
  @Override public AutoBuffer write(AutoBuffer bb) { return bb.putA1(_mem,_mem.length); }
  @Override public C8Chunk read(AutoBuffer bb) {
    _mem = bb.bufClose();
    _start = -1;
    _len = _mem.length>>3;
    assert _mem.length == _len<<3;
    return this;
  }
}
