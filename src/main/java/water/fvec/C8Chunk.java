package water.fvec;

import water.*;

// The empty-compression function, where data is in 'long's.
public class C8Chunk extends Chunk {
  protected static final long _NA = Long.MIN_VALUE;
  C8Chunk( byte[] bs ) { _mem=bs; _start = -1; _len = _mem.length>>3; }
  @Override protected final long at8_impl( int i ) {
    long res = UDP.get8(_mem,i<<3);
    return  res == _NA?_vec._iNA:res;
  }
  @Override protected final double atd_impl( int i ) {
    long res = UDP.get8(_mem,i<<3);
    return res == _NA?_vec._fNA:res;
  }
  @Override boolean set8_impl(int idx, long l) { return false; }
  @Override boolean set8_impl(int i, double d) { return false; }
  @Override boolean set4_impl(int i, float f ) { return false; }
  @Override boolean hasFloat() { return false; }
  @Override public AutoBuffer write(AutoBuffer bb) { return bb.putA1(_mem,_mem.length); }
  @Override public C8Chunk read(AutoBuffer bb) {
    _mem = bb.bufClose();
    _start = -1;
    _len = _mem.length>>3;
    assert _mem.length == _len<<3;
    return this;
  }
  @Override NewChunk inflate_impl(NewChunk nc) { throw H2O.fail(); }
}
