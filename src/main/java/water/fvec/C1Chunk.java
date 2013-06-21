package water.fvec;

import water.*;

// The empty-compression function, where data is in UNSIGNED bytes
public class C1Chunk extends Chunk {
  static final int OFF=0;
  static protected final long _NA = 0xFF;
  C1Chunk(byte[] bs) { _mem=bs; _start = -1; _len = _mem.length; }
  @Override public long   get ( int    i ) {
    long res = 0xFF&_mem[i+OFF];
    assert (res == _NA) || !_vec.isNA(res);
    return (res == _NA)?_vec._iNA:res;
  }
  @Override public double getd( int    i ) {
    long res = 0xFF&_mem[i+OFF];
    assert (res == _NA) || !_vec.isNA((double)res);
    return (res == _NA)?_vec._fNA:res;
  }
  @Override void   append2 ( long l, int exp ) { throw H2O.fail(); }
  @Override public AutoBuffer write(AutoBuffer bb) { return bb.putA1(_mem,_mem.length); }
  @Override public C1Chunk read(AutoBuffer bb) {
    _mem = bb.bufClose();
    _start = -1;
    _len = _mem.length;
    return this;
  }
  public int get2(int off) { return UDP.get2(_mem,off+OFF); }
  public int get4(int off) { return UDP.get4(_mem,off+OFF); }
}
