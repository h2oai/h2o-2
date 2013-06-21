package water.fvec;

import water.*;

// The empty-compression function, where data is in UNSIGNED bytes
public class C1Chunk extends Chunk {
  static final int OFF=0;
  C1Chunk( byte[] bs ) { super(0xFF); _mem=bs; _start = -1; _len = _mem.length; }
  @Override public long   at8_impl( int    i ) { return 0xFF&_mem[i+OFF]; }
  @Override public double atd_impl( int    i ) {
    int res = 0xFF&_mem[i+OFF];
    return (res == NA())?Double.NaN:res;
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
