package water.fvec;

import water.*;

// The empty-compression function, where data is in UNSIGNED bytes
public class C1Vector extends BigVector {
  static final int OFF=0;
  C1Vector( byte[] bs ) { _mem=bs; _start = -1; _len = _mem.length; }
  @Override long   at_impl ( int    i ) { return 0xFF&_mem[i+OFF]; }
  @Override double atd_impl( int    i ) { return 0xFF&_mem[i+OFF]; }
  @Override void   append2 ( long l, int exp ) { throw H2O.fail(); }
  @Override public AutoBuffer write(AutoBuffer bb) { return bb.putA1(_mem,_mem.length); }
  @Override public C1Vector read(AutoBuffer bb) { 
    _mem = bb.bufClose(); 
    _start = -1;
    _len = _mem.length;
    return this; 
  }
  public int get2(int off) { return UDP.get2(_mem,off+OFF); }
  public int get4(int off) { return UDP.get4(_mem,off+OFF); }
}
