package water.fvec;

import water.*;

/**
 * The empty-compression function, if all elements fit directly on UNSIGNED bytes.
 * Cannot store 0xFF, the value is a marker for N/A.
 */
public class C1Chunk extends Chunk {
  static final int OFF=0;
  static protected final long _NA = 0xFF;
  C1Chunk(byte[] bs) { _mem=bs; _start = -1; _len = _mem.length; }
  @Override protected final long at8_impl( int i ) {
    long res = 0xFF&_mem[i+OFF];
    if( res == _NA ) throw new IllegalArgumentException("at8 but value is missing");
    return res;
  }
  @Override protected final double atd_impl( int i ) {
    long res = 0xFF&_mem[i+OFF];
    return (res == _NA)?Double.NaN:res;
  }
  @Override protected final boolean isNA_impl( int i ) { return (0xFF&_mem[i+OFF]) == _NA; }
  @Override boolean set_impl(int i, long l) {
    if( !(0 <= l && l < 255) ) return false;
    _mem[i+OFF] = (byte)l;
    return true;
  }
  @Override boolean set_impl(int i, double d) { return false; }
  @Override boolean set_impl(int i, float f ) { return false; }
  @Override boolean setNA_impl(int idx) { _mem[idx+OFF] = (byte)_NA; return true; }
  @Override boolean hasFloat() { return false; }
  @Override public AutoBuffer write(AutoBuffer bb) { return bb.putA1(_mem,_mem.length); }
  @Override public C1Chunk read(AutoBuffer bb) {
    _mem = bb.bufClose();
    _start = -1;
    _len = _mem.length;
    return this;
  }
  @Override NewChunk inflate_impl(NewChunk nc) {
    nc.set_len(nc.set_sparseLen(0));
    final int len = len();
    for( int i=0; i<len; i++ ) {
      int res = 0xFF&_mem[i+OFF];
      if( res == _NA ) nc.addNA();
      else             nc.addNum(res,0);
    }
    return nc;
  }
}
