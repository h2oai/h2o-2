package water.fvec;

import water.*;

/**
 * The empty-compression function, where data is in 'float's.
 */
public class C4FChunk extends Chunk {
  C4FChunk( byte[] bs ) { _mem=bs; _start = -1; _len = _mem.length>>2; }
  @Override protected final long at8_impl( int i ) {
    float res = UDP.get4f(_mem,i<<2);
    if( Float.isNaN(res) ) throw new IllegalArgumentException("at8 but value is missing");
    return (long)res;
  }
  @Override protected final double atd_impl( int i ) {
    float res = UDP.get4f(_mem,i<<2);
    return Float.isNaN(res)?Double.NaN:res;
  }
  @Override protected final boolean isNA_impl( int i ) { return Float.isNaN(UDP.get4f(_mem,i<<2)); }
  @Override boolean set_impl(int idx, long l) { return false; }
  @Override boolean set_impl(int i, double d) { return false; }
  @Override boolean set_impl(int i, float f ) {
    UDP.set4f(_mem,i<<2,f);
    return true;
  }
  @Override boolean setNA_impl(int idx) { UDP.set4f(_mem,(idx<<2),Float.NaN); return true; }
  @Override boolean hasFloat() { return true; }
  @Override public AutoBuffer write(AutoBuffer bb) { return bb.putA1(_mem,_mem.length); }
  @Override public C4FChunk read(AutoBuffer bb) {
    _mem = bb.bufClose();
    _start = -1;
    _len = _mem.length>>2;
    assert _mem.length == _len<<2;
    return this;
  }
  @Override NewChunk inflate_impl(NewChunk nc) {
    nc.set_len(nc.set_sparseLen(0));
    final int len = len();
    for( int i=0; i<len; i++ ) {
      float res = UDP.get4f(_mem,(i<<2));
      if( Float.isNaN(res) ) nc.addNum(Double.NaN);
      else nc.addNum(res);
    }
    return nc;
  }
  // 3.3333333e33
  public int pformat_len0() { return 14; }
  public String pformat0() { return "% 13.7e"; }
}
