package water.fvec;

import water.AutoBuffer;
import water.MemoryManager;
import water.UDP;

/**
 * The empty-compression function, where data is in 'double's.
 */
public class C8DChunk extends Chunk {
  public C8DChunk (double [] vals){
    _start = -1;
    _mem = MemoryManager.malloc1(vals.length << 3);
    _len = vals.length;
    for(int i = 0, off = 0; i < vals.length; ++i, off += 8)
      UDP.set8d(_mem,off,vals[i]);
  }
  C8DChunk( byte[] bs ) { _mem=bs; _start = -1; _len = _mem.length>>3; }
  @Override protected final long   at8_impl( int i ) {
    double res = UDP.get8d(_mem,i<<3);
    if( Double.isNaN(res) ) throw new IllegalArgumentException("at8 but value is missing");
    return (long)res;
  }
  @Override protected final double   atd_impl( int i ) { return              UDP.get8d(_mem,i<<3) ; }
  @Override protected final boolean isNA_impl( int i ) { return Double.isNaN(UDP.get8d(_mem,i<<3)); }
  @Override boolean set_impl(int idx, long l) { return false; }
  @Override boolean set_impl(int i, double d) {
    UDP.set8d(_mem,i<<3,d);
    return true;
  }
  @Override boolean set_impl(int i, float f ) {
    UDP.set8d(_mem,i<<3,f);
    return true;
  }
  @Override boolean setNA_impl(int idx) { UDP.set8d(_mem,(idx<<3),Double.NaN); return true; }
  @Override boolean hasFloat() { return true; }
  @Override public AutoBuffer write(AutoBuffer bb) {return bb.putA1(_mem,_mem.length);}
  @Override public C8DChunk read(AutoBuffer bb) {
    _mem = bb.bufClose();
    _start = -1;
    _len = _mem.length>>3;
    assert _mem.length == _len<<3;
    return this;
  }
  @Override NewChunk inflate_impl(NewChunk nc) {
    nc.alloc_doubles(len());
    for( int i=0; i< len(); i++ )
      nc.doubles()[i] = UDP.get8d(_mem,(i<<3));
    nc.set_len(nc.set_sparseLen(len()));
    return nc;
  }
  // 3.3333333e33
  public int pformat_len0() { return 22; }
  public String pformat0() { return "% 21.15e"; }
}
