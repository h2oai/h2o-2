package water.fvec;

import water.*;

/**
 * The empty-compression function, if all elements fit directly on UNSIGNED bytes.
 * [In particular, this is the compression style for data read in from files.]
 */
public class C1NChunk extends Chunk {
  static final int OFF=0;
  C1NChunk(byte[] bs) { _mem=bs; _start = -1; _len = _mem.length; }
  @Override protected final long   at8_impl( int i ) { return 0xFF&_mem[i]; }
  @Override protected final double atd_impl( int i ) { return 0xFF&_mem[i]; }
  @Override protected final boolean isNA_impl( int i ) { return false; }
  @Override boolean set_impl(int i, long l  ) { return false; }
  @Override boolean set_impl(int i, double d) { return false; }
  @Override boolean set_impl(int i, float f ) { return false; }
  @Override boolean setNA_impl(int idx) { return false; }
  @Override boolean hasFloat() { return false; }
  @Override public AutoBuffer write(AutoBuffer bb) { return bb.putA1(_mem,_mem.length); }
  @Override public C1NChunk read(AutoBuffer bb) {
    _mem = bb.bufClose();
    _start = -1;
    _len = _mem.length;
    return this;
  }
  @Override NewChunk inflate_impl(NewChunk nc) {
    nc.alloc_exponent(len());
    nc.alloc_mantissa(len());
    for( int i=0; i< len(); i++ )
      nc.mantissa()[i] = 0xFF&_mem[i+OFF];
    nc.set_len(nc.set_sparseLen(len()));
    return nc;
  }
}
