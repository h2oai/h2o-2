package water.fvec;

import water.*;

// The empty-compression function, where data is in UNSIGNED bytes
public class C1NChunk extends Chunk {
  static final int OFF=0;
  C1NChunk(byte[] bs) { _mem=bs; _start = -1; _len = _mem.length; }
  @Override protected final long   at8_impl( int i ) { return 0xFF&_mem[i+OFF]; }
  @Override protected final double atd_impl( int i ) { return 0xFF&_mem[i+OFF]; }
  @Override boolean hasFloat() { return false; }
  @Override public AutoBuffer write(AutoBuffer bb) { return bb.putA1(_mem,_mem.length); }
  @Override public C1NChunk read(AutoBuffer bb) {
    _mem = bb.bufClose();
    _start = -1;
    _len = _mem.length;
    return this;
  }
  public int get2(int off) { return UDP.get2(_mem,off+OFF); }
  public int get4(int off) { return UDP.get4(_mem,off+OFF); }
  @Override boolean set8_impl(int i, long l) {
    if( !(0 <= l && l < 256) ) return false;
    _mem[i+OFF] = (byte)l;
    return true; 
  }
  @Override boolean set8_impl(int i, double d) { return false; }
  @Override NewChunk inflate_impl(NewChunk nc) {
    for( int i=0; i<_mem.length; i++ )
      nc._ls[i] = at8_impl(i);
    return nc;
  }
}
