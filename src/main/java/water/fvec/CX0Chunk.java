package water.fvec;

import water.*;

/** SPARSE boolean; no NAs.  A list of rows that are non-zero. */
public class CX0Chunk extends Chunk {
  static final int OFF = 4;
  transient int _last;          // 1-entry cache of last accessed row
  // Dense constructor
  public CX0Chunk(long[] ls, int len2, int nzcnt) {
    _start = -1; _len = len2; _last=OFF;
    byte[] buf = new byte[nzcnt*2+OFF];
    UDP.set4(buf,0,len2);
    int j = OFF;
    if( len2 > 65535 ) throw H2O.unimpl();
    for( int i=0; i<len2; i++ )
      if( ls[i] != 0 ) j += UDP.set2(buf,j,(short)i);
    _mem = buf;
  }
  // Sparse constructor
  public CX0Chunk(int[] xs, int len2, int len) {
    _start = -1; _len = len2; _last=OFF;
    byte[] buf = new byte[len*2+OFF];
    UDP.set4(buf,0,len2);
    if( len2 > 65535 ) throw H2O.unimpl();
    for( int i=0; i<len; i++ )
      UDP.set2(buf,(i<<1)+OFF,(short)xs[i]);
    _mem = buf;
  }

  @Override protected long at8_impl(int idx) {
    int y = _last;              // Read once; racy 1-entry cache
    int x = UDP.get2(_mem,y)&0xFFFF;
    if( idx == x ) return 1;
    if( idx > x && y < _mem.length && idx < UDP.get2(_mem,y+2) ) return 0;

    int lo=0, hi = (_mem.length-OFF)>>>1;
    while( hi-lo > 1 ) {        // Binary search the row
      int mid = (hi+lo)>>>1;
      x = UDP.get2(_mem,(mid<<1)+OFF)&0xFFFF;
      if( idx < x ) hi = mid;
      else          lo = mid;
    }
    y = (lo<<1)+OFF;
    _last = y;                  // Write once; racy 1-entry cache
    x = UDP.get2(_mem,y)&0xFFFF;
    return idx==x ? 1 : 0;
  }
  @Override protected double atd_impl(int idx) { return at8_impl(idx); }
  @Override protected final boolean isNA_impl( int i ) { return false; }
  @Override boolean set_impl(int idx, long l)   { return false; }
  @Override boolean set_impl(int idx, double d) { return false; }
  @Override boolean set_impl(int idx, float f ) { return false; }
  @Override boolean setNA_impl(int idx)         { return false; }
  @Override boolean hasFloat ()                 { return false; }
  @Override public AutoBuffer write(AutoBuffer bb) { return bb.putA1(_mem, _mem.length); }
  @Override public Chunk read(AutoBuffer bb) {
    _mem   = bb.bufClose();
    _start = -1;
    _len = UDP.get4(_mem,0);
    return this;
  }
  @Override NewChunk inflate_impl(NewChunk nc) {
    nc._ls = MemoryManager.malloc8 (_len);
    nc._xs = MemoryManager.malloc4 (_len);
    for( int i=OFF; i<_mem.length; i+=2 )
      nc._ls[UDP.get2(_mem,i)&0xFFFF] = 1;
    return nc;
  }
}
