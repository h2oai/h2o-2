package water.fvec;

import water.*;

/** SPARSE boolean; no NAs.  A list of rows that are non-zero. */
public class CX0Chunk extends Chunk {
  static final int OFF = 4;
  public CX0Chunk(long[] ls, int len, int nzcnt) {
    _mem = compress(ls,len,nzcnt); _start = -1; _len = len;
  }
  @Override protected long at8_impl(int idx) {
    int lo=0, hi = (_mem.length-OFF)>>>1;
    while( hi-lo > 1 ) {        // Binary search the row
      int mid = (hi+lo)>>>1;
      int x = UDP.get2(_mem,(mid<<1)+OFF)&0xFFFF;
      if( idx < x ) hi = mid;
      else          lo = mid;
    }
    int x = UDP.get2(_mem,(lo<<1)+OFF)&0xFFFF;
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
    nc._ds = null;
    nc._ls = MemoryManager.malloc8 (_len);
    nc._xs = MemoryManager.malloc4 (_len);
    for( int i=OFF; i<_mem.length; i+=2 )
      nc._ls[UDP.get2(_mem,i)&0xFFFF] = 1;
    return nc;
  }

  // Compress a NewChunk long array
  static byte[] compress( long ls[], int len, int nzcnt ) {
    byte[] buf = new byte[nzcnt*2+OFF];
    UDP.set4(buf,0,len);
    int j = OFF;
    for( int i=0; i<len; i++ )
      if( ls[i] != 0 ) j += UDP.set2(buf,j,(short)i);
    return buf;
  }
}
