package water.fvec;

import water.AutoBuffer;
import water.H2O;
import water.UDP;

/** SPARSE boolean; no NAs.  A list of rows that are non-zero. */
public class CX0Chunk extends Chunk {
  static final int OFF = 2;
  public CX0Chunk(long[] ls, int nzcnt) { 
    _mem = compress(ls,nzcnt); _start = -1; _len = UDP.get2(_mem,0)&0xFFFF;
  }
  @Override protected long at8_impl(int idx) {
    int lo=0, hi = (_mem.length-OFF)>>>1;
    while( lo+1 != hi ) {        // Binary search the row
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
    _len = UDP.get2(_mem,0)&0xFFFF;
    return this;
  }
  @Override NewChunk inflate_impl(NewChunk nc) {
    throw H2O.unimpl();
  }

  // Compress a NewChunk long array
  static byte[] compress( long ls[], int nzcnt ) {
    if( ls.length > 65536 ) {
      System.out.println("len="+ls.length+" nz="+nzcnt);
      throw H2O.unimpl();
    }
    byte[] buf = new byte[nzcnt*2+OFF];
    UDP.set2(buf,0,(short)ls.length);
    int j = OFF;
    for( int i=0; i<ls.length; i++ )
      if( ls[i] != 0 ) j += UDP.set2(buf,j,(short)i);
    return buf;
  }
}
