package water.fvec;

import water.*;
import water.parser.DParseTask;

/** SPARSE shorts.  A list of rows that are non-zero, and the value. */
public class CX2Chunk extends Chunk {
  static final int OFF = 4;     // _len in 1st 2 bytes
  public CX2Chunk(long[] ls, int xs[], int len, int nzcnt, int nacnt) {
    _mem = compress(ls,xs,len,nzcnt,nacnt); _start = -1; _len = len;
  }
  private int at_impl(int idx) {
    int lo=0, hi = (_mem.length-OFF)>>>2;
    int cnt=0;
    while( lo+1 != hi ) {
      int mid = (hi+lo)>>>1;
      int x = UDP.get2(_mem,(mid<<2)+OFF+0)&0xFFFF;
      if( idx < x ) hi = mid;
      else          lo = mid;
    }
    int x =           UDP.get2(_mem,(lo<<2)+OFF+0)&0xFFFF;
    return idx == x ? UDP.get2(_mem,(lo<<2)+OFF+2) : 0;
  }
  @Override protected long at8_impl(int idx) {
    int v = at_impl(idx);
    if( v==C2Chunk._NA ) throw new IllegalArgumentException("at8 but value is missing");
    return v;
  }
  @Override protected double atd_impl(int idx) {
    int v = at_impl(idx);
    return v==C2Chunk._NA ? Double.NaN : v;
  }

  @Override protected final boolean isNA_impl( int i ) { return at_impl(i)==C2Chunk._NA;  }
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
    for( int i=OFF; i<_mem.length; i+=4 ) {
      int row = UDP.get2(_mem,i+0)&0xFFFF;
      int x   = UDP.get2(_mem,i+2);
      if( x==C2Chunk._NA ) nc._xs[row] = Integer.MIN_VALUE; else nc._ls[row] = x;
    }
    return nc;
  }

  // Compress a NewChunk long array
  static byte[] compress( long ls[], int xs[], int len, int nzcnt, int nacnt ) {
    byte[] buf = new byte[(nzcnt+nacnt)*(2+2)+OFF]; // 2 bytes row, 2 bytes val
    UDP.set4(buf,0,len);
    int j = OFF;
    for( int i=0; i<len; i++ )
      if( ls[i] != 0 || xs[i] != 0 )
        j +=
          UDP.set2(buf,j  ,(short)i) +
          UDP.set2(buf,j+2,(short)(ls[i]==0 ? C2Chunk._NA : ls[i]*DParseTask.pow10(xs[i])));
    assert j==buf.length;
    return buf;
  }
}
