package water.fvec;

import water.*;
import water.parser.DParseTask;

/** SPARSE shorts.  A list of rows that are non-zero, and the value. */
public class CX2Chunk extends Chunk {
  static final int OFF = 4;     // _len in 1st 2 bytes
  transient int _last;          // 1-entry cache of last accessed row
  // Dense constructor
  public CX2Chunk(long[] ls, int xs[], int len2, int nzcnt, int nacnt) {
    _start = -1; _len = len2;
    byte[] buf = new byte[(nzcnt+nacnt)*(2+2)+OFF]; // 2 bytes row, 2 bytes val
    UDP.set4(buf,0,len2);
    int j = OFF;
    for( int i=0; i<len2; i++ ) {
      int scale = xs[i]==Integer.MIN_VALUE+1 ? 0 : xs[i];
      if( ls[i] != 0 || scale != 0 )
        j +=
          UDP.set2(buf,j  ,(short)i) +
          UDP.set2(buf,j+2,(short)(ls[i]==0 ? C2Chunk._NA : ls[i]*DParseTask.pow10(scale)));
    }
    assert j==buf.length;
    _mem = buf;
  }
  // Sparse constructor
  public CX2Chunk(long[] ls, int xs[], int len2, int len) {
    _start = -1; _len = len2;
    byte[] buf = new byte[len*4+OFF];
    UDP.set4(buf,0,len2);
    if( len2 > 65535 ) throw H2O.unimpl();
    for( int i=0; i<len; i++ ) {
      if( !(Short.MIN_VALUE <= ls[i] && ls[i] < Short.MAX_VALUE) )
        throw H2O.unimpl();
      UDP.set2(buf,(i<<2)+OFF+0,(short)xs[i]);
      UDP.set2(buf,(i<<2)+OFF+2,(short)ls[i]);
    }
    _mem = buf;
  }

  private int at_impl(int idx) {
    int y = _last;              // Read once; racy 1-entry cache
    int x = UDP.get2(_mem,y+0)&0xFFFF;
    if( idx == x ) return UDP.get2(_mem,y+2);
    if( idx > x && y < _mem.length && idx < UDP.get2(_mem,y+4) ) return 0;

    int lo=0, hi = (_mem.length-OFF)>>>2;
    int cnt=0;
    while( lo+1 != hi ) {
      int mid = (hi+lo)>>>1;
      x = UDP.get2(_mem,(mid<<2)+OFF+0)&0xFFFF;
      if( idx < x ) hi = mid;
      else          lo = mid;
    }
    y = (lo<<2)+OFF+0;
    _last = y;                  // Write once; racy 1-entry cache
    x =               UDP.get2(_mem,y+0)&0xFFFF;
    return idx == x ? UDP.get2(_mem,y+2) : 0;
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
    nc._ls = MemoryManager.malloc8 (_len);
    nc._xs = MemoryManager.malloc4 (_len);
    for( int i=OFF; i<_mem.length; i+=4 ) {
      int row = UDP.get2(_mem,i+0)&0xFFFF;
      int x   = UDP.get2(_mem,i+2);
      if( x==C2Chunk._NA ) nc._xs[row] = Integer.MIN_VALUE; else nc._ls[row] = x;
    }
    return nc;
  }
}
