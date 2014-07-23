package water.fvec;

import water.*;

/**
 * The empty-compression function, where data is in long-pairs for UUIDs
 */
public class C16Chunk extends Chunk {
  protected static final long _LO_NA = Long.MAX_VALUE;
  protected static final long _HI_NA = 0;
  C16Chunk( byte[] bs ) { _mem=bs; _start = -1; _len = _mem.length>>4; }
  @Override protected final long   at8_impl( int i ) { throw new IllegalArgumentException("at8 but 16-byte UUID");  }
  @Override protected final double atd_impl( int i ) { throw new IllegalArgumentException("atd but 16-byte UUID");  }
  @Override protected final boolean isNA_impl( int i ) { return UDP.get8(_mem,(i<<4))==_LO_NA && UDP.get8(_mem,(i<<4)+8)==_HI_NA; }
  @Override protected long at16l_impl(int idx) { 
    long lo = UDP.get8(_mem,(idx<<4)  );
    long hi = UDP.get8(_mem,(idx<<4)+8);
    if( lo==_LO_NA && hi==_HI_NA ) throw new IllegalArgumentException("at16 but value is missing");
    return lo;
  }
  @Override protected long at16h_impl(int idx) { 
    long lo = UDP.get8(_mem,(idx<<4)  );
    long hi = UDP.get8(_mem,(idx<<4)+8);
    if( lo==_LO_NA && hi==_HI_NA ) throw new IllegalArgumentException("at16 but value is missing");
    return hi;
  }
  @Override boolean set_impl(int idx, long l) { return false; }
  @Override boolean set_impl(int i, double d) { return false; }
  @Override boolean set_impl(int i, float f ) { return false; }
  @Override boolean setNA_impl(int idx) { UDP.set8(_mem,(idx<<4),_LO_NA); UDP.set8(_mem,(idx<<4),_HI_NA); return true; }
  @Override boolean hasFloat() { return false; }
  @Override public AutoBuffer write(AutoBuffer bb) { return bb.putA1(_mem,_mem.length); }
  @Override public C16Chunk read(AutoBuffer bb) {
    _mem = bb.bufClose();
    _start = -1;
    _len = _mem.length>>4;
    assert _mem.length == _len<<4;
    return this;
  }
  @Override NewChunk inflate_impl(NewChunk nc) {
    nc.set_len(nc.set_sparseLen(0));
    for( int i=0; i< len(); i++ ) {
      long lo = UDP.get8(_mem,(i<<4)  );
      long hi = UDP.get8(_mem,(i << 4) + 8);
      nc.addUUID(lo, hi);
    }
    return nc;
  }
  public int pformat_len0() { return 36; }
}
