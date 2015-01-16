package water.fvec;

import water.AutoBuffer;
import water.H2O;
import water.MemoryManager;
import water.UDP;

import java.util.Iterator;

/**
 * Created by tomasnykodym on 3/18/14.
 * Sparse chunk.
 */
public class CXIChunk extends Chunk {
  protected transient int _sparseLen;            // Number of elements in this chunk
  protected transient int _valsz; // byte size of stored value
  protected transient int _valsz_log; //
  protected transient int _ridsz; // byte size of stored (chunk-relative) row nums
  protected static final int OFF = 6;
  protected transient volatile int _lastOff = OFF;


  private static final long [] NAS = {C1Chunk._NA,C2Chunk._NA,C4Chunk._NA,C8Chunk._NA};

  protected CXIChunk(int len, int nzs, int valsz, byte [] buf){
    assert (valsz == 0 || valsz == 1 || valsz == 2 || valsz == 4 || valsz == 8);
    _len = len;
    _ridsz = len >= 65535?4:2;
    int log = 0;
    while((1 << log) < valsz)++log;
    assert valsz == 0 || (1 << log) == valsz;
    _valsz = valsz;
    _valsz_log = log;
    UDP.set4(buf,0,len);
    byte b = (byte) _ridsz;
    buf[4] = b;
    buf[5] = (byte) _valsz;
    _mem = buf;
    _sparseLen = nzs;
    assert (_mem.length - OFF) % (_valsz + _ridsz) == 0:"unexpected mem.length in sparse chunk: mem.length = " + (_mem.length - OFF) + "val_sz = " + _valsz + ", rowId_sz = " + _ridsz;
  }

  @Override public final boolean isSparse() {return true;}
  @Override public final int sparseLen(){return _sparseLen;}
  @Override public final int nonzeros(int [] arr){
    int len = sparseLen();
    int off = OFF;
    final int inc = _valsz + _ridsz;
    for(int i = 0; i < len; ++i, off += inc)
      arr[i] = _ridsz == 2 ? UDP.get2(_mem, off)&0xFFFF : UDP.get4(_mem, off) ;
    return len;
  }

  @Override boolean set_impl(int idx, long l)   { return false; }
  @Override boolean set_impl(int idx, double d) { return false; }
  @Override boolean set_impl(int idx, float f ) { return false; }
  @Override boolean setNA_impl(int idx)         { return false; }

  @Override protected long at8_impl(int idx) {
    int off = findOffset(idx);
    if(getId(off) != idx)return 0;
    long v = getIValue(off);
    if( v== NAS[_valsz_log])
      throw new IllegalArgumentException("at8 but value is missing");
    return v;
  }
  @Override protected double atd_impl(int idx) {
    int off = findOffset(idx);
    if(getId(off) != idx)return 0;
    long v =  getIValue(off);
    return (v == NAS[_valsz_log])?Double.NaN:v;
  }

  @Override protected boolean isNA_impl( int i ) {
    int off = findOffset(i);
    if(getId(off) != i)return false;
    return getIValue(off) == NAS[_valsz_log];
  }

  @Override boolean hasFloat ()                 { return false; }

  @Override public String toString(){
    return getClass().getSimpleName() + "( start = " + _start + ", len = " + _len + " sparseLen = " + _sparseLen + " valSz = " + _valsz + " rIdSz = " + _ridsz + ")";
  }
  @Override NewChunk inflate_impl(NewChunk nc) {
    final int slen = sparseLen();
    nc.set_sparseLen(slen);
    nc.set_len(len());
    nc.alloc_mantissa(slen);
    nc.alloc_exponent(slen);
    nc.alloc_indices(slen);
    int off = OFF;
    for( int i = 0; i < slen; ++i, off += _ridsz + _valsz) {
      nc.indices()[i] = getId(off);
      long v = getIValue(off);
      if(v == NAS[_valsz_log])
        nc.setNA_impl2(i);
      else
        nc.mantissa()[i] = v;
    }
    return nc;
  }

  // get id of nth (chunk-relative) stored element
  protected final int getId(int off){
    return _ridsz == 2
      ?UDP.get2(_mem,off)&0xFFFF
      :UDP.get4(_mem,off);
  }
  // get offset of nth (chunk-relative) stored element
  private final int getOff(int n){return OFF + (_ridsz + _valsz)*n;}
  // extract integer value from an (byte)offset
  protected final long getIValue(int off){
    switch(_valsz){
      case 1: return _mem[off+ _ridsz]&0xFF;
      case 2: return UDP.get2(_mem, off + _ridsz);
      case 4: return UDP.get4(_mem, off + _ridsz);
      case 8: return UDP.get8(_mem, off + _ridsz);
      default: throw H2O.unimpl();
   }
  }

  // find offset of the chunk-relative row id, or -1 if not stored (i.e. sparse zero)
  protected final int findOffset(int idx) {
    if(idx >= _len)throw new IndexOutOfBoundsException();
    int sparseLen = sparseLen();
    if(sparseLen == 0)return 0;
    final byte [] mem = _mem;
    if(idx <= getId(OFF))  // easy cut off accessing the zeros prior first nz
      return OFF;
    int last = mem.length - _ridsz - _valsz;
    if(idx >= getId(last))  // easy cut off accessing of the tail zeros
      return last;
    final int off = _lastOff;
    int lastIdx = getId(off);
    // check the last accessed elem + one after
    if( idx == lastIdx ) return off;
    if(idx > lastIdx){
      // check the next one (no need to check bounds, already checked at the beginning)
      final int nextOff = off + _ridsz + _valsz;
      int nextId =  getId(nextOff);
      if(idx < nextId) return off;
      if(idx == nextId){
        _lastOff = nextOff;
        return nextOff;
      }
    }
    // binary search
    int lo=0, hi = sparseLen;
    while( lo+1 != hi ) {
      int mid = (hi+lo)>>>1;
      if( idx < getId(getOff(mid))) hi = mid;
      else          lo = mid;
    }
    int y =  getOff(lo);
    _lastOff = y;
    return y;
  }

  @Override public AutoBuffer write(AutoBuffer bb) { return bb.putA1(_mem, _mem.length); }
  @Override public Chunk read(AutoBuffer bb) {
    _mem   = bb.bufClose();
    _start = -1;
    _len = UDP.get4(_mem,0);
    _ridsz = _mem[4];
    _valsz = _mem[5];
    _sparseLen = (_mem.length - OFF) / (_valsz + _ridsz);
    assert (_mem.length - OFF) % (_valsz + _ridsz) == 0:"unexpected mem.length in sparse chunk: mem.length = " + (_mem.length - OFF) + "val_sz = " + _valsz + ", rowId_sz = " + _ridsz;
    int x = _valsz;
    int log = 0;
    while(x > 1){
      x = x >>> 1;
      ++log;
    }
    _valsz_log = log;
    return this;
  }


  @Override public final int nextNZ(int rid){
    final int off = rid == -1?OFF:findOffset(rid);
/*    if(rid == -1) {
      _offCache = OFF;
      return getId(OFF);
    }
    int off = _offCache; */
    int x = getId(off);
/*    if(x != rid) {
      off = _offCache = rid == -1 ? OFF : findOffset(rid);
      x = getId(off);
    }*/
    if(x > rid)return x;
    if(off < _mem.length - _ridsz - _valsz)
      return getId(off + _ridsz + _valsz);
    return _len;
  }
  public abstract  class Value {
    protected int _off = 0;
    public int rowInChunk(){return getId(_off);}
    public abstract long asLong();
    public abstract double asDouble();
    public abstract boolean isNA();
  }

  public final class SparseIterator implements Iterator<Value> {
    final Value _val;
    public SparseIterator(Value v){_val = v;}
    @Override public final boolean hasNext(){return _val._off < _mem.length - (_ridsz + _valsz);}
    @Override public final Value next(){
      if(_val._off == 0)_val._off = OFF;
      else _val._off += (_ridsz + _valsz);
      return _val;
    }
    @Override public final void remove(){throw new UnsupportedOperationException();}
  }
  public Iterator<Value> values(){
    return new SparseIterator(new Value(){
      @Override public final long asLong(){
        long v = getIValue(_off);
        if(v == NAS[(_valsz >>> 1) - 1]) throw new IllegalArgumentException("at8 but value is missing");
        return v;
      }
      @Override public final double asDouble() {
      long v = getIValue(_off);
      return (v == NAS[_valsz_log -1])?Double.NaN:v;
      }
      @Override public final boolean isNA(){
        long v = getIValue(_off);
        return (v == NAS[_valsz_log]);
      }
    });
  }
}
