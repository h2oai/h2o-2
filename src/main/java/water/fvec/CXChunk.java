package water.fvec;

import water.AutoBuffer;
import water.H2O;
import water.MemoryManager;
import water.UDP;
import water.parser.DParseTask;

import java.util.Iterator;

/**
 * Created by tomasnykodym on 3/18/14.
 * Sparse chunk.
 */
public class CXChunk extends Chunk {
  private transient int _elsz; // byte size of stored value
  private transient int _idsz; // byte size of stored (chunk-relative) row nums
  protected static final int OFF = 6;
  private transient int _lastOff = OFF;
  private transient boolean _isInt = true;

  private static final long [] NAS = {C2Chunk._NA,C4Chunk._NA,C8Chunk._NA};


  protected CXChunk(long [] ls, int [] xs, int [] ids, int len, int nz, int elementSz, boolean isInt){
    assert elementSz > 0 || isInt;
    _len = len;
    _isInt = isInt;
    _elsz = elementSz;
    _idsz = (len >= 65535)?4:2;
    byte[] buf = MemoryManager.malloc1((nz)*(_idsz+elementSz)+OFF); // 2 bytes row, 2 bytes val
    UDP.set4(buf,0,len);
    int off = OFF;
    final int inc = elementSz + _idsz;
    for( int i=0; i<nz; i++, off += inc ) {
      if(_idsz == 2)
        UDP.set2(buf,off,(short)ids[i]);
      else
        UDP.set4(buf,off,ids[i]);
      if(_elsz == 0)continue;
      if(isInt){
        assert xs[i] == Integer.MIN_VALUE || xs[i] >= 0; // assert we have int or NA
        final long lval = xs[i] == Integer.MIN_VALUE?NAS[(_elsz>>>1)-1]:ls[i]*DParseTask.pow10i(xs[i]);
        switch(elementSz){
          case 2:
            short sval = (short)lval;
            assert sval == lval;
            UDP.set2(buf,off+_idsz,sval);
            break;
          case 4:
            int ival = (int)lval;
            assert ival == lval;
            UDP.set4(buf, off+_idsz, ival);
            break;
          case 8:
            UDP.set8(buf, off+_idsz, lval);
            break;
          default:
            throw H2O.unimpl();
        }
      } else { // fp number
        assert 4 == elementSz || elementSz == 8; // only doubles and floats for now
        final double dval = xs[i] == Integer.MIN_VALUE?Double.NaN:ls[i]*DParseTask.pow10(xs[i]);
        if(elementSz == 8)
          UDP.set8d(buf, off+_idsz, dval);
        else
          UDP.set4f(buf, off + _idsz, (float) dval);
      }
    }
    assert off==buf.length;
    _mem = buf;
  }

  @Override public final boolean isSparse() {return true;}
  @Override public final int sparseLen(){return (_mem.length - OFF) / (_elsz + 2);}
  @Override public final int nonzeros(int [] arr){
    int len = sparseLen();
    int off = OFF;
    final int inc = _elsz + 2;
    for(int i = 0; i < len; ++i, off += inc) arr[i] = UDP.get2(_mem, off)&0xFFFF;
    return len;
  }

  @Override boolean set_impl(int idx, long l)   { return false; }
  @Override boolean set_impl(int idx, double d) { return false; }
  @Override boolean set_impl(int idx, float f ) { return false; }
  @Override boolean setNA_impl(int idx)         { return false; }

  @Override protected long at8_impl(int idx) {
    if(_isInt){
      int off = findOffset(idx);
      if(getId(off) != idx)return 0;
      long v = getIValue(off);
      if( v== NAS[(_idsz >>> 1)-1]) throw new IllegalArgumentException("at8 but value is missing");
      return v;
    } else {
      double d =  getFValue(findOffset(idx));
      if(Double.isNaN(d)) throw new IllegalArgumentException("at8 but value is missing");
      return (long)d;
    }
  }
  @Override protected double atd_impl(int idx) {
    int off = findOffset(idx);
    if(getId(off) != idx)return 0;
    if(_isInt) {
      long v =  getIValue(off);
      return (v == NAS[(_elsz >>> 1) -1])?Double.NaN:v;
    }
    return getFValue(off);
  }

  @Override protected boolean isNA_impl( int i ) {
    int off = findOffset(i);
    if(getId(off) != i)return false;
    return _isInt?getIValue(off) == NAS[(_elsz>>>1)-1]:Double.isNaN(off);
  }

  @Override boolean hasFloat ()                 { return false; }

  @Override NewChunk inflate_impl(NewChunk nc) {
    final int len = sparseLen();
    nc._len2 = _len;
    nc._len = sparseLen();
    if(_isInt){
      nc._ls = MemoryManager.malloc8 (len);
      nc._xs = MemoryManager.malloc4 (len);
      nc._id = MemoryManager.malloc4 (len);
      int off = OFF;
      for( int i = 0; i < len; ++i, off += _idsz + _elsz) {
        nc._id[i] = getId(off);
        long v = getIValue(off);
        if(v == NAS[(_elsz >>> 1) - 1]){
          nc._ls[i] = Long.MAX_VALUE;
          nc._xs[i] = Integer.MIN_VALUE;
        } else nc._ls[i] = v;
      }
    } else {
      nc._ds = MemoryManager.malloc8d(nc._id.length);
      nc._id = MemoryManager.malloc4 (len);
      assert nc._id.length == nc._len;
      int off = OFF;
      for(int i = 0; i < nc._len; ++i, off += _idsz + _elsz){
        nc._ds[i] = getFValue(off);
        nc._id[i] = getId(off);
      }
    }
    return nc;
  }

  // get id of nth (chunk-relative) stored element
  protected final int getId(int off){
    return _idsz == 2
      ?UDP.get2(_mem,off)&0xFFFF
      :UDP.get4(_mem,off);
  }
  // get offset of nth (chunk-relative) stored element
  private final int getOff(int n){return OFF + (_idsz+_elsz)*n;}
  // extract integer value from an (byte)offset
  protected final long getIValue(int off){
    assert _isInt;
    switch(_elsz){
      case 2: return UDP.get2(_mem, off + _idsz);
      case 4: return UDP.get4(_mem, off + _idsz);
      case 8: return UDP.get8(_mem, off + _idsz);
      default: throw H2O.unimpl();
   } 
  }
  // extract fp value from an (byte)offset
  protected final double getFValue(int off){
    if(_elsz == 8) return UDP.get8d(_mem, off + _elsz);
    throw H2O.unimpl();
  }



  // find offset of the chunk-relative row id, or -1 if not stored (i.e. sparse zero)
  protected final int findOffset(int idx) {
    if(idx >= _len)throw new IndexOutOfBoundsException();
    final int off = _lastOff;
    int lastIdx = getId(off);
    // check the last accessed elem
    if( idx == lastIdx ) return _lastOff;
    if(idx > lastIdx){
      // check the next one
      final int nextOff = off + _idsz + _elsz;
      if(nextOff < _mem.length){
        int nextId =  getId(nextOff);
        if(idx < nextId)return -1;
        if(idx == nextId){
          _lastOff = nextOff;
          return nextOff;
        }
      }
    }
    // no match so far, do binary search
    int lo=0, hi = sparseLen();
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
    _idsz = _mem[5];
    _elsz = _mem[6];
    if(_elsz < 0){
      _isInt = false;
      _elsz = - _elsz;
    }
    return this;
  }

  public int skipCnt(int rid){
    int off = _lastOff;
    int currentId = getId(off);
    if(rid != currentId) off = findOffset(rid);
    if(off < _mem.length - _idsz - _elsz)
      return getId(off + _idsz + _elsz) - rid;
    return 0;
  }
  public final class Value {
    protected int off = OFF;
    public int rowInChunk(){return getId(off);}
    public long asLong(){
      if(_isInt){
        long v = getIValue(off);
        if(v == NAS[(_elsz >>> 1) - 1]) throw new IllegalArgumentException("at8 but value is missing");
        return v;
      } else {
        double d = getFValue(off);
        if(Double.isNaN(d))throw new IllegalArgumentException("at8 but value is missing");
        return (long)d;
      }
    }
    public double asDouble(){
      if(_isInt) {
        long v = getIValue(off);
        return (v == NAS[(_elsz >>> 1) - 1])?Double.NaN:v;
      } else
        return getFValue(off);
    }
    public boolean isNa(){
      if(_isInt) {
        long v = getIValue(off);
        return (v == NAS[(_elsz >>> 1) - 1]);
      } else
        return Double.isNaN(getFValue(off));
    }
  }

  public Iterator<Value> values(){
    final Value val = new Value();
    return new Iterator<Value>(){
      @Override public boolean hasNext(){return val.off != _mem.length - (_idsz + _elsz);}
      @Override public Value next(){
        val.off += (_idsz + _elsz);
        return val;
      }
      @Override public void remove(){throw new UnsupportedOperationException();}
    };
  }
}
