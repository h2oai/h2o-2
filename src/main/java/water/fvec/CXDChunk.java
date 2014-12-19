package water.fvec;

import water.H2O;
import water.MemoryManager;
import water.UDP;

import java.util.Iterator;

/**
 * Created by tomasnykodym on 3/26/14.
 */
public class CXDChunk extends CXIChunk {

//  static byte [] toBytes(double [] vals, int [] ids, int slen){
//    int ridsz = vals.length >= 65535?4:2;
//    int elemsz = ridsz + 8;
//    byte [] res = MemoryManager.malloc1(slen*elemsz+OFF);
//    for(int i = 0, off = OFF; i < slen; ++i, off += elemsz) {
//      int id = ids[i];
//      if(elemsz == 2)
//        UDP.set2(res,off,(short)id);
//      else
//        UDP.set4(res,off,id);
//      UDP.set8d(res, off + ridsz, vals[id]);
//    }
//    return res;
//  }
//  public CXDChunk(double [] vals, int [] ids, int slen){
//    super(vals.length,slen,8,toBytes(vals,ids,slen));
//  }
  protected CXDChunk(int len, int nzs, int valsz, byte [] buf){super(len,nzs,valsz,buf);}

  // extract fp value from an (byte)offset
  protected final double getFValue(int off){
    if(_valsz == 8) return UDP.get8d(_mem, off + _ridsz);
    throw H2O.unimpl();
  }

  @Override protected long at8_impl(int idx) {
    int off = findOffset(idx);
    if(getId(off) != idx)return 0;
    double d = getFValue(off);
    if(Double.isNaN(d)) throw new IllegalArgumentException("at8 but value is missing");
    return (long)d;
  }
  @Override protected double atd_impl(int idx) {
/*    int off = _offCache;
    int prevIdx = getId(off);
    if(prevIdx == idx)
      return getFValue(off);
    if(prevIdx < idx) {
      int nextIdx = getId(off + _ridsz + _valsz);
      if(nextIdx > idx) return 0;
      if(nextIdx == idx) {
        _offCache = (off += _ridsz + _valsz);
        return getFValue(off);
      }
    }*/
    int off = findOffset(idx);
    if(getId(off) != idx)return 0;
    return getFValue(off);
  }

  @Override protected boolean isNA_impl( int i ) {
    int off = findOffset(i);
    if(getId(off) != i)return false;
    return Double.isNaN(getFValue(off));
  }

  @Override final boolean hasFloat () { return true; }

  @Override NewChunk inflate_impl(NewChunk nc) {
    final int slen = sparseLen();
    nc.set_len(len());
    nc.set_sparseLen(slen);
    nc.alloc_doubles(slen);
    nc.alloc_indices(slen);
    int off = OFF;
    for( int i = 0; i < slen; ++i, off += _ridsz + _valsz) {
      nc.indices()[i] = getId(off);
      nc.doubles()[i] = getFValue(off);
    }
    return nc;
  }

  public Iterator<Value> values(){
    return new SparseIterator(new Value(){
      @Override public final long asLong(){
        double d = getFValue(_off);
        if(Double.isNaN(d)) throw new IllegalArgumentException("at8 but value is missing");
        return (long)d;
      }
      @Override public final double asDouble() {return getFValue(_off);}
      @Override public final boolean isNA(){
        double d = getFValue(_off);
        return Double.isNaN(d);
      }
    });
  }

  public int pformat_len0() { return 22; }
  public String pformat0() { return "% 21.15e"; }

}
