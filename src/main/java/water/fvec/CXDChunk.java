package water.fvec;

import water.H2O;
import water.MemoryManager;
import water.UDP;

/**
 * Created by tomasnykodym on 3/26/14.
 */
public class CXDChunk extends CXIChunk {
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
    final int len = sparseLen();
    nc._len2 = _len;
    nc._len = sparseLen();
    nc._ds = MemoryManager.malloc8d(nc._len);
    nc._id = MemoryManager.malloc4 (len);
    int off = OFF;
    for( int i = 0; i < len; ++i, off += _ridsz + _valsz) {
      nc._id[i] = getId(off);
      nc._ds[i] = getFValue(off);
    }
    return nc;
  }

}
