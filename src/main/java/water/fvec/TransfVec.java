package water.fvec;

import water.*;
import water.util.Utils;

/**
 * Dummy vector transforming values of given vector according to given domain mapping.
 */
public class TransfVec extends Vec {
  final Key   _masterVecKey;
  final int[] _domMap;
  final int   _domMin;
  transient Vec _masterVec;

  public TransfVec(Key masterVecKey, int[] domMap, int domMin, Key key, long[] espc) {
    this(masterVecKey, domMap, domMin, null, key, espc);
  }
  public TransfVec(Key masterVecKey, int[] domMap, int domMin, String[] domain, Key key, long[] espc) {
    super(key, espc);
    _masterVecKey = masterVecKey;
    _domMap = domMap;
    _domMin = domMin;
    _domain = domain;
  }

  @Override public Vec masterVec() {
    if (_masterVec==null) _masterVec = DKV.get(_masterVecKey).get();
    return _masterVec;
  }

  @Override public Chunk elem2BV(int cidx) {
    Chunk c = masterVec().elem2BV(cidx);
    return new TransfChunk(c, _domMap, _domMin, this);
  }

  static class TransfChunk extends Chunk {
    final Chunk _c;
    final int[] _domMap;
    final int   _domMin;
    public TransfChunk(Chunk c, int[] domMap, int domMin, Vec vec) { _c  = c; _domMap = domMap; _domMin = domMin; _len = _c._len; _start = _c._start; _vec = vec; }
    @Override protected long at8_impl(int idx) { return _domMap[(int)_c.at8_impl(idx)-_domMin]; }
    @Override protected double atd_impl(int idx) { double d = 0; return _c.isNA0(idx) ? Double.NaN : ( (d=at8_impl(idx)) == -1 ? Double.NaN : d ) ;  }
    @Override protected boolean isNA_impl(int idx) {
      if (_c.isNA_impl(idx)) return true;
      return at8_impl(idx) == -1; // this case covers situation when there is no mapping
    }

    @Override boolean set_impl(int idx, long l)   { return false; }
    @Override boolean set_impl(int idx, double d) { return false; }
    @Override boolean set_impl(int idx, float f)  { return false; }
    @Override boolean setNA_impl(int idx)         { return false; }
    @Override boolean hasFloat() { return _c.hasFloat(); }
    @Override NewChunk inflate_impl(NewChunk nc)     { throw new UnsupportedOperationException(); }
    @Override public AutoBuffer write(AutoBuffer bb) { throw new UnsupportedOperationException(); }
    @Override public Chunk read(AutoBuffer bb)       { throw new UnsupportedOperationException(); }
  }

  /** Compose this vector with given transformation. Always return a new vector */
  public Vec compose(int[] transfMap) { return compose(this, transfMap, true);  }

  /**
   * Compose given origVector with given transformation. Always returns a new vector.
   * Original vector is kept if keepOrig is true.
   * @param origVec
   * @param transfMap
   * @param keepOrig
   * @return a new instance of {@link TransfVec} composing transformation of origVector and tranfsMap
   */
  public static Vec compose(TransfVec origVec, int[] transfMap, boolean keepOrig) {
    // Do a mapping from INT -> ENUM -> this vector ENUM
    int[] domMap = Utils.compose(origVec._domMap, transfMap);
    Vec result = origVec.masterVec().makeTransf(domMap, origVec._domain);
    if (!keepOrig) DKV.remove(origVec._key);
    return result;
  }
}
