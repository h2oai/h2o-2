package water.fvec;

import water.*;

/**
 * Dummy vector transforming values of given vector according to given domain mapping.
 */
public class TransfVec extends Vec {
  final Key   _masterVecKey;
  final int[] _domMap;
  transient Vec _masterVec;

  public TransfVec(Key masterVecKey, int[] domMap, Key key, long[] espc) {
    super(key, espc);
    _masterVecKey = masterVecKey;
    _domMap = domMap;
  }

  private Vec masterVec() {
    if (_masterVec==null) _masterVec = DKV.get(_masterVecKey).get();
    return _masterVec;
  }

  @Override public Chunk elem2BV(int cidx) {
    Chunk c = masterVec().elem2BV(cidx);
    return new TransfChunk(c, _domMap);
  }

  @Override public void remove( Futures fs ) {
    // The TransfVec is a just wrapper vector => it should not delete underlying chunks or underlying vector.
  }

  static class TransfChunk extends Chunk {
    Chunk _c;
    int[] _domMap;
    public TransfChunk(Chunk c, int[] domMap) { _c  = c; _domMap = domMap; }
    @Override protected long at8_impl(int idx) { return _domMap[(int)_c.at8_impl(idx)]; }
    @Override protected double atd_impl(int idx) { return at8_impl(idx);  }
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
}
