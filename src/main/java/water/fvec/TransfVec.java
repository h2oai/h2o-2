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

  static class TransfChunk extends Chunk {
    Chunk _c;
    int[] _domMap;

    public TransfChunk(Chunk c, int[] domMap) { super(); _c  = c; _domMap = domMap; }

    @Override protected double atd_impl(int idx) {
      double val = _c.atd_impl(idx);
      return _domMap[(int)val];
    }

    @Override protected long at8_impl(int idx) {
      long val = _c.at8_impl(idx);
      return _domMap[(int)val];
    }

    @Override protected boolean isNA_impl(int idx) { return _c.isNA_impl(idx); }

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
