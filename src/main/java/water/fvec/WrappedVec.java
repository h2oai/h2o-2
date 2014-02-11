package water.fvec;

import java.util.Arrays;

import water.*;
import water.util.Utils;

/**
 * Dummy vector transforming values of given vector according to given domain mapping.
 *
 * <p>The mapping is defined by a simple hash map composed of two arrays.
 * The first array contains values. Index of values is index into the second array {@link #_indexes}
 * which contains final value (i.e., index to domain array).</p>
 *
 * <p>If {@link #_indexes} array is null, then index of found value is used directly.</p>
 *
 * <p>To avoid virtual calls or additional null check for {@link #_indexes} the vector
 * returns two implementation of underlying chunk ({@link TransfChunk} when {@link #_indexes} is not <code>null</code>,
 * and {@link FlatTransfChunk} when {@link #_indexes} is <code>null</code>.</p>
 */
public abstract class WrappedVec extends Vec {
  /** A key for underlying vector which contains values which are transformed by this vector. */
  final Key   _masterVecKey;
  /** Cached instances of underlying vector. */
  transient Vec _masterVec;
  public WrappedVec(Key masterVecKey, Key key, long[] espc) {
    super(key, espc);
    _masterVecKey = masterVecKey;
  }

  @Override public Vec masterVec() {
    if (_masterVec==null) _masterVec = DKV.get(_masterVecKey).get();
    return _masterVec;
  }
}
