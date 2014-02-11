package water.fvec;

import water.Key;

/** 
 *  A simple wrapper for looking at only a subset of rows
 */
public abstract class SubsetVec extends WrappedVec {
  public SubsetVec(Key masterVecKey, Key key, long[] espc) {
    super(masterVecKey,key, espc);
  }

}
