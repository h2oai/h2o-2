package water.exec;

import water.fvec.*;

/** A (global) Vec with local lifetime.
 *  @author cliffc@0xdata.com
 */
public class TmpVec extends Vec {
  // Reference counted local vecs.  No expectation of the refcnts being passed
  // around the cloud; local counting only.
  int _refcnt;
  TmpVec( Vec v ) { super(v); }
}
