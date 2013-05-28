package water;

import jsr166y.CountedCompleter;
import water.fvec.Vec;
import water.fvec.CVec;

/** Map/Reduce style distributed computation. */
public abstract class MRTask2<T extends MRTask2> extends DTask implements Cloneable {

  // Run some useful function over this <strong>local</strong> CVec, and record
  // the results in the <em>this<em> MRTask2.
  abstract public void map( long start, CVec cvec );

  // Combine results from 'mrt' into 'this' MRTask2.  Both 'this' and 'mrt' are
  // guaranteed to either have map() run on them, or be the results of a prior
  // reduce().
  public void reduce( T mrt ) { }

  // Sub-class init on the 1st remote instance of this object, for initializing
  // node-local shared data structures.
  public void init() { }

  // Top-level blocking call.
  public final T invoke( Vec... vecs ) { 
    checkCompatible(vecs);      // Check for compatible vectors
    _vecs = vecs; 
    _nlo = 0;  _nhi = H2O.CLOUD.size(); 
    compute2();
    return self(); 
  }
  
  // The Vectors to work on
  private Vec[] _vecs;            // Vectors to work on

  // Remote/Global work: other nodes we are awaiting results from
  private int _nlo, _nhi;         // Range of NODEs to work on - remotely
  transient private RPC<T> _nleft, _nrite;
  // Local work: range of local chunks we are working on
  transient private int _lo, _hi;   // Range of CVecs to work on - locally
  transient private T _left, _rite; // In-progress execution tree

  // Support for fluid-programming with strong types
  private final T self() { return (T)this; }

  // Called by invoke() on local in a top-level call.  Called by dinvoke() on a
  // remote, same as invoke() except nlo/nhi filled in.  Guaranteed that the
  // H2O.SELF.index() is in the {nlo to (nhi-1)} range.
  @Override public final void compute2() {
    
    // Check for global vs local work
    if( _nlo < _nhi-1 ) {       // Still have global work?
      throw H2O.unimpl();
    }
    assert _nlo == _nhi-1 && _nlo == H2O.SELF.index();

    // Local work
    throw H2O.unimpl();
  }


  ///** Do all the keys in the list associated with this Node.  Roll up the
  // * results into <em>this<em> MRTask2. */
  //@Override public final void lcompute() {
  //  if( _hi-_lo >= 2 ) { // Multi-key case: just divide-and-conquer to 1 key
  //    final int mid = (_lo+_hi)>>>1; // Mid-point
  //    assert _left == null && _rite == null;
  //    _left = (MRTask2)clone();
  //    _rite = (MRTask2)clone();
  //    _left._hi = mid;          // Reset mid-point
  //    _rite._lo = mid;          // Also set self mid-point
  //    setPendingCount(1);       // One fork awaiting completion
  //    _left.fork();             // Runs in another thread/FJ instance
  //    _rite.compute2();         // Runs in THIS F/J thread
  //  } else {
  //    if( _hi > _lo )           // Single key?
  //      map(_keys[_lo]);        // Get it, run it locally
  //    tryComplete();            // And this task is complete
  //  }
  //}
  //
  //@Override public final void lonCompletion( CountedCompleter caller ) {
  //  // Reduce results into 'this' so they collapse going up the execution tree.
  //  // NULL out child-references so we don't accidentally keep large subtrees
  //  // alive: each one may be holding large partial results.
  //  if( _left != null ) reduceAlsoBlock(_left); _left = null;
  //  if( _rite != null ) reduceAlsoBlock(_rite); _rite = null;
  //}
  // Cancel/kill all work as we can, then rethrow... do not invisibly swallow
  // exceptions (which is the F/J default)
  @Override public final boolean onExceptionalCompletion(Throwable ex, CountedCompleter caller ) {
    if( _nleft != null ) _nleft.cancel(true); _nleft = null;
    if( _nrite != null ) _nrite.cancel(true); _nrite = null;
    _left = null;
    _rite = null;
    return super.onExceptionalCompletion(ex, caller);
  }

  // Check that the vectors are all compatible: same number of rows per chunk
  private static void checkCompatible( Vec[] vecs ) {
    int nchunks = vecs[0].nChunks();
    for( Vec vec : vecs )
      if( vec.nChunks() != nchunks )
        throw new IllegalArgumentException("Vectors different numbers of chunks, "+nchunks+" and "+vec.nChunks());
    // Also check each chunk has same rows
    for( int i=0; i<nchunks; i++ ) {
      long es = vecs[0].chunk2StartElem(i);
      for( Vec vec : vecs )
        if( vec.chunk2StartElem(i) != es )
          throw new IllegalArgumentException("Vector chunks different numbers of rows, "+es+" and "+vec.chunk2StartElem(i));
    }
  }
}
