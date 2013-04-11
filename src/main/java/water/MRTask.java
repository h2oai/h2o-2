package water;
import jsr166y.CountedCompleter;

/** Map/Reduce style distributed computation. */
public abstract class MRTask extends DRemoteTask {

  transient private int _lo, _hi; // Range of keys to work on
  transient private MRTask _left, _rite; // In-progress execution tree

  public void init() {
    _lo = 0;
    _hi = _keys.length;
  }

  /** Run some useful function over this <strong>local</strong> key, and
   * record the results in the <em>this<em> MRTask. */
  abstract public void map( Key key );

  long _reservedMem = 0;
  boolean _hasReservedMem = false;


  /** Do all the keys in the list associated with this Node.  Roll up the
   * results into <em>this<em> MRTask. */
  @Override public final void compute2() {
    if( _hi-_lo >= 2 ) { // Multi-key case: just divide-and-conquer to 1 key
      final int mid = (_lo+_hi)>>>1; // Mid-point
      assert _left == null && _rite == null;
      MRTask l = (MRTask)clone();
      MRTask r = (MRTask)clone();
      if(!_hasReservedMem){ // try to reserve memory needed for computing subtree rooted at this node.
        long memReq = (_hi - _lo + 1)*(ValueArray.CHUNK_SZ + memReqPerChunk());
        if(_hasReservedMem = MemoryManager.tryReserveTaskMem(memReq))
          _reservedMem = memReq; // remember the amount of memory reserved to free it when done
      }
      _left = l;
      _rite = r;
      // pass the memory allocation info on
      // (pass only the hasMemory flag, the actual amount of reserved memory is stored only by the node which successfully reserved it and which will free it when done)
      _left._hasReservedMem = _hasReservedMem;
      _rite._hasReservedMem = _hasReservedMem;
      _left._hi = mid;          // Reset mid-point
      _rite._lo = mid;          // Also set self mid-point
      setPendingCount(2);
      if(_hasReservedMem) { // we have enough memory => run in parallel
        _left.fork();             // Runs in another thread/FJ instance
        _rite.fork();             // Runs in another thread/FJ instance
      } else {
        // not enough memory => go single threaded
        _left.compute2();
        _rite.compute2();
      }
    } else {
      if( _hi > _lo )           // Single key?
        map(_keys[_lo]);        // Get it, run it locally
    }
    tryComplete();              // And this task is complete
  }

  @Override public final void onCompletion( CountedCompleter caller ) {
    // Reduce results into 'this' so they collapse going up the execution tree.
    // NULL out child-references so we don't accidentally keep large subtrees
    // alive: each one may be holding large partial results.
    if( _left != null ) reduceAlsoBlock(_left); _left = null;
    if( _rite != null ) reduceAlsoBlock(_rite); _rite = null;
    MemoryManager.freeTaskMem(_reservedMem);
  }
  @Override public final boolean onExceptionalCompletion(Throwable ex, CountedCompleter caller ) {
    MemoryManager.freeTaskMem(_reservedMem);
    return super.onExceptionalCompletion(ex, caller);
  }

}
