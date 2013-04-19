package water;

import jsr166y.CountedCompleter;
import jsr166y.ForkJoinPool.ManagedBlocker;

/** Map/Reduce style distributed computation. */
public abstract class MRTask extends DRemoteTask {

  transient private int _lo, _hi; // Range of keys to work on
  transient private MRTask _left, _rite; // In-progress execution tree

  static final long log2(long x){
    long y = x >> 1;
    while(y > 0){
      x = y;
      y = x >> 1;
    }
    return x > 0?y+1:y;
  }

  public void init() {
    _lo = 0;
    _hi = _keys.length;
    long reqMem = (log2(_hi - _lo)+2)*memOverheadPerChunk();
    MemoryManager.reserveTaskMem(reqMem); // min. memory required to run at least single threaded
    _reservedMem = reqMem;
  }

  /** Run some useful function over this <strong>local</strong> key, and
   * record the results in the <em>this<em> MRTask. */
  abstract public void map( Key key );

  transient long _reservedMem;
  /** Do all the keys in the list associated with this Node.  Roll up the
   * results into <em>this<em> MRTask. */
  @Override public final void compute2() {
    if( _hi-_lo >= 2 ) { // Multi-key case: just divide-and-conquer to 1 key
      final int mid = (_lo+_hi)>>>1; // Mid-point
      assert _left == null && _rite == null;
      MRTask l = (MRTask)clone();
      MRTask r = (MRTask)clone();
      _left = l; l._reservedMem = 0;
      _rite = r; r._reservedMem = 0;
      _left._hi = mid;          // Reset mid-point
      _rite._lo = mid;          // Also set self mid-point
      setPendingCount(1);
      // Compute min. memory required to run the right branch in parallel.  Min
      // memory equals to the max memory used if the right branch will be
      // executed single threaded (but in parallel with our left branch).
      // Assuming all memory is kept in the tasks and it is halved by reduce
      // operation, the min memory is proportional to the depth of the right
      // subtree.
      long reqMem = (log2(_hi - mid)+3)*memOverheadPerChunk();
      if(MemoryManager.tryReserveTaskMem(reqMem)){
        _reservedMem += reqMem;   // Remember the amount of reserved memory to free it later.
        _left.fork();             // Runs in another thread/FJ instance
      } else {
        _left.compute2();
      }
      _rite.compute2();         // Runs in THIS F/J thread
    } else {
      if( _hi > _lo )           // Single key?
        map(_keys[_lo]);        // Get it, run it locally
      tryComplete();            // And this task is complete
    }
  }

  private final void returnReservedMemory(){
    if(_reservedMem > 0)MemoryManager.freeTaskMem(_reservedMem);
  }

  @Override public final void onCompletion( CountedCompleter caller ) {
    // Reduce results into 'this' so they collapse going up the execution tree.
    // NULL out child-references so we don't accidentally keep large subtrees
    // alive: each one may be holding large partial results.
    if( _left != null ) reduceAlsoBlock(_left); _left = null;
    if( _rite != null ) reduceAlsoBlock(_rite); _rite = null;
    returnReservedMemory();
  }
  @Override public final boolean onExceptionalCompletion(Throwable ex, CountedCompleter caller ) {
    _left = null;
    _rite = null;
    returnReservedMemory();
    return super.onExceptionalCompletion(ex, caller);
  }
}
