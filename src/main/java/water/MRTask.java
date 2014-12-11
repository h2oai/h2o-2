package water;

import jsr166y.CountedCompleter;

/** Map/Reduce style distributed computation. */
public abstract class MRTask<T extends MRTask> extends DRemoteTask<T> {

  transient protected int _lo, _hi; // Range of keys to work on
  transient private T _left, _rite; // In-progress execution tree

  // This method is another backpressure mechanism to make sure we do not
  // exhaust system's resources by running too many tasks at the same time.
  // Tasks are expected to reserve memory before proceeding with their
  // execution and making sure they release it when done.
  public long memOverheadPerChunk() { return 0; }

  static final long log2(long a) {
    long x = a, y = 0;
    while( (x >>= 1) > 0 ) ++y;
    return (a > (1L << y)) ? y+1 : y;
  }

  @Override public void init() {
    _fs = new Futures();
    _lo = 0;
    _hi = _keys.length;
    long reqMem = (log2(_hi - _lo)+2)*memOverheadPerChunk();
    MemoryManager.reserveTaskMem(reqMem); // min. memory required to run at least single threaded
    _reservedMem = reqMem;
  }

  /** Run some useful function over this <strong>local</strong> key, and
   * record the results in the <em>this</em> MRTask. */
  abstract public void map( Key key );

  protected boolean _runSingleThreaded = false;
  transient long _reservedMem;
  /** Do all the keys in the list associated with this Node.  Roll up the
   * results into <em>this</em> MRTask. */
  @Override public final void lcompute() {
    if( _hi-_lo >= 2 ) { // Multi-key case: just divide-and-conquer to 1 key
      final int mid = (_lo+_hi)>>>1; // Mid-point
      assert _left == null && _rite == null;
      T l = clone();
      T r = clone();
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

      if(!_runSingleThreaded && MemoryManager.tryReserveTaskMem(reqMem)){
        _reservedMem += reqMem;   // Remember the amount of reserved memory to free it later.
        _left.fork();             // Runs in another thread/FJ instance
      } else {
        _left.compute2();
      }
      _rite.compute2();         // Runs in THIS F/J thread
    } else {
      if( _hi > _lo ) {         // Single key?
        try {
          map(_keys[_lo]);      // Get it, run it locally
        } catch( RuntimeException re ) { // Catch user-map-thrown exceptions
          throw H2O.setDetailMessage(re,re.getMessage()+" while mapping key "+_keys[_lo]);
        } catch( AssertionError re ) { // Catch user-map-thrown exceptions
          throw H2O.setDetailMessage(re,re.getMessage()+" while mapping key "+_keys[_lo]);
        } catch( OutOfMemoryError re ) { // Catch user-map-thrown exceptions
          throw H2O.setDetailMessage(re,re.getMessage()+" while mapping key "+_keys[_lo]);
        }
      }
      tryComplete();            // And this task is complete
    }
  }

  private final void returnReservedMemory(){
    if(_reservedMem > 0)MemoryManager.freeTaskMem(_reservedMem);
  }

  @Override public void lonCompletion( CountedCompleter caller ) {
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

  // Caveat Emptor:
  // Hopefully used for debugging only... not only are these likely to change
  // in the near future, there's very few guarantees placed on these values.
  // At various points they are chunk-number ranges (before & during maps), and
  // stale values that *look* like ranges but are not (during reduces) or maybe
  // they will morph into row#'s (new not-yet-ready api) and/or forms of
  // "visited" flags (also new api).
  public final int lo() { return _lo; }
  public final int hi() { return _hi; }
}
