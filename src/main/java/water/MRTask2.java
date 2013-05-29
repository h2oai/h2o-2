package water;

import java.util.concurrent.ExecutionException;
import java.util.Arrays;
import jsr166y.CountedCompleter;
import sun.misc.Unsafe;         // 
import water.fvec.BigVector;
import water.fvec.Vec;
import water.nbhm.UtilUnsafe;
import water.util.Log;

/** Map/Reduce style distributed computation. */
public abstract class MRTask2<T extends MRTask2> extends DTask implements Cloneable {

  // The Vectors to work on
  private Vec[] _vecs;            // Vectors to work on

  // Run some useful function over this <strong>local</strong> BigVector, and
  // record the results in the <em>this<em> MRTask2.
  abstract public void map( long start, int len, BigVector bv );

  // Combine results from 'mrt' into 'this' MRTask2.  Both 'this' and 'mrt' are
  // guaranteed to either have map() run on them, or be the results of a prior
  // reduce().  Reduce is optional if, e.g., the result is some output vector.
  public void reduce( T mrt ) { }

  // Sub-class init on the 1st remote instance of this object, for initializing
  // node-local shared data structures.
  public void init() { }

  // Remote/Global work: other nodes we are awaiting results from
  protected int _nlo, _nhi;           // Range of NODEs to work on - remotely
  transient protected RPC<T> _nleft, _nrite;
  transient boolean _topLocal;        // Top-level local call, returning results over the wire
  // Local work: range of local chunks we are working on
  transient protected int _lo, _hi;   // Range of BigVectors to work on - locally
  transient protected T _left, _rite; // In-progress execution tree
  transient protected T _res;         // Result

  // We can add more things to block on - in case we want a bunch of lazy tasks
  // produced by children to all end before this top-level task ends.
  // Semantically, these will all complete before we return from the top-level
  // task.  Pragmatically, we block on a finer grained basis.
  transient protected Futures _fs; // More things to block on

  // Support for fluid-programming with strong types
  private final T self() { return (T)this; }

  // Top-level blocking call.
  public final T invoke( Vec... vecs ) { 
    checkCompatible(vecs);      // Check for compatible vectors
    _vecs = vecs;               // Record vectors to work on
    _nlo = 0;  _nhi = H2O.CLOUD.size(); // Do Whole Cloud
    setupLocal();               // Local setup
    H2O.submitTask(this);       // Begin normal execution on a FJ thread
    try { get(); }              // Block until done
    catch( InterruptedException e ) { Log.errRTExcept(e); }
    catch(   ExecutionException e ) { Log.errRTExcept(e); }
    return self();
  }
  
  // Called once on remote at top level, probably with a subset of the cloud.
  @Override public final void dinvoke(H2ONode sender) {
    setupLocal();               // Local setup
    compute2();                 // Do The Main Work
    // nothing here... must do any post-work-cleanup in onCompletion
  }

  // Setup for local work: fire off any global work to cloud neighbors; do all
  // chunks; call user's init.
  private final void setupLocal() {
    _topLocal = true;
    // Check for global vs local work
    if( _nlo < _nhi-1 ) {       // Have global work?
      // Note: some top-level calls have no other global work, so
      // "topLocal==true" does not imply "nlo < nhi-1".
      int selfidx = H2O.SELF.index();
      if( _nlo   < selfidx ) _nleft = remote_compute(_nlo, selfidx );
      if( selfidx+1 < _nhi ) _nrite = remote_compute(selfidx+1,_nhi);
    }
    _lo = 0;  _hi = _vecs[0].nChunks(); // Do All Chunks
    init();                     // Setup any user's shared local structures
  }

  // Make an RPC call to some node in the middle of the given range.  Add a
  // pending completion to self, so that we complete when the RPC completes.
  private final RPC<T> remote_compute( int lo, int hi ) {
    int mid = (hi+lo)>>>1;
    T rpc = clone();
    rpc._nlo = lo;
    rpc._nhi = hi;
    addToPendingCount(1);       // Not complete until the RPC returns
    // Set self up as needing completion by this RPC: when the ACK comes back
    // we'll get a wakeup.
    return new RPC(H2O.CLOUD._memary[mid], rpc).addCompleter(this).call();
  }

  // Called from FJ threads to do local work.  The first called Task (which is
  // also the last one to Complete) also reduces any global work.
  @Override public final void compute2() {
    if( _hi-_lo >= 2 ) { // Multi-chunk case: just divide-and-conquer to 1 chunk
      final int mid = (_lo+_hi)>>>1; // Mid-point
      assert _left == null && _rite == null && _res == null;
      _left = clone(); 
      _rite = clone(); 
      _left._hi = mid;          // Reset mid-point
      _rite._lo = mid;          // Also set self mid-point
      addToPendingCount(1);     // One fork awaiting completion
      _left.fork();             // Runs in another thread/FJ instance
      _rite.compute2();         // Runs in THIS F/J thread
      return;                   // Not complete until the fork completes
    } 
    // Zero or 1 chunks, and further chunk might not be homed here
    if( _hi > _lo ) {           // Single chunk?
      Vec v0 = _vecs[0];        // Use 0th vector to gate home/not-home
      long start = v0.chunk2StartElem(_lo);
      Key dkey = v0.chunkKey(_lo);
      if( dkey.home() ) {       // And chunk is homed here?
        BigVector bv = v0.elem2BV(start);
        if( _vecs.length > 1 ) throw H2O.unimpl();
        map(start, bv._len, bv); // Get it, run it locally
        _res = self();          // Save results since called map() at least once!
      }
    }
    tryComplete();              // And this task is complete
  }

  // OnCompletion - reduce the left & right into self
  @Override public final void onCompletion( CountedCompleter caller ) {
    // Reduce results into 'this' so they collapse going up the execution tree.
    // NULL out child-references so we don't accidentally keep large subtrees
    // alive since each one may be holding large partial results.
    reduce2(_left); _left = null;
    reduce2(_rite); _rite = null;
    // Only on the top local call, have more completion work
    if( _topLocal ) postLocal();
  }
      
  // Call 'reduce' on pairs of mapped MRTask2's.
  // Collect all pending Futures from both parties as well.
  private void reduce2( MRTask2<T> mrt ) {
    if( mrt == null ) return;
    if( _res == null ) _res = mrt._res;
    else if( mrt._res != null ) _res.reduce(mrt._res);
    if( _fs == null ) _fs = mrt._fs;
    else _fs.add(mrt._fs);
  }

  // Work done after all the main local work is done.
  // Gather/reduce remote work.
  // Block for other queued pending tasks.
  // Copy any final results into 'this', such that a return of 'this' has the results.
  private void postLocal() {
    reduce3(_nleft);            // Reduce global results from neighbors. 
    reduce3(_nrite);
    if( _fs != null )           // Block on all other pending tasks, also
      _fs.blockForPending(); 
    // Finally, must return all results in 'this' because that is the API -
    // what the user expects
    if( _res != null && _res != this )
      copyOver(_res);
  }

  // Block for RPCs to complete, then reduce global results into self results
  private void reduce3( RPC<T> rpc ) {
    if( rpc == null ) return;
    T mrt = rpc.get();          // This is a blocking remote call
    // Unlike reduce2, results are in mrt directly not mrt._res.
    if( _res == null ) _res = mrt;
    else _res.reduce(mrt);
    assert mrt._fs == null;     // No blockable results from remote
  }

  // Cancel/kill all work as we can, then rethrow... do not invisibly swallow
  // exceptions (which is the F/J default)
  @Override public final boolean onExceptionalCompletion(Throwable ex, CountedCompleter caller ) {
    if( _nleft != null ) _nleft.cancel(true); _nleft = null;
    if( _nrite != null ) _nrite.cancel(true); _nrite = null;
    _left = null;
    _rite = null;
    return super.onExceptionalCompletion(ex, caller);
  }

  // Local Clone - setting final-field completer
  @Override protected T clone() {
    try {
      T x = (T)super.clone();
      x.setCompleter(this); // Set completer, what used to be a final field
      x._topLocal = false;  // Not a top job
      x._nleft = x._nrite = null;
      x. _left = x. _rite = null;
      x._fs = null;         // Clone does not depend on extent futures
      x.setPendingCount(0); // Volatile write for completer field; reset pending count also
      return x;
    } catch( CloneNotSupportedException e ) {
      throw Log.errRTExcept(e);
    }
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
