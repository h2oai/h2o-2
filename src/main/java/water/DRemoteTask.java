package water;

import java.util.ArrayList;
import java.util.concurrent.*;

import jsr166y.CountedCompleter;
import jsr166y.ForkJoinPool;
import water.DException.DistributedException;
import water.Job.JobCancelledException;
import water.util.Log;

/**  A Distributed DTask.
 * Execute a set of Keys on the home for each Key.
 * Limited to doing a map/reduce style.
 */
public abstract class DRemoteTask<T extends DRemoteTask> extends DTask<T> implements Cloneable, ForkJoinPool.ManagedBlocker {
  // Keys to be worked over
  protected Key[] _keys;
  // One-time flips from false to true
  transient protected boolean _is_local, _top_level;
  // Other RPCs we are waiting on
  transient private RPC<T> _lo, _hi;
  // Local work we are waiting on
  transient private T _local;

  // We can add more things to block on - in case we want a bunch of lazy tasks
  // produced by children to all end before this top-level task ends.
  // Semantically, these will all complete before we return from the top-level
  // task.  Pragmatically, we block on a finer grained basis.
  transient protected volatile Futures _fs; // More things to block on

  // Combine results from 'drt' into 'this' DRemoteTask
  abstract public void reduce( T drt );

  // Support for fluid-programming with strong types
  private final T self() { return (T)this; }

  // Super-class init on the 1st remote instance of this object.  Caller may
  // choose to clone/fork new instances, but then is reponsible for setting up
  // those instances.
  public void init() { }

  // Invokes the task on all nodes
  public T invokeOnAllNodes() {
    H2O cloud = H2O.CLOUD;
    Key[] args = new Key[cloud.size()];
    String skey = "RunOnAll"+Key.rand();
    for( int i = 0; i < args.length; ++i )
      args[i] = Key.make(skey,(byte)0,Key.DFJ_INTERNAL_USER,cloud._memary[i]);
    invoke(args);
    for( Key arg : args ) DKV.remove(arg);
    return self();
  }

  // Invoked with a set of keys
  public T dfork ( Key... keys ) { keys(keys); _top_level=true; compute2(); return self(); }
  public void keys( Key... keys ) { _keys = flatten(keys); }
  public T invoke( Key... keys ) {
    try {
      ForkJoinPool.managedBlock(dfork(keys));
    } catch(InterruptedException  iex) { Log.errRTExcept(iex); }

    // Intent was to quietlyJoin();
    // Which forks, then QUIETLY join to not propagate local exceptions out.
    return self();
  }

  // Return true if blocking is unnecessary, which is true if the Task isDone.
  @Override public boolean isReleasable() {  return isDone();  }
  // Possibly blocks the current thread.  Returns true if isReleasable would
  // return true.  Used by the FJ Pool management to spawn threads to prevent
  // deadlock is otherwise all threads would block on waits.
  @Override public boolean block() throws InterruptedException {
    while( !isDone() ) {
      try { get(); }
      catch(ExecutionException eex) { // skip the execution part
        Throwable tex = eex.getCause();
        if( tex instanceof                 Error) throw (                Error)tex;
        if( tex instanceof  DistributedException) throw ( DistributedException)tex;
        if( tex instanceof JobCancelledException) throw (JobCancelledException)tex;
        throw new RuntimeException(tex);
      }
      catch(CancellationException cex) { Log.errRTExcept(cex); }
    }
    return true;
  }

  // Decide to do local-work or remote-work
  @Override public final void compute2() {
    if( _is_local )
      lcompute();
    else
      dcompute();
  }

  // Decide to do local-completion or remote-completion
  @Override public final void onCompletion( CountedCompleter caller ) {
    if( _is_local ) lonCompletion(caller);
    else            donCompletion(caller);
  }

  // Real Work(tm)!
  public abstract void lcompute(); // Override to specify local work
  private   final void dcompute() {// Work to do the distribution
    // Split out the keys into disjointly-homed sets of keys.
    // Find the split point.  First find the range of home-indices.
    H2O cloud = H2O.CLOUD;
    int lo=cloud._memary.length, hi=-1;
    for( Key k : _keys ) {
      int i = k.home(cloud);
      if( i<lo ) lo=i;
      if( i>hi ) hi=i;        // lo <= home(keys) <= hi
    }

    // Classic fork/join, but on CPUs.
    // Split into 3 arrays of keys: lo keys, hi keys and self keys
    final ArrayList<Key> locals = new ArrayList<Key>();
    final ArrayList<Key> lokeys = new ArrayList<Key>();
    final ArrayList<Key> hikeys = new ArrayList<Key>();
    int self_idx = cloud.nidx(H2O.SELF);
    int mid = (lo+hi)>>>1;    // Mid-point
    for( Key k : _keys ) {
      int idx = k.home(cloud);
      if( idx == self_idx ) locals.add(k);
      else if( idx < mid )  lokeys.add(k);
      else                  hikeys.add(k);
    }

    // Launch off 2 tasks for the other sets of keys, and get a place-holder
    // for results to block on.
    _lo = remote_compute(lokeys);
    _hi = remote_compute(hikeys);

    // Setup for local recursion: just use the local keys.
    if( locals.size() != 0 ) {  // Shortcut for no local work
      _local = clone();         // 'this' is completer for '_local', so awaits _local completion
      _local._is_local = true;
      _local._keys = locals.toArray(new Key[locals.size()]); // Keys, including local keys (if any)
      _local.init();            // One-time top-level init
      H2O.submitTask(_local);   // Begin normal execution on a FJ thread
    } else {
      tryComplete();            // No local work, so just immediate tryComplete
    }
  }

  // Real Completion(tm)!
  public        void lonCompletion( CountedCompleter caller ) { } // Override for local completion
  private final void donCompletion( CountedCompleter caller ) {   // Distributed completion
    assert _lo == null || _lo.isDone();
    assert _hi == null || _hi.isDone();
    // Fold up results from left & right subtrees
    if( _lo    != null ) reduce2(_lo.get());
    if( _hi    != null ) reduce2(_hi.get());
    if( _local != null ) reduce2(_local   );
    // Note: in theory (valid semantics) we could push these "over the wire"
    // and block for them as we're blocking for the top-level initial split.
    // However, that would require sending "isDone" flags over the wire also.
    // MUCH simpler to just block for them all now, and send over the empty set
    // of not-yet-blocked things.
    if(_local != null && _local._fs != null )
      _local._fs.blockForPending(); // Block on all other pending tasks, also
    _keys = null;                   // Do not return _keys over wire
    if( _top_level ) postGlobal();
  };
  // Override to do work after all the forks have returned
  protected void postGlobal(){}

  // 'Reduce' left and right answers.  Gather exceptions
  private void reduce2( T drt ) {
    if( drt == null ) return;
    reduce(drt);
  }

  private final RPC<T> remote_compute( ArrayList<Key> keys ) {
    if( keys.size() == 0 ) return null;
    DRemoteTask rpc = clone();
    rpc.setCompleter(null);
    rpc._keys = keys.toArray(new Key[keys.size()]);
    addToPendingCount(1);       // Block until the RPC returns
    // Set self up as needing completion by this RPC: when the ACK comes back
    // we'll get a wakeup.
    return new RPC(keys.get(0).home_node(), rpc).addCompleter(this).call();
  }

  private static Key[] flatten( Key[] args ) { return args; }

  public Futures getFutures() {
    if( _fs == null ) synchronized(this) { if( _fs == null ) _fs = new Futures(); }
    return _fs;
  }

  public void alsoBlockFor( Future f ) {
    if( f == null ) return;
    getFutures().add(f);
  }

  public void alsoBlockFor( Futures fs ) {
    if( fs == null ) return;
    getFutures().add(fs);
  }

  protected void reduceAlsoBlock( T drt ) {
    reduce(drt);
    alsoBlockFor(drt._fs);
  }

  @Override public T clone() {
    T dt = (T)super.clone();
    dt.setCompleter(this); // Set completer, what used to be a final field
    dt._fs = null;         // Clone does not depend on extent futures
    dt.setPendingCount(0); // Volatile write for completer field; reset pending count also
    return dt;
  }
}
