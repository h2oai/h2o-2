package water;

import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import water.util.Log;
import jsr166y.CountedCompleter;

/**  A Distributed DTask.
 * Execute a set of Keys on the home for each Key.
 * Limited to doing a map/reduce style.
 */
public abstract class DRemoteTask extends DTask<DRemoteTask> implements Cloneable {
  // Keys to be worked over
  protected Key[] _keys;
  // One-time flips from false to true
  transient private boolean _is_local;
  // Other RPCs we are waiting on
  transient private RPC<DRemoteTask> _lo, _hi;
  // Local work we are waiting on
  transient private DRemoteTask _local;

  // We can add more things to block on - in case we want a bunch of lazy tasks
  // produced by children to all end before this top-level task ends.
  // Semantically, these will all complete before we return from the top-level
  // task.  Pragmatically, we block on a finer grained basis.
  transient private volatile Futures _fs; // More things to block on

  // Combine results from 'drt' into 'this' DRemoteTask
  abstract public void reduce( DRemoteTask drt );

  // Super-class init on the 1st remote instance of this object.  Caller may
  // choose to clone/fork new instances, but then is reponsible for setting up
  // those instances.
  public void init() { }

  // Invokes the task on all nodes
  public void invokeOnAllNodes() {
    H2O cloud = H2O.CLOUD;
    Key[] args = new Key[cloud.size()];
    String skey = "RunOnAll__"+UUID.randomUUID().toString();
    for( int i = 0; i < args.length; ++i )
      args[i] = Key.make(skey,(byte)0,Key.DFJ_INTERNAL_USER,cloud._memary[i]);
    invoke(args);
    for( Key arg : args ) DKV.remove(arg);
  }

  // Invoked with a set of keys
  public DRemoteTask invoke( Key... keys ) { dfork(keys).join(); return this; }
  public DRemoteTask dfork( Key... keys ) { keys(keys); compute2(); return this; }
  public void keys( Key... keys ) { _keys = flatten(keys); }

  // Decide to do local-work or remote-work
  @Override public final void compute2() {
    if( _is_local ) lcompute();
    else            dcompute();
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
      _local = clone();
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
    if( _lo    != null ) reduce(_lo.get());
    if( _hi    != null ) reduce(_hi.get());
    if( _local != null ) reduce(_local   );
    // Note: in theory (valid semantics) we could push these "over the wire"
    // and block for them as we're blocking for the top-level initial split.
    // However, that would require sending "isDone" flags over the wire also.
    // MUCH simpler to just block for them all now, and send over the empty set
    // of not-yet-blocked things.
    if( _local != null && _local._fs != null )
      _local._fs.blockForPending(); // Block on all other pending tasks, also
    _keys = null;                   // Do not return _keys over wire
  };

  private final RPC<DRemoteTask> remote_compute( ArrayList<Key> keys ) {
    if( keys.size() == 0 ) return null;
    DRemoteTask rpc = clone();
    rpc._keys = keys.toArray(new Key[keys.size()]);
    addToPendingCount(1);       // Block until the RPC returns
    // Set self up as needing completion by this RPC: when the ACK comes back
    // we'll get a wakeup.
    return new RPC(keys.get(0).home_node(), rpc).addCompleter(this).call();
  }

  private static Key[] flatten( Key[] args ) {
    if( args.length==1 ) {
      Value val = DKV.get(args[0]);
      // Arraylet: expand into the chunk keys
      if( val != null && val.isArray() ) {
        ValueArray ary = val.get();
        Key[] keys = new Key[(int)ary.chunks()];
        for( int i=0; i<keys.length; i++ )
          keys[i] = ary.getChunkKey(i);
        return keys;
      }
    }
    assert !has_key_of_keys(args);
    return args;
  }

  private static boolean has_key_of_keys( Key[] args ) {
    for( Key k : args )
      if( k._kb[0] == Key.KEY_OF_KEYS )
        return true;
    return false;
  }

  private byte has_remote_keys( ) {
    for( Key k : _keys )
      if( !k.home() )
        return 1;
    return 2;
  }

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

  protected void reduceAlsoBlock( DRemoteTask drt ) {
    reduce(drt);
    alsoBlockFor(drt._fs);
  }

  // Misc

  public static double[][] merge(double[][] a, double[][] b) {
    double[][] res = new double[a.length + b.length][];
    System.arraycopy(a, 0, res, 0, a.length);
    System.arraycopy(b, 0, res, a.length, b.length);
    return res;
  }

  public static int[] merge(int[] a, int[] b) {
    int[] res = new int[a.length + b.length];
    System.arraycopy(a, 0, res, 0, a.length);
    System.arraycopy(b, 0, res, a.length, b.length);
    return res;
  }

  public static String[] merge(String[] a, String[] b) {
    String[] res = new String[a.length + b.length];
    System.arraycopy(a, 0, res, 0, a.length);
    System.arraycopy(b, 0, res, a.length, b.length);
    return res;
  }

  @Override protected DRemoteTask clone() {
    try {
      DRemoteTask dt = (DRemoteTask)super.clone();
      dt.setCompleter(this); // Set completer, what used to be a final field
      dt._fs = null;         // Clone does not depend on extent futures
      dt.setPendingCount(0); // Volatile write for completer field; reset pending count also
      return dt;
    } catch( CloneNotSupportedException e ) {
      throw  Log.errRTExcept(e);
    }
  }
}
