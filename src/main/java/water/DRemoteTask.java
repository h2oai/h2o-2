package water;
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.google.common.base.Throwables;

/**  A Distributed DTask.
 * Execute a set of Keys on the home for each Key.
 * Limited to doing a map/reduce style.
 */
public abstract class DRemoteTask extends DTask<DRemoteTask> implements Cloneable {
  // Keys to be worked over
  protected Key[] _keys;

  // We can add more things to block on - in case we want a bunch of lazy tasks
  // produced by children to all end before this top-level task ends.
  transient private volatile Futures _fs; // More things to block on

  // Combine results from 'drt' into 'this' DRemoteTask
  abstract public void reduce( DRemoteTask drt );

  // Super-class init on the 1st remote instance of this object.  Caller may
  // choose to clone/fork new instances, but then is reponsible for setting up
  // those instances.
  public void init() { }

  public long memOverheadPerChunk(){return 0;}

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


  // Top-level remote execution hook (called from RPC).  Was passed the keys to
  // execute in _arg.  Fires off jobs to remote machines based on Keys.
  @Override public DRemoteTask invoke( H2ONode sender ) { return dfork().get(); }

  // Invoked with a set of keys
  public DRemoteTask invoke( Key... keys ) { return fork(keys).get(); }
  public DFuture fork( Key... keys ) { keys(keys); return dfork(); }
  public void keys( Key... keys ) { _keys = flatten(keys); }

  public DFuture dfork( ) {

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
    DFuture f = new DFuture(remote_compute(lokeys), remote_compute(hikeys));

    // Setup for local recursion: just use the local keys.
    _keys = locals.toArray(new Key[locals.size()]); // Keys, including local keys (if any)
    if( _keys.length != 0 ) {   // Shortcut for no local work
      init();                   // One-time top-level init
      H2O.submitTask(this);// Begin normal execution on a FJ thread
    }
    return f;             // Block for results from the log-tree splits
  }

  // Junk class only used to allow blocking all results, both local & remote
  public class DFuture {
    private final RPC<DRemoteTask> _lo, _hi;
    DFuture(RPC<DRemoteTask> lo, RPC<DRemoteTask> hi ) {
      _lo = lo;  _hi = hi;
    }
    // Block until completed, without having to catch exceptions
    public DRemoteTask get() {
      try {
        if( _keys.length != 0 )
          DRemoteTask.this.get(); // Block until the self-task is done
      } catch( InterruptedException ie ) {
        throw new RuntimeException(ie);
      } catch( ExecutionException ee ) {
        throw new RuntimeException(ee);
      }
      // Block for remote exec & reduce results into _drt
      if( _lo != null ) reduce(_lo.get());
      if( _hi != null ) reduce(_hi.get());
      if( _fs != null ) _fs.blockForPending(); // Block on all other pending tasks, also
      return DRemoteTask.this;
    }
  };

  private final RPC<DRemoteTask> remote_compute( ArrayList<Key> keys ) {
    if( keys.size() == 0 ) return null;
    DRemoteTask rpc = clone();
    rpc._keys = keys.toArray(new Key[keys.size()]);

    return RPC.call(keys.get(0).home_node(), rpc);// keep the same priority
  }

  private final Key[] flatten( Key[] args ) {
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

  private boolean has_key_of_keys( Key[] args ) {
    for( Key k : args )
      if( k._kb[0] == Key.KEY_OF_KEYS )
        return true;
    return false;
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
    } catch( CloneNotSupportedException cne ) {
      throw Throwables.propagate(cne);
    }
  }
}
