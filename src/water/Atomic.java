package water;

/**
 * Atomic update of a Key
 *
 * @author <a href="mailto:cliffc@0xdata.com"></a>
 * @version 1.0
 */

public abstract class Atomic extends DTask {
  public Key _key;              // Transaction key

  // User's function to be run atomically.  The Key's Value is fetched from the
  // home STORE, and the bits are passed in.  The returned bits are atomically
  // installed as the new Value (the function is retried until it runs
  // atomically).  The original bits are supposed to be read-only.
  abstract public byte[] atomic( byte[] bits );

  /** Executed on the transaction key's <em>home</em> node after any successful
   *  atomic update.
   */
  // override this if you need to perform some action after the update succeeds (eg cleanup)
  public void onSuccess(){}

  // Only invoked remotely; this is now the key's home and can be directly executed
  @Override public final Atomic invoke( H2ONode sender ) {  compute(); return this; }

  /** Block until it completes, even if run remotely */
  public final Atomic invoke( Key key ) {
    RPC<Atomic> rpc = fork(key);
    if( rpc != null ) rpc.get(); // Block for it
    return this;
  }

  // Fork off
  public final RPC<Atomic> fork(Key key) {
    _key = key;
    if( key.home() ) {          // Key is home?
      compute();                // Also, run it blocking/now
      return null;
    } else {                    // Else run it remotely
      return RPC.call(key.home_node(),this);
    }
  }

  // The (remote) workhorse:
  @Override public final void compute( ) {
    assert _key.home();         // Key is at Home!
    Futures fs = new Futures(); // Must block on all invalidates eventually
    while( true ) {
      Value val1 = DKV.get(_key);
      byte[] bits1 = null;
      if( val1 != null ) {      // Got a mapping?
        bits1 = val1.get();     // Get the bits
        if( bits1 == null )     // Assume XTN failure & try again
          continue;             // No bits?  deleted value already?
      }

      // Run users' function.  This is supposed to read-only from bits1 and
      // return new bits2 to atomically install.
      byte[] bits2 = atomic(bits1);
      assert bits1 == null || bits1 != bits2; // No returning the same array
      if( bits2 == null ) break; // they gave up

      // Attempt atomic update
      Value val2 = new Value(_key, bits2.length, bits2);
      Value res = DKV.DputIfMatch(_key,val2,val1,fs);
      if( res == val1 ) break;  // Success?
    }                           // and retry
    onSuccess();                // Call user's post-XTN function
    _key = null;                // No need for key no more
    fs.blockForPending();         // Block for any pending invalidates on the atomic update
    tryComplete();              // Tell F/J this task is done
  }
}
