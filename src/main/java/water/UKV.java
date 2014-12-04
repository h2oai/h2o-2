package water;

import water.fvec.Vec;

/**
 * User-View Key/Value Store
 *
 * This class handles user-view keys, and hides ArrayLets from the end user.
 *
 *
 * @author <a href="mailto:cliffc@h2o.ai"></a>
 * @version 1.0
 */
public abstract class UKV {

  // This put is a top-level user-update, and not a reflected or retried
  // update.  i.e., The User has initiated a change against the K/V store.
  static public void put( Key key, Value val ) {
    Futures fs = new Futures();
    put(key,val,fs);
    fs.blockForPending();         // Block for remote-put to complete
  }
  static public void put( Key key, Iced val, Futures fs ) { put(key,new Value(key, val),fs); }
  // Do the DKV.put.  DISALLOW this interface for Lockables.  Lockables all
  // have to use the Lockable interface for all updates.
  static public void put( Key key, Value val, Futures fs ) {
    assert !val.isLockable();
    Value res = DKV.put(key,val,fs);
    assert res == null || !res.isLockable();
  }
  // Recursively remove, gathering all the pending remote key-deletes
  static public void remove( Key key ) { remove(key,new Futures()).blockForPending(); }
  static public Futures remove( Key key, Futures fs ) {
    if( key.isVec() ) {
      Value val = DKV.get(key);
      if (val == null) return fs;
      ((Vec)val.get()).remove(fs);
    }
    DKV.remove(key,fs);
    return fs;
  }

  // User-Weak-Get a Key from the distributed cloud.
  // Right now, just gets chunk#0 from a ValueArray, or a normal Value otherwise.
  static public Value getValue( Key key ) {
    Value val = DKV.get(key);
    return val;
  }

  static public void put(String s, Value v) { put(Key.make(s), v); }
  static public void remove(String s) { remove(Key.make(s)); }

  // Also, allow auto-serialization
  static public void put( Key key, Freezable fr ) {
    if( fr == null ) UKV.remove(key);
    else UKV.put(key,new Value(key, fr));
  }

  static public void put( Key key, Iced fr ) {
    if( fr == null ) UKV.remove(key);
    else UKV.put(key,new Value(key, fr));
  }

  public static <T extends Iced> T get(Key k) { 
    Value v = DKV.get(k);
    return (v == null) ? null : (T)v.get();
  }
  public static <T extends Freezable> T get(Key k, Class<T> C) { 
    Value v = DKV.get(k);
    return (v == null) ? null : v.get(C);
  }
}
