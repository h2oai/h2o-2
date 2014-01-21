package water;

import water.fvec.Frame;
import water.fvec.Vec;

/**
 * User-View Key/Value Store
 *
 * This class handles user-view keys, and hides ArrayLets from the end user.
 *
 *
 * @author <a href="mailto:cliffc@0xdata.com"></a>
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
  static public void put( Key key, Value val, Futures fs ) {
    Value res = DKV.put(key,val,fs);
    // If the old Value was a large array, we need to delete the leftover
    // chunks - they are unrelated to the new Value which might be either
    // bigger or smaller than the old Value.
    if( res != null && res.isArray() )
      remove(res,fs);
  }
  static public void remove( Key key ) { removeAll(new Key[]{key}); }
  static public void removeAll(Key[] keys) {
    Futures fs = new Futures();
    for(Key key: keys) remove(key,fs);
    fs.blockForPending();       // Block until all is deleted
  }
  // Recursively remove, gathering all the pending remote key-deletes
  static public void remove( Key key, Futures fs ) {
    Value val = DKV.get(key,32,H2O.GET_KEY_PRIORITY); // Get the existing Value, if any
    DKV.remove(key,fs); // Might need to be atomic with the above?
    remove(val,fs);
  }
  // Remove the Chunk parts of Frames and Vecs and ValueArrays
  static private void remove( Value val, Futures fs ) {
    if( val == null ) return;   // Trivial delete
    if( val.isArray() ) {       // See if this is an Array
      ValueArray ary = val.get();
      for( long i=0; i<ary.chunks(); i++ ) // Delete all the chunks
        DKV.remove(ary.getChunkKey(i),fs);
    }
    if( val.isVec  () ) ((Vec  )val.get()).remove(fs);
    if( val.isFrame() ) ((Frame)val.get()).remove(fs);
  }

  // User-Weak-Get a Key from the distributed cloud.
  // Right now, just gets chunk#0 from a ValueArray, or a normal Value otherwise.
  static public Value getValue( Key key ) {
    Value val = DKV.get(key);
    if( val != null && val.isArray() ) {
      Key k2 = ValueArray.getChunkKey(0,key);
      Value vchunk0 = DKV.get(k2);
      assert vchunk0 != null : "missed looking for key "+k2+" from "+key;
      return vchunk0;           // Else just get the prefix asked for
    }
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
