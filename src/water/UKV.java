package water;


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
  // This is a WEAK update: it is only strongly ordered with other updates to
  // the SAME key on the SAME node.
  static public void put( Key key, Value val ) {
    Futures fs = new Futures();
    put(key,val,fs);
    fs.blockForPending();         // Block for remote-put to complete
  }
  static public void put( Key key, Value val, Futures fs ) {
    Value res = DKV.put(key,val,fs);
    // If the old Value was a large array, we need to delete the leftover
    // chunks - they are unrelated to the new Value which might be either
    // bigger or smaller than the old Value.
    if( res != null && res._isArray!=0 ) {
      ValueArray ary = ValueArray.value(res);
      for( long i=0; i<ary.chunks(); i++ ) // Delete all the chunks
        DKV.remove(ary.getChunkKey(i),fs);
    }
    if( key._kb[0] == Key.KEY_OF_KEYS ) // Key-of-keys?
      for( Key k : res.flatten() )      // Then recursively delete
        remove(k,fs);
    if( res != null ) res.freeMem();
  }

  static public void remove( Key key ) {
    Futures fs = new Futures();
    remove(key,fs);             // Recursively delete, gather pending deletes
    fs.blockForPending();         // Block until all is deleted
  }
  // Recursively remove, gathering all the pending remote key-deletes
  static private void remove( Key key, Futures fs ) {
    Value val = DKV.get(key,32); // Get the existing Value, if any
    if( val == null ) return;    // Trivial delete
    if( val._isArray != 0 ) { // See if this is an Array
      ValueArray ary = ValueArray.value(val);
      for( long i=0; i<ary.chunks(); i++ ) // Delete all the chunks
        remove(ary.getChunkKey(i),fs);
    }
    if( key._kb[0] == Key.KEY_OF_KEYS ) // Key-of-keys?
      for( Key k : val.flatten() )      // Then recursively delete
        remove(k,fs);
    DKV.remove(key,fs);
  }

  // User-Weak-Get a Key from the distributed cloud.
  // Right now, just gets chunk#0 from a ValueArray, or a normal Value otherwise.
  static public Value get( Key key ) {
    Value val = DKV.get(key);
    if( val != null && val._isArray !=0 ) {
      Key k2 = ValueArray.getChunkKey(0,key);
      Value vchunk0 = DKV.get(k2);
      assert vchunk0 != null : "missed looking for key "+k2+" from "+key;
      return vchunk0;           // Else just get the prefix asked for
    }
    return val;
  }

  static public void put(String s, Value v) { put(Key.make(s), v); }
  //static public Value get(String s) { return get(Key.make(s)); }
  static public void remove(String s) { remove(Key.make(s)); }

  // Also, allow auto-serialization
  static public void put( Key key, Freezable fr ) {
    if( fr == null ) UKV.put(key, null);
    else UKV.put(key,new Value(key, fr.write(new AutoBuffer()).buf()));
  }

  public static <T extends Freezable> T get(Key k, T t) {
    Value v = UKV.get(k);
    if( v == null ) return null;
    return v.get(t);
  }
}
