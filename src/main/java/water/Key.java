package water;

import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

/**
* Keys
*
* This class defines:
* - A Key's bytes (name) & hash
* - Known Disk & memory replicas.
* - A cache of somewhat expensive to compute stuff related to the current
* Cloud, plus a byte of the desired replication factor.
*
* Keys are expected to be a high-count item, hence the care about size.
*
* Keys are *interned* in the local K/V store, a non-blocking hash set and are
* kept pointer equivalent (via the interning) for cheap compares. The
* interning only happens after a successful insert in the local H2O.STORE via
* H2O.put_if_later.
*
* @author <a href="mailto:cliffc@0xdata.com"></a>
* @version 1.0
*/
public final class Key extends Iced implements Comparable {

  // The Key!!!
  // Limited to 512 random bytes - to fit better in UDP packets.
  public static final int KEY_LENGTH = 512;
  public byte[] _kb;            // Key bytes, wire-line protocol
  transient int _hash;          // Hash on key alone (and not value)

  // The user keys must be ASCII, so the values 0..31 are reserved for system
  // keys. When you create a system key, please do add its number to this list

  public static final byte ARRAYLET_CHUNK = 0;

  public static final byte KEY_OF_KEYS = 1;

  public static final byte BUILT_IN_KEY = 2; // C.f. Constants.BUILT_IN_KEY_*

  public static final byte JOB = 3;

  public static final byte HDFS_INTERNAL_BLOCK = 10;

  public static final byte HDFS_INODE = 11;

  public static final byte HDFS_BLOCK_INFO = 12;

  public static final byte HDFS_BLOCK_SHADOW = 13;

  public static final byte DFJ_INTERNAL_USER = 14;

  public static final byte USER_KEY = 32;

  // 64 bits of Cloud-specific cached stuff. It is changed atomically by any
  // thread that visits it and has the wrong Cloud. It has to be read *in the
  // context of a specific Cloud*, since a re-read may be for another Cloud.
  private transient volatile long _cache;
  private static final AtomicLongFieldUpdater<Key> _cacheUpdater =
    AtomicLongFieldUpdater.newUpdater(Key.class, "_cache");

  // Accessors and updaters for the Cloud-specific cached stuff.
  // The Cloud index, a byte uniquely identifying the last 256 Clouds. It
  // changes atomically with the _cache word, so we can tell which Cloud this
  // data is a cache of.
  private static int cloud( long cache ) { return (int)(cache>>> 0)&0x00FF; }
  // Shortcut node index for Home replica#0. This replica is responsible for
  // breaking ties on writes. 'char' because I want an unsigned 16bit thing,
  // limit of 65534 Cloud members. -1 is reserved for a bare-key
  private static int home ( long cache ) { return (int)(cache>>> 8)&0xFFFF; }
  // Our replica #, or -1 if we're not one of the first 127 replicas. This
  // value is found using the Cloud distribution function and changes for a
  // changed Cloud.
  private static int replica(long cache) { return (byte)(cache>>>24)&0x00FF; }
  // Desired replication factor. Can be zero for temp keys. Not allowed to
  // later, because it messes with e.g. meta-data on disk.
  private static int desired(long cache) { return (int)(cache>>>32)&0x00FF; }

  private static long build_cache( int cidx, int home, int replica, int desired ) {
    return // Build the new cache word
        ((long)(cidx &0xFF)<< 0) |
        ((long)(home &0xFFFF)<< 8) |
        ((long)(replica&0xFF)<<24) |
        ((long)(desired&0xFF)<<32) |
        ((long)(0 )<<40);
  }

  public int home ( H2O cloud ) { return home (cloud_info(cloud)); }
  public int replica( H2O cloud ) { return replica(cloud_info(cloud)); }
  public int desired( ) { return desired(_cache); }
  public boolean home() { return H2O.CLOUD._memary[home(H2O.CLOUD)]==H2O.SELF; }
  public H2ONode home_node( ) {
    H2O cloud = H2O.CLOUD;
    return cloud._memary[home(cloud)];
  }

  // Update the cache, but only to strictly newer Clouds
  private boolean set_cache( long cache ) {
    while( true ) { // Spin till get it
      long old = _cache; // Read once at the start
      if( !H2O.larger(cloud(cache),cloud(old)) ) // Rolling backwards?
        // Attempt to set for an older Cloud. Blow out with a failure; caller
        // should retry on a new Cloud.
        return false;
      assert cloud(cache) != cloud(old) || cache == old;
      if( old == cache ) return true; // Fast-path cutout
      if( _cacheUpdater.compareAndSet(this,old,cache) ) return true;
      // Can fail if the cache is really old, and just got updated to a version
      // which is still not the latest, and we are trying to update it again.
    }
  }
  // Return the info word for this Cloud. Use the cache if possible
  public long cloud_info( H2O cloud ) {
    long x = _cache;
    // See if cached for this Cloud. This should be the 99% fast case.
    if( cloud(x) == cloud._idx ) return x;

    // Cache missed! Probaby it just needs (atomic) updating.
    // But we might be holding the stale cloud...
    // Figure out home Node in this Cloud
    char home = (char)cloud.D(this,0);
    // Figure out what replica # I am, if any
    int desired = desired(x);
    int replica = -1;
    for( int i=0; i<desired; i++ ) {
      int idx = cloud.D(this,i);
      if( idx >= 0 && cloud._memary[idx] == H2O.SELF ) {
        replica = i;
        break;
      }
    }
    long cache = build_cache(cloud._idx,home,replica,desired);
    set_cache(cache); // Attempt to upgrade cache, but ignore failure
    return cache; // Return the magic word for this Cloud
  }

  // Default desired replication factor. Unless specified otherwise, all new
  // k-v pairs start with this replication factor.
  public static final byte DEFAULT_DESIRED_REPLICA_FACTOR = 2;

  // Construct a new Key.
  private Key(byte[] kb) {
    if( kb.length > KEY_LENGTH ) throw new IllegalArgumentException("Key length would be "+kb.length);
    _kb = kb;
    // For arraylets, arrange that the first 64Megs/Keys worth spread nicely,
    // but that the next 64Meg (and each 64 after that) target the same node,
    // so that HDFS blocks hit only 1 node in the typical spread.
    int i=0; // Include these header bytes or not
    int chk = 0; // Chunk number, for chunks beyond 64Meg
    if( kb.length >= 10 && kb[0] == ARRAYLET_CHUNK && kb[1] == 0 ) {
      long off = UDP.get8(kb,2);
      if( (off >> 20) >= 64 ) { // Is offset >= 64Meg?
        i += 2+8; // Skip the length bytes; they are now not part of hash
        chk = (int)(off >>> (6+20)); // Divide by 64Meg; comes up with a "block number"
      }
    }
    int hash = 0;
    // Quicky hash: http://en.wikipedia.org/wiki/Jenkins_hash_function
    for( ; i<kb.length; i++ ) {
      hash += kb[i];
      hash += (hash << 10);
      hash ^= (hash >> 6);
    }
    hash += (hash << 3);
    hash ^= (hash >> 11);
    hash += (hash << 15);
    _hash = hash+chk; // Add sequential block numbering
  }

  // Make new Keys.  Optimistically attempt interning, but no guarantee.
  static public Key make(byte[] kb, byte rf) {
    if( rf == -1 ) throw new IllegalArgumentException();
    Key key = new Key(kb);
    Key key2 = H2O.getk(key); // Get the interned version, if any
    if( key2 != null ) // There is one! Return it instead
      return key2;
    // Set the cache with desired replication factor, and a fake cloud index
    H2O cloud = H2O.CLOUD; // Read once
    key._cache = build_cache(cloud._idx-1,0,0,rf);
    key.cloud_info(cloud); // Now compute & cache the real data
    return key;
  }
  static public Key make(byte[] kb) { return make(kb,DEFAULT_DESIRED_REPLICA_FACTOR); }
  static public Key make(String s) { return make(decodeKeyName(s));}
  static public Key make(String s, byte rf) { return make(decodeKeyName(s), rf);}
  static public Key make() { return make( UUID.randomUUID().toString() ); }

  // Make a particular system key that is homed to given node and possibly
  // specifies also other 2 replicas. Works for both IPv4 and IPv6 addresses.
  // If the addresses are not specified, returns a key with no home information.
  static public Key make(String s, byte rf, byte systemType, H2ONode... replicas) {
    return make(decodeKeyName(s),rf,systemType,replicas);
  }


  // Make a Key which is homed to specific nodes.
  static public Key make(byte[] kb, byte rf, byte systemType, H2ONode... replicas) {
    // no more than 3 replicas allowed to be stored in the key
    assert 0 <=replicas.length && replicas.length<=3;
    assert systemType<32; // only system keys allowed
    // Key byte layout is:
    // 0 - systemType, from 0-31
    // 1 - replica-count, plus up to 3 bits for ip4 vs ip6
    // 2-n - zero, one, two or 3 IP4 (4+2 bytes) or IP6 (16+2 bytes) addresses
    // n+ - kb.length, repeat of the original kb
    AutoBuffer ab = new AutoBuffer();
    ab.put1(systemType).put1(replicas.length);
    for( H2ONode h2o : replicas )
      h2o.write(ab);
    ab.putA1(kb);
    return make(Arrays.copyOf(ab.buf(),ab.position()),rf);
  }

  // Custom Serialization Reader: Keys must be interned on construction.
  public Key read(AutoBuffer bb) { return make(bb.getA1()); }
  public AutoBuffer write(AutoBuffer bb) { return bb.putA1(_kb); }


  // Expand a KEY_OF_KEYS into an array of keys
  public Key[] flatten() {
    assert (_kb[0]&0xFF)==KEY_OF_KEYS;
    Value val = DKV.get(this);
    if( val == null ) return null;
    return ((Key.Ary)val.get())._keys;
  }


  // User keys must be all ASCII, but we only check the 1st byte
  public boolean user_allowed() {
    return (_kb[0]&0xFF) >= 32;
  }

  // Returns the type of the key.
  public int type() {
    return ((_kb[0]&0xff)>=32) ? USER_KEY : (_kb[0]&0xff);
  }


  public static final char MAGIC_CHAR = '$';
  private static final char[] HEX = "0123456789abcdef".toCharArray();

  /** Converts the key to HTML displayable string.
   *
   * For user keys returns the key itself, for system keys returns their
   * hexadecimal values.
   *
   * @return key as a printable string
   */
  public String toString() {
    int len = _kb.length;
    while( --len >= 0 ) {
      char a = (char) _kb[len];
      if (' ' <= a && a <= '#') continue;
      // then we have $ which is not allowed
      if ('%' <= a && a <= '~') continue;
      // already in the one above
      //if( 'a' <= a && a <= 'z' ) continue;
      //if( 'A' <= a && a <= 'Z' ) continue;
      //if( '0' <= a && a <= '9' ) continue;
      break;
    }
    if (len>=0) {
      StringBuilder sb = new StringBuilder();
      sb.append(MAGIC_CHAR);
      for( int i = 0; i <= len; ++i ) {
        byte a = _kb[i];
        sb.append(HEX[(a >> 4) & 0x0F]);
        sb.append(HEX[(a >> 0) & 0x0F]);
      }
      sb.append(MAGIC_CHAR);
      for( int i = len + 1; i < _kb.length; ++i ) sb.append((char)_kb[i]);
      return sb.toString();
    } else {
      return new String(_kb);
    }
  }

  private static byte[] decodeKeyName(String what) {
    if( what==null ) return null;
    if (what.charAt(0) == MAGIC_CHAR) {
      int len = what.indexOf(MAGIC_CHAR,1);
      String tail = what.substring(len+1);
      byte[] res = new byte[(len-1)/2 + tail.length()];
      int r = 0;
      for( int i = 1; i < len; i+=2 ) {
        char h = what.charAt(i);
        char l = what.charAt(i+1);
        h -= Character.isDigit(h) ? '0' : ('a' - 10);
        l -= Character.isDigit(l) ? '0' : ('a' - 10);
        res[r++] = (byte)(h << 4 | l);
      }
      System.arraycopy(tail.getBytes(), 0, res, r, tail.length());
      return res;
    } else {
      return what.getBytes();
    }
  }

  public int hashCode() { return _hash; }
  public boolean equals( Object o ) {
    if( this == o ) return true;
    Key k = (Key)o;
    return Arrays.equals(k._kb,_kb);
  }

  public int compareTo(Object o) {
    assert (o instanceof Key);
    return this.toString().compareTo(o.toString());
  }

  // Simple wrapper class defining an array-of-keys that is serializable.
  // Note that if you modify any fields of a POJO that is part of a Value,
  // - this is not the recommended programming style,
  // - those changes are visible to all on the node,
  // - but not to other nodes
  // - and the POJO might be dropped by the MemoryManager and reconstitued from
  //   disk and/or the byte array back to it's original form.
  public static class Ary extends Iced {
    public final Key[] _keys;
    Ary( Key[] keys ) { _keys = keys; }
  }
}
