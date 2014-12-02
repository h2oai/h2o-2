package water;

import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import water.util.UnsafeUtils;

/**
* Keys
*
* This class defines:
* - A Key's bytes (name) and hash
* - Known Disk and memory replicas.
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
* @author <a href="mailto:cliffc@h2o.ai"></a>
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

  public static final byte BUILT_IN_KEY = 2; // C.f. Constants.BUILT_IN_KEY_*

  public static final byte JOB = 3;

  public static final byte VEC = 4;
  public static final byte DVEC = 5;
  public static final byte VGROUP = 6; // vector group

  public static final byte DFJ_INTERNAL_USER = 7;

  public static final byte HIDDEN_USER_KEY = 31;

  public static final byte USER_KEY = 32;

  // *Desired* distribution function on keys & replication factor. Replica #0
  // is the master, replica #1, 2, 3, etc represent additional desired
  // replication nodes. Note that this function is just the distribution
  // function - it does not DO any replication, nor does it dictate any policy
  // on how fast replication occurs. Returns -1 if the desired replica
  // is nonsense, e.g. asking for replica #3 in a 2-Node system.
  int D( int repl ) {
    int hsz = H2O.CLOUD.size();
  
    // See if this is a specifically homed Key
    if( !user_allowed() && repl < _kb[1] ) { // Asking for a replica# from the homed list?
      assert _kb[0] != Key.DVEC;
      H2ONode h2o = H2ONode.intern(_kb,2+repl*(4+2/*serialized bytesize of H2OKey*/));
      // Reverse the home to the index
      int idx = h2o.index();
      if( idx >= 0 ) return idx;
      // Else homed to a node which is no longer in the cloud!
      // Fall back to the normal home mode
    }
  
    // Distribution of Fluid Vectors is a special case.
    // Fluid Vectors are grouped into vector groups, each of which must have
    // the same distribution of chunks so that MRTask2 run over group of
    // vectors will keep data-locality.  The fluid vecs from the same group
    // share the same key pattern + each has 4 bytes identifying particular
    // vector in the group.  Since we need the same chunks end up on the same
    // node in the group, we need to skip the 4 bytes containing vec# from the
    // hash.  Apart from that, we keep the previous mode of operation, so that
    // ByteVec would have first 64MB distributed around cloud randomly and then
    // go round-robin in 64MB chunks.
    if( _kb[0] == DVEC ) {
      // Homed Chunk?
      if( _kb[1] != -1 ) throw H2O.unimpl();
      // For round-robin on Chunks in the following pattern:
      // 1 Chunk-per-node, until all nodes have 1 chunk (max parallelism).
      // Then 2 chunks-per-node, once around, then 4, then 8, then 16.
      // Getting several chunks-in-a-row on a single Node means that stencil
      // calculations that step off the end of one chunk into the next won't
      // force a chunk local - replicating the data.  If all chunks round robin
      // exactly, then any stencil calc will double the cached volume of data
      // (every node will have it's own chunk, plus a cached next-chunk).
      // Above 16-chunks-in-a-row we hit diminishing returns.
      int cidx = UnsafeUtils.get4(_kb, 1 + 1 + 4); // Chunk index
      int x = cidx/hsz; // Multiples of cluster size
      // 0 -> 1st trip around the cluster;            nidx= (cidx- 0*hsz)>>0
      // 1,2 -> 2nd & 3rd trip; allocate in pairs:    nidx= (cidx- 1*hsz)>>1
      // 3,4,5,6 -> next 4 rounds; allocate in quads: nidx= (cidx- 3*hsz)>>2
      // 7-14 -> next 8 rounds in octets:             nidx= (cidx- 7*hsz)>>3
      // 15+ -> remaining rounds in groups of 16:     nidx= (cidx-15*hsz)>>4
      int z = x==0 ? 0 : (x<=2 ? 1 : (x<=6 ? 2 : (x<=14 ? 3 : 4)));
      int nidx = (cidx-((1<<z)-1)*hsz)>>z;
      return ((nidx+repl)&0x7FFFFFFF) % hsz;
    }
  
    // Easy Cheesy Stupid:
    return ((_hash+repl)&0x7FFFFFFF) % hsz;
  }


  /** List of illegal characters which are not allowed in user keys. */
  public static final CharSequence ILLEGAL_USER_KEY_CHARS = " !@#$%^&*()+={}[]|\\;:\"'<>,/?";

  // 64 bits of Cloud-specific cached stuff. It is changed atomically by any
  // thread that visits it and has the wrong Cloud. It has to be read *in the
  // context of a specific Cloud*, since a re-read may be for another Cloud.
  private transient volatile long _cache;
  private static final AtomicLongFieldUpdater<Key> _cacheUpdater =
    AtomicLongFieldUpdater.newUpdater(Key.class, "_cache");

  public final boolean isVec () { return _kb != null && _kb.length > 0 && _kb[0] == VEC; }
  public final boolean isChunkKey() { return _kb != null && _kb.length > 0 && _kb[0] == DVEC; }
  public final Key getVecKey() { assert isChunkKey(); return water.fvec.Vec.getVecKey(this); }

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
  public boolean home() { return home_node()==H2O.SELF; }
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
    char home = (char)D(0);
    // Figure out what replica # I am, if any
    int desired = desired(x);
    int replica = -1;
    for( int i=0; i<desired; i++ ) {
      int idx = D(i);
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
    // Quicky hash: http://en.wikipedia.org/wiki/Jenkins_hash_function
    int hash = 0;
    for( byte b : kb ) {
      hash += b;
      hash += (hash << 10);
      hash ^= (hash >> 6);
    }
    hash += (hash << 3);
    hash ^= (hash >> 11);
    hash += (hash << 15);
    _hash = hash;
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

  // A random string, useful as a Key name or partial Key suffix.
  static public String rand() {
    UUID uid = UUID.randomUUID();
    long l1 = uid.getLeastSignificantBits();
    long l2 = uid. getMostSignificantBits();
    return "_"+Long.toHexString(l1)+Long.toHexString(l2);
  }

  static public Key make(byte[] kb) { return make(kb,DEFAULT_DESIRED_REPLICA_FACTOR); }
  static public Key make(String s) { return make(decodeKeyName(s));}
  static public Key make(String s, byte rf) { return make(decodeKeyName(s), rf);}
  static public Key make() { return make(rand()); }

  // Make a particular system key that is homed to given node and possibly
  // specifies also other 2 replicas. Works for both IPv4 and IPv6 addresses.
  // If the addresses are not specified, returns a key with no home information.
  static public Key make(String s, byte rf, byte systemType, H2ONode... replicas) {
    return make(decodeKeyName(s),rf,systemType,replicas);
  }
  static public Key make(byte rf, byte systemType, H2ONode... replicas) {
    return make(rand(),rf,systemType,replicas);
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
    // 2-5- 4 bytes of chunk#, or -1 for masters
    // n+ - repeat of the original kb
    AutoBuffer ab = new AutoBuffer();
    ab.put1(systemType).put1(replicas.length);
    for( H2ONode h2o : replicas )
      h2o.write(ab);
    ab.put4(-1);
    ab.putA1(kb,kb.length);
    return make(Arrays.copyOf(ab.buf(),ab.position()),rf);
  }

  // Hide a user key by turning it into a system key of type HIDDEN_USER_KEY
  final public static Key makeSystem(String s) {
    byte[] kb= decodeKeyName(s);
    byte[] kb2 = new byte[kb.length+1];
    System.arraycopy(kb,0,kb2,1,kb.length);
    kb2[0] = Key.BUILT_IN_KEY;
    return Key.make(kb2);
  }

  // Custom Serialization Reader: Keys must be interned on construction.
  @Override public final Key read(AutoBuffer bb) { return make(bb.getA1()); }
  @Override public final AutoBuffer write(AutoBuffer bb) { return bb.putA1(_kb); }
  @Override public final AutoBuffer writeJSON(AutoBuffer bb) { return bb.putJSONStr(toString()); }

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
  @Override
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
    if( what.length()==0 ) return null;
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

  @Override public int hashCode() { return _hash; }
  @Override public boolean equals( Object o ) {
    if( o == null || ((Key)(o))._kb == null || _kb == null) return false;
    if( this == o ) return true;
    Key k = (Key)o;
    return Arrays.equals(k._kb,_kb);
  }

  @Override public int compareTo(Object o) {
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

  public static String toPrettyString(Key k) {
    StringBuilder sb = new StringBuilder("Key { type: ");
    switch( k._kb[0] ) {
      case 0: sb.append("arraylet chunk"); break;
      case 2: sb.append("build-in"); break;
      case 3: sb.append("job"); break;
      case 4: sb.append("vec"); break;
      case 5: sb.append("dvec"); break;
      case 6: sb.append("vgroup"); break;
      case 7: sb.append("DFJ internal"); break;
      case 31: sb.append("hidden user"); break;
      case 32: sb.append("user"); break;
      default: sb.append("UNKNOWN"); break;
    }
    sb.append(",replicas: ").append(k._kb[1]).append(",");
    sb.append(k.toString()).append("}");
    return sb.toString();
  }
}
