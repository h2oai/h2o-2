package water.fvec;

import java.util.Arrays;

import water.*;

/**
 * A single distributed vector column.
 * <p>
 * A distributed vector has a count of elements, an element-to-chunk mapping, a
 * Java type (mostly determines rounding on store and display), and functions
 * to directly load elements without further indirections.  The data is
 * compressed, or backed by disk or both.  *Writing* to elements may throw if the
 * backing data is read-only (file backed).
 * <p>
 * <pre>
 *  Vec Key format is: Key. VEC - byte, 0 - byte,   0    - int, normal Key bytes.
 * DVec Key format is: Key.DVEC - byte, 0 - byte, chunk# - int, normal Key bytes.
 * </pre>
 *
 * The main API is at, set, and isNA:<br>
 *<pre>
 *   double  at  ( long row );  // Returns the value expressed as a double.  NaN if missing.
 *   long    at8 ( long row );  // Returns the value expressed as a long.  Throws if missing.
 *   boolean isNA( long row );  // True if the value is missing.
 *   set( long row, double d ); // Stores a double; NaN will be treated as missing.
 *   set( long row, long l );   // Stores a long; throws if l exceeds what fits in a double & any floats are ever set.
 *   setNA( long row );         // Sets the value as missing.
 * </pre>
 *
 * Note this dangerous scenario: loading a missing value as a double, and
 * setting it as a long: <pre>
 *   set(row,(long)at(row)); // Danger!
 *</pre>
 * The cast from a Double.NaN to a long produces a zero!  This code will
 * replace a missing value with a zero.
 *
 * @author Cliff Click
 */
public class Vec extends Iced {
  /** Log-2 of Chunk size. */
  public static final int LOG_CHK = 20; // Chunks are 1<<20, or 1Meg
  /** Chunk size.  Bigger increases batch sizes, lowers overhead costs, lower
   * increases fine-grained parallelism. */
  static final long CHUNK_SZ = 1L << LOG_CHK;

  /** Key mapping a Value which holds this Vec.  */
  final public Key _key;        // Top-level key
  /** Element-start per chunk.  Always zero for chunk 0.  One more entry than
   *  chunks, so the last entry is the total number of rows.  This field is
   *  dead/ignored in subclasses that are guaranteed to have fixed-sized chunks
   *  such as file-backed Vecs. */
  final private long _espc[];
  /** Enum/factor/catagorical names. */
  public String [] _domain;
  /** If we have active writers, then all cached roll-ups/reductions
   *  (e.g. _min, _max) unavailable.  We won't even try to compute them (under
   *  the assumption that we'll about to see a zillions writes/sec). */
  private boolean _activeWrites;
  /** min/max/mean of this Vec lazily computed.  */
  private RollupStats _rs;

  /** Main default constructor; requires the caller understand Chunk layout
   *  already, along with count of missing elements.  */
  Vec( Key key, long espc[] ) {
    assert key._kb[0]==Key.VEC;
    _key = key;
    _espc = espc;
  }

  /** Make a new vector with the same size and data layout as the old one, and
   *  initialized to zero. */
  public Vec makeZero() { return makeCon(0); }
  /** Make a new vector with the same size and data layout as the old one, and
   *  initialized to a constant. */
  public Vec makeCon( final long l ) {
    Futures fs = new Futures();
    if( _espc == null ) throw H2O.unimpl(); // need to make espc for e.g. NFSFileVecs!
    int nchunks = nChunks();
    Vec v0 = new Vec(group().addVecs(1)[0],_espc);
    long row=0;                 // Start row
    for( int i=0; i<nchunks; i++ ) {
      long nrow = chunk2StartElem(i+1); // Next row
      DKV.put(v0.chunkKey(i),new C0LChunk(l,(int)(nrow-row)),fs);
      row = nrow;
    }
    DKV.put(v0._key,v0,fs);
    fs.blockForPending();
    return v0;
  }
  public Vec makeCon( final double d ) {
    Futures fs = new Futures();
    if( _espc == null ) throw H2O.unimpl(); // need to make espc for e.g. NFSFileVecs!
    int nchunks = nChunks();
    Vec v0 = new Vec(group().addVecs(1)[0],_espc);
    long row=0;                 // Start row
    for( int i=0; i<nchunks; i++ ) {
      long nrow = chunk2StartElem(i+1); // Next row
      DKV.put(v0.chunkKey(i),new C0DChunk(d,(int)(nrow-row)),fs);
      row = nrow;
    }
    DKV.put(v0._key,v0,fs);
    fs.blockForPending();
    return v0;
  }

  /** Number of elements in the vector.  Overridden by subclasses that compute
   *  length in an alternative way, such as file-backed Vecs. */
  public long length() { return _espc[_espc.length-1]; }

  /** Number of chunks.  Overridden by subclasses that compute chunks in an
   *  alternative way, such as file-backed Vecs. */
  public int nChunks() { return _espc.length-1; }

  /** Is the column a factor/catagorical/enum?  Note: all "isEnum()" columns
   *  are are also "isInt()" but not vice-versa. */
  public final boolean isEnum(){return _domain != null;}

  /** Map the integer value for a enum/factor/catagorical to it's String.
   *  Error if it is not an ENUM.  */
  public String domain(long i) { return _domain[(int)i]; }

  /** Return an array of domains.  This is eagerly manifested for enum or
   *  catagorical columns.  */
  public String[] domain() { return _domain; }

  /** Convert an integer column to an enum column, with just number strings for
   *  the factors or levels.  */
  public void asEnum() {
    if( _domain!=null ) return;
    if( !isInt() ) throw new IllegalArgumentException("Cannot convert a float column to an enum.");
    long min = (long)min(), max = (long)max();
    if( min < 0 || max > 10000L ) throw H2O.unimpl();
    _domain = new String[(int)max+1];
    for( int i=0; i<=(int)max; i++ )
      _domain[i] = Integer.toString(i);
  }

  /** Default read/write behavior for Vecs.  File-backed Vecs are read-only. */
  protected boolean readable() { return true ; }
  /** Default read/write behavior for Vecs.  AppendableVecs are write-only. */
  protected boolean writable() { return true; }

  /** Return column min - lazily computed as needed. */
  public double min()  { return rollupStats()._min; }
  /** Return column max - lazily computed as needed. */
  public double max()  { return rollupStats()._max; }
  /** Return column mean - lazily computed as needed. */
  public double mean() { return rollupStats()._mean; }
  /** Return column standard deviation - lazily computed as needed. */
  public double sigma(){ return rollupStats()._sigma; }
  /** Return column missing-element-count - lazily computed as needed. */
  public long  naCnt() { return rollupStats()._naCnt; }
  /** Is all integers? */
  public boolean isInt(){return rollupStats()._isInt; }
  /** Size of compressed vector data. */
  public long byteSize(){return rollupStats()._size; }


  /** A private class to compute the rollup stats */
  RollupStats rollupStats() {
    if( _rs != null ) return _rs;
    if( _activeWrites ) throw new IllegalArgumentException("Cannot ask for roll-up stats while the vector is being actively written.");
    return (_rs=new RollupStats().doAll(this));
  }  

  private static class RollupStats extends MRTask2<RollupStats> {
    double _min=Double.MAX_VALUE, _max=-Double.MAX_VALUE, _mean, _sigma;
    long _rows, _naCnt, _size;
    boolean _isInt=true;
    @Override public void map( Chunk c ) {
      _size = c.byteSize();
      for( int i=0; i<c._len; i++ ) {
        double d = c.at0(i);
        if( Double.isNaN(d) ) _naCnt++;
        else {
          if( d < _min ) _min = d;
          if( d > _max ) _max = d;
          _mean += d;
          _rows++;
          if( _isInt && ((long)d) != d ) _isInt = false;
        }
      }
      _mean = _mean / _rows;
      for( int i=0; i<c._len; i++ ) {
        if( !c.isNA0(i) ) {
          double d = c.at0(i);
          _sigma += (d - _mean) * (d - _mean);
        }
      }
      _sigma = Math.sqrt(_sigma / (_rows - 1));
    }
    @Override public void reduce( RollupStats rs ) {
      _min = Math.min(_min,rs._min);
      _max = Math.max(_max,rs._max);
      _naCnt += rs._naCnt;
      _mean = (_mean*_rows + rs._mean*rs._rows)/(_rows + rs._rows);
      double delta = _mean - rs._mean;
      _sigma = _sigma + rs._sigma + delta*delta * _rows*rs._rows / (_rows+rs._rows);
      _rows += rs._rows;
      _size += rs._size;
      _isInt &= rs._isInt;
    }
  }

  /** Mark this vector as being actively written into, and clear the rollup stats. */
  private void setActiveWrites() { _activeWrites = true;  _rs = null; }

  /** Writing into this Vector from *some* chunk.  Immediately clear all caches
   *  (_min, _max, _mean, etc).  Can be called repeatedly from one or all
   *  chunks.  Per-chunk row-counts will not be changing, just row contents and
   *  caches of row contents. */
  void startWriting( ) {
    if( _activeWrites ) return; // Already set
    if( !writable() ) throw new IllegalArgumentException("Vector not writable");
    setActiveWrites();          // Set locally eagerly
    // Set remotely lazily.  This will trigger a cloud-wide invalidate of the
    // existing Vec, and eventually we'll have to load a fresh copy of the Vec
    // with activeWrites turned on, and caching disabled.  This TAtomic is not
    // lazy to avoid a race with deleting the vec - a "open/set8/close/remove"
    // sequence can race the "SetActive" with the "remove".
    new TAtomic<Vec>() {
      @Override public Vec atomic(Vec v) { v.setActiveWrites(); return v; }
    }.invoke(_key);
  }

  /** Stop writing into this Vec.  Rollup stats will again (lazily) be
   * computed. */
  public void postWrite() {
    if( _activeWrites ) {
      _activeWrites=false;
      new TAtomic<Vec>() {
        @Override public Vec atomic(Vec v) { v._activeWrites=false; return v; }
      }.invoke(_key);
    }
  }

  /** Convert a row# to a chunk#.  For constant-sized chunks this is a little
   *  shift-and-add math.  For variable-sized chunks this is a binary search,
   *  with a sane API (JDK has an insane API).  Overridden by subclasses that
   *  compute chunks in an alternative way, such as file-backed Vecs. */
  int elem2ChunkIdx( long i ) {
    assert 0 <= i && i < length() : "0 <= "+i+" < "+length();
    int x = Arrays.binarySearch(_espc, i);
    int res = x<0?-x - 2:x;
    int lo=0, hi = nChunks();
    while( lo < hi-1 ) {
      int mid = (hi+lo)>>>1;
      if( i < _espc[mid] ) hi = mid;
      else                 lo = mid;
    }
    if(res != lo)
      assert(res == lo):res + " != " + lo + ", i = " + i + ", espc = " + Arrays.toString(_espc);
    return lo;
  }

  /** Convert a chunk-index into a starting row #.  For constant-sized chunks
   *  this is a little shift-and-add math.  For variable-sized chunks this is a
   *  table lookup. */
  public long chunk2StartElem( int cidx ) { return _espc[cidx]; }

  /** Get a Chunk Key from a chunk-index.  Basically the index-to-key map. */
  public Key chunkKey(int cidx ) {
    byte [] bits = _key._kb.clone();
    bits[0] = Key.DVEC;
    UDP.set4(bits,6,cidx); // chunk#
    return Key.make(bits);
  }
  /** Get a Chunk's Value by index.  Basically the index-to-key map,
   *  plus the {@link DKV.get}.  Warning: this pulls the data locally;
   *  using this call on every Chunk index on the same node will
   *  probably trigger an OOM!  */
  public Value chunkIdx( int cidx ) {
    Value val = DKV.get(chunkKey(cidx));
    assert val != null;
    return val;
  }

  /** Make a new random Key that fits the requirements for a Vec key. */
  static Key newKey(){return newKey(Key.make());}

  /** Make a new Key that fits the requirements for a Vec key, based on the
   *  passed-in key.  Used to make Vecs that back over e.g. disk files. */
  static Key newKey(Key k) {
    byte [] kb = k._kb;
    byte [] bits = MemoryManager.malloc1(kb.length+4+4+1+1);
    bits[0] = Key.VEC;
    bits[1] = -1;         // Not homed
    UDP.set4(bits,2,0);   // new group, so we're the first vector
    UDP.set4(bits,6,-1);  // 0xFFFFFFFF in the chunk# area
    System.arraycopy(kb, 0, bits, 4+4+1+1, kb.length);
    return Key.make(bits);
  }

  /** Make a Vector-group key.  */
  private Key groupKey(){
    byte [] bits = _key._kb.clone();
    UDP.set4(bits, 2, -1);
    UDP.set4(bits, 6, -1);
    return Key.make(bits);
  }
  /**
   * Get the group this vector belongs to.
   * In case of a group with only one vector, the object actually does not exist in KV store.
   *
   * @return VectorGroup this vector belongs to.
   */
  final VectorGroup group() {
    Key gKey = groupKey();
    Value v = DKV.get(gKey);
    if(v != null)return v.get(VectorGroup.class);
    // no group exists so we have to create one
    return new VectorGroup(gKey,1);
  }

  /** The Chunk for a chunk#.  Warning: this loads the data locally!  */
  public Chunk elem2BV( int cidx ) {
    long start = chunk2StartElem(cidx); // Chunk# to chunk starting element#
    Value dvec = chunkIdx(cidx);        // Chunk# to chunk data
    Chunk c = dvec.get();               // Chunk data to compression wrapper
    if( c._start == start ) return c;   // Already filled-in
    assert c._start == -1 || c._start == start; // Second term in case multi-thread access
    c._start = start;          // Fields not filled in by unpacking from Value
    c._vec = this;             // Fields not filled in by unpacking from Value
    return c;
  }
  /** The Chunk for a row#.  Warning: this loads the data locally!  */
  public final Chunk chunk( long i ) {
    return elem2BV(elem2ChunkIdx(i));
  }

  /** Next Chunk from the current one. */
  final Chunk nextBV( Chunk bv ) {
    int cidx = bv.cidx()+1;
    Chunk next =  cidx == nChunks() ? null : elem2BV(cidx);
    assert next == null || next.cidx() == cidx;
    return next;
  }

  /** Fetch element the slow way, as a long.  Floating point values are
   *  silently rounded to an integer.  Throws if the value is missing. */
  public final long  at8( long i ) { return chunk(i).at8(i); }
  /** Fetch element the slow way, as a double.  Missing values are
   *  returned as Double.NaN instead of throwing. */
  public final double at( long i ) { return chunk(i).at (i); }
  /** Fetch the missing-status the slow way. */
  public final boolean isNA(long row){ return chunk(row).isNA(row); }

  /** Write element the slow way, as a long.  There is no way to write a
   *  missing value with this call.  Under rare circumstances this can throw:
   *  if the long does not fit in a double (value is larger magnitude than
   *  2^52), AND float values are stored in Vector.  In this case, there is no
   *  common compatible data representation. */
  public final long   set( long i, long   l) { return chunk(i).set(i,l); }
  /** Write element the slow way, as a double.  Double.NaN will be treated as
   *  a set of a missing element. */
  public final double set( long i, double d) { return chunk(i).set(i,d); }
  /** Write element the slow way, as a float.  Float.NaN will be treated as
   *  a set of a missing element. */
  public final float  set( long i, float  f) { return chunk(i).set(i,f); }
  /** Set the element as missing the slow way.  */
  public final boolean setNA( long i ) { return chunk(i).setNA(i); }

  /** Pretty print the Vec: [#elems, min/mean/max]{chunks,...} */
  @Override public String toString() {
    String s = "["+length()+(_rs==null ? "" : ","+_rs._min+"/"+_rs._mean+"/"+_rs._max+", "+PrettyPrint.bytes(_rs._size)+", {");
    int nc = nChunks();
    for( int i=0; i<nc; i++ ) {
      s += chunkKey(i).home_node()+":"+chunk2StartElem(i)+":";
      // Stupidly elem2BV loads all data locally
      s += elem2BV(i).getClass().getSimpleName().replaceAll("Chunk","")+", ";
    }
    return s+"}]";
  }


  /**
   * Class representing the group of vectors.
   *
   * Vectors from the same group have same distribution of chunks among nodes.
   * Each vector is member of exactly one group.  Default group of one vector
   * is created for each vector.  Group of each vector can be retrieved by
   * calling group() method;
   *
   * The expected mode of operation is that user wants to add new vectors
   * matching the source.  E.g. parse creates several vectors (one for each
   * column) which are all colocated and are colocated with the original
   * bytevector.
   *
   * To do this, user should first ask for the set of keys for the new vectors
   * by calling addVecs method on the target group.
   *
   * Vectors in the group will have the same keys except for the prefix which
   * specifies index of the vector inside the group.  The only information the
   * group object carries is it's own key and the number of vectors it
   * contains(deleted vectors still count).
   *
   * Because vectors(and chunks) share the same key-pattern with the group,
   * default group with only one vector does not have to be actually created,
   * it is implicit.
   *
   * @author tomasnykodym
   *
   */
  static class VectorGroup extends Iced{
    final int _len;
    final Key _key;
    private VectorGroup(Key key, int len){_key = key;_len = len;}

    public Key vecKey(int vecId){
      byte [] bits = _key._kb.clone();
      UDP.set4(bits,2,vecId);//
      return Key.make(bits);
    }

    /**
     * Task to atomically add vectors into existing group.
     *
     * @author tomasnykodym
     *
     */
    private static class AddVecs2GroupTsk extends TAtomic<VectorGroup>{
      final Key _key;
      final int _addN;
      int _finalN;
      private AddVecs2GroupTsk(Key key, int n){_key = key; _addN = _finalN = n;}
      @Override public VectorGroup atomic(VectorGroup old) {
        if(old == null) return new VectorGroup(_key, ++_finalN);
        return new VectorGroup(_key, _finalN = (_addN + old._len));
      }
    }
    public void reserveKeys(final int n){
      AddVecs2GroupTsk tsk = new AddVecs2GroupTsk(_key, n);
      tsk.invoke(_key);
    }
    /**
     * Gets the next n keys of this group.
     * Performs atomic udpate of the group object to assure we get unique keys.
     * The group size will be udpated by adding n.
     *
     * @param n
     * @return arrays of unique keys belonging to this group.
     */
    Key [] addVecs(final int n){
      AddVecs2GroupTsk tsk = new AddVecs2GroupTsk(_key, n);
      tsk.invoke(_key);
      Key [] res = new Key[n];
      for(int i = 0; i < n; ++i)
        res[i] = vecKey(i + tsk._finalN - n);
      return res;
    }
  }
}

