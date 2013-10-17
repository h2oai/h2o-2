package water.fvec;

import java.util.Arrays;
import java.util.UUID;

import water.*;
import water.H2O.H2OCountedCompleter;
import water.util.Utils;

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
  public static final int LOG_CHK = ValueArray.LOG_CHK; // Same as VA to help conversions
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
  /** Enum/factor/categorical names. */
  public String [] _domain;
  /** RollupStats: min/max/mean of this Vec lazily computed.  */
  double _min, _max, _mean, _sigma;
  long _size;
  boolean _isInt;
  /** The count of missing elements.... or -2 if we have active writers and no
   *  rollup info can be computed (because the vector is being rapidly
   *  modified!), or -1 if rollups have not been computed since the last
   *  modification.   */
  volatile long _naCnt=-1;

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

  // Create a vector transforming values according given domain map
  public Vec makeTransf(final int[] domMap) { return makeTransf(domMap, null); }
  public Vec makeTransf(final int[] domMap, final String[] domain) {
    Futures fs = new Futures();
    if( _espc == null ) throw H2O.unimpl();
    Vec v0 = new TransfVec(this._key, domMap, domain, group().addVecs(1)[0],_espc);
    DKV.put(v0._key,v0,fs);
    fs.blockForPending();
    return v0;
  }
  /**
   * Adapt given vector <code>v</code> to this vector.
   * I.e., unify domains and call makeTransf().
   */
  public Vec adaptTo(Vec v, boolean exact) {
    int[] domain = null;
    String[] sdomain = _domain == null
        ? Utils.toStringMap(domain = new CollectDomain(this).doAll(this).domain()) // it is number-column
        : domain(); // it is enum
    int[] domMap = Model.getDomainMapping(null, v._domain, sdomain, exact);
    if (domain!=null) {
      // do a mapping from INT -> ENUM -> this vector ENUM
      domMap = Utils.compose(Utils.mapping(domain), domMap);
    }
    return this.makeTransf(domMap, sdomain);
  }

  /** Number of elements in the vector.  Overridden by subclasses that compute
   *  length in an alternative way, such as file-backed Vecs. */
  public long length() { return _espc[_espc.length-1]; }

  /** Number of chunks.  Overridden by subclasses that compute chunks in an
   *  alternative way, such as file-backed Vecs. */
  public int nChunks() { return _espc.length-1; }

  /** Is the column a factor/categorical/enum?  Note: all "isEnum()" columns
   *  are are also "isInt()" but not vice-versa. */
  public final boolean isEnum(){return _domain != null;}

  /** Map the integer value for a enum/factor/categorical to it's String.
   *  Error if it is not an ENUM.  */
  public String domain(long i) { return _domain[(int)i]; }

  /** Return an array of domains.  This is eagerly manifested for enum or
   *  categorical columns.  Returns null for non-Enum/factor columns. */
  public String[] domain() { return _domain; }

  /** Convert an integer column to an enum column, with just number strings for
   *  the factors or levels.  */
  public void asEnum() {
    if( _domain!=null ) return;
    if( !isInt() ) throw new IllegalArgumentException("Cannot convert a float column to an enum.");
    _domain = defaultLevels();
    DKV.put(_key,this);
  }

  public String[] defaultLevels() {
    long min = (long)min(), max = (long)max();
    if( min < 0 || max > 100000L ) throw H2O.unimpl();
    String domain[] = new String[(int)max+1];
    for( int i=0; i<(int)max+1; i++ )
      domain[i] = Integer.toString(i);
    return domain;
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

  Vec setRollupStats( RollupStats rs ) {
    _min  = rs._min; _max = rs._max; _mean = rs._mean;
    _sigma = Math.sqrt(rs._sigma / (rs._rows - 1));
    _size =rs._size;
    _isInt= rs._isInt;
    if( rs._rows == 0 )         // All rows missing?  Then no rollups
      _min = _max = _mean = _sigma = Double.NaN;
    _naCnt= rs._naCnt;          // Volatile write last to announce all stats ready
    return this;
  }

  /** Compute the roll-up stats as-needed, and copy into the Vec object */
  Vec rollupStats() {
    if( _naCnt >= 0 ) return this;
    Vec vthis = DKV.get(_key).get();
    if( vthis._naCnt==-2 ) throw new IllegalArgumentException("Cannot ask for roll-up stats while the vector is being actively written.");
    if( vthis._naCnt>= 0 ) {    // KV store has a better answer
      _min  = vthis._min;   _max   = vthis._max;
      _mean = vthis._mean;  _sigma = vthis._sigma;
      _size = vthis._size;  _isInt = vthis._isInt;
      _naCnt= vthis._naCnt;  // Volatile write last to announce all stats ready
      return this;
    }
    // Compute the hard way
    final RollupStats rs = new RollupStats().doAll(this);
    setRollupStats(rs);
    // Now do this remotely also
    new TAtomic<Vec>() {
      @Override public Vec atomic(Vec v) {
        if( v!=null && v._naCnt == -1 ) v.setRollupStats(rs);  return v;
      }
    }.fork(_key);
    return this;
  }

  // Allow a bunch of rollups to run in parallel
  public void rollupStats(Futures fs) {
    if( _naCnt != -1 ) return;
    H2OCountedCompleter cc = new H2OCountedCompleter() {
        final Vec _vec=Vec.this;
        @Override public void compute2() {_vec.rollupStats(); tryComplete();}
      };
    H2O.submitTask(cc);
    fs.add(cc);
  }


  /** A private class to compute the rollup stats */
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
    }
    @Override public void reduce( RollupStats rs ) {
      _min = Math.min(_min,rs._min);
      _max = Math.max(_max,rs._max);
      _naCnt += rs._naCnt;
      double delta = _mean - rs._mean;
      _mean = (_mean*_rows + rs._mean*rs._rows)/(_rows + rs._rows);
      _sigma = _sigma + rs._sigma + delta*delta * _rows*rs._rows / (_rows+rs._rows);
      _rows += rs._rows;
      _size += rs._size;
      _isInt &= rs._isInt;
    }
    @Override public boolean logVerbose() {
      return !H2O.DEBUG;
    }
  }

  /** Writing into this Vector from *some* chunk.  Immediately clear all caches
   *  (_min, _max, _mean, etc).  Can be called repeatedly from one or all
   *  chunks.  Per-chunk row-counts will not be changing, just row contents and
   *  caches of row contents. */
  void preWriting( ) {
    if( _naCnt == -2 ) return; // Already set
    _naCnt = -2;
    if( !writable() ) throw new IllegalArgumentException("Vector not writable");
    // Set remotely lazily.  This will trigger a cloud-wide invalidate of the
    // existing Vec, and eventually we'll have to load a fresh copy of the Vec
    // with active writing turned on, and caching disabled.
    new TAtomic<Vec>() {
      @Override public Vec atomic(Vec v) { if( v!=null ) v._naCnt=-2; return v; }
    }.invoke(_key);
  }

  /** Stop writing into this Vec.  Rollup stats will again (lazily) be computed. */
  public void postWrite() {
    if( _naCnt==-2 ) {
      _naCnt=-1;
      new TAtomic<Vec>() {
        @Override public Vec atomic(Vec v) { if( v!=null && v._naCnt==-2 ) v._naCnt=-1; return v; }
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

  /** Number of rows in chunk. Does not fetch chunk content. */
  public int chunkLen( int cidx ) { return (int) (_espc[cidx + 1] - _espc[cidx]); }

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
    assert val != null : "Missing chunk "+cidx+" for "+_key;
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
  public final VectorGroup group() {
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
    String s = "["+length()+(_naCnt<0 ? ", {" : ","+_min+"/"+_mean+"/"+_max+", "+PrettyPrint.bytes(_size)+", {");
    int nc = nChunks();
    for( int i=0; i<nc; i++ ) {
      s += chunkKey(i).home_node()+":"+chunk2StartElem(i)+":";
      // Stupidly elem2BV loads all data locally
      s += elem2BV(i).getClass().getSimpleName().replaceAll("Chunk","")+", ";
    }
    return s+"}]";
  }

  public void remove( Futures fs ) {
    for( int i=0; i<nChunks(); i++ )
      UKV.remove(chunkKey(i),fs);
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
  public static class VectorGroup extends Iced{
    final int _len;
    final Key _key;
    private VectorGroup(Key key, int len){_key = key;_len = len;}
    public VectorGroup() {
      byte[] bits = new byte[26];
      bits[0] = Key.VEC;
      bits[1] = -1;
      UDP.set4(bits, 2, -1);
      UDP.set4(bits, 6, -1);
      UUID uu = UUID.randomUUID();
      UDP.set8(bits,10,uu.getLeastSignificantBits());
      UDP.set8(bits,18,uu. getMostSignificantBits());
      _key = Key.make(bits);
      _len = 0;
    }

    public Key vecKey(int vecId){
      byte [] bits = _key._kb.clone();
      UDP.set4(bits,2,vecId);//
      return Key.make(bits);
    }

    /**
     * Task to atomically add vectors into existing group.
     * @author tomasnykodym
     */
    private static class AddVecs2GroupTsk extends TAtomic<VectorGroup>{
      final Key _key;
      int _n;          // INPUT: Keys to allocate; OUTPUT: start of run of keys
      private AddVecs2GroupTsk(Key key, int n){_key = key; _n = n;}
      @Override public VectorGroup atomic(VectorGroup old) {
        int n = _n;             // how many
        // If the old group is missing, assume it is the default group-of-self
        // (having 1 ID already allocated for self), not a new group with
        // zero prior vectors.
        _n = old==null ? 1 : old._len; // start of allocated key run
        return new VectorGroup(_key, n+_n);
      }
    }
    // reserve range of keys and return index of first new available key
    public int reserveKeys(final int n){
      AddVecs2GroupTsk tsk = new AddVecs2GroupTsk(_key, n);
      tsk.invoke(_key);
      return tsk._n;
    }
    /**
     * Gets the next n keys of this group.
     * Performs atomic udpate of the group object to assure we get unique keys.
     * The group size will be udpated by adding n.
     *
     * @param n
     * @return arrays of unique keys belonging to this group.
     */
    public Key [] addVecs(final int n){
      AddVecs2GroupTsk tsk = new AddVecs2GroupTsk(_key, n);
      tsk.invoke(_key);
      Key [] res = new Key[n];
      for(int i = 0; i < n; ++i)
        res[i] = vecKey(i + tsk._n);
      return res;
    }

    @Override public String toString() {
      return "VecGrp "+_key.toString()+", next free="+_len;
    }

    @Override public boolean equals( Object o ) {
      if( !(o instanceof VectorGroup) ) return false;
      return ((VectorGroup)o)._key.equals(_key);
    }
    @Override public int hashCode() {
      return _key.hashCode();
    }
  }

  public static class CollectDomain extends MRTask2<CollectDomain> {
    final int _nclass;
    final int _ymin;
    byte _dom[];
    public CollectDomain(Vec v) { _ymin = (int) v.min(); _nclass = (int)(v.max()-_ymin+1); }
    @Override public void map(Chunk ys) {
      _dom = new byte[_nclass];
      int ycls=0;
      for( int row=0; row<ys._len; row++ ) {
        if (ys.isNA0(row)) continue;
        ycls = (int)ys.at80(row)-_ymin;
        _dom[ycls] = 1;
      }
    }
    @Override public void reduce( CollectDomain that ) { Utils.or(_dom,that._dom); }
    public int[] domain() {
      int cnt = 0;
      for (int i=0; i<_dom.length; i++) if (_dom[i]>0) cnt++;
      int[] dom = new int[cnt];
      cnt=0;
      for (int i=0; i<_dom.length; i++) if (_dom[i]>0) dom[cnt++] = i+_ymin;
      return dom;
    }
  }
}

