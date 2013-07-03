package water.fvec;

import java.util.Arrays;

import water.*;

// A single distributed vector column.
//
// A distributed vector has a count of elements, an element-to-chunk mapping, a
// Java type (mostly determines rounding on store), and functions to directly
// load elements without further indirections.  The data is compressed, or
// backed by disk or both.  *Writing* to elements may fail, either because the
// backing data is read-only (file backed) or does not fit in the compression
// scheme.
//
//  Vec Key format is: Key. VEC - byte, 0 - byte,   0    - int, normal Key bytes.
// DVec Key format is: Key.DVEC - byte, 0 - byte, chunk# - int, normal Key bytes.
public class Vec extends Iced {
  public enum DType {U,I,F,E,S,NA};
  public static final int LOG_CHK = 20; // Chunks are 1<<20, or 1Meg
  public static final long CHUNK_SZ = 1L << LOG_CHK;
  protected DType _dtype = DType.U;
  public DType dtype(){return _dtype;}
  final public Key _key;        // Top-level key
  // Element-start per chunk.  Always zero for chunk 0.  One more entry than
  // chunks, so the last entry is the total number of rows.  This field is
  // dead/ignored in subclasses that are guaranteed to have fixed-sized chunks
  // such as file-backed Vecs.
  final long _espc[];
  String [] _domain;
  // If we have active writers, then all cached roll-ups/reductions (e.g. _min,
  // _max) unavailable.  We won't even try to compute them (under the
  // assumption that we'll about to see a zillions writes/sec).
  boolean _activeWrites;
  // Short-cut for all integer data
  public boolean _isInt;  // true if column is all integer data
  // min/max/mean lazily computed.
  double _min, _max;

  Vec( Key key, long espc[], boolean isInt, double min, double max ) {
    assert key._kb[0]==Key.VEC;
    _key = key;
    _espc = espc;
    _isInt = isInt;
    _min = min == Double.MIN_VALUE ? Double.NaN : min;
    _max = max;
  }

  // Make a new vector same size as the old one, and initialized to zero.
  public static Vec makeZero( Vec v ) {
    Futures fs = new Futures();
    if( v._espc == null ) throw H2O.unimpl(); // need to make espc for e.g. NFSFileVecs!
    int nchunks = v.nChunks();
    Vec v0 = new Vec(v.group().addVecs(1)[0],v._espc,true,0,0);
    long row=0;                 // Start row
    for( int i=0; i<nchunks; i++ ) {
      long nrow = v.chunk2StartElem(i+1); // Next row
      DKV.put(v0.chunkKey(i),new C0LChunk(0L,(int)(nrow-row)),fs);
      row = nrow;
    }
    DKV.put(v0._key,v0,fs);
    fs.blockForPending();
    return v0;
  }

  // Number of elements in the vector.  Overridden by subclasses that compute
  // length in an alternative way, such as file-backed Vecs.
  public long length() { return _espc[_espc.length-1]; }

  // Number of chunks.  Overridden by subclasses that compute chunks in an
  // alternative way, such as file-backed Vecs.
  public int nChunks() { return _espc.length-1; }

  // Default read/write behavior for Vecs.
  // File-backed Vecs are read-only.
  // AppendableVecs are write-only.
  public boolean readable() { return true ; }
  public boolean writable() { return true; }

  // Return column min & max - lazily computing as needed.
  public double min() {
    if( _min == Double.NaN ) throw H2O.unimpl();
    return _min;
  }
  public double max() {
    if( _min == Double.NaN ) throw H2O.unimpl();
    return _max;
  }
  void setActiveWrites() { _activeWrites = true; _min = _max = Double.NaN; }
  // Writing into this Vector from *some* chunk.  Immediately clear all caches
  // (_min, _max, _mean, etc).  Can be called repeatedly from one or all
  // chunks.  Per-chunk row-counts will not be changing, just row contents and
  // caches of row contents.
  void startWriting() {
    if( _activeWrites ) return;      // Already set
    if( !writable() ) throw new IllegalArgumentException("Vector not writable");
    setActiveWrites();               // Set locally eagerly
    // Set remotely lazily.  This will trigger a cloud-wide invalidate of the
    // existing Vec, and eventually we'll have to load a fresh copy of the Vec
    // with activeWrites turned on, and caching disabled.  This TAtomic is not
    // lazy to avoid a race with deleting the vec - a "open/set8/close/remove"
    // sequence can race the "SetActive" with the "remove".
    new SetActiveWrites().invoke(_key);
  }
  private static final class SetActiveWrites extends TAtomic<Vec> {
    @Override public Vec atomic(Vec v) { v.setActiveWrites(); return v; }
  }

  // Convert a row# to a chunk#.  For constant-sized chunks this is a little
  // shift-and-add math.  For variable-sized chunks this is a binary search,
  // with a sane API (JDK has an insane API).  Overridden by subclasses that
  // compute chunks in an alternative way, such as file-backed Vecs.
  int elem2ChunkIdx( long i ) {
    assert 0 <= i && i < length();
    int x = Arrays.binarySearch(_espc, i);
    int res = x<0?-x - 2:x;
    int lo=0, hi = nChunks();
    while( lo < hi-1 ) {
      int mid = (hi+lo)>>>1;
      if( i < _espc[mid] ) hi = mid;
      else                 lo = mid;
    }
    if(res != lo)
      assert(res == lo):res + " != " + lo;
    return lo;
  }

  // Convert a chunk-index into a starting row #.  For constant-sized chunks
  // this is a little shift-and-add math.  For variable-sized chunks this is a
  // table lookup.
  public long chunk2StartElem( int cidx ) { return _espc[cidx]; }

  // Get a Chunk.  Basically the index-to-key map, plus a DKV.get.  Can be
  // overridden for e.g., all-constant vectors where the Value is special.
  public Key chunkKey(int cid){
    byte [] bits = _key._kb.clone();
    bits[0] = Key.DVEC;
    UDP.set4(bits,6,cid); // chunk#
    return Key.make(bits);
  }
  public Value chunkIdx( int cidx ) {
    Value val = DKV.get(chunkKey(cidx));
    assert val != null;
    return val;
  }
  protected static Key newKey(){return newKey(Key.make());}
  protected static Key newKey(Key k){
    byte [] kb = k._kb;
    byte [] bits = MemoryManager.malloc1(kb.length+4+4+1+1);
    bits[0] = Key.VEC;
    bits[1] = -1;         // Not homed
    UDP.set4(bits,2,0);   // new group, so we're the first vector
    UDP.set4(bits,6,-1);  // 0xFFFFFFFF in the   chunk# area
    System.arraycopy(kb, 0, bits, 4+4+1+1, kb.length);
    return Key.make(bits);
  }
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
  public VectorGroup group(){
    Key gKey = groupKey();
    Value v = DKV.get(gKey);
    if(v != null)return v.get(VectorGroup.class);
    // no group exists so we have to create one
    return new VectorGroup(gKey,1);
  }

  // Convert a global row# to a chunk-local row#.
  private final int elem2ChunkElem( long i, int cidx ) {
    return (int)(i - chunk2StartElem(cidx));
  }
  // Matching CVec for a given element
  public Chunk elem2BV( int cidx ) {
    long start = chunk2StartElem(cidx); // Chunk# to chunk starting element#
    Value dvec = chunkIdx(cidx);        // Chunk# to chunk data
    Chunk bv = dvec.get();              // Chunk data to compression wrapper
    if( bv._start == start ) return bv; // Already filled-in
    assert bv._start == -1;
    bv._start = start;          // Fields not filled in by unpacking from Value
    bv._vec = this;             // Fields not filled in by unpacking from Value
    return bv;
  }

  // Next BigVector from the current one
  Chunk nextBV( Chunk bv ) {
    int cidx = bv.cidx()+1;
    Chunk next =  cidx == nChunks() ? null : elem2BV(cidx);
    assert next == null || next.cidx() == cidx;
    return next;
  }

  // Fetch element the slow way
  public long  at8( long i ) { return elem2BV(elem2ChunkIdx(i)).at8(i); }
  public double at( long i ) { return elem2BV(elem2ChunkIdx(i)).at (i); }

  // Write element the slow way
  public long   set8( long i, long   l) { return elem2BV(elem2ChunkIdx(i)).set8(i,l); }
  public double set8( long i, double d) { return elem2BV(elem2ChunkIdx(i)).set8(i,d); }

  // handling of NAs
  long   _iNA = Long.MIN_VALUE+111; // "random" small number, not to clash with the MIN value
  double _fNA = Double.NaN;
  boolean _replaceNAs;

  public final void setNAs(double fNA, long iNa){
    _replaceNAs = false;
    _iNA = iNa;
    _fNA = fNA;
  }
  /**
   * NAs can be replaced on the fly by user supplied value.
   * @param fval
   * @param ival
   */
  public final void replaceNAs(double fval, long ival){
    _replaceNAs = true;
    _iNA = ival;
    _fNA = fval;
  }
  public final void replaceNAs(double fval){
    if(!Double.isNaN(fval))replaceNAs(fval,(long)fval);
    else {
      _fNA = fval;
      _replaceNAs = false;
    }
  }
  public final void replaceNAs(long ival){replaceNAs(ival, ival);}
  public final boolean isNA(long l){
    return !_replaceNAs && l == _iNA;
  }
  public final boolean isNA(double d){
    return !_replaceNAs && (Double.isNaN(d) || d == _fNA);
  }

  // [#elems, min/mean/max]
  @Override public String toString() {
    return "["+length()+(Double.isNaN(_min) ? "" : ","+_min+"/"+_max)+"]";
  }


  /**
   * Class representing the group of vectors.
   *
   * Vectors from the same group have same distribution of chunks among nodes.
   * Each vector is member of exactly one group. Default group of one vector is created for each vector.
   * Group of each vector can be retrieved by calling group() method;
   *
   * The expected mode of operation is that user wants to add new vectors matching the source.
   * E.g. parse creates several vectors (on for each column) which are all colocated and are
   * colocated with the original bytevector.
   *
   * To do this, user should first ask for the set of keys for the new vectors by calling addVecs method on the
   * target group.
   *
   * Vectors in the group will have the same keys except for the prefix which specifies index of the vector inside the group.
   * The only information the group object carries is it's own key and the number of vectors it contains(deleted vectors still count).
   *
   * Because vectors(and chunks) share the same key-pattern with the group, default group with only one vector does not have to be actually created, it is implicit.
   *
   * @author tomasnykodym
   *
   */
  public static class VectorGroup extends Iced{
    public final int _len;
    public final Key _key;
    private VectorGroup(Key key, int len){_key = key;_len = len;}

    public Key vecKey(int vecId){
      byte [] bits = _key._kb.clone();
      UDP.set4(bits,2,vecId);//
      return Key.make(bits);
    }
    public int vecId(Key k){
      return UDP.get4(k._kb, 2);
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
      public AddVecs2GroupTsk(Key key, int n){_key = key; _addN = _finalN = n;}
      @Override public VectorGroup atomic(VectorGroup old) {
        if(old == null) return new VectorGroup(_key, ++_finalN);
        return new VectorGroup(_key, _finalN = (_addN + old._len));
      }
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

