package water.fvec;

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
  final public Key _key;        // Top-level key
  // Element-start per chunk.  Always zero for chunk 0.  One more entry than
  // chunks, so the last entry is the total number of rows.  This field is
  // dead/ignored in subclasses that are guaranteed to have fixed-sized chunks
  // such as file-backed Vecs.
  final long _espc[];

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
    Vec v0 = new Vec(newKey(),v._espc,true,0,0);
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

  public static Key newKey() {
    byte [] kb = Key.make()._kb;
    byte[] bits = new byte[1+1+4+kb.length];
    bits[0] = Key.VEC;
    bits[1] = 0; // Not homed
    UDP.set4(bits,2,-1); // 0xFFFFFFFF in the chunk# area
    System.arraycopy(kb,0,bits,1+1+4,kb.length);
    return Key.make(bits);
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
  void start_writing() {
    if( _activeWrites ) return;      // Already set
    setActiveWrites();               // Set locally eagerly
    // Set remotely lazily.  This will trigger a cloud-wide invalidate of the
    // existing Vec, and eventually we'll have to load a fresh copy of the Vec
    // with activeWrites turned on, and caching disabled
    new TAtomic<Vec>() { @Override public Vec atomic(Vec v) { v.setActiveWrites(); return v; } }.fork(_key);
  }

  // Convert a row# to a chunk#.  For constant-sized chunks this is a little
  // shift-and-add math.  For variable-sized chunks this is a binary search,
  // with a sane API (JDK has an insane API).  Overridden by subclasses that
  // compute chunks in an alternative way, such as file-backed Vecs.
  int elem2ChunkIdx( long i ) {
    assert 0 <= i && i < length();
    int lo=0, hi = nChunks();
    while( lo < hi-1 ) {
      int mid = (hi+lo)>>>1;
      if( i < _espc[mid] ) hi = mid;
      else                 lo = mid;
    }
    return lo;
  }

  // Convert a chunk-index into a starting row #.  For constant-sized chunks
  // this is a little shift-and-add math.  For variable-sized chunks this is a
  // table lookup.
  public long chunk2StartElem( int cidx ) { return _espc[cidx]; }

  // Convert a chunk index into a data chunk key.  It's just the main Key with
  // the given chunk#.
  public Key chunkKey( int cidx ) {
    byte[] bits = _key._kb.clone(); // Copy the Vec key
    bits[0] = Key.DVEC;             // Data chunk key
    bits[1] = -1;                   // Not homed
    UDP.set4(bits,2,cidx);          // Chunk#
    return Key.make(bits);
  }

  // Get a Chunk.  Basically the index-to-key map, plus a DKV.get.  Can be
  // overridden for e.g., all-constant vectors where the Value is special.
  public Value chunkIdx( int cidx ) {
    Value val = DKV.get(chunkKey(cidx));
    assert val != null;
    return val;
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
    int cidx = elem2ChunkIdx(bv._start+bv._len);
    return cidx == nChunks() ? null : elem2BV(cidx);
  }

  // Fetch element the slow way
  public long  at8( long i ) { return elem2BV(elem2ChunkIdx(i)).at8(i); }
  public double at( long i ) { return elem2BV(elem2ChunkIdx(i)).at (i); }

  // Write element the slow way
  public long set8( long i, long l ) { return elem2BV(elem2ChunkIdx(i)).set8(i,l); }

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
}

