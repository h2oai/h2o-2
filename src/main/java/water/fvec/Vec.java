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
  final Key _key;               // Top-level key
  // Element-start per chunk.  Always zero for chunk 0.  One more entry than
  // chunks, so the last entry is the total number of rows.  This field is
  // dead/ignored in subclasses that are guaranteed to have fixed-sized chunks
  // such as file-backed Vecs.
  final long _espc[];

  Vec( Key key, long espc[] ) {
    assert key._kb[0]==Key.VEC;
    _key = key;
    _espc = espc; 
  }

  // Number of elements in the vector.  Overridden by subclasses that compute
  // length in an alternative way, such as file-backed Vecs.
  long length() { return _espc[_espc.length-1]; }

  // Number of chunks.  Overridden by subclasses that compute chunks in an
  // alternative way, such as file-backed Vecs.
  public int nChunks() { return _espc.length-1; }

  // Default read/write behavior for Vecs
  public boolean readable() { return true ; }
  public boolean writable() { return false; }

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
  public BigVector elem2BV( long i, int cidx ) {
    long start = chunk2StartElem(cidx); // Chunk# to chunk starting element#
    Value dvec = chunkIdx(cidx);        // Chunk# to chunk data
    BigVector bv = dvec.get();          // Chunk data to compression wrapper
    if( bv._start == start ) return bv; // Already filled-in
    assert bv._start == -1;
    bv._start = start;          // Fields not filled in by unpacking from Value
    bv._vec = this;             // Fields not filled in by unpacking from Value
    return bv;
  }

  // Fetch element the slow way
  long at( long i ) { return elem2BV(i,elem2ChunkIdx(i)).at(i); }
  double atd( long i ) { throw H2O.unimpl(); }
}
