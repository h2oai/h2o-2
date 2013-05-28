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
  // chunks, so the last entry is the total number of rows.
  final long _espc[];

  Vec( Key key, long espc[] ) {
    assert key._kb[0]==Key.VEC;
    _key = key;
    _espc = espc; 
  }

  // Number of elements in the vector
  long length() { return _espc[_espc.length-1]; }

  // Number of chunks
  public int nChunks() { return _espc.length-1; }

  // Convert a row# to a chunk#.  For constant-sized chunks this is a little
  // shift-and-add math.  For variable-sized chunks this is a binary search,
  // with a sane API (JDK has an insane API).
  int elem2ChunkIdx( long i ) {
    assert 0 <= i && i < length();
    int lo=0, hi = nChunks();
    while( lo < hi ) {
      int mid = (hi-lo)>>1;
      if( i < _espc[mid] ) hi = mid;
      else                 lo = mid;
    }
    return lo;
  }

  // Convert a chunk-index into a starting row #.  For constant-sized chunks
  // this is a little shift-and-add math.  For variable-sized chunks this is
  // probably a table lookup.
  public long chunk2StartElem( int cidx ) { return _espc[cidx]; }

  // Convert a chunk index into a data chunk key.  It's just the main Key with
  // the given chunk#.
  public Key chunkKey( int cidx ) {
    assert 0 <= cidx && cidx < nChunks();
    byte[] bits = _key._kb.clone(); // Copy the Vec key
    bits[0] = Key.DVEC;             // Data chunk key
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

  // Fetch element
  long at( long i ) {
    int cidx = elem2ChunkIdx(i);
    Value dvec = chunkIdx(cidx);
    return dvec.at(elem2ChunkElem(i, cidx));
  }

  double atd( long row ) { throw H2O.unimpl(); }
}
