package water.fvec;

import water.*;

// A compression scheme, over a chunk - a single array of bytes.  The *actual*
// BigVector header info is in the Vec struct - which contains info to find all
// the bytes of the distributed vector.  This struct is basically a 1-entry
// chunk cache of the total vector.  Subclasses of this abstract class
// implement (possibly empty) compression schemes.
public abstract class Chunk extends Iced {
  public long _start;           // Start element; filled after AutoBuffer.read
  public int _len;              // Number of elements in this chunk
  byte[] _mem; // Short-cut to the embedded memory; WARNING: holds onto a large array
  Vec _vec;    // Owning Vec; filled after AutoBuffer.read

  // The zero-based API.  Somewhere between 10% to 30% faster in a tight-loop
  // over the data than the generic at() API.  Probably no gain on larger
  // loops.  The row reference is zero-based on the chunk, and should
  // range-check by the JIT as expected.
  public final double at0( int i ) { return atd_impl(i); }
  public final long  at80( int i ) { return at8_impl(i); }
  public final boolean isNA0( int i ) { return valueIsNA(at80(i)); }

  // Load a double or long value from the 1-entry chunk cache, or miss-out.
  // This version uses absolute element numbers, but must convert them to
  // chunk-relative indices - requiring a load from an aliasing local var,
  // leading to lower quality JIT'd code (similar issue to using iterator
  // objects).  
  // Slightly slower than 'at0'; range checks within a chunk
  public final double at( long i ) {
    long x = i-_start;
    if( 0 <= x && x < _len ) return atd_impl((int)x);
    throw new ArrayIndexOutOfBoundsException(""+_start+" <= "+i+" < "+(_start+_len));
  }
  // Slightly slower than 'at80'; range checks within a chunk
  public final long at8( long i ) {
    long x = i-_start;
    if( 0 <= x && x < _len ) return at8_impl((int)x);
    throw new ArrayIndexOutOfBoundsException(""+_start+" <= "+i+" < "+(_start+_len));
  }
  public final boolean isNA( long i ) { return valueIsNA(at8(i)); }

  // Slightly slower than 'at0'; goes (very) slow outside the chunk.  First
  // outside-chunk fetches & caches whole chunk; maybe takes multiple msecs.
  // 2nd & later touches in the same outside-chunk probably run 100x slower
  // than inside-chunk accesses.
  public final double at_slow( long i ) {
    long x = i-_start;
    if( 0 <= x && x < _len ) return atd_impl((int)x);
    return _vec.at8(i);          // Go Slow
  }
  public final long at8_slow( long i ) {
    long x = i-_start;
    if( 0 <= x && x < _len ) return at8_impl((int)x);
    return _vec.at8(i);          // Go Slow
  }
  public final boolean isNA_slow( long i ) { return valueIsNA(at8_slow(i)); }

  abstract protected double atd_impl(int idx);
  abstract protected long   at8_impl(int idx);
  // Chunk-specific append of data
  abstract void append2( long l, int exp );
  // Chunk-specific implementations of read & write
  public abstract AutoBuffer write(AutoBuffer bb);
  public abstract Chunk  read (AutoBuffer bb);
  public final boolean valueIsNA(long val){return val == _vec._iNA;}

}
