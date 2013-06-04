package water.fvec;

import water.AutoBuffer;
import water.H2O;
import water.Iced;

// A compression scheme, over a chunk - a single array of bytes.  The *actual*
// BigVector header info is in the Vec struct - which contains info to find all
// the bytes of the distributed vector.  This struct is basically a 1-entry
// chunk cache of the total vector.  Subclasses of this abstract class
// implement (possibly empty) compression schemes.
public abstract class BigVector extends Iced {
  public long _start;           // Start element; filled after AutoBuffer.read
  public int _len;              // Number of elements in this chunk
  byte[] _mem; // Short-cut to the embedded memory; WARNING: holds onto a large array
  Vec _vec;    // Owning Vec; filled after AutoBuffer.read
  // Load a long value from the 1-entry chunk cache, or miss-out to "go slow".
  // This version uses absolute element numbers, but must convert them to
  // chunk-relative indices - requiring a load from an aliasing local var,
  // leading to lower quality JIT'd code (similar issue to using iterator
  // objects).
  public final long at( long i ) { 
    long x = i-_start;
    if( 0 <= x && x < _len ) return at_impl((int)x);
    return _vec.at(i);          // Go Slow
  }
  // The zero-based API.  Somewhere between 10% to 30% faster in a tight-loop
  // over the data than the generic at() API.  Probably no gain on larger
  // loops.  The row reference is zero-based on the chunk, and should
  // range-check by the JIT as expected.
  public final long at0( int i ) { return at_impl(i); }
  // Double variant of the above 'long' variant.
  public final double atd( long i ) { 
    long x = i-_start;
    if( 0 <= x && x < _len ) return atd_impl((int)x);
    return _vec.atd(i);
  }
  // Chunk-specific decompression of chunk-relative indexed data
  abstract long   at_impl ( int i );
  abstract double atd_impl( int i );
  // Chunk-specific append of data
  abstract void append2( long   l );
  abstract void append2( double d );
  // Chunk-specific implementations of read & write
  public abstract AutoBuffer write(AutoBuffer bb);
  public abstract BigVector  read (AutoBuffer bb);
}
