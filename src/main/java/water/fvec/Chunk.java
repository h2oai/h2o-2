package water.fvec;

import water.*;

// A compression scheme, over a chunk - a single array of bytes.  The *actual*
// BigVector header info is in the Vec struct - which contains info to find all
// the bytes of the distributed vector.  This struct is basically a 1-entry
// chunk cache of the total vector.  Subclasses of this abstract class
// implement (possibly empty) compression schemes.
public abstract class Chunk extends Iced implements Cloneable {
  public long _start;           // Start element; filled after AutoBuffer.read
  public int _len;              // Number of elements in this chunk
  Chunk _chk;                   // Normally==this, changed if chunk is written to
  byte[] _mem; // Short-cut to the embedded memory; WARNING: holds onto a large array
  Vec _vec;    // Owning Vec; filled after AutoBuffer.read
  Chunk() { _chk=this; }

  // The zero-based API.  Somewhere between 10% to 30% faster in a tight-loop
  // over the data than the generic at() API.  Probably no gain on larger
  // loops.  The row reference is zero-based on the chunk, and should
  // range-check by the JIT as expected.
  public final double at0( int i ) { return _chk.atd_impl(i); }
  public final long  at80( int i ) { return _chk.at8_impl(i); }
  public final boolean isNA0( int i ) { return valueIsNA(at80(i)); }

  // Load a double or long value from the 1-entry chunk cache, or miss-out.
  // This version uses absolute element numbers, but must convert them to
  // chunk-relative indices - requiring a load from an aliasing local var,
  // leading to lower quality JIT'd code (similar issue to using iterator
  // objects).
  // Slightly slower than 'at0'; range checks within a chunk
  public final double at( long i ) {
    long x = i-_start;
    if( 0 <= x && x < _len ) return at0((int)x);
    throw new ArrayIndexOutOfBoundsException(""+_start+" <= "+i+" < "+(_start+_len));
  }
  // Slightly slower than 'at80'; range checks within a chunk
  public final long at8( long i ) {
    long x = i-_start;
    if( 0 <= x && x < _len ) return at80((int)x);
    throw new ArrayIndexOutOfBoundsException(""+_start+" <= "+i+" < "+(_start+_len));
  }
  public final boolean isNA( long i ) { return valueIsNA(at8(i)); }

  // Slightly slower than 'at0'; goes (very) slow outside the chunk.  First
  // outside-chunk fetches & caches whole chunk; maybe takes multiple msecs.
  // 2nd & later touches in the same outside-chunk probably run 100x slower
  // than inside-chunk accesses.
  public final double at_slow( long i ) {
    long x = i-_start;
    if( 0 <= x && x < _len ) return at0((int)x);
    return _vec.at8(i);          // Go Slow
  }
  public final long at8_slow( long i ) {
    long x = i-_start;
    if( 0 <= x && x < _len ) return at80((int)x);
    return _vec.at8(i);          // Go Slow
  }
  public final boolean isNA_slow( long i ) { return valueIsNA(at8_slow(i)); }

  // Write into a chunk.  May rewrite/replace chunks if the chunk needs to be
  // "inflated" to hold larger values.  Returns the input value.
  public final long set8(long i, long l) {
    long x = i-_start;
    if( !(0 <= x && x < _len) ) return _vec.set8(i,l); // Go Slow
    return set80((int)x,l);
  }
  public final long set80(int idx, long l) {
    if( _chk==this ) {
      assert !(this instanceof NewChunk) : "Cannot direct-write into a NewChunk, only append";
      _vec.startWriting();      // One-shot writing-init
      _chk = clone();           // Flag this chunk as having been written into
      _chk._chk = _chk;         // Clone has NOT been written into
    }
    if( _chk.set8_impl(idx,l) ) return l;
    // Must inflate the chunk
    NewChunk nc = new NewChunk(null/*_vec*/,_vec.elem2ChunkIdx(_start));
    nc._vec = _vec;
    nc._ls = new long[_len];
    nc._xs = new int [_len];
    nc._len= _len;
    _chk = inflate_impl(nc);
    nc.set8_impl(idx,l);
    return l;
  }

  public final double set8(long i, double d) {
    long x = i-_start;
    if( !(0 <= x && x < _len) ) return _vec.set8(i,d); // Go Slow
    return set80((int)x,d);
  }
  public final double set80(int idx, double d) {
    if( _chk==this ) {
      assert !(this instanceof NewChunk) : "Cannot direct-write into a NewChunk, only append";
      _vec.startWriting();      // One-shot writing-init
      _chk = clone();           // Flag this chunk as having been written into
      _chk._chk = _chk;         // Clone has NOT been written into
    }
    if( _chk.set8_impl(idx,d) ) return d;
    // Must inflate the chunk
    NewChunk nc = new NewChunk(null/*_vec*/,_vec.elem2ChunkIdx(_start));
    nc._vec = _vec;
    nc._ls = null;
    nc._xs = null;
    nc._ds = new double[_len];
    nc._len= _len;
    _chk = inflate_impl(nc);
    nc.set8_impl(idx,d);
    return d;
  }

  public final float set4(long i, float d) {
    long x = i-_start;
    if( !(0 <= x && x < _len) ) return _vec.set4(i,d); // Go Slow
    return set40((int)x,d);
  }
  public final float set40(int idx, float d) {
    if( _chk==this ) {
      assert !(this instanceof NewChunk) : "Cannot direct-write into a NewChunk, only append";
      _vec.startWriting();      // One-shot writing-init
      _chk = clone();           // Flag this chunk as having been written into
      _chk._chk = _chk;         // Clone has NOT been written into
    }
    if( _chk.set4_impl(idx,d) ) return d;
    // Must inflate the chunk
    NewChunk nc = new NewChunk(null/*_vec*/,_vec.elem2ChunkIdx(_start));
    nc._vec = _vec;
    nc._ls = null;
    nc._xs = null;
    nc._ds = new double[_len];
    nc._len= _len;
    _chk = inflate_impl(nc);
    nc.set4_impl(idx,d);
    return d;
  }

  // After writing we must call close() to register the bulk changes
  public void close( int cidx, Futures fs ) {
    if( _chk instanceof NewChunk )_chk = ((NewChunk)_chk).close(fs);
    if( _chk == this ) return;
    DKV.put(_vec.chunkKey(cidx),_chk,fs); // Write updated chunk back into K/V
  }

  public int cidx() { return _vec.elem2ChunkIdx(_start); }

  // Chunk-specific readers.
  abstract protected double atd_impl(int idx);
  abstract protected long   at8_impl(int idx);
  // Chunk-specific writer.  Returns false if the value does not fit in the
  // current compression scheme.
  abstract boolean set8_impl (int idx, long l );
  abstract boolean set8_impl (int idx, double d );
  abstract boolean set4_impl (int idx, float f );
  // Chunk-specific bulk inflator back to NewChunk.  Used when writing into a
  // chunk and written value is out-of-range for an update-in-place operation.
  // Bulk copy from the compressed form into the nc._ls array.
  abstract NewChunk inflate_impl(NewChunk nc);
  // Any floats?
  abstract boolean hasFloat();
  // Chunk-specific implementations of read & write
  public abstract AutoBuffer write(AutoBuffer bb);
  public abstract Chunk  read (AutoBuffer bb);
  public final boolean valueIsNA(long val){return val == _vec._iNA;}

  @Override public Chunk clone() { return (Chunk)super.clone(); }
  @Override public String toString() { return getClass().getSimpleName(); }

  public long byteSize() {
    long s= _mem == null ? 0 : _mem.length;
    s += (2+5)*8 + 12; // 2 hdr words, 5 other words, @8bytes each, plus mem array hdr
    if( _chk != null && _chk != this ) s += _chk.byteSize();
    return s;
  }
}
