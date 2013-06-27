package water.fvec;

import water.*;
import water.fvec.Vec;
import java.util.Arrays;

// A NEW single distributed vector column.
//
// The NEW vector has no data, and takes no space.  It supports distributed
// parallel writes to it, via calls to append2.  Such writes happen in parallel
// and no guarantees about order are made.  Writes *will* be local to the node
// doing them, specifically to allow control over locality.  By default, writes
// will go local-homed chunks with no compression; there is a final 'close' to
// the NEW vector which may do compression, and will collect info like row-
// counts-per-chunk, min/max/mean, etc.; the final 'close' will return some
// other Vec type.  NEW Vectors do NOT support reads!
public class AppendableVec extends Vec {
  long _espc[];
  boolean _hasFloat;            // True if we found a float chunk

  AppendableVec( Key key ) {
    super(key,null,false,Double.MAX_VALUE,Double.MIN_VALUE);
    _espc = new long[4];
  }

  // A NewVector chunk was "closed" - completed.  Add it's info to the roll-up.
  // This call is made in parallel across all node-local created chunks, but is
  // not called distributed.
  synchronized void closeChunk( int cidx, int len, boolean hasFloat, double min, double max ) {
    while( cidx >= _espc.length )
      _espc = Arrays.copyOf(_espc,_espc.length<<1);
    _espc[cidx] = len;
    _hasFloat |= hasFloat;
    // Roll-up totals for each chunk as it closes
    if( min < _min ) _min = min;
    if( max > _max ) _max = max;
  }

  // Class 'reduce' call on new vectors; to combine the roll-up info.
  // Called single-threaded from the M/R framework.
  public void reduce( AppendableVec nv ) {
    if( this == nv ) return;    // Trivially done

    // Combine arrays of elements-per-chunk
    long e1[] = nv._espc;       // Shorter array of longs?
    if( e1.length > _espc.length ) {
      e1 = _espc;               // Keep the shorter one in e1
      _espc = nv._espc;         // Keep longer in the object
    }
    for( int i=0; i<e1.length; i++ ) // Copy non-zero elements over
      if( e1[i] != 0 && _espc[i]==0 )
        _espc[i] = e1[i];
  }


  // "Close" out a NEW vector - rewrite it to a plain Vec that supports random
  // reads, plus computes rows-per-chunk, min/max/mean, etc.
  public Vec close(Futures fs) {
    // Compute #chunks
    int nchunk = _espc.length;
    while( nchunk > 0 && _espc[nchunk-1] == 0 ) nchunk--;

    // Compute elems-per-chunk.
    // Roll-up elem counts, so espc[i] is the starting element# of chunk i.
    // TODO: Complete fail: loads all data locally - will force OOM.  Needs to be
    // an RPC to test Key existence, and return length & other metadata
    long espc[] = new long[nchunk+1]; // Shorter array
    long x=0;                   // Total row count so far
    for( int i=0; i<nchunk; i++ ) {
      espc[i] = x;              // Start elem# for chunk i
      x += _espc[i];            // Raise total elem count
    }
    espc[nchunk]=x;             // Total element count in last
    // Replacement plain Vec for AppendableVec.
    Vec vec = new Vec(_key,espc,!_hasFloat,_min,_max);
    DKV.put(_key,vec,fs);       // Inject the header
    return vec;
  }

  // Default read/write behavior for AppendableVecs
  public boolean readable() { return false; }
  public boolean writable() { return true ; }

  @Override public Chunk elem2BV( int cidx ) { return new NewChunk(this,cidx); }

  // None of these are supposed to be called while building the new vector
  public Value chunkIdx( int cidx ) { throw H2O.fail(); }
  public long length() { throw H2O.fail(); }
  public int nChunks() { throw H2O.fail(); }
  int elem2ChunkIdx( long i ) { throw H2O.fail(); }
  public long chunk2StartElem( int cidx ) { throw H2O.fail(); }
  public long   get ( long i ) { throw H2O.fail(); }
  public double getd( long i ) { throw H2O.fail(); }
}
