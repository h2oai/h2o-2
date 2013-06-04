package water.fvec;

import water.*;
import water.fvec.Vec;

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
public class NewVec extends Vec {

  NewVec( Key key ) { super(key,null); }

  // "Close" out a NEW vector - rewrite it to a plain Vec that supports random
  // reads, plus computes rows-per-chunk, min/max/mean, etc.
  Vec close() {
    throw H2O.unimpl();
  }

  // Default read/write behavior for NewVecs
  public boolean readable() { return false; }
  public boolean writable() { return true ; }

  @Override public BigVector elem2BV( long i, int cidx ) { return new NewVector(cidx,this); }

  // None of these are supposed to be called while building the new vector
  public Value chunkIdx( int cidx ) { throw H2O.fail(); }
  long length() { throw H2O.fail(); }
  public int nChunks() { throw H2O.fail(); }
  int elem2ChunkIdx( long i ) { throw H2O.fail(); }
  public long chunk2StartElem( int cidx ) { throw H2O.fail(); }
  long   at ( long i ) { throw H2O.fail(); }
  double atd( long i ) { throw H2O.fail(); }
}
