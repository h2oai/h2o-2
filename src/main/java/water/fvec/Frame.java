package water.fvec;

import water.*;

// A collection of named Vecs.  Essentially an R-like data-frame.
public class Frame extends Iced {
  transient public Key _key;
  public String[] _names;
  public Vec[] _vecs;
  public Vec _vec0;             // First readable vec

  public Frame( Key k, String[] names, Vec[] vecs ) { _key=k; _names=names; _vecs=vecs; }
  public void add( String name, Vec vec ) {
    throw H2O.unimpl();
  }

  int length() { return _vecs.length; }

  // Return first readable vector
  public Vec firstReadable() {
    if( _vec0 != null ) return _vec0;
    for( Vec v : _vecs )
      if( v != null && v.readable() )
        return (_vec0 = v);
    return null;
  }

  // Check that the vectors are all compatible: same number of rows per chunk
  public void checkCompatible( ) {
    Vec v0 = firstReadable();
    int nchunks = v0.nChunks();
    for( Vec vec : _vecs ) {
      if( vec instanceof AppendableVec ) continue; // New Vectors are endlessly compatible
      if( vec.nChunks() != nchunks )
        throw new IllegalArgumentException("Vectors different numbers of chunks, "+nchunks+" and "+vec.nChunks());
    }
    // Also check each chunk has same rows
    for( int i=0; i<nchunks; i++ ) {
      long es = v0.chunk2StartElem(i);
      for( Vec vec : _vecs )
        if( !(vec instanceof AppendableVec) && vec.chunk2StartElem(i) != es )
          throw new IllegalArgumentException("Vector chunks different numbers of rows, "+es+" and "+vec.chunk2StartElem(i));
    }
  }

  // Close all AppendableVec
  public void closeAppendables() {
    _vec0 = null;               // Reset cache
    for( int i=0; i<_vecs.length; i++ ) {
      Vec v = _vecs[i];
      if( v != null && v instanceof AppendableVec )
        _vecs[i] = ((AppendableVec)v).close();
    }
  }

  // True if any Appendables exist
  public boolean hasAppendables() {
    for( Vec v : _vecs )
      if( v instanceof AppendableVec )
        return true;
    return false;
  }

  // Remove all embedded Vecs
  public void remove() {
    for( Vec v : _vecs )
      UKV.remove(v._key);
  }
  @Override public Frame init( Key k ) { _key=k; return this; }
  @Override public String toString() {
    String s="{"+_names[0];
    for( int i=1; i<_names.length; i++ )
      s += ","+_names[i];
    return s+"}";
  }
}
