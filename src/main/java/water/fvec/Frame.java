package water.fvec;

import water.*;
import water.fvec.Vec.VectorGroup;

import java.io.InputStream;
import java.util.Arrays;

// A collection of named Vecs.  Essentially an R-like data-frame.
public class Frame extends Iced {
  transient public Key _key;
  public String[] _names;
  public Vec[] _vecs;
  public Vec _col0;             // First readable vec


  public Frame( Key k, String[] names, Vec[] vecs ) {
    _key=k; _names=names; _vecs=vecs;
  }
  // Find a named column
  public int find( String name ) {
    for( int i=0; i<_names.length; i++ )
      if( name.equals(_names[i]) ) 
        return i;
    return -1;
  }

  // Add a named column
  public void add( String name, Vec vec ) {
    // needs a compatibility-check????
    _names = Arrays.copyOf(_names,_names.length+1);
    _vecs  = Arrays.copyOf(_vecs ,_vecs .length+1);
    _names[_names.length-1] = name;
    _vecs [_vecs .length-1] = vec ;
  }
  // Remove a named column
  public Vec remove( String name ) { return remove(find(name)); }
  // Remove a numbered column
  public Vec remove( int idx ) { 
    int len = _names.length;
    if( idx < 0 || idx >= len ) return null;
    Vec v = _vecs[idx];
    System.arraycopy(_names,idx+1,_names,idx,len-idx-1);
    System.arraycopy(_vecs ,idx+1,_vecs ,idx,len-idx-1);
    _names = Arrays.copyOf(_names,len-1);
    _vecs  = Arrays.copyOf(_vecs ,len-1);
    return v;
  }


  public final Vec[] vecs() {
    return _vecs;
  }
  int numCols() { return _vecs.length; }

  // Return first readable vector
  public Vec firstReadable() {
    if( _col0 != null ) return _col0;
    for( Vec v : _vecs )
      if( v != null && v.readable() )
        return (_col0 = v);
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
  public void closeAppendables() {closeAppendables(new Futures());}
  // Close all AppendableVec
  public void closeAppendables(Futures fs) {
    _col0 = null;               // Reset cache
    for( int i=0; i<_vecs.length; i++ ) {
      Vec v = _vecs[i];
      if( v != null && v instanceof AppendableVec )
        _vecs[i] = ((AppendableVec)v).close(fs);
    }
  }


  // True if any Appendables exist
  public boolean hasAppendables() {
    for( Vec v : _vecs )
      if( v instanceof AppendableVec )
        return true;
    return false;
  }

  public void remove(Futures fs){
    if(_vecs.length > 0){
      VectorGroup vg = _vecs[0].group();
      for( Vec v : _vecs )
        UKV.remove(v._key,fs);
       DKV.remove(vg._key);
    }
    _names = new String[0];
    _vecs = new Vec[0];
  }
  // Remove all embedded Vecs
  public void remove() {
    remove(new Futures());
  }
  @Override public Frame init( Key k ) { _key=k; return this; }
  @Override public String toString() {
    String s="{"+_names[0];
    for( int i=1; i<_names.length; i++ )
      s += ","+_names[i];
    return s+"}";
  }
}
