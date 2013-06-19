package water.fvec;

import water.*;

// A collection of named Vecs.  Essentially an R-like data-frame.
public class Frame extends Iced {
  transient public Key _key;
  public String[] _names;
  public Vec[] _vecs;

  public Frame( Key k, String[] names, Vec[] vecs ) { _key=k; _names=names; _vecs=vecs; }
  public void add( String name, Vec vec ) {
    throw H2O.unimpl();
  }
  @Override public Frame init( Key k ) { _key=k; return this; }
  @Override public String toString() {
    String s="{"+_names[0];
    for( int i=1; i<_names.length-1; i++ )
      s += ","+_names[i];
    return s+"}";
  }
  // Remove all embedded Vecs
  public void remove() {
    for( Vec v : _vecs )
      UKV.remove(v._key);
  }
}
