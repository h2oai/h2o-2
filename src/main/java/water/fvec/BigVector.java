package water.fvec;

import water.AutoBuffer;
import water.H2O;
import water.Iced;

// A compression scheme, over an array of bytes.
public abstract class BigVector extends Iced {
  public long _start;           // Start element; filled after AutoBuffer.read
  public int _len;              // Number of elements in this chunk
  byte[] _mem; // Short-cut to the embedded memory; WARNING: holds onto a large array
  Vec _vec;    // Owning Vec; filled after AutoBuffer.read
  public long at( long i ) { 
    long x = i-_start;
    if( 0 <= x && x < _len ) return at_impl((int)x);
    return _vec.at(i);
  }
  // The zero-based API.  Somewhere between 10% to 30% faster in a tight-loop
  // over the data than the generic at() API.  Probably no gain on larger loops.
  public long at0( int i ) { return at_impl(i); }

  public double atd( long i ) { 
    long x = i-_start;
    if( 0 <= x && x < _len ) return atd_impl((int)x);
    return _vec.atd(i);
  }
  abstract long   at_impl ( int i );
  abstract double atd_impl( int i );
  public abstract AutoBuffer write(AutoBuffer bb);
  public abstract BigVector  read (AutoBuffer bb);
}
