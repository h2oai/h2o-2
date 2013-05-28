package water.fvec;

import water.Iced;

// A compression scheme, over an array of bytes.
public abstract class CVec extends Iced {
  byte[] _mem; // Short-cut to the embedded memory; WARNING: holds onto a large array
  public long   at ( int i ) { assert 0 <= i && i < length(); return at_impl (i); }
  public double atd( int i ) { assert 0 <= i && i < length(); return atd_impl(i); }
  public abstract int length();
  abstract long   at_impl ( int i );
  abstract double atd_impl( int i );
}
