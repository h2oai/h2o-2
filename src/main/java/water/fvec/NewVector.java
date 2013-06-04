package water.fvec;

import water.*;
import java.util.Arrays;

// An uncompressed chunk of data, support an append operation
public class NewVector extends BigVector {
  final int _cidx;
  transient long _ls[];
  transient double _ds[];

  NewVector( int cidx, Vec vec ) { _vec = vec; _cidx = cidx; }

  // Fast-path append long data
  @Override void append2( long l ) {
    if( _ls == null || _len >= _ls.length ) append2slow(l);
    else _ls[_len++] = l;  
  }
  // Slow-path append data
  void append2slow( long l ) {
    if( _ds != null ) throw H2O.unimpl();
    if( _ls == null ) _ls = new long[1024];
    if( _len >= _ls.length ) {
      if( _len >= ValueArray.CHUNK_SZ )
        throw new ArrayIndexOutOfBoundsException(_len);
      _ls = Arrays.copyOf(_ls,_len<<1);
    }
    _ls[_len++] = l;
  }
  @Override void append2( double d ) { throw H2O.unimpl(); }

  // Do any final actions on a completed NewVector.  Mostly: compress it, and
  // do a DKV put on an appropriate Key.  The original NewVector goes dead
  // (does not live on inside the K/V store).
  public void close(Futures fs) {
    DKV.put(_vec.chunkKey(_cidx),compress(),fs);
  }

  // Study this NewVector and determine an appropriate compression scheme.
  // Return the data so compressed.
  BigVector compress() {
    if( _ds != null ) throw H2O.unimpl();
    if( _ls != null ) {
      long min=Long.MAX_VALUE,max=Long.MIN_VALUE;
      for( long l : _ls ) {
        if( l < min ) min = l;
        if( l > max ) max = l;
      }
      if( max-min < 255 ) {     // Span fits in a byte?
        if( max < 255 ) {       // Span fits in an unbiased byte?
          byte[] bs = new byte[_len];
          for( int i=0; i<_len; i++ )
            bs[i] = (byte)_ls[i];
          C0Vector c0 = new C0Vector();
          c0._mem = bs;
          c0._start = -1;
          c0._len = _len;
          return c0;
        }
      }
    }
    throw H2O.unimpl();
  }
  
  @Override long   at_impl ( int i ) { throw H2O.fail(); }
  @Override double atd_impl( int i ) { throw H2O.fail(); }
  @Override public AutoBuffer write(AutoBuffer bb) { throw H2O.fail(); }
  @Override public NewVector read(AutoBuffer bb) { throw H2O.fail(); }
}
