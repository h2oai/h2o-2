package water.fvec;

import water.*;
import java.util.Arrays;

// An uncompressed chunk of data, support an append operation
public class NewVector extends BigVector {
  final int _cidx;
  transient long _ls[];
  transient double _ds[];

  NewVector( int cidx ) { _cidx = cidx; }

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
  @Override long   at_impl ( int i ) { throw H2O.fail(); }
  @Override double atd_impl( int i ) { throw H2O.fail(); }
  @Override public AutoBuffer write(AutoBuffer bb) { throw H2O.fail(); }
  @Override public NewVector read(AutoBuffer bb) { throw H2O.fail(); }
}
