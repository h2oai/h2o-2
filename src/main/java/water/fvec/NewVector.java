package water.fvec;

import water.*;
import java.util.Arrays;

// An uncompressed chunk of data, support an append operation
public class NewVector extends BigVector {
  final int _cidx;
  transient long _ls[];         // Mantissa
  transient int _xs[];          // Exponent

  NewVector( AppendableVec vec, int cidx ) { 
    _vec = vec;                 // Owning AppendableVec
    _cidx = cidx;               // This chunk#
    _ls = new long[4];          // A little room for data
    _xs = new int [4];
  }

  // Fast-path append long data
  @Override void append2( long l, int x ) {
    if( _len >= _ls.length ) append2slow();
    _ls[_len] = l;  
    _xs[_len] = x;
    _len++;
  }
  // Slow-path append data
  void append2slow( ) {
    if( _len > ValueArray.CHUNK_SZ )
      throw new ArrayIndexOutOfBoundsException(_len);
    _ls = Arrays.copyOf(_ls,_len<<1);
    _xs = Arrays.copyOf(_xs,_len<<1);
  }

  // Do any final actions on a completed NewVector.  Mostly: compress it, and
  // do a DKV put on an appropriate Key.  The original NewVector goes dead
  // (does not live on inside the K/V store).
  public void close(Futures fs) {
    DKV.put(_vec.chunkKey(_cidx),compress(),fs);
    ((AppendableVec)_vec).closeChunk(_cidx,_len);
  }

  // Study this NewVector and determine an appropriate compression scheme.
  // Return the data so compressed.
  BigVector compress() {

    // See if we can sanely normalize all the data to the same fixed-point.
    int  xmin = Integer.MAX_VALUE, xmax=Integer.MIN_VALUE;
    long lmin = Long   .MAX_VALUE, lmax=Long   .MIN_VALUE;
    for( int i=0; i<_len; i++ ) {
      if( _xs[i] < xmin ) xmin = _xs[i];
      if( _xs[i] > xmax ) xmax = _xs[i];
      if( _ls[i] < lmin ) lmin = _ls[i];
      if( _ls[i] > lmax ) lmax = _ls[i];
    }

    water.util.Log.unwrap(System.err,"COMPRESS: "+lmin+"e"+xmin+" - "+lmax+"e"+xmax);
    //double min=Double.MAX_VALUE,max=Double.MIN_VALUE;
    //for( int i=0; i<_len; i++ ) {
    //  double d = _ls[i]*pow10(_xs[i]);
    //  if( d < min ) min = d;
    //  if( d > max ) max = d;
    //}
    if( xmin != 0 || xmax != 0 ) throw H2O.unimpl();

    // Compress column into a byte
    if( lmax-lmin < 255 ) {     // Span fits in a byte?
      if( lmax < 127 ) {        // Span fits in an unbiased byte?
        byte[] bs = new byte[_len];
        for( int i=0; i<_len; i++ )
          bs[i] = (byte)_ls[i];
        return new C0Vector(bs);
      } 
      throw H2O.unimpl();       // need biased byte
    } 

    // Compress column into a short
    if( lmax-lmin < 65535 ) {   // Span fits in an unsigned short?
      if( lmax < 32767 ) {      // Span fits in an unbiased short?
        byte[] bs = new byte[_len<<1];
        for( int i=0; i<_len; i++ )
          UDP.set2(bs,i<<1,(short)_ls[i]);
        return new C2Vector(bs);
      } 
      throw H2O.unimpl();       // need biased short
    } 

    throw H2O.unimpl();
  }
  
  @Override long   at_impl ( int i ) { throw H2O.fail(); }
  @Override double atd_impl( int i ) { throw H2O.fail(); }
  @Override public AutoBuffer write(AutoBuffer bb) { throw H2O.fail(); }
  @Override public NewVector read(AutoBuffer bb) { throw H2O.fail(); }
}
