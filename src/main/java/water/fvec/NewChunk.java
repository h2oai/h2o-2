package water.fvec;

import java.util.Arrays;
import water.*;
import water.parser.DParseTask;

// An uncompressed chunk of data, support an append operation
public class NewChunk extends Chunk {
  final int _cidx;
  transient long _ls[];         // Mantissa
  transient int _xs[];          // Exponent
  transient double _min, _max, _sum;

  NewChunk( AppendableVec vec, int cidx ) {
    super(Long.MIN_VALUE);
    _vec = vec;                 // Owning AppendableVec
    _cidx = cidx;               // This chunk#
    _ls = new long[4];          // A little room for data
    _xs = new int [4];
    _min = Double.MAX_VALUE;
    _max = Double.MIN_VALUE;
    _sum = 0;
  }

  protected final boolean isNA(int idx){
    return _ls[idx] == 0 && _xs[idx] == Integer.MIN_VALUE;
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
    ((AppendableVec)_vec).closeChunk(_cidx,_len,_min,_max,_sum);
  }

  // Study this NewVector and determine an appropriate compression scheme.
  // Return the data so compressed.
  static final int MAX_FLOAT_MANTISSA = 0x7FFFFF;
  Chunk compress() {

    // See if we can sanely normalize all the data to the same fixed-point.
    int  xmin = Integer.MAX_VALUE;   // min exponent found
    long lemin= 0, lemax=lemin; // min/max at xmin fixed-point
    boolean overflow=false;
    boolean floatOverflow = false;
    for( int i=0; i<_len; i++ ) {
      if(isNA(i))continue;
      long l = _ls[i];
      int  x = _xs[i];
      // Compute per-chunk min/sum/max
      double d = l*DParseTask.pow10(x);
      if( d < _min ) _min = d;
      if( d > _max ) _max = d;
      _sum += d;
      if( l==0 ) x=0;           // Canonicalize zero exponent
      long t;
      while( l!=0 && (t=l/10)*10==l ) { l=t; x++; }
      floatOverflow = Math.abs(l) > MAX_FLOAT_MANTISSA;
      if(i == 0){
        xmin = x;
        lemin = lemax = l;
        continue;
      }
      // Remove any trailing zeros / powers-of-10
      if(overflow || (overflow = (Math.abs(xmin-x)) >=10))continue;
      // Track largest/smallest values at xmin scale.  Note overflow.
      if( x < xmin ) {
        lemin *= DParseTask.pow10i(xmin-x);
        lemax *= DParseTask.pow10i(xmin-x);
        xmin = x;               // Smaller xmin
      }
      // *this* value, as a long scaled at the smallest scale
      long le = l*DParseTask.pow10i(x-xmin);
      if( le < lemin ) lemin=le;
      if( le > lemax ) lemax=le;
    }

    // Exponent scaling: replacing numbers like 1.3 with 13e-1.  '13' fits in a
    // byte and we scale the column by 0.1.  A set of numbers like
    // {1.2,23,0.34} then is normalized to always be represented with 2 digits
    // to the right: {1.20,23.00,0.34} and we scale by 100: {120,2300,34}.
    // This set fits in a 2-byte short.

    // We use exponent-scaling for bytes & shorts only; it's uncommon (and not
    // worth it) for larger numbers.  We need to get the exponents to be
    // uniform, so we scale up the largest lmax by the largest scale we need
    // and if that fits in a byte/short - then it's worth compressing.  Other
    // wise we just flip to a float or double representation.
    if(overflow || ((xmin != 0) && floatOverflow || -35 > xmin || xmin > 35))
      return new C8DChunk(bufF(3));
    if( xmin != 0 ) {
      if(lemax-lemin < 255 ) // Fits in scaled biased byte?
        return new C1SChunk(bufX(lemin,xmin,C1SChunk.OFF,0),(int)lemin,DParseTask.pow10(xmin));
      if(lemax-lemin < 65535 )
        return new C2SChunk(bufX(lemin,xmin,C2SChunk.OFF,1),(int)lemin,DParseTask.pow10(xmin));

      return new C4FChunk(bufF(2));
    }

    // Compress column into a byte
    if( lemax-lemin < 255 ) {         // Span fits in a byte?
      if( 0 <= lemin && lemax < 255 ) // Span fits in an unbiased byte?
        return new C1Chunk(bufX(0,0,C1Chunk.OFF,0));
      return new C1SChunk(bufX(lemin,0,C1SChunk.OFF,0),(int)lemin,1);
    }

    // Compress column into a short
    if( lemax-lemin < 65535 ) {               // Span fits in a biased short?
      if( -32767 <= lemin && lemax <= 32767 ) // Span fits in an unbiased short?
        return new C2Chunk(bufX(0,0,C2Chunk.OFF,1));
      return new C2SChunk(bufX(lemin,0,C2SChunk.OFF,1),(int)lemin,1);
    }

    // Compress column into ints
    if( Integer.MIN_VALUE < lemin && lemax <= Integer.MAX_VALUE )
      return new C4Chunk(bufX(0,0,0,2));

    return new C8Chunk(bufX(0,0,0,3));
  }

  // Compute a compressed integer buffer
  private byte[] bufX( long bias, int scale, int off, int log ) {
    byte[] bs = new byte[(_len<<log)+off];
    for( int i=0; i<_len; i++ ) {
      if(isNA(i)){
        switch( log ) {
          case 0:          bs [i    +off] = (byte)0xFF;         break;
          case 1: UDP.set2(bs,(i<<1)+off,   Short.MIN_VALUE);     break;
          case 2: UDP.set4(bs,(i<<2)+off,   Integer.MIN_VALUE); break;
          case 3: UDP.set8(bs,(i<<3)+off,   Long.MIN_VALUE);    break;
          default: H2O.fail();
        }
      } else {
        int x = _xs[i]-scale;
        long le = x >= 0
            ? _ls[i]*DParseTask.pow10i( x)
            : _ls[i]/DParseTask.pow10i(-x);
        le -= bias;
        switch( log ) {
        case 0:          bs [i    +off] = (byte)le ; break;
        case 1: UDP.set2(bs,(i<<1)+off,  (short)le); break;
        case 2: UDP.set4(bs,(i<<2)+off,    (int)le); break;
        case 3: UDP.set8(bs,(i<<3)+off,         le); break;
        default: H2O.fail();
        }
      }
    }
    return bs;
  }

  // Compute a compressed float buffer
  private byte[] bufF( int log ) {
    byte[] bs = new byte[_len<<log];
    for( int i=0; i<_len; i++ ) {
      if(isNA(i)){
        switch( log ) {
          case 2: UDP.set4f(bs,(i<<2), Float.NaN);  break;
          case 3: UDP.set8d(bs,(i<<3), Double.NaN); break;
        }
      } else {
        double le = _ls[i]*DParseTask.pow10(_xs[i]);
        switch( log ) {
        case 2: UDP.set4f(bs,(i<<2), (float)le); break;
        case 3: UDP.set8d(bs,(i<<3),        le); break;
        default: H2O.fail();
        }
      }
    }
    return bs;
  }

  @Override public long   at8_impl( int i ) { throw H2O.fail(); }
  @Override public double atd_impl( int i ) { throw H2O.fail(); }
  @Override public AutoBuffer write(AutoBuffer bb) { throw H2O.fail(); }
  @Override public NewChunk read(AutoBuffer bb) { throw H2O.fail(); }
}
