package water.fvec;

import java.util.Arrays;
import water.parser.Enum;
import water.*;
import water.parser.DParseTask;

// An uncompressed chunk of data, support an append operation
public class NewChunk extends Chunk {
  final int _cidx;
  transient long _ls[];         // Mantissa
  transient int _xs[];          // Exponent
  transient double _ds[];       // Doubles, for inflating via doubles
  transient double _min, _max;
  int _naCnt;
  int _strCnt;

  NewChunk( Vec vec, int cidx ) {
    _vec = vec;
    _cidx = cidx;               // This chunk#
    _ls = new long[4];          // A little room for data
    _xs = new int [4];
    _min =  Double.MAX_VALUE;
    _max = -Double.MAX_VALUE;
  }

  public byte type(){
    if(_naCnt == _len)
      return AppendableVec.NA;
    if(_strCnt > 0 && _strCnt + _naCnt == _len)
      return AppendableVec.ENUM;
    return AppendableVec.NUMBER;
  }
  protected final boolean isNA(int idx) {
    return (_ds == null) ? (_ls[idx] == 0 && _xs[idx] != 0) : Double.isNaN(_ds[idx]);
  }

  public void addNA(){
    append2(0,Integer.MIN_VALUE); ++_naCnt;
  }
  private boolean _hasFloat;
  public void addNum(long val, int exp){
    if(val == 0)exp = 0;
    _hasFloat |= (exp < 0);
    append2(val,exp);
  }
  public void addEnum(int e){
    append2(0,e); ++_strCnt;
  }

  // Fast-path append long data
  void append2( long l, int x ) {
    if( _len >= _ls.length ) append2slow();
    _ls[_len] = l;
    _xs[_len] = x;
    _len++;
  }
  // Slow-path append data
  void append2slow( ) {
    if( _len > Vec.CHUNK_SZ )
      throw new ArrayIndexOutOfBoundsException(_len);
    _ls = Arrays.copyOf(_ls,_len<<1);
    _xs = Arrays.copyOf(_xs,_len<<1);
  }
  void invalid() { append2(0,Integer.MIN_VALUE); }
  void setInvalid(int idx) { _ls[idx]=0; _xs[idx] = Integer.MIN_VALUE; }

  // Do any final actions on a completed NewVector.  Mostly: compress it, and
  // do a DKV put on an appropriate Key.  The original NewVector goes dead
  // (does not live on inside the K/V store).
  public Chunk close(Futures fs) {
    Chunk chk = compress();
    if(_vec instanceof AppendableVec)
      ((AppendableVec)_vec).closeChunk(this);
    return chk;
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

    if(_naCnt == _len) // ALL NAs, nothing to do
      return new C0DChunk(Double.NaN,_len);
    // Enum?  We assume that columns with ALL strings (and NAs) are enums if
    // there were less than 65k unique vals.  If there were some numbers, we
    // assume it is a numcol with strings being NAs.
    if( type() == AppendableVec.ENUM) {
      // find their max val
      int sz = Integer.MIN_VALUE;
      for(int x:_xs) if(x > sz)sz = x;
      if( sz < Enum.MAX_ENUM_SIZE ) {
        if(sz < 255){ // we can fit into 1Byte
          byte [] bs = MemoryManager.malloc1(_len);
          for(int i = 0; i < _len; ++i)bs[i] = ((_xs[i] >= 0)?(byte)(0xFF&_xs[i]):(byte)0xFF);
          int [] vals = new int[256];
          for(int i = 0; i < bs.length; ++i)if(bs[i] >= 0)++vals[bs[i]];
          return new C1Chunk(bs);
        } else if(sz < 65535){ // 2 bytes
          byte [] bs = MemoryManager.malloc1(_len << 1);
          for(int i = 0; i < _len; ++i)UDP.set2(bs, i << 1, ((_xs[i] >= 0)?(short)_xs[i]:(short)C2Chunk._NA));
          return new C2Chunk(bs);
        } else throw H2O.unimpl();
      }
    }
    // If the data was set8 as doubles, we (weanily) give up on compression and
    // just store it as a pile-o-doubles.
    if( _ds != null )
      return new C8DChunk(bufF(3));

    // Look at the min & max & scaling.  See if we can sanely normalize the
    // data in some fixed-point format.
    boolean first = true;
    for( int i=0; i<_len; i++ ) {
      if( isNA(i) ) continue;
      long l = _ls[i];
      int  x = _xs[i];
      // Compute per-chunk min/sum/max
      double d = l*DParseTask.pow10(x);
      if( d < _min ) _min = d;
      if( d > _max ) _max = d;
      if( l==0 ) x=0;           // Canonicalize zero exponent
      long t;
      while( l!=0 && (t=l/10)*10==l ) { l=t; x++; }
      floatOverflow = Math.abs(l) > MAX_FLOAT_MANTISSA;
      if( first ) {
        first = false;
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

    // Constant column?
    if( _min==_max ) {
      return ((long)_min  == _min)
          ?new C0LChunk((long)_min,_len)
          :new C0DChunk(_min, _len);

    }

    // Boolean column? (or in general two value column)
    if (lemax-lemin == 1 && lemin == 0) {
      int bpv = _naCnt > 0 ? 2 : 1;
      byte[] cbuf = bufB(CBSChunk.OFF, bpv);
      return new CBSChunk(cbuf, cbuf[0], cbuf[1]);
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
        return new C1SChunk( bufX(lemin,xmin,C1SChunk.OFF,0),(int)lemin,DParseTask.pow10(xmin));
      if(lemax-lemin < 65535 )
        return new C2SChunk( bufX(lemin,xmin,C2SChunk.OFF,1),(int)lemin,DParseTask.pow10(xmin));
      return new C4FChunk( bufF(2));
    }
    // Compress column into a byte
    if( 0<=lemin && lemax <= 255 && ((_naCnt + _strCnt)==0) )
      return new C1NChunk( bufX(0,0,C1NChunk.OFF,0));
    if( lemax-lemin < 255 ) {         // Span fits in a byte?
      if( 0 <= lemin && lemax < 255 ) // Span fits in an unbiased byte?
        return new C1Chunk( bufX(0,0,C1Chunk.OFF,0));
      return new C1SChunk( bufX(lemin,0,C1SChunk.OFF,0),(int)lemin,1);
    }

    // Compress column into a short
    if( lemax-lemin < 65535 ) {               // Span fits in a biased short?
      if( Short.MIN_VALUE < lemin && lemax <= Short.MAX_VALUE ) // Span fits in an unbiased short?
        return new C2Chunk( bufX(0,0,C2Chunk.OFF,1));
      int bias = (int)(lemin-(Short.MIN_VALUE+1));
      return new C2SChunk( bufX(bias,0,C2SChunk.OFF,1),bias,1);
    }

    // Compress column into ints
    if( Integer.MIN_VALUE < lemin && lemax <= Integer.MAX_VALUE )
      return new C4Chunk( bufX(0,0,0,2));

    return new C8Chunk( bufX(0,0,0,3));
  }

  // Compute a compressed integer buffer
  private byte[] bufX( long bias, int scale, int off, int log ) {
    byte[] bs = new byte[(_len<<log)+off];
    for( int i=0; i<_len; i++ ) {
      if( isNA(i) ) {
        switch( log ) {
          case 0:          bs [i    +off] = (byte)(C1Chunk._NA); break;
          case 1: UDP.set2(bs,(i<<1)+off,   (short)C2Chunk._NA); break;
          case 2: UDP.set4(bs,(i<<2)+off,     (int)C4Chunk._NA); break;
          case 3: UDP.set8(bs,(i<<3)+off,          C8Chunk._NA); break;
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
          case 2: UDP.set4f(bs,(i<<2), Float .NaN); break;
          case 3: UDP.set8d(bs,(i<<3), Double.NaN); break;
        }
      } else {
        double le = _ds == null ? _ls[i]*DParseTask.pow10(_xs[i]) : _ds[i];
        switch( log ) {
        case 2: UDP.set4f(bs,(i<<2), (float)le); break;
        case 3: UDP.set8d(bs,(i<<3),        le); break;
        default: H2O.fail();
        }
      }
    }
    return bs;
  }

  // Compute compressed boolean buffer
  private byte[] bufB(int off, int bpv) {
    assert bpv == 1 || bpv == 2 : "Only bit vectors with/without NA are supported";
    int clen  = off + CBSChunk.clen(_len, bpv);
    byte bs[] = new byte[clen];
    int  boff = 0;
    byte b    = 0;
    int  idx  = off;
    for (int i=0; i<_len; i++) {
      byte val = isNA(i) ? CBSChunk._NA : (byte) _ls[i];
      switch (bpv) {
        case 1: assert val!=CBSChunk._NA;
                b = CBSChunk.write1b(b, val, boff); break;
        case 2: b = CBSChunk.write2b(b, val, boff); break;
      }
      boff += bpv;
      if (boff>8-bpv) { bs[idx] = b; boff = 0; b = 0; idx++; }
    }
    // Save the gap = number of unfilled bits and bpv value
    bs[0] = (byte) (boff == 0 ? 0 : 8-boff);
    bs[1] = (byte) bpv;
    // Flush last byte
    if (boff>0) bs[idx++] = b;
    /*for (int i=0; i<idx; i++) {
      if (i==0 || i==1) System.err.println(bs[i]);
      else System.err.println(bs[i] + " = " + Integer.toBinaryString(bs[i]));
    }*/
    return bs;
  }

  // Set & At on NewChunks are weird: only used after inflating some other
  // chunk.  At this point the NewChunk is full size, no more appends allowed,
  // and the xs exponent array should be only full of zeros.  Accesses must be
  // in-range and refer to the inflated values of the original Chunk.
  @Override boolean set8_impl(int i, long l) {
    if( _ds != null ) throw H2O.unimpl();
    _ls[i]=l; _xs[i]=0;
    return true;
  }
  @Override boolean set8_impl(int i, double d) {
    if( _ls != null ) throw H2O.unimpl();
    _ds[i]=d;
    return true;
  }
  @Override public long   at8_impl( int i ) { assert _xs[i]==0 && _ds==null; return _ls[i]; }
  @Override public double atd_impl( int i ) { assert _xs==null; return _ds[i]; }
  @Override boolean hasFloat() { return _hasFloat; }
  @Override public AutoBuffer write(AutoBuffer bb) { throw H2O.fail(); }
  @Override public NewChunk read(AutoBuffer bb) { throw H2O.fail(); }
  @Override NewChunk inflate_impl(NewChunk nc) { throw H2O.fail(); }
}
