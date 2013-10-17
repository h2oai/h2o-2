package water.fvec;

import java.util.Arrays;
import java.util.GregorianCalendar;

import water.parser.Enum;
import water.parser.ValueString;
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

  public NewChunk( Vec vec, int cidx ) {
    _vec = vec;
    _cidx = cidx;               // This chunk#
    _ls = new long[4];          // A little room for data
    _xs = new int [4];
    _min =  Double.MAX_VALUE;
    _max = -Double.MAX_VALUE;
  }

  // Constructor used when inflating a Chunk
  public NewChunk( Chunk C ) {
    _vec = C._vec;
    _cidx = _vec.elem2ChunkIdx(C._start); // This chunk#
    _len = C._len;
    if( C.hasFloat() || C instanceof C0DChunk ) {
      _ds = MemoryManager.malloc8d(_len);
    } else {
      _ls = MemoryManager.malloc8 (_len);
      _xs = MemoryManager.malloc4 (_len);
    }
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
  public void addNum(long val, int exp) {
    if(val == 0)exp = 0;
    _hasFloat |= (exp < 0);
    append2(val,exp);
  }
  public void addEnum(int e) {
    append2(0,e); ++_strCnt;
  }
  public void addNum(double d) {
    if(_ds == null) {
      assert _len == 0;
      _ds = new double[1];
    }
    if( _len >= _ds.length ) {
      if( _len > Vec.CHUNK_SZ )
        throw new ArrayIndexOutOfBoundsException(_len);
      _ds = Arrays.copyOf(_ds,_len<<1);
    }
    _ds[_len] = d;
    _len++;
    _hasFloat = true;
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
    _ls = MemoryManager.arrayCopyOf(_ls,_len<<1);
    _xs = MemoryManager.arrayCopyOf(_xs,_len<<1);
  }
  void invalid() { append2(0,Integer.MIN_VALUE); }
  void setInvalid(int idx) { _ls[idx]=0; _xs[idx] = Integer.MIN_VALUE; }

  /*
   *
   *
   *
   * private long attemptTimeParse( ValueString str ) {
    long t0 = attemptTimeParse_0(str); // "yyyy-MM-dd HH:mm:ss.SSS"
    if( t0 != Long.MIN_VALUE ) return t0;
    long t1 = attemptTimeParse_1(str); // "dd-MMM-yy"
    if( t1 != Long.MIN_VALUE ) return t1;
    return Long.MIN_VALUE;
  }
  // So I just brutally parse "yyyy-MM-dd HH:mm:ss.SSS"
  private long attemptTimeParse_0( ValueString str ) {
    final byte[] buf = str._buf;
    int i=str._off;
    final int end = i+str._length;
    while( i < end && buf[i] == ' ' ) i++;
    if   ( i < end && buf[i] == '"' ) i++;
    if( (end-i) < 19 ) return Long.MIN_VALUE;
    int yy=0, MM=0, dd=0, HH=0, mm=0, ss=0, SS=0;
    yy = digit(yy,buf[i++]);
    yy = digit(yy,buf[i++]);
    yy = digit(yy,buf[i++]);
    yy = digit(yy,buf[i++]);
    if( yy < 1970 ) return Long.MIN_VALUE;
    if( buf[i++] != '-' ) return Long.MIN_VALUE;
    MM = digit(MM,buf[i++]);
    MM = digit(MM,buf[i++]);
    if( MM < 1 || MM > 12 ) return Long.MIN_VALUE;
    if( buf[i++] != '-' ) return Long.MIN_VALUE;
    dd = digit(dd,buf[i++]);
    dd = digit(dd,buf[i++]);
    if( dd < 1 || dd > 31 ) return Long.MIN_VALUE;
    if( buf[i++] != ' ' ) return Long.MIN_VALUE;
    HH = digit(HH,buf[i++]);
    HH = digit(HH,buf[i++]);
    if( HH < 0 || HH > 23 ) return Long.MIN_VALUE;
    if( buf[i++] != ':' ) return Long.MIN_VALUE;
    mm = digit(mm,buf[i++]);
    mm = digit(mm,buf[i++]);
    if( mm < 0 || mm > 59 ) return Long.MIN_VALUE;
    if( buf[i++] != ':' ) return Long.MIN_VALUE;
    ss = digit(ss,buf[i++]);
    ss = digit(ss,buf[i++]);
    if( ss < 0 || ss > 59 ) return Long.MIN_VALUE;
    if( i<end && buf[i] == '.' ) {
      i++;
      if( i<end ) SS = digit(SS,buf[i++]);
      if( i<end ) SS = digit(SS,buf[i++]);
      if( i<end ) SS = digit(SS,buf[i++]);
      if( SS < 0 || SS > 999 ) return Long.MIN_VALUE;
    }
    if( i<end && buf[i] == '"' ) i++;
    if( i<end ) return Long.MIN_VALUE;
    return new GregorianCalendar(yy,MM,dd,HH,mm,ss).getTimeInMillis()+SS;
  }

  // So I just brutally parse "dd-MMM-yy".
  public static final byte MMS[][][] = new byte[][][] {
    {"jan".getBytes(),null},
    {"feb".getBytes(),null},
    {"mar".getBytes(),null},
    {"apr".getBytes(),null},
    {"may".getBytes(),null},
    {"jun".getBytes(),"june".getBytes()},
    {"jul".getBytes(),"july".getBytes()},
    {"aug".getBytes(),null},
    {"sep".getBytes(),"sept".getBytes()},
    {"oct".getBytes(),null},
    {"nov".getBytes(),null},
    {"dec".getBytes(),null}
  };
  private long attemptTimeParse_1( ValueString str ) {
    final byte[] buf = str._buf;
    int i=str._off;
    final int end = i+str._length;
    while( i < end && buf[i] == ' ' ) i++;
    if   ( i < end && buf[i] == '"' ) i++;
    if( (end-i) < 8 ) return Long.MIN_VALUE;
    int yy=0, MM=0, dd=0;
    dd = digit(dd,buf[i++]);
    if( buf[i] != '-' ) dd = digit(dd,buf[i++]);
    if( dd < 1 || dd > 31 ) return Long.MIN_VALUE;
    if( buf[i++] != '-' ) return Long.MIN_VALUE;
    byte[]mm=null;
    OUTER: for( ; MM<MMS.length; MM++ ) {
      byte[][] mms = MMS[MM];
      INNER: for( int k=0; k<mms.length; k++ ) {
        mm = mms[k];
        if( mm == null ) continue;
        for( int j=0; j<mm.length; j++ )
          if( mm[j] != Character.toLowerCase(buf[i+j]) )
            continue INNER;
        break OUTER;
      }
    }
    if( MM == MMS.length ) return Long.MIN_VALUE; // No matching month
    i += mm.length;             // Skip month bytes
    MM++;                       // 1-based month
    if( buf[i++] != '-' ) return Long.MIN_VALUE;
    yy = digit(yy,buf[i++]);
    yy = digit(yy,buf[i++]);
    yy += 2000;                 // Y2K bug
    if( i<end && buf[i] == '"' ) i++;
    if( i<end ) return Long.MIN_VALUE;
    return new GregorianCalendar(yy,MM,dd).getTimeInMillis();
  }

   */


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
          for(int i = 0; i < _len; ++i) bs[i] = (byte)(_xs[i] >= 0 ? (0xFF&_xs[i]) : C1Chunk._NA);
          return new C1Chunk(bs);
        } else if( sz <= 65535 ) { // 2 bytes
          int bias = 0, off = 0;
          if(sz >= 32767){
            bias = 32767;
            off = C2SChunk.OFF;
          }
          byte [] bs = MemoryManager.malloc1((_len << 1) + off);
          for(int i = 0; i < _len; ++i){
            if(_xs[i] >= 0) assert (short)(_xs[i]-bias) == (_xs[i]-bias);
            UDP.set2(bs, off + (i << 1), (short)((_xs[i] > 0)? _xs[i]-bias : C2Chunk._NA));
          }
          return bias == 0 ? new C2Chunk(bs) : new C2SChunk(bs,bias,1);
        } else throw H2O.unimpl();
      }
    }
    // If the data was set8 as doubles, we (weanily) give up on compression and
    // just store it as a pile-o-doubles.
    if( _ds != null )return chunkF();

    // Look at the min & max & scaling.  See if we can sanely normalize the
    // data in some fixed-point format.
    boolean first = true;
    boolean hasNA = false;

    for( int i=0; i<_len; i++ ) {
      if( isNA(i) ) { hasNA = true; continue;}
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
    if(!hasNA && _min==_max ) {
      return ((long)_min  == _min)
          ?new C0LChunk((long)_min,_len)
          :new C0DChunk(_min, _len);
    }

    // Boolean column?
    if (_max == 1 && _min == 0 && xmin == 0) {
      int bpv = _strCnt+_naCnt > 0 ? 2 : 1;
      byte[] cbuf = bufB(CBSChunk.OFF, bpv);
      return new CBSChunk(cbuf, cbuf[0], cbuf[1]);
    }


    final boolean fpoint = xmin < 0 || _min < Long.MIN_VALUE || _max > Long.MAX_VALUE;
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
    if(overflow || fpoint && floatOverflow || -35 > xmin || xmin > 35)
      return chunkF();
    if( fpoint ) {
      if(lemax-lemin < 255 ) // Fits in scaled biased byte?
        return new C1SChunk( bufX(lemin,xmin,C1SChunk.OFF,0),(int)lemin,DParseTask.pow10(xmin));
      if(lemax-lemin < 65535 ) { // we use signed 2B short, add -32k to the bias!
        long bias = 32767 + lemin;
        return new C2SChunk( bufX(bias,xmin,C2SChunk.OFF,1),(int)bias,DParseTask.pow10(xmin));
      }
      if(lemax - lemin < Integer.MAX_VALUE)
        return new C4SChunk(bufX(lemin, xmin,C4SChunk.OFF,2),(int)lemin,DParseTask.pow10(xmin));
      return chunkF();
    } // else an integer column
    // Compress column into a byte
    if(xmin == 0 &&  0<=lemin && lemax <= 255 && ((_naCnt + _strCnt)==0) )
      return new C1NChunk( bufX(0,0,C1NChunk.OFF,0));
    if( lemax-lemin < 255 ) {         // Span fits in a byte?
      if(0 <= _min && _max < 255 ) // Span fits in an unbiased byte?
        return new C1Chunk( bufX(0,0,C1Chunk.OFF,0));
      return new C1SChunk( bufX(lemin,xmin,C1SChunk.OFF,0),(int)lemin,DParseTask.pow10i(xmin));
    }

    // Compress column into a short
    if( lemax-lemin < 65535 ) {               // Span fits in a biased short?
      if( xmin == 0 && Short.MIN_VALUE < lemin && lemax <= Short.MAX_VALUE ) // Span fits in an unbiased short?
        return new C2Chunk( bufX(0,0,C2Chunk.OFF,1));
      int bias = (int)(lemin-(Short.MIN_VALUE+1));
      return new C2SChunk( bufX(bias,xmin,C2SChunk.OFF,1),bias,DParseTask.pow10i(xmin));
    }
    // Compress column into ints
    if(Integer.MIN_VALUE < _min && _max <= Integer.MAX_VALUE )
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
  private Chunk chunkF() {
    if(_ds == null){
      double [] ds = MemoryManager.malloc8d(_len);
      for(int i = 0; i < _len; ++i)
        ds[i] = isNA(i)?Double.NaN:_ls[i]*DParseTask.pow10(_xs[i]);
      _ds = ds; // can't assign to _ds bfr cause it would mess with isNA
    }
    boolean isFloat = true;
    for(double d:_ds)isFloat = isFloat && ((float)d == d);
    byte [] bs;
    if(isFloat){ // fits loss-lessly into a float
      bs = MemoryManager.malloc1(_len*4);
      for(int i = 0; i < _len; ++i)
        UDP.set4f(bs, 4*i, (float)_ds[i]);
      return new C4FChunk(bs);
    } else { // have to use double
      bs = MemoryManager.malloc1(_len*8);
      for(int i = 0; i < _len; ++i)
        UDP.set8d(bs, 8*i, _ds[i]);
      return new C8DChunk(bs);
    }
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
      byte val = isNA(i) ? CBSChunk._NA : _ls[i] == 0?(byte)0:(byte)1;
      switch (bpv) {
      case 1: assert val!=CBSChunk._NA : "Found NA row "+i+", naCnt="+_naCnt+", strcnt="+_strCnt;
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
  @Override boolean set_impl(int i, long l) {
    if( _ds != null ) throw H2O.unimpl();
    _ls[i]=l; _xs[i]=0;
    return true;
  }
  @Override boolean set_impl(int i, double d) {
    if( _ls != null ) {
      _ds = MemoryManager.malloc8d(_len);
      for( int j = 0; j<_len; j++ ) {
        long l = at8_impl(j);
        _ds[j] = l;
        if( _ds[j] != l )  throw H2O.unimpl();
      }
      _ls = null;  _xs = null;
    }
    _ds[i]=d;
    return true;
  }
  @Override boolean set_impl(int i, float f) {  return set_impl(i,(double)f); }
  @Override boolean setNA_impl(int i) {
    if( isNA(i) ) return true;
    if( _ls != null ) { _ls[i] = 0; _xs[i] = Integer.MIN_VALUE; }
    if( _ds != null ) { _ds[i] = Double.NaN; }
    _naCnt++;
    return true;
  }
  @Override public long   at8_impl( int i ) {
    if( _ls == null ) return (long)_ds[i];
    return _ls[i]*DParseTask.pow10i(_xs[i]);
  }
  @Override public double atd_impl( int i ) {
    if( _ds == null ) return at8_impl(i);
    assert _xs==null; return _ds[i];
  }
  @Override public boolean isNA_impl( int i ) { return isNA(i); }
  @Override public AutoBuffer write(AutoBuffer bb) { throw H2O.fail(); }
  @Override public NewChunk read(AutoBuffer bb) { throw H2O.fail(); }
  @Override NewChunk inflate_impl(NewChunk nc) { throw H2O.fail(); }
  @Override boolean hasFloat() { throw H2O.fail(); }
}
