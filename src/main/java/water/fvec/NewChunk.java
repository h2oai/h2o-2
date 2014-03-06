package water.fvec;

import java.util.Arrays;
import java.util.GregorianCalendar;

import water.parser.Enum;
import water.parser.ValueString;
import water.*;
import water.parser.DParseTask;

// An uncompressed chunk of data, supporting an append operation
public class NewChunk extends Chunk {
  final int _cidx;
  // We can record the following (mixed) data types:
  // 1- doubles, in _ds including NaN for NA & 0; _ls==_xs==null
  // 2- scaled decimals from parsing, in _ls & _xs; _ds==null
  // 3- zero: requires _ls==0 && _xs==0
  // 4- NA: either _ls==0 && _xs==Integer.MIN_VALUE, OR _ds=NaN
  // 5- Enum: _xs==(Integer.MIN_VALUE+1) && _ds==null
  // Chunk._len is the count of elements appended
  // Sparse: if _len != _len2, then _ls/_ds are compressed to non-zero's only,
  // and _xs is the row number.  Still _len2 is count of elements including
  // zeros, and _len is count of non-zeros.
  transient long   _ls[];       // Mantissa
  transient int    _xs[];       // Exponent, or if _ls==0, NA or Enum or Rows
  transient double _ds[];       // Doubles, for inflating via doubles
  int _len2;                    // Actual rows, if the data is sparse
  int _naCnt=-1;                // Count of NA's   appended
  int _strCnt;                  // Count of Enum's appended
  int _nzCnt;                   // Count of non-zero's appended
  final int _timCnt[] = new int[ParseTime.TIME_PARSE.length]; // Count of successful time parses

  public NewChunk( Vec vec, int cidx ) { _vec = vec; _cidx = cidx; }

  // Constructor used when inflating a Chunk.
  public NewChunk( Chunk C ) {
    this(C._vec,C._vec.elem2ChunkIdx(C._start));
    _len = _len2 = C._len;
  }

  // Pre-sized newchunks.
  public NewChunk( Vec vec, int cidx, int len ) {
    this(vec,cidx);
    _ds = new double[len];
    Arrays.fill(_ds,Double.NaN);
    _len = _len2 = len;
  }

  // Heuristic to decide the basic type of a column
  public byte type() {
    if( _naCnt == -1 ) {        // No rollups yet?
      int nas=0, ss=0, nzs=0;
      if( _ds != null ) {
        assert _ls==null && _xs==null;
        for( double d : _ds ) if( Double.isNaN(d) ) nas++; else if( d!=0 ) nzs++;
      } else {
        assert _ds==null;
        if( _ls != null )
          for( int i=0; i<_len; i++ )
            if( isNA(i) ) nas++;
            else {
              if( isEnum(i)   ) ss++;
              if( _ls[i] != 0 ) nzs++;
            }
      }
      _nzCnt=nzs;  _strCnt=ss;  _naCnt=nas;
    }
    // Now run heuristic for type
    if(_naCnt == _len2)          // All NAs ==> NA Chunk
      return AppendableVec.NA;
    if(_strCnt > 0 && _strCnt + _naCnt == _len2)
      return AppendableVec.ENUM; // All are Strings+NAs ==> Enum Chunk
    // Larger of time & numbers
    int timCnt=0; for( int t : _timCnt ) timCnt+=t;
    int nums = _len2-_naCnt-timCnt;
    return timCnt >= nums ? AppendableVec.TIME : AppendableVec.NUMBER;
  }
  protected final boolean isNA(int idx) {
    return (_ds == null) ? (_ls[idx] == 0 && _xs[idx] == Integer.MIN_VALUE) : Double.isNaN(_ds[idx]);
  }
  protected final boolean isEnum(int idx) {
    return _ls!=null && _xs[idx]==Integer.MIN_VALUE+1;
  }

  public void addEnum(int e) { append2(e,Integer.MIN_VALUE+1); }
  public void addNA  (     ) { append2(0,Integer.MIN_VALUE  ); }
  public void addNum(long val, int exp) {
    if( val == 0 ) exp = 0;// Canonicalize zero
    long t;                // Remove extra scaling
    while( exp < 0 && exp > -9999999 && (t=val/10)*10==val ) { val=t; exp++; }
    append2(val,exp);
  }
  // Fast-path append double data
  public void addNum(double d) {
    if( _ds==null||_len >= _ds.length ) append2slowd();
    _ds[_len++] = d;  _len2++;
  }
  // Append all of 'nc' onto the current NewChunk.  Kill nc.
  public void add( NewChunk nc ) {
    if( nc._len == 0 ) return;
    if( _ds != null ) throw H2O.unimpl();
    while( _len+nc._len >= _xs.length )
      _xs = MemoryManager.arrayCopyOf(_xs,_xs.length<<1);
    _ls = MemoryManager.arrayCopyOf(_ls,_xs.length);
    System.arraycopy(nc._ls,0,_ls,_len,nc._len);
    System.arraycopy(nc._xs,0,_xs,_len,nc._len);
    _len2= (_len += nc._len);
    nc._ls = null;  nc._xs = null;  nc._len = nc._len2 = 0;
  }
  // PREpend all of 'nc' onto the current NewChunk.  Kill nc.
  public void addr( NewChunk nc ) {
    long  [] tmpl = _ls; _ls = nc._ls; nc._ls = tmpl;
    int   [] tmpi = _xs; _xs = nc._xs; nc._xs = tmpi;
    double[] tmpd = _ds; _ds = nc._ds; nc._ds = tmpd;
    int      tmp  = _len; _len=nc._len; nc._len=tmp;
    _len2=_len;
    add(nc);
  }

  // Fast-path append long data
  void append2( long l, int x ) {
    if( _ls==null||_len >= _ls.length ) append2slow();
    if( _len2 != _len ) {         // Sparse?
      if( x!=0 ) cancel_sparse(); // NA?  Give it up!
      else if( l==0 ) { _len2++; return; } // Just One More Zero
      else x = _len2;             // NZ: set the row over the xs field
    }
    _ls[_len  ] = l;
    _xs[_len++] = x;  _len2++;
  }

  private void cancel_sparse() {
    long ls[] = MemoryManager.malloc8(_len2+1);
    for( int i=0; i<_len; i++ ) // Inflate ls to hold values
      ls[_xs[i]] = _ls[i];
    _ls = ls;
    _xs = MemoryManager.malloc4(_len2+1);
    _len = _len2;           // Not compressed now!
  }

  // Slow-path append data
  private void append2slowd( ) {
    if( _len > Vec.CHUNK_SZ )
      throw new ArrayIndexOutOfBoundsException(_len);
    assert _ls==null && _len2==_len;
    _ds = _ds==null ? MemoryManager.malloc8d(4) : MemoryManager.arrayCopyOf(_ds,_len<<1);
  }
  // Slow-path append data
  private void append2slow( ) {
    if( _len > Vec.CHUNK_SZ )
      throw new ArrayIndexOutOfBoundsException(_len);
    assert _ds==null;
    if( _len2 == _len ) { // Check for sparse-ness now & then
      int nzcnt=0;
      for( int i=0; i<_len; i++ ) {
        if( _ls[i]!=0 ) nzcnt++;
        if( _xs[i]!=0 ) { nzcnt = Vec.CHUNK_SZ; break; } // Only non-specials sparse
      }
      if( _len >= 32 && nzcnt*8 <= _len ) { // Heuristic for sparseness
        _len=0;
        for( int i=0; i<_len2; i++ )
          if( _ls[i] != 0 ) {
            _xs[_len  ] = i;    // Row number in xs
            _ls[_len++] = _ls[i]; // Sparse value in ls
          }
        return;                 // Compressed, so lots of room now
      }
    }
    _xs = _ls==null ? MemoryManager.malloc4(4) : MemoryManager.arrayCopyOf(_xs,_len<<1);
    _ls = _ls==null ? MemoryManager.malloc8(4) : MemoryManager.arrayCopyOf(_ls,_len<<1);
  }

  // Do any final actions on a completed NewVector.  Mostly: compress it, and
  // do a DKV put on an appropriate Key.  The original NewVector goes dead
  // (does not live on inside the K/V store).
  public Chunk new_close() {
    Chunk chk = compress();
    if(_vec instanceof AppendableVec)
      ((AppendableVec)_vec).closeChunk(this);
    return chk;
  }
  public void close(Futures fs) { close(_cidx,fs); }

  // Study this NewVector and determine an appropriate compression scheme.
  // Return the data so compressed.
  static final int MAX_FLOAT_MANTISSA = 0x7FFFFF;
  Chunk compress() {
    // Check for basic mode info: all missing or all strings or mixed stuff
    byte mode = type();
    if( mode==AppendableVec.NA ) // ALL NAs, nothing to do
      return new C0DChunk(Double.NaN,_len);
    boolean rerun=false;
    for( int i=0; i<_len; i++ )
      if( mode==AppendableVec.ENUM   && !isEnum(i) ||
          mode==AppendableVec.NUMBER &&  isEnum(i) )
        { setNA_impl(i); rerun = true; }  // Smack any mismatched string/numbers
    if( rerun ) { _naCnt = -1;  type(); } // Re-run rollups after dropping all numbers/enums

    // If the data was set8 as doubles, we do a quick check to see if it's
    // plain longs.  If not, we give up and use doubles.
    if( _ds != null ) {
      int i=0;
      for( ; i<_len; i++ ) // Attempt to inject all doubles into longs
        if( !Double.isNaN(_ds[i]) && (double)(long)_ds[i] != _ds[i] ) break;
      if( i<_len ) return chunkD();
      _ls = new long[_ds.length]; // Else flip to longs
      _xs = new int [_ds.length];
      for( i=0; i<_len; i++ )   // Inject all doubles into longs
        if( Double.isNaN(_ds[i]) ) _xs[i] = Integer.MIN_VALUE;
        else                       _ls[i] = (long)_ds[i];
      _ds = null;
    }

    // IF (_len2 > _len) THEN Sparse
    // Check for compressed *during appends*.  Here we know:
    // - No specials; _xs[]==0.
    // - No floats; _ds==null
    // - NZ length in _len, actual length in _len2.
    // - Huge ratio between _len2 and _len, and we do NOT want to inflate to
    //   the larger size; we need to keep it all small all the time.
    // - Rows in _xs

    // Data in some fixed-point format, not doubles
    // See if we can sanely normalize all the data to the same fixed-point.
    int  xmin = Integer.MAX_VALUE;   // min exponent found
    long lemin= 0, lemax=lemin; // min/max at xmin fixed-point
    boolean overflow=false;
    boolean floatOverflow = false;
    boolean first = true;
    double min = _len2==_len ?  Double.MAX_VALUE : 0;
    double max = _len2==_len ? -Double.MAX_VALUE : 0;
    int p10iLength = DParseTask.powers10i.length;

    for( int i=0; i<_len; i++ ) {
      if( isNA(i) ) continue;
      long l = _ls[i];
      int  x = _xs[i];
      if( x==Integer.MIN_VALUE+1 || _len2 != _len ) x=0; // Replace enum flag with no scaling
      assert l!=0 || x==0;      // Exponent of zero is always zero
      // Compute per-chunk min/max
      double d = l*DParseTask.pow10(x);
      if( d < min ) min = d;
      if( d > max ) max = d;
      long t;                   // Remove extra scaling
      while( l!=0 && (t=l/10)*10==l ) { l=t; x++; }
      floatOverflow = Math.abs(l) > MAX_FLOAT_MANTISSA;
      if( first ) {
        first = false;
        xmin = x;
        lemin = lemax = l;
        continue;
      }
      // Track largest/smallest values at xmin scale.  Note overflow.
      if( x < xmin ) {
        if( overflow || (overflow = ((xmin-x) >=p10iLength)) ) continue;
        lemin *= DParseTask.pow10i(xmin-x);
        lemax *= DParseTask.pow10i(xmin-x);
        xmin = x;               // Smaller xmin
      }
      // *this* value, as a long scaled at the smallest scale
      if( overflow || (overflow = ((x-xmin) >=p10iLength)) ) continue;
      long le = l*DParseTask.pow10i(x-xmin);
      if( le < lemin ) lemin=le;
      if( le > lemax ) lemax=le;
    }

    if(_len2 != _len){ // sparse? compare xmin/lemin/lemax with 0
      lemin = Math.min(0, lemin);
      lemax = Math.max(0, lemax);
    }

    // Constant column?
    if( _naCnt==0 && min==max ) {
      return ((long)min  == min)
          ? new C0LChunk((long)min,_len2)
          : new C0DChunk(      min,_len2);
    }

    // Boolean column?
    if (max == 1 && min == 0 && xmin == 0 && !overflow) {
      if( _nzCnt*32 < _len2 && _naCnt==0 && _len2 < 65535 && xmin == 0 ) // Very sparse? (and not too big?)
        if( _len2 == _len ) return new CX0Chunk(_ls,_len2,_nzCnt); // Dense  constructor
        else                return new CX0Chunk(_xs,_len2,_len  ); // Sparse constructor
      int bpv = _strCnt+_naCnt > 0 ? 2 : 1;   // Bit-vector
      byte[] cbuf = bufB(bpv);
      return new CBSChunk(cbuf, cbuf[0], cbuf[1]);
    }

    final boolean fpoint = xmin < 0 || min < Long.MIN_VALUE || max > Long.MAX_VALUE;

    // Result column must hold floats?
    // Highly sparse but not a bitvector or constant?
    if( !fpoint && (_nzCnt+_naCnt)*8 < _len2 && _len2 < 65535 && xmin==0 && // (and not too big?)
        lemin > Short.MIN_VALUE && lemax <= Short.MAX_VALUE ) // Only handling unbiased shorts here
      if( _len2==_len ) return new CX2Chunk(_ls,_xs,_len2,_nzCnt,_naCnt);  // Sparse byte chunk
      else              return new CX2Chunk(_ls,_xs,_len2,_len);

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
    if( overflow || (fpoint && floatOverflow) || -35 > xmin || xmin > 35 )
      return chunkD();
    if( fpoint ) {
      if((int)lemin == lemin && (int)lemax == lemax){
        if(lemax-lemin < 255 && (int)lemin == lemin ) // Fits in scaled biased byte?
          return new C1SChunk( bufX(lemin,xmin,C1SChunk.OFF,0),(int)lemin,DParseTask.pow10(xmin));
        if(lemax-lemin < 65535 ) { // we use signed 2B short, add -32k to the bias!
          long bias = 32767 + lemin;
          return new C2SChunk( bufX(bias,xmin,C2SChunk.OFF,1),(int)bias,DParseTask.pow10(xmin));
        }
        if(lemax - lemin < Integer.MAX_VALUE)
          return new C4SChunk(bufX(lemin, xmin,C4SChunk.OFF,2),(int)lemin,DParseTask.pow10(xmin));
      }
      return chunkD();
    } // else an integer column
    // Compress column into a byte
    if(xmin == 0 &&  0<=lemin && lemax <= 255 && ((_naCnt + _strCnt)==0) )
      return new C1NChunk( bufX(0,0,C1NChunk.OFF,0));
    if(lemin < Integer.MIN_VALUE)return new C8Chunk( bufX(0,0,0,3));
    if( lemax-lemin < 255 ) {         // Span fits in a byte?
      if(0 <= min && max < 255 )      // Span fits in an unbiased byte?
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
    if( Integer.MIN_VALUE < min && max <= Integer.MAX_VALUE )
      return new C4Chunk( bufX(0,0,0,2));
    return new C8Chunk( bufX(0,0,0,3));
  }

  // Compute a compressed integer buffer
  private byte[] bufX( long bias, int scale, int off, int log ) {
    if( _len2 != _len ) cancel_sparse();
    byte[] bs = new byte[(_len2<<log)+off];
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
        int x = (_xs[i]==Integer.MIN_VALUE+1 ? 0 : _xs[i])-scale;
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

  // Compute a compressed double buffer
  private Chunk chunkD() {
    assert _len2==_len;
    final byte [] bs = MemoryManager.malloc1(_len*8);
    for(int i = 0; i < _len; ++i)
      UDP.set8d(bs, 8*i, _ds != null?_ds[i]:(isNA(i)||isEnum(i))?Double.NaN:_ls[i]*DParseTask.pow10(_xs[i]));
    return new C8DChunk(bs);
  }

  // Compute compressed boolean buffer
  private byte[] bufB(int bpv) {
    assert bpv == 1 || bpv == 2 : "Only bit vectors with/without NA are supported";
    final int off = CBSChunk.OFF;
    int clen  = off + CBSChunk.clen(_len2, bpv);
    byte bs[] = new byte[clen];
    // Save the gap = number of unfilled bits and bpv value
    bs[0] = (byte) (((_len2*bpv)&7)==0 ? 0 : (8-((_len2*bpv)&7)));
    bs[1] = (byte) bpv;

    if( _len2 != _len ) {       // Sparse bitvector?
      assert bpv==1;            // No NAs
      for (int i=0; i<_len; i++) {
        int row = _xs[i];
        bs[(row>>3)+off] = CBSChunk.write1b(bs[(row>>3)+off],(byte)1,row&7);
      }
      return bs;
    }

    // Dense bitvector
    int  boff = 0;
    byte b    = 0;
    int  idx  = CBSChunk.OFF;
    for (int i=0; i<_len; i++) {
      if( bpv==1 ) {
        assert !isNA(i);
        b = CBSChunk.write1b(b, (byte)_ls[i], boff);
      } else {
        byte val = isNA(i) ? CBSChunk._NA : (byte)_ls[i];
        b = CBSChunk.write2b(b, val, boff);
      }
      boff += bpv;
      if (boff>8-bpv) { bs[idx] = b; boff = 0; b = 0; idx++; }
    }
    assert bs[0] == (byte) (boff == 0 ? 0 : 8-boff);
    // Flush last byte
    if (boff>0) bs[idx++] = b;
    return bs;
  }

  // Set & At on NewChunks are weird: only used after inflating some other
  // chunk.  At this point the NewChunk is full size, no more appends allowed,
  // and the xs exponent array should be only full of zeros.  Accesses must be
  // in-range and refer to the inflated values of the original Chunk.
  @Override boolean set_impl(int i, long l) {
    if( _ds != null ) throw H2O.unimpl();
    if( _len2 != _len ) throw H2O.unimpl();
    _ls[i]=l; _xs[i]=0;
    return true;
  }
  @Override public boolean set_impl(int i, double d) {
    if( _ls != null ) {         // Flip to using doubles
      if( _len2 != _len ) throw H2O.unimpl();
      double ds[] = MemoryManager.malloc8d(_len);
      for( int j = 0; j<_len; j++ )
        ds[j] = (isNA(j) || isEnum(j)) ? Double.NaN : _ls[j]*Math.pow(10,_xs[j]);
      _ds = ds;  _ls = null;  _xs = null;
    }
    _ds[i]=d;
    return true;
  }
  @Override boolean set_impl(int i, float f) {  return set_impl(i,(double)f); }
  @Override boolean setNA_impl(int i) {
    if( isNA(i) ) return true;
    if( _len2 != _len ) throw H2O.unimpl();
    if( _ls != null ) { _ls[i] = 0; _xs[i] = Integer.MIN_VALUE; }
    if( _ds != null ) { _ds[i] = Double.NaN; }
    return true;
  }
  @Override public long   at8_impl( int i ) {
    if( _len2 != _len ) throw H2O.unimpl();
    if( _ls == null ) return (long)_ds[i];
    return _ls[i]*DParseTask.pow10i(_xs[i]);
  }
  @Override public double atd_impl( int i ) {
    if( _len2 != _len ) throw H2O.unimpl();
    if( _ds == null ) return at8_impl(i);
    assert _xs==null; return _ds[i];
  }
  @Override public boolean isNA_impl( int i ) {
    if( _len2 != _len ) throw H2O.unimpl();
    return isNA(i);
  }
  @Override public AutoBuffer write(AutoBuffer bb) { throw H2O.fail(); }
  @Override public NewChunk read(AutoBuffer bb) { throw H2O.fail(); }
  @Override NewChunk inflate_impl(NewChunk nc) { throw H2O.fail(); }
  @Override boolean hasFloat() { throw H2O.fail(); }
  @Override public String toString() { return "NewChunk._len="+_len; }
}
