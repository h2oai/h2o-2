package water.fvec;

import java.util.*;

import water.*;
import water.util.Log;

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
  // Sparse: if _sparseLen != _len, then _ls/_ds are compressed to non-zero's only,
  // and _xs is the row number.  Still _len is count of elements including
  // zeros, and _sparseLen is count of non-zeros.
  transient long   _ls[];       // Mantissa
  transient int    _xs[];       // Exponent, or if _ls==0, NA or Enum or Rows
  transient int    _id[];       // Indices (row numbers) of stored values, used for sparse
  transient double _ds[];       // Doubles, for inflating via doubles

  long  [] alloc_mantissa(int l) { return _ls = MemoryManager.malloc8(l); }
  int   [] alloc_exponent(int l) { return _xs = MemoryManager.malloc4(l); }
  int   [] alloc_indices(int l)  { return _id = MemoryManager.malloc4(l); }
  double[] alloc_doubles(int l)  { return _ds = MemoryManager.malloc8d(l); }

  final protected long  [] mantissa() { return _ls; }
  final protected int   [] exponent() { return _xs; }
  final protected int   []  indices() { return _id; }
  final protected double[]  doubles() { return _ds; }

  @Override public boolean isSparse() { return sparse(); }

  int _sparseLen;
  int set_sparseLen(int l) { _sparseLen = l; return l;}
  @Override public int sparseLen() { return _sparseLen; }

  int _naCnt=-1;                // Count of NA's   appended
  int _strCnt;                  // Count of Enum's appended
  int _nzCnt;                   // Count of non-zero's appended
  int _uuidCnt;                 // Count of UUIDs
  final int _timCnt[] = new int[ParseTime.TIME_PARSE.length]; // Count of successful time parses
  public static final int MIN_SPARSE_RATIO = 32;

  public NewChunk( Vec vec, int cidx ) { _vec = vec; _cidx = cidx; }

  public NewChunk( Vec vec, int cidx, boolean sparse ) {
    _vec = vec; _cidx = cidx;
    if(sparse) {
      _ls = new long[128];
      _xs = new int[128];
      _id = new int[128];
    }
  }

  public NewChunk(double [] ds) {
    _cidx = -1;
    _vec = null;
    _ds = ds;
    _sparseLen = _len = ds.length;
  }

  public NewChunk setSparseRatio(int s) {
    _sparseRatio = s;
    return this;
  }

  private int _sparseRatio = MIN_SPARSE_RATIO;
  public NewChunk(double [] ds, int [] ids, int len, int sparseLen, int sparseRatio) {
    _cidx = -1;
    _vec = null;
    _ds = ds;
    _len = len;
    _id = ids;
    _sparseLen = sparseLen;
    _sparseRatio = sparseRatio;
  }
  // Constructor used when inflating a Chunk.
  public NewChunk( Chunk C ) {
    this(C._vec, C._vec.elem2ChunkIdx(C._start));
    _start = C._start;
  }

  public NewChunk( Vec vec, int cidx, double [] vals) {
    this(vec,cidx);
    _ds = vals;
    _sparseLen = _len = vals.length;
    _nzCnt = 0;
    for(double d:vals){
      if(Double.isNaN(d)) ++_naCnt;
      else if(d != 0)
        _nzCnt++;
    }
  }
  // Pre-sized newchunks.
  public NewChunk( Vec vec, int cidx, int len ) {
    this(vec,cidx);
    _ds = new double[len];
    Arrays.fill(_ds,Double.NaN);
    _sparseLen = _len = len;
  }

  public final class Value {
    int _gId; // row number in dense (ie counting zeros)
    int _lId; // local array index of this value, equal to _gId if dense

    public Value(int lid, int gid){_lId = lid; _gId = gid;}
    public final int rowId0(){return _gId;}
    public void add2Chunk(NewChunk c){
      if(isNA2(_lId)) c.addNA();
      else if(_ds == null) c.addNum(_ls[_lId],_xs[_lId]);
      else {
        if( _ls != null ) c.addUUID(_ls[_lId], Double.doubleToRawLongBits(_ds[_lId]));
        else c.addNum(_ds[_lId]);
      }
    }
  }

  public Iterator<Value> values(){ return values(0,Integer.MAX_VALUE);}
  public Iterator<Value> values(int fromIdx, int toIdx){
    final int lId, gId;
    final int to = Math.min(toIdx, _len);
    if (sparse()) {
      int x = Arrays.binarySearch(_id, 0, _sparseLen, fromIdx);
      if (x < 0) x = -x - 1;
      lId = x;
      gId = x == _sparseLen ? _len : _id[x];
    } else
      lId = gId = fromIdx;
    final Value v = new Value(lId, gId);
    final Value next = new Value(lId, gId);
    return new Iterator<Value>() {
      @Override
      public final boolean hasNext() {
        return next._gId < to;
      }

      @Override
      public final Value next() {
        if (!hasNext()) throw new NoSuchElementException();
        v._gId = next._gId;
        v._lId = next._lId;
        next._lId++;
        if (sparse()) next._gId = next._lId < _sparseLen ? _id[next._lId] : _len;
        else next._gId++;
        return v;
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }


  // Heuristic to decide the basic type of a column
  public byte type() {
    if( _naCnt == -1 ) {        // No rollups yet?
      int nas=0, ss=0, nzs=0;
      if( _ds != null && _ls != null ) { // UUID?
        for( int i=0; i<_sparseLen; i++ )
          if( _xs != null && _xs[i]==Integer.MIN_VALUE )  nas++;
          else if( _ds[i] !=0 || _ls[i] != 0 ) nzs++;
        _uuidCnt = _len-nas;
      } else if( _ds != null ) { // Doubles?
        assert _xs==null;
        for( int i = 0; i < _sparseLen; ++i) if( Double.isNaN(_ds[i]) ) nas++; else if( _ds[i]!=0 ) nzs++;
      } else {                  // Longs and enums?
        if( _ls != null )
          for( int i=0; i<_sparseLen; i++ )
            if( isNA2(i) ) nas++;
            else {
              if( isEnum2(i)   ) ss++;
              if( _ls[i] != 0 ) nzs++;
            }
      }
      _nzCnt=nzs;  _strCnt=ss;  _naCnt=nas;
    }
    // Now run heuristic for type
    if(_naCnt == _len)          // All NAs ==> NA Chunk
      return AppendableVec.NA;
    if(_strCnt > 0 && _strCnt + _naCnt == _len)
      return AppendableVec.ENUM; // All are Strings+NAs ==> Enum Chunk
    // UUIDs?
    if( _uuidCnt > 0 ) return AppendableVec.UUID;
    // Larger of time & numbers
    int timCnt=0; for( int t : _timCnt ) timCnt+=t;
    int nums = _len-_naCnt-timCnt;
    return timCnt >= nums ? AppendableVec.TIME : AppendableVec.NUMBER;
  }

  protected final boolean isNA2(int idx) {
    if (isUUID()) return _ls[idx]==C16Chunk._LO_NA && Double.doubleToRawLongBits(_ds[idx])==C16Chunk._HI_NA;
    return (_ds == null) ? (_ls[idx] == Long.MAX_VALUE && _xs[idx] == Integer.MIN_VALUE) : Double.isNaN(_ds[idx]);
  }
  protected final boolean isEnum2(int idx) {
    return _xs!=null && _xs[idx]==Integer.MIN_VALUE+1;
  }
  protected final boolean isEnum(int idx) {
    if(_id == null)return isEnum2(idx);
    int j = Arrays.binarySearch(_id,0,_sparseLen,idx);
    return j>=0 && isEnum2(j);
  }

  public void addEnum(int e) {append2(e,Integer.MIN_VALUE+1);}
  public void addNA() {
    if( isUUID() ) addUUID(C16Chunk._LO_NA,C16Chunk._HI_NA);
    else if (_ds != null) addNum(Double.NaN);
    else append2(Long.MAX_VALUE,Integer.MIN_VALUE);
  }
  public void addNum (long val, int exp) {
    if( isUUID() ) addNA();
    else if(_ds != null) {
      assert _ls == null;
      addNum(val*PrettyPrint.pow10(exp));
    } else {
      if( val == 0 ) exp = 0;// Canonicalize zero
      long t;                // Remove extra scaling
      while( exp < 0 && exp > -9999999 && (t=val/10)*10==val ) { val=t; exp++; }
      append2(val,exp);
    }
  }
  // Fast-path append double data
  public void addNum(double d) {
    if( isUUID() ) { addNA(); return; }
    if(_id == null || d != 0) {
      if(_ls != null)switch_to_doubles();
      if( _ds == null || _sparseLen >= _ds.length ) {
        append2slowd();
        // call addNum again since append2slow might have flipped to sparse
        addNum(d);
        assert _sparseLen <= _len;
        return;
      }
      if(_id != null)_id[_sparseLen] = _len;
      _ds[_sparseLen++] = d;
    }
    _len++;
    assert _sparseLen <= _len;
  }
  // Append a UUID, stored in _ls & _ds
  public void addUUID( long lo, long hi ) {
    if( _ls==null || _ds== null || _sparseLen >= _ls.length )
      append2slowUUID();
    _ls[_sparseLen] = lo;
    _ds[_sparseLen++] = Double.longBitsToDouble(hi);
    _len++;
    assert _sparseLen <= _len;
  }
  public void addUUID( Chunk c, long row ) {
    if( c.isNA(row) ) addUUID(C16Chunk._LO_NA,C16Chunk._HI_NA);
    else addUUID(c.at16l(row),c.at16h(row));
  }
  public void addUUID( Chunk c, int row ) {
    if( c.isNA0(row) ) addUUID(C16Chunk._LO_NA,C16Chunk._HI_NA);
    else addUUID(c.at16l0(row),c.at16h0(row));
  }

  public final boolean isUUID(){return _ls != null && _ds != null; }
  public final boolean sparse(){return _id != null;}

  public void addZeros(int n){
    if(!sparse()) for(int i = 0; i < n; ++i)addNum(0,0);
    else _len += n;
  }
  // Append all of 'nc' onto the current NewChunk.  Kill nc.
  public void add( NewChunk nc ) {
    assert _cidx >= 0;
    assert _sparseLen <= _len;
    assert nc._sparseLen <= nc._len:"_sparseLen = " + nc._sparseLen + ", _len = " + nc._len;
    if( nc._len == 0 ) return;
    if(_len == 0){
      _ls = nc._ls; nc._ls = null;
      _xs = nc._xs; nc._xs = null;
      _id = nc._id; nc._id = null;
      _ds = nc._ds; nc._ds = null;
      _sparseLen = nc._sparseLen;
      _len = nc._len;
      return;
    }
    if(nc.sparse() != sparse()){ // for now, just make it dense
      cancel_sparse();
      nc.cancel_sparse();
    }
    if( _ds != null ) throw H2O.unimpl();
    while( _sparseLen+nc._sparseLen >= _xs.length )
      _xs = MemoryManager.arrayCopyOf(_xs,_xs.length<<1);
    _ls = MemoryManager.arrayCopyOf(_ls,_xs.length);
    System.arraycopy(nc._ls,0,_ls,_sparseLen,nc._sparseLen);
    System.arraycopy(nc._xs,0,_xs,_sparseLen,nc._sparseLen);
    if(_id != null) {
      assert nc._id != null;
      _id = MemoryManager.arrayCopyOf(_id,_xs.length);
      System.arraycopy(nc._id,0,_id,_sparseLen,nc._sparseLen);
      for(int i = _sparseLen; i < _sparseLen + nc._sparseLen; ++i) _id[i] += _len;
    } else assert nc._id == null;

    _sparseLen += nc._sparseLen;
    _len += nc._len;
    nc._ls = null;  nc._xs = null; nc._id = null; nc._sparseLen = nc._len = 0;
    assert _sparseLen <= _len;
  }
  // PREpend all of 'nc' onto the current NewChunk.  Kill nc.
  public void addr( NewChunk nc ) {
    long  [] tmpl = _ls; _ls = nc._ls; nc._ls = tmpl;
    int   [] tmpi = _xs; _xs = nc._xs; nc._xs = tmpi;
             tmpi = _id; _id = nc._id; nc._id = tmpi;
    double[] tmpd = _ds; _ds = nc._ds; nc._ds = tmpd;
    int      tmp  = _sparseLen; _sparseLen=nc._sparseLen; nc._sparseLen=tmp;
             tmp  = _len; _len = nc._len; nc._len = tmp;
    add(nc);
  }

  // Fast-path append long data
  void append2( long l, int x ) {
    if(_id == null || l != 0){
      if(_ls == null || _sparseLen == _ls.length) {
        append2slow();
        // again call append2 since calling append2slow might have changed things (eg might have switched to sparse and l could be 0)
        append2(l,x);
        return;
      }
      _ls[_sparseLen] = l;
      _xs[_sparseLen] = x;
      if(_id  != null)_id[_sparseLen] = _len;
      _sparseLen++;
    }
    _len++;
    assert _sparseLen <= _len;
  }

  // Slow-path append data
  private void append2slowd() {
    final int CHUNK_SZ = 1 << H2O.LOG_CHK;
    if( _sparseLen > CHUNK_SZ )
      throw new ArrayIndexOutOfBoundsException(_sparseLen);
    assert _ls==null;
    if(_ds != null && _ds.length > 0){
      if(_id == null){ // check for sparseness
        int nzs = 0; // assume one non-zero for the element currently being stored
        for(double d:_ds)if(d != 0)++nzs;
        if((nzs+1)*_sparseRatio < _len)
          set_sparse(nzs);
      } else _id = MemoryManager.arrayCopyOf(_id, _sparseLen << 1);
      _ds = MemoryManager.arrayCopyOf(_ds,_sparseLen<<1);
    } else {
      alloc_doubles(4);
      if (sparse()) alloc_indices(4);
    }
    assert _sparseLen == 0 || _ds.length > _sparseLen :"_ds.length = " + _ds.length + ", _sparseLen = " + _sparseLen;
  }
  // Slow-path append data
  private void append2slowUUID() {
    final int CHUNK_SZ = 1 << H2O.LOG_CHK;
    if( _sparseLen > CHUNK_SZ )
      throw new ArrayIndexOutOfBoundsException(_sparseLen);
    if( _ds==null && _ls!=null ) { // This can happen for columns with all NAs and then a UUID
      _xs=null;
      alloc_doubles(_sparseLen);
      Arrays.fill(_ls,C16Chunk._LO_NA);
      Arrays.fill(_ds,Double.longBitsToDouble(C16Chunk._HI_NA));
    }
    if( _ls != null && _ls.length > 0 ) {
      _ls = MemoryManager.arrayCopyOf(_ls,_sparseLen<<1);
      _ds = MemoryManager.arrayCopyOf(_ds,_sparseLen<<1);
    } else {
      alloc_mantissa(4);
      alloc_doubles(4);
    }
    assert _sparseLen == 0 || _ls.length > _sparseLen:"_ls.length = " + _ls.length + ", _sparseLen = " + _sparseLen;
  }
  // Slow-path append data
  private void append2slow( ) {
    final int CHUNK_SZ = 1 << H2O.LOG_CHK;
    if( _sparseLen > CHUNK_SZ )
      throw new ArrayIndexOutOfBoundsException(_sparseLen);
    assert _ds==null;
    if(_ls != null && _ls.length > 0){
      if(_id == null){ // check for sparseness
        int nzs = 0;
        for(int i = 0; i < _ls.length; ++i) if(_ls[i] != 0 || _xs[i] != 0)++nzs;
        if((nzs+1)*_sparseRatio < _len){
          set_sparse(nzs);
          assert _sparseLen == 0 || _sparseLen  <= _ls.length:"_sparseLen = " + _sparseLen + ", _ls.length = " + _ls.length + ", nzs = " + nzs +  ", len2 = " + _len;
          assert _id.length == _ls.length;
          assert _sparseLen <= _len;
          return;
        }
      } else {
        // verify we're still sufficiently sparse
        if((_sparseRatio*(_sparseLen) >> 1) > _len)  cancel_sparse();
        else _id = MemoryManager.arrayCopyOf(_id,_sparseLen<<1);
      }
      _ls = MemoryManager.arrayCopyOf(_ls,_sparseLen<<1);
      _xs = MemoryManager.arrayCopyOf(_xs,_sparseLen<<1);
    } else {
      alloc_mantissa(4);
      alloc_exponent(4);
      if (_id != null) alloc_indices(4);
    }
    assert _sparseLen == 0 || _sparseLen < _ls.length:"_sparseLen = " + _sparseLen + ", _ls.length = " + _ls.length;
    assert _id == null || _id.length == _ls.length;
    assert _sparseLen <= _len;
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

  protected void switch_to_doubles(){
    assert _ds == null;
    double [] ds = MemoryManager.malloc8d(_sparseLen);
    for(int i = 0; i < _sparseLen; ++i)
      if(isNA2(i) || isEnum2(i))ds[i] = Double.NaN;
      else  ds[i] = _ls[i]*PrettyPrint.pow10(_xs[i]);
    _ls = null;
    _xs = null;
    _ds = ds;
  }
  protected void set_sparse(int nzeros){
    if(_sparseLen == nzeros && _len != 0)return;
    if(_id != null){ // we have sparse represenation but some 0s in it!
      int [] id = MemoryManager.malloc4(nzeros);
      int j = 0;
      if(_ds != null){
        double [] ds = MemoryManager.malloc8d(nzeros);
        for(int i = 0; i < _sparseLen; ++i){
          if(_ds[i] != 0){
            ds[j] = _ds[i];
            id[j] = _id[i];
            ++j;
          }
        }
        _ds = ds;
      } else {
        long [] ls = MemoryManager.malloc8(nzeros);
        int [] xs = MemoryManager.malloc4(nzeros);
        for(int i = 0; i < _sparseLen; ++i){
          if(_ls[i] != 0){
            ls[j] = _ls[i];
            xs[j] = _xs[i];
            id[j] = _id[i];
            ++j;
          }
        }
        _ls = ls;
        _xs = xs;
      }
      _id = id;
      assert j == nzeros;
      _sparseLen = nzeros;
      return;
    }
    assert _sparseLen == _len:"_sparseLen = " + _sparseLen + ", _len = " + _len + ", nzeros = " + nzeros;
    int zs = 0;
    if(_ds == null){
      if (_len == 0) {
        _ls = new long[0];
        _xs = new int[0];
        _id = new int[0];
        _sparseLen = 0;
        return;
      } else {
        assert nzeros < _ls.length;
        _id = MemoryManager.malloc4(_ls.length);
        for (int i = 0; i < _sparseLen; ++i) {
          if (_ls[i] == 0 && _xs[i] == 0) ++zs;
          else {
            _ls[i - zs] = _ls[i];
            _xs[i - zs] = _xs[i];
            _id[i - zs] = i;
          }
        }
      }
    } else {
      assert nzeros < _ds.length;
      _id = alloc_indices(_ds.length);
      for(int i = 0; i < _sparseLen; ++i){
        if(_ds[i] == 0)++zs;
        else {
          _ds[i-zs] = _ds[i];
          _id[i-zs] = i;
        }
      }
    }
    assert zs == (_sparseLen - nzeros);
    _sparseLen = nzeros;
  }
  protected void cancel_sparse(){
    if(_sparseLen != _len){
      if(_ds == null){
        int []  xs = MemoryManager.malloc4(_len);
        long [] ls = MemoryManager.malloc8(_len);
        for(int i = 0; i < _sparseLen; ++i){
          xs[_id[i]] = _xs[i];
          ls[_id[i]] = _ls[i];
        }
        _xs = xs;
        _ls = ls;
      } else {
        double [] ds = MemoryManager.malloc8d(_len);
        for(int i = 0; i < _sparseLen; ++i) ds[_id[i]] = _ds[i];
        _ds = ds;
      }
      _sparseLen = _len;
    }
    _id = null;
  }
  // Study this NewVector and determine an appropriate compression scheme.
  // Return the data so compressed.
  static final int MAX_FLOAT_MANTISSA = 0x7FFFFF;

  public Chunk compress() {
    Chunk res = compress2();
    assert _len == res.len();
    assert !sparse() || !res.isSparse() || sparseLen() == res.sparseLen();
    // force everything to null after compress to free up the memory
    _id = null;
    _xs = null;
    _ds = null;
    _ls = null;
    return res;
  }

  private static long leRange(long lemin, long lemax){
    if(lemin < 0 && lemax >= (Long.MAX_VALUE + lemin))
      return Long.MAX_VALUE; // if overflow return 64 as the max possible value
    long res = lemax - lemin;
    assert res >= 0;
    return res;
  }

  private Chunk compress2() {
    // Check for basic mode info: all missing or all strings or mixed stuff
    byte mode = type();
    if( mode==AppendableVec.NA ) // ALL NAs, nothing to do
      return new C0DChunk(Double.NaN,_sparseLen);
    boolean rerun=false;
    if(mode == AppendableVec.ENUM){
      for( int i=0; i<_sparseLen; i++ )
        if(isEnum2(i))
          _xs[i] = 0;
        else if(!isNA2(i)){
          setNA_impl2(i);
          ++_naCnt;
        }
        // Smack any mismatched string/numbers
    } else if(mode == AppendableVec.NUMBER){
      for( int i=0; i<_sparseLen; i++ )
        if(isEnum2(i)) {
          setNA_impl2(i);
          rerun = true;
        }
    }
    if( rerun ) { _naCnt = -1;  type(); } // Re-run rollups after dropping all numbers/enums
    boolean sparse = false;
    // sparse? treat as sparse iff we have at least MIN_SPARSE_RATIOx more zeros than nonzeros
    if(_sparseRatio*(_naCnt + _nzCnt) < _len) {
      set_sparse(_naCnt + _nzCnt);
      sparse = true;
    } else if(_sparseLen != _len)
      cancel_sparse();


    // If the data is UUIDs there's not much compression going on
    if( _ds != null && _ls != null )
      return chunkUUID();

    // If the data was set8 as doubles, we do a quick check to see if it's
    // plain longs.  If not, we give up and use doubles.
    if( _ds != null ) {
      int i=0;
      boolean isConstant = true;
      boolean isInteger = true;
      if ( sparse ) {
        isConstant = _sparseLen == 0;
        for( ; i< _sparseLen; i++ ) {
          if (!Double.isNaN(_ds[i])) isInteger &= (double) (long) _ds[i] == _ds[i];
        }
      } else {
        assert(_ds.length >= _len);
        for( ; i< _len; i++ ) {
          if (!Double.isNaN(_ds[i])) isInteger &= (double) (long) _ds[i] == _ds[i];
          isConstant &= _ds[i] == _ds[0];
        }
        assert(_sparseLen == _len);
      }
      if (!isInteger) {
        if (isConstant) return new C0DChunk(_ds[0], _len);
        if (sparse) return new CXDChunk(len(), sparseLen(), 8, bufD(8));
        else return chunkD();
      }

      _ls = new long[_ds.length]; // Else flip to longs
      _xs = new int [_ds.length];
      double [] ds = _ds;
      _ds = null;
      final int naCnt = _naCnt;
      for( i=0; i< _sparseLen; i++ )   // Inject all doubles into longs
        if( Double.isNaN(ds[i]) )setNA_impl2(i);
        else                     _ls[i] = (long)ds[i];
      // setNA_impl2 will set _naCnt to -1!
      // we already know what the naCnt is (it did not change!) so set it back to correct value
      _naCnt = naCnt;
    }

    // IF (_len > _sparseLen) THEN Sparse
    // Check for compressed *during appends*.  Here we know:
    // - No specials; _xs[]==0.
    // - No floats; _ds==null
    // - NZ length in _sparseLen, actual length in _len.
    // - Huge ratio between _len and _sparseLen, and we do NOT want to inflate to
    //   the larger size; we need to keep it all small all the time.
    // - Rows in _xs

    // Data in some fixed-point format, not doubles
    // See if we can sanely normalize all the data to the same fixed-point.
    int  xmin = Integer.MAX_VALUE;   // min exponent found
    boolean floatOverflow = false;
    double min = Double.POSITIVE_INFINITY;
    double max = Double.NEGATIVE_INFINITY;
    int p10iLength = PrettyPrint.powers10i.length;
    long llo=Long   .MAX_VALUE, lhi=Long   .MIN_VALUE;
    int  xlo=Integer.MAX_VALUE, xhi=Integer.MIN_VALUE;

    for( int i=0; i<_sparseLen; i++ ) {
      if( isNA2(i) ) continue;
      long l = _ls[i];
      int  x = _xs[i];
      assert x != Integer.MIN_VALUE:"l = " + l + ", x = " + x;
      if( x==Integer.MIN_VALUE+1) x=0; // Replace enum flag with no scaling
      assert l!=0 || x==0:"l == 0 while x = " + x + " ls = " + Arrays.toString(_ls);      // Exponent of zero is always zero
      long t;                   // Remove extra scaling
      while( l!=0 && (t=l/10)*10==l ) { l=t; x++; }
      // Compute per-chunk min/max
      double d = l*PrettyPrint.pow10(x);
      if( d < min ) { min = d; llo=l; xlo=x; }
      if( d > max ) { max = d; lhi=l; xhi=x; }
      floatOverflow = l < Integer.MIN_VALUE+1 || l > Integer.MAX_VALUE;
      xmin = Math.min(xmin,x);
    }

    if(_len != _sparseLen){ // sparse?  then compare vs implied 0s
      if( min > 0 ) { min = 0; llo=0; xlo=0; }
      if( max < 0 ) { max = 0; lhi=0; xhi=0; }
      xmin = Math.min(xmin,0);
    }

    // Constant column?
    if( _naCnt==0 && (min==max)) {
      if (llo == lhi && xlo == 0 && xhi == 0)
        return new C0LChunk(llo, len());
      else if ((long)min == min)
        return new C0LChunk((long)min, len());
      else
        return new C0DChunk(min, len());
    }

    // Compute min & max, as scaled integers in the xmin scale.
    // Check for overflow along the way
    boolean overflow = ((xhi-xmin) >= p10iLength) || ((xlo-xmin) >= p10iLength);
    long lemax=0, lemin=0;
    if( !overflow ) {           // Can at least get the power-of-10 without overflow
      long pow10 = PrettyPrint.pow10i(xhi-xmin);
      lemax = lhi*pow10;
      // Hacker's Delight, Section 2-13, checking overflow.
      // Note that the power-10 is always positive, so the test devolves this:
      if( (lemax/pow10) != lhi ) overflow = true;
      // Note that xlo might be > xmin; e.g. { 101e-49 , 1e-48}.
      long pow10lo = PrettyPrint.pow10i(xlo-xmin);
      lemin = llo*pow10lo;
      if( (lemin/pow10lo) != llo ) overflow = true;
    }

    // Boolean column?
    if (max == 1 && min == 0 && xmin == 0 && !overflow) {
      if(sparse) { // Very sparse?
        return  _naCnt==0
          ? new CX0Chunk(_len,_sparseLen,bufS(0))// No NAs, can store as sparse bitvector
          : new CXIChunk(_len,_sparseLen,1,bufS(1)); // have NAs, store as sparse 1byte values
      }

      int bpv = _strCnt+_naCnt > 0 ? 2 : 1;   // Bit-vector
      byte[] cbuf = bufB(bpv);
      return new CBSChunk(cbuf, cbuf[0], cbuf[1]);
    }

    final boolean fpoint = xmin < 0 || min < Long.MIN_VALUE || max > Long.MAX_VALUE;

    if( sparse ) {
      if(fpoint) return new CXDChunk(_len,_sparseLen,8,bufD(8));
      int sz = 8;
      if( Short.MIN_VALUE <= min && max <= Short.MAX_VALUE ) sz = 2;
      else if( Integer.MIN_VALUE <= min && max <= Integer.MAX_VALUE ) sz = 4;
      return new CXIChunk(_len,_sparseLen,sz,bufS(sz));
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
    if( overflow || (fpoint && floatOverflow) || -35 > xmin || xmin > 35 )
      return chunkD();
    final long leRange = leRange(lemin,lemax);
    if( fpoint ) {
      if( (int)lemin == lemin && (int)lemax == lemax ) {
        if(leRange < 255) // Fits in scaled biased byte?
          return new C1SChunk( bufX(lemin,xmin,C1SChunk.OFF,0),lemin,PrettyPrint.pow10(xmin));
        if(leRange < 65535) { // we use signed 2B short, add -32k to the bias!
          long bias = 32767 + lemin;
          return new C2SChunk( bufX(bias,xmin,C2SChunk.OFF,1),bias,PrettyPrint.pow10(xmin));
        }
      }
      if(leRange < 4294967295l) {
        long bias = 2147483647l + lemin;
        return new C4SChunk( bufX(bias,xmin,C4SChunk.OFF,2),bias,PrettyPrint.pow10(xmin));
      }
      return chunkD();
    } // else an integer column

    // Compress column into a byte
    if(xmin == 0 &&  0<=lemin && lemax <= 255 && ((_naCnt + _strCnt)==0) )
      return new C1NChunk( bufX(0,0,C1NChunk.OFF,0));
    if( lemin < Integer.MIN_VALUE ) return new C8Chunk( bufX(0,0,0,3));
    if( leRange < 255 ) {    // Span fits in a byte?
      if(0 <= min && max < 255 ) // Span fits in an unbiased byte?
        return new C1Chunk( bufX(0,0,C1Chunk.OFF,0));
      return new C1SChunk( bufX(lemin,xmin,C1SChunk.OFF,0),lemin,PrettyPrint.pow10i(xmin));
    }

    // Compress column into a short
    if( leRange < 65535 ) {               // Span fits in a biased short?
      if( xmin == 0 && Short.MIN_VALUE < lemin && lemax <= Short.MAX_VALUE ) // Span fits in an unbiased short?
        return new C2Chunk( bufX(0,0,C2Chunk.OFF,1));
      int bias = (int)(lemin-(Short.MIN_VALUE+1));
      return new C2SChunk( bufX(bias,xmin,C2SChunk.OFF,1),bias,PrettyPrint.pow10i(xmin));
    }
    // Compress column into ints
    if( Integer.MIN_VALUE < min && max <= Integer.MAX_VALUE )
      return new C4Chunk( bufX(0,0,0,2));
    return new C8Chunk( bufX(0,0,0,3));
  }

  private static long [] NAS = {C1Chunk._NA,C2Chunk._NA,C4Chunk._NA,C8Chunk._NA};

  // Compute a sparse integer buffer
  private byte[] bufS(final int valsz){
    int log = 0;
    while((1 << log) < valsz)++log;
    assert valsz == 0 || (1 << log) == valsz;
    final int ridsz = _len >= 65535?4:2;
    final int elmsz = ridsz + valsz;
    int off = CXIChunk.OFF;
    byte [] buf = MemoryManager.malloc1(off + _sparseLen*elmsz,true);
    for( int i=0; i<_sparseLen; i++, off += elmsz ) {
      if(ridsz == 2)
        UDP.set2(buf,off,(short)_id[i]);
      else
        UDP.set4(buf,off,_id[i]);
      if(valsz == 0){
        assert _xs[i] == 0 && _ls[i] == 1;
        continue;
      }
      assert _xs[i] == Integer.MIN_VALUE || _xs[i] >= 0:"unexpected exponent " + _xs[i]; // assert we have int or NA
      final long lval = _xs[i] == Integer.MIN_VALUE?NAS[log]:_ls[i]*PrettyPrint.pow10i(_xs[i]);
      switch(valsz){
        case 1:
          buf[off+ridsz] = (byte)lval;
          break;
        case 2:
          short sval = (short)lval;
          UDP.set2(buf,off+ridsz,sval);
          break;
        case 4:
          int ival = (int)lval;
          UDP.set4(buf, off+ridsz, ival);
          break;
        case 8:
          UDP.set8(buf, off+ridsz, lval);
          break;
        default:
          throw H2O.unimpl();
      }
    }
    assert off==buf.length;
    return buf;
  }


  //  static byte [] toBytes(double [] vals, int [] ids, int slen){
//    int ridsz = vals.length >= 65535?4:2;
//    int elemsz = ridsz + 8;
//    byte [] res = MemoryManager.malloc1(slen*elemsz+OFF);
//    for(int i = 0, off = OFF; i < slen; ++i, off += elemsz) {
//      int id = ids[i];
//      if(elemsz == 2)
//        UDP.set2(res,off,(short)id);
//      else
//        UDP.set4(res,off,id);
//      UDP.set8d(res, off + ridsz, vals[id]);
//    }
//    return res;
//  }
//  public CXDChunk(double [] vals, int [] ids, int slen){
//    super(vals.length,slen,8,toBytes(vals,ids,slen));
//  }


  // Compute a sparse float buffer
  private byte[] bufD(final int valsz){
    int log = 0;
    while((1 << log) < valsz)++log;
    assert (1 << log) == valsz;
    final int ridsz = _len >= 65535?4:2;
    final int elmsz = ridsz + valsz;
    int off = CXDChunk.OFF;
    byte [] buf = MemoryManager.malloc1(off + _sparseLen*elmsz,true);
    for( int i=0; i<_sparseLen; i++, off += elmsz ) {
      if(ridsz == 2)
        UDP.set2(buf,off,(short)_id[i]);
      else
        UDP.set4(buf,off,_id[i]);
      final double dval = _ds == null?isNA2(i)?Double.NaN:_ls[i]*PrettyPrint.pow10(_xs[i]):_ds[i];
      switch(valsz){
        case 4:
          UDP.set4f(buf, off + ridsz, (float) dval);
          break;
        case 8:
          UDP.set8d(buf, off + ridsz, dval);
          break;
        default:
          throw H2O.unimpl();
      }
    }
    assert off==buf.length;
    return buf;
  }
  // Compute a compressed integer buffer
  private byte[] bufX( long bias, int scale, int off, int log ) {
    byte[] bs = new byte[(_len<<log)+off];
    int j = 0;
    for( int i=0; i<_len; i++ ) {
      long le = -bias;
      if(_id == null || _id.length == 0 || (j < _id.length && _id[j] == i)){
        if( isNA2(j) ) {
          le = NAS[log];
        } else {
          int x = (_xs[j]==Integer.MIN_VALUE+1 ? 0 : _xs[j])-scale;
          le += x >= 0
              ? _ls[j]*PrettyPrint.pow10i( x)
              : _ls[j]/PrettyPrint.pow10i(-x);
        }
        ++j;
      }
      switch( log ) {
        case 0:          bs [i    +off] = (byte)le ; break;
        case 1: UDP.set2(bs,(i<<1)+off,  (short)le); break;
        case 2: UDP.set4(bs,(i<<2)+off,    (int)le); break;
        case 3: UDP.set8(bs,(i<<3)+off,         le); break;
        default: throw H2O.fail();
      }
    }
    assert j == _sparseLen:"j = " + j + ", len = " + _sparseLen + ", len2 = " + _len + ", id[j] = " + _id[j];
    return bs;
  }

  // Compute a compressed double buffer
  private Chunk chunkD() {
    if (H2O.SINGLE_PRECISION) {
      final byte[] bs = MemoryManager.malloc1(_len * 4, true);
      int j = 0;
      for (int i = 0; i < _len; ++i) {
        float f = 0;
        if (_id == null || _id.length == 0 || (j < _id.length && _id[j] == i)) {
          f = _ds != null ? (float)_ds[j] : (isNA2(j) || isEnum(j)) ? Float.NaN : (float)(_ls[j] * PrettyPrint.pow10(_xs[j]));
          ++j;
        }
        UDP.set4f(bs, 4 * i, f);
      }
      assert j == _sparseLen : "j = " + j + ", _sparseLen = " + _sparseLen;
      return new C4FChunk(bs);
    } else {
      final byte[] bs = MemoryManager.malloc1(_len * 8, true);
      int j = 0;
      for (int i = 0; i < _len; ++i) {
        double d = 0;
        if (_id == null || _id.length == 0 || (j < _id.length && _id[j] == i)) {
          d = _ds != null ? _ds[j] : (isNA2(j) || isEnum(j)) ? Double.NaN : _ls[j] * PrettyPrint.pow10(_xs[j]);
          ++j;
        }
        UDP.set8d(bs, 8 * i, d);
      }
      assert j == _sparseLen : "j = " + j + ", _sparseLen = " + _sparseLen;
      return new C8DChunk(bs);
    }
  }

  // Compute a compressed UUID buffer
  private Chunk chunkUUID() {
    final byte [] bs = MemoryManager.malloc1(_len*16,true);
    int j = 0;
    for( int i = 0; i < _len; ++i ) {
      long lo = 0, hi=0;
      if(_id == null || _id.length == 0 || (j < _id.length && _id[j] == i ) ) {
        lo = _ls[j];
        hi = Double.doubleToRawLongBits(_ds[j++]);
        if( _xs != null && _xs[j] == Integer.MAX_VALUE){// NA?
          lo = Long.MIN_VALUE; hi = 0;                  // Canonical NA value
        }
      }
      UDP.set8(bs, 16*i  , lo);
      UDP.set8(bs, 16*i+8, hi);
    }
    assert j == _sparseLen:"j = " + j + ", _sparseLen = " + _sparseLen;
    return new C16Chunk(bs);
  }

  // Compute compressed boolean buffer
  private byte[] bufB(int bpv) {
    assert bpv == 1 || bpv == 2 : "Only bit vectors with/without NA are supported";
    final int off = CBSChunk.OFF;
    int clen  = off + CBSChunk.clen(_len, bpv);
    byte bs[] = new byte[clen];
    // Save the gap = number of unfilled bits and bpv value
    bs[0] = (byte) (((_len*bpv)&7)==0 ? 0 : (8-((_len*bpv)&7)));
    bs[1] = (byte) bpv;

    // Dense bitvector
    int  boff = 0;
    byte b    = 0;
    int  idx  = CBSChunk.OFF;
    int j = 0;
    for (int i=0; i<_len; i++) {
      byte val = 0;
      if(_id == null || (j < _id.length && _id[j] == i)) {
        assert bpv == 2 || !isNA2(j);
        val = (byte)(isNA2(j)?CBSChunk._NA:_ls[j]);
        ++j;
      }
      if( bpv==1 )
        b = CBSChunk.write1b(b, val, boff);
      else
        b = CBSChunk.write2b(b, val, boff);
      boff += bpv;
      if (boff>8-bpv) { assert boff == 8; bs[idx] = b; boff = 0; b = 0; idx++; }
    }
    assert j == _sparseLen;
    assert bs[0] == (byte) (boff == 0 ? 0 : 8-boff):"b[0] = " + bs[0] + ", boff = " + boff + ", bpv = " + bpv;
    // Flush last byte
    if (boff>0) bs[idx] = b;
    return bs;
  }

  // Set & At on NewChunks are weird: only used after inflating some other
  // chunk.  At this point the NewChunk is full size, no more appends allowed,
  // and the xs exponent array should be only full of zeros.  Accesses must be
  // in-range and refer to the inflated values of the original Chunk.
  @Override boolean set_impl(int i, long l) {
    if( _ds   != null ) return set_impl(i,(double)l);
    if(_sparseLen != _len){ // sparse?
      int idx = Arrays.binarySearch(_id,0,_sparseLen,i);
      if(idx >= 0)i = idx;
      else cancel_sparse(); // for now don't bother setting the sparse value
    }
    _ls[i]=l; _xs[i]=0;
    _naCnt = -1;
    return true;
  }

  @Override public boolean set_impl(int i, double d) {
    if(_ds == null){
      assert _sparseLen == 0 || _ls != null;
      switch_to_doubles();
    }
    if(_sparseLen != _len){ // sparse?
      int idx = Arrays.binarySearch(_id,0,_sparseLen,i);
      if(idx >= 0)i = idx;
      else cancel_sparse(); // for now don't bother setting the sparse value
    }
    while(i >= _len) append2slowd();
    _ds[i] = d;
    _naCnt = -1;
    return true;
  }
  @Override boolean set_impl(int i, float f) {  return set_impl(i,(double)f); }

  protected final boolean setNA_impl2(int i) {
    if( isNA2(i) ) return true;
    if( _ls != null ) { _ls[i] = Long.MAX_VALUE; _xs[i] = Integer.MIN_VALUE; }
    if( _ds != null ) { _ds[i] = Double.NaN; }
    _naCnt = -1;
    return true;
  }
  @Override boolean setNA_impl(int i) {
    if( isNA_impl(i) ) return true;
    if(_sparseLen != _len){
      int idx = Arrays.binarySearch(_id,0,_sparseLen,i);
      if(idx >= 0) i = idx;
      else cancel_sparse(); // todo - do not necessarily cancel sparse here
    }
    return setNA_impl2(i);
  }
  @Override public long   at8_impl( int i ) {
    if( _len != _sparseLen ) {
      int idx = Arrays.binarySearch(_id,0,_sparseLen,i);
      if(idx >= 0) i = idx;
      else return 0;
    }
    if(isNA2(i))throw new RuntimeException("Attempting to access NA as integer value.");
    if( _ls == null ) return (long)_ds[i];
    return _ls[i]*PrettyPrint.pow10i(_xs[i]);
  }
  @Override public double atd_impl( int i ) {
    if( _len != _sparseLen ) {
      int idx = Arrays.binarySearch(_id,0,_sparseLen,i);
      if(idx >= 0) i = idx;
      else return 0;
    }
    // if exponent is Integer.MIN_VALUE (for missing value) or >=0, then go the integer path (at8_impl)
    // negative exponents need to be handled right here
    if( _ds == null ) return isNA2(i) || _xs[i] >= 0 ? at8_impl(i) : _ls[i]*Math.pow(10,_xs[i]);
    assert _xs==null; return _ds[i];
  }
  @Override protected long at16l_impl(int idx) {
    if(_ls[idx] == C16Chunk._LO_NA) throw new RuntimeException("Attempting to access NA as integer value.");
    return _ls[idx];
  }
  @Override protected long at16h_impl(int idx) {
    long hi = Double.doubleToRawLongBits(_ds[idx]);
    if(hi == C16Chunk._HI_NA) throw new RuntimeException("Attempting to access NA as integer value.");
    return hi;
  }
  @Override public boolean isNA_impl( int i ) {
    if( _len != _sparseLen ) {
      int idx = Arrays.binarySearch(_id,0,_sparseLen,i);
      if(idx >= 0) i = idx;
      else return false;
    }
    return isNA2(i);
  }
  @Override public AutoBuffer write(AutoBuffer bb) { throw H2O.fail(); }
  @Override public NewChunk read(AutoBuffer bb) { throw H2O.fail(); }
  @Override NewChunk inflate_impl(NewChunk nc) { throw H2O.fail(); }
  @Override boolean hasFloat() { throw H2O.fail(); }
  @Override public String toString() { return "NewChunk._sparseLen="+_sparseLen; }
}
