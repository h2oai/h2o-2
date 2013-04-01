package water;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;

/**
* Large Arrays & Arraylets
*
* Large arrays are broken into 1Meg chunks (except the last chunk which may be
* from 1 to 2Megs). Large arrays have a metadata section in this ValueArray.
*
* @author <a href="mailto:cliffc@0xdata.com"></a>
* @version 1.0
*/


public class ValueArray extends Iced implements Cloneable {

  public static final int LOG_CHK = 20; // Chunks are 1<<20, or 1Meg
  public static final long CHUNK_SZ = 1L << LOG_CHK;

  // --------------------------------------------------------
  // Large datasets need metadata. :-)
  //
  // We describe datasets as either being "raw" ascii or unformatted binary
  // data, or as being "structured binary" data - i.e., binary, floats, or
  // ints. Structured data is efficient for doing math & matrix manipulation.
  //
  // Structured data is limited to being 2-D, a collection of rows and columns.
  // The count of columns is expected to be small - from 1 to 1000.  The count
  // of rows is unlimited and could be more than 2^32.  We expect data in
  // columns to be highly compressable within a column, as data with a dynamic
  // range of less than 1 byte is common (or equivalently, floating point data
  // with only 2 or 3 digits of accuracy).  Because data volumes matter (when
  // you have billions of rows!), we want to compress the data while leaving it
  // in an efficient-to-use format.
  //
  // The primary compression is to use 1-byte, 2-byte, or 4-byte columns, with
  // an optional offset & scale factor.  These are described in the meta-data.

  public transient Key _key;     // Main Array Key
  public final Column[] _cols;   // The array of column descriptors; the X dimension
  public long[] _rpc;            // Row# for start of each chunk
  public long _numrows;      // Number of rows; the Y dimension.  Can be >>2^32
  public final int _rowsize;     // Size in bytes for an entire row

  public ValueArray(Key key, long numrows, int rowsize, Column[] cols ) {
    // Always some kind of rowsize.  For plain unstructured data use a single
    // byte column format.
    assert rowsize > 0;
    _numrows = numrows;
    _rowsize = rowsize;
    _cols = cols;
    init(key);
  }

  // Plain unstructured data wrapper.  Just a vast byte array
  public ValueArray(Key key, long len ) { this(key,len,1,new Column[]{new Column(len)}); }

  // Variable-sized chunks.  Pass in the number of whole rows in each chunk.
  public ValueArray(Key key, int[] rows, int rowsize, Column[] cols ) {
    assert rowsize > 0;
    _rowsize = rowsize;
    _cols = cols;
    _key = key;
    // Roll-up summary the number rows in each chunk, to the starting row# per chunk.
    _rpc = new long[rows.length+1];
    long sum = 0;
    for( int i=0; i<rows.length; i++ ) { // Variable-sized chunks
      int r = rows[i];                   // Rows in chunk# i
      assert r*rowsize < (CHUNK_SZ*4);   // Keep them reasonably sized please
      _rpc[i] = sum;                     // Starting row# for chunk i
      sum += r;
    }
    _rpc[_rpc.length-1] = _numrows = sum;
    assert rpc(0) == rows[0];   // Some quicky sanity checks
    assert rpc(chunks()-1) == rows[(int)(chunks()-1)];
  }

  public int [] getColumnIds(String [] colNames){
    HashMap<String,Integer> colMap = new HashMap<String,Integer>();
    for(int i = 0; i < colNames.length; ++i)colMap.put(colNames[i],i);
    int [] res = new int [colNames.length];
    Arrays.fill(res, -1);
    Integer idx = null;
    for(int i = 0; i < _cols.length; ++i)
      if((idx = colMap.get(_cols[i]._name)) != null)
        res[idx] = i;
    return res;
  }

  @Override public ValueArray clone() {
    try { return (ValueArray)super.clone(); }
    catch( CloneNotSupportedException cne ) { throw H2O.unimpl(); }
  }

  // Init of transient fields from deserialization calls
  @Override public final ValueArray init( Key key ) {
    _key = key;
    return this;
  }

  /** Pretty print! */
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("VA[").append(_numrows).append("][").append(_cols.length).append("]{");
    sb.append("bpr=").append(_rowsize).append(", rpc=").append(_rpc).append(", ");
    sb.append("chunks=").append(chunks()).append(", key=").append(_key);
    sb.append("}");
    return sb.toString();
  }

  /** An array of column names */
  public final String[] colNames() {
    String[] names = new String[_cols.length];
    for( int i=0; i<names.length; i++ )
      names[i] = _cols[i]._name;
    return names;
  }


  /**Returns the width of a row.*/
  public int rowSize() { return _rowsize; }
  public long numRows() { return _numrows; }
  public int numCols() { return _cols.length; }
  public long length() { return _numrows*_rowsize; }
  public boolean hasInvalidRows(int colnum) { return _cols[colnum]._n != _numrows; }

  /** Rows in this chunk */
  public int rpc(long chunknum) {
    if( (long)(int)chunknum!=chunknum ) throw H2O.unimpl(); // more than 2^31 chunks?
    if( _rpc != null ) return (int)(_rpc[(int)chunknum+1]-_rpc[(int)chunknum]);
    int rpc = (int)(CHUNK_SZ/_rowsize);
    long chunks = Math.max(1,_numrows/rpc);
    assert chunknum < chunks;
    if( chunknum < chunks-1 )   // Not last chunk?
      return rpc;               // Rows per chunk
    return (int)(_numrows - (chunks-1)*rpc);
  }

  /** Row number at the start of this chunk */
  public long startRow( long chunknum) {
    if( (long)(int)chunknum!=chunknum ) throw H2O.unimpl(); // more than 2^31 chunks?
    if( _rpc != null ) return _rpc[(int)chunknum];
    int rpc = (int)(CHUNK_SZ/_rowsize); // Rows per chunk
    return rpc*chunknum;
  }

  // Which row in the chunk?
  public int rowInChunk( long chknum, long rownum ) {
    return (int)(rownum - startRow(chknum));
  }

  /** Number of chunks */
  public long chunks() {
    if( _rpc != null ) return _rpc.length-1; // one row# per chunk
    // Else uniform-size chunks
    int rpc = (int)(CHUNK_SZ/_rowsize); // Rows per chunk
    return Math.max(1,_numrows/rpc);
  }

  /** Chunk number containing a row */
  private long chknum( long rownum ) {
    if( _rpc == null ) {
      int rpc = (int)(CHUNK_SZ/_rowsize);
      return Math.min(rownum/rpc,Math.max(1,_numrows/rpc)-1);
    }
    int bs = Arrays.binarySearch(_rpc,rownum);
    if( bs < 0 ) bs = -bs-2;
    while( _rpc[bs+1]==rownum ) bs++;
    return bs;
  }

  // internal convience class for building structured ValueArrays
  static public class Column extends Iced implements Cloneable {
    public String _name;
    // Domain of the column - all the strings which represents the column's
    // domain.  The order of the strings corresponds to numbering utilized in
    // dataset.  Null for numeric columns.
    public String[] _domain;
    public double _min, _max, _mean; // Min/Max/mean/var per column; requires a 1st pass
    public double _sigma;            // Requires a 2nd pass
    // Number of valid values; different than the rows for the entire ValueArray in some rows
    // have bad data
    public long _n;
    public int  _base;  // Base
    public char _scale; // Actual value is (((double)(stored_value+base))/scale); 1,10,100,1000
    public char _off;   // Offset within a row
    public byte _size;  // Size is 1,2,4 or 8 bytes, or -4,-8 for float/double data

    public Column() { _min = Double.MAX_VALUE; _max = -Double.MAX_VALUE; _scale = 1; }
    // Plain unstructured byte array; min/max/mean/sigma are all bogus.
    // No 'NA' options, e.g. 255 is a valid datum.
    public Column(long len) {
      _min=0; _max=255; _mean=128; _n = len; _scale=1; _size=1;
    }
    public final boolean isFloat() { return _size < 0 || _scale != 1; }
    public final boolean isScaled() { return _scale != 1; }
    /** Compute size of numeric integer domain */
    public final long    numDomainSize() { return (long) ((_max - _min)+1); }
    @Override public Column clone() {
      try { return (Column)super.clone(); }
      catch( CloneNotSupportedException cne ) { throw H2O.unimpl(); }
    }

    private static boolean eq(double x, double y, double precision){
      return (Math.abs(x-y) < precision);
    }
    @Override
    public boolean equals(Object other){
      if(!(other instanceof Column)) return false;
      Column c = (Column)other;
      return
          _base  == c._base  &&
          _scale == c._scale &&
          _max   == c._max   &&
          _min   == c._min   &&
          (eq(_mean,c._mean,1e-5)  || Double.isNaN(_mean) && Double.isNaN(c._mean))  &&
          (eq(_sigma,c._sigma,1e-5)|| Double.isNaN(_sigma)&& Double.isNaN(c._sigma)) &&
          _n     == c._n     &&
          _size  == c._size  &&
          (_name == null && c._name  == null || _name  != null && c._name  != null && _name.equals(c._name));
    }
  }

  // Get a usable pile-o-bits
  public AutoBuffer getChunk( long chknum ) { return getChunk(getChunkKey(chknum)); }
  public AutoBuffer getChunk( Key key ) {
    byte[] b = DKV.get(key).getBytes();
    assert b.length == rpc(getChunkIndex(key))*_rowsize : "actual="+b.length+" expected="+rpc(getChunkIndex(key))*_rowsize;
    return new AutoBuffer(b);
  }

  // Value extracted, then scaled & based - the double version. Note that this
  // is not terrible efficient, and that 99% of this code I expect to be loop-
  // invariant when run inside real numeric loops... but that the compiler will
  // probably need help pulling out the loop invariants.
  public double datad(long rownum, int colnum) {
    long chknum = chknum(rownum);
    return datad(getChunk(chknum),rowInChunk(chknum,rownum),colnum);
  }

  // This is a version where the colnum data is not yet pulled out.
  public double datad(AutoBuffer ab, int row_in_chunk, int colnum) {
    return datad(ab,row_in_chunk,_cols[colnum]);
  }

  // This is a version where all the loop-invariants are hoisted already.
  public double datad(AutoBuffer ab, int row_in_chunk, Column col) {
    int off = (row_in_chunk * _rowsize) + col._off;
    double res=0;
    switch( col._size ) {
    case  1: res = ab.get1 (off); break;
    case  2: res = ab.get2 (off); break;
    case  4: res = ab.get4 (off); break;
    case  8:return ab.get8 (off); // No scale/offset for long data
    case -4:return ab.get4f(off); // No scale/offset for float data
    case -8:return ab.get8d(off); // No scale/offset for double data
    }
    // Apply scale & base for the smaller numbers
    return (res+col._base)/col._scale;
  }

  // Value extracted, then scaled & based - the integer version.
  public long data(long rownum, int colnum) {
    long chknum = chknum(rownum);
    return data(getChunk(chknum),rowInChunk(chknum,rownum),colnum);
  }
  public long data(AutoBuffer ab, int row_in_chunk, int colnum) {
    return data(ab,row_in_chunk,_cols[colnum]);
  }
  // This is a version where all the loop-invariants are hoisted already.
  public long data(AutoBuffer ab, int row_in_chunk, Column col) {
    int off = (row_in_chunk * _rowsize) + col._off;
    long res=0;
    switch( col._size ) {
    case  1: res = ab.get1 (off); break;
    case  2: res = ab.get2 (off); break;
    case  4: res = ab.get4 (off); break;
    case  8:return ab.get8 (off); // No scale/offset for long data
    case -4:return (long)ab.get4f(off); // No scale/offset for float data
    case -8:return (long)ab.get8d(off); // No scale/offset for double data
    }
    // Apply scale & base for the smaller numbers
    assert col._scale==1;
    return (res + col._base);
  }

  // Test if the value is valid, or was missing in the orginal dataset
  public boolean isNA(long rownum, int colnum) {
    long chknum = chknum(rownum);
    return isNA(getChunk(chknum),rowInChunk(chknum,rownum),colnum);
  }
  public boolean isNA(AutoBuffer ab, int row_in_chunk, int colnum ) {
    return isNA(ab,row_in_chunk,_cols[colnum]);
  }
  // Test if the value is valid, or was missing in the orginal dataset
  // This is a version where all the loop-invariants are hoisted already.
  public boolean isNA(AutoBuffer ab, int row_in_chunk, Column col ) {
    int off = (row_in_chunk * _rowsize) + col._off;
    switch( col._size ) {
    case  1: return ab.get1(off) == 255;
    case  2: return ab.get2(off) == 65535;
    case  4: return ab.get4(off) == Integer.MIN_VALUE;
    case  8: return ab.get8(off) == Long.MIN_VALUE;
    case -4: return  Float.isNaN(ab.get4f(off));
    case -8: return Double.isNaN(ab.get8d(off));
    }
    return true;
  }

  // Get the proper Key for a given chunk number
  public Key getChunkKey( long chknum ) {
    assert 0 <= chknum && chknum < chunks() : "AIOOB "+chknum+" < "+chunks();
    return getChunkKey(chknum,_key);
  }
  public static Key getChunkKey( long chknum, Key arrayKey ) {
    byte[] buf = new AutoBuffer().put1(Key.ARRAYLET_CHUNK).put1(0)
      .put8(chknum<<LOG_CHK).putA1(arrayKey._kb,arrayKey._kb.length).buf();
    return Key.make(buf,(byte)arrayKey.desired());
  }

  // Get the root array Key from a random arraylet sub-key
  public static Key getArrayKey( Key k ) { return Key.make(getArrayKeyBytes(k)); }
  public static byte[] getArrayKeyBytes( Key k ) {
    assert k._kb[0] == Key.ARRAYLET_CHUNK;
    return Arrays.copyOfRange(k._kb,2+8,k._kb.length);
  }

  // Get the chunk-index from a random arraylet sub-key
  public static long getChunkIndex(Key k) {
    assert k._kb[0] == Key.ARRAYLET_CHUNK;
    return UDP.get8(k._kb, 2) >> LOG_CHK;
  }
  public static long getChunkOffset(Key k) { return getChunkIndex(k)<<LOG_CHK; }

  // ---
  // Read a (possibly VERY large file) and put it in the K/V store and return a
  // Value for it. Files larger than 2Meg are broken into arraylets of 1Meg each.
  static public Key readPut(String keyname, InputStream is) throws IOException {
    return readPut(Key.make(keyname), is);
  }

  static public Key readPut(Key k, InputStream is) throws IOException {
    return readPut(k, is, null);
  }

  static public Key readPut(Key k, InputStream is, Job job) throws IOException {
    readPut(k,is,job,new Futures()).blockForPending();
    return k;
  }

  static private Futures readPut(Key key, InputStream is, Job job, Futures fs) throws IOException {
    UKV.remove(key);
    byte[] oldbuf, buf = null;
    int off = 0, sz = 0;
    long szl = off;
    long cidx = 0;
    while( true ) {
      oldbuf = buf;
      buf = MemoryManager.malloc1((int)CHUNK_SZ);
      off=0;
      while( off<CHUNK_SZ && (sz = is.read(buf,off,(int)(CHUNK_SZ-off))) != -1 )
        off+=sz;
      szl += off;
      if( off<CHUNK_SZ ) break;
      if( job != null && job.cancelled() ) break;
      Key ckey = getChunkKey(cidx++,key);
      DKV.put(ckey,new Value(ckey,buf),fs);
    }
    assert is.read(new byte[1]) == -1 || job.cancelled();

    // Last chunk is short, read it; combine buffers and make the last chunk larger
    if( cidx > 0 ) {
      Key ckey = getChunkKey(cidx-1,key); // Get last chunk written out
      assert DKV.get(ckey).memOrLoad()==oldbuf; // Maybe false-alarms under high-memory-pressure?
      byte[] newbuf = Arrays.copyOf(oldbuf,(int)(off+CHUNK_SZ));
      System.arraycopy(buf,0,newbuf,(int)CHUNK_SZ,off);
      DKV.put(ckey,new Value(ckey,newbuf),fs); // Overwrite the old too-small Value
    } else {
      Key ckey = getChunkKey(cidx,key);
      DKV.put(ckey,new Value(ckey,Arrays.copyOf(buf,off)),fs);
    }
    UKV.put(key,new ValueArray(key,szl),fs);
    return fs;
  }

  // Wrap a InputStream over this ValueArray
  public InputStream openStream() {
    return new InputStream() {
      private AutoBuffer _ab;
      private long _chkidx;
      @Override public int available() throws IOException {
        if( _ab==null || _ab.remaining()==0 ) {
          if( _chkidx >= chunks() ) return 0;
          _ab = getChunk(_chkidx++);
        }
        return _ab.remaining();
      }
      @Override public void close() { _chkidx = chunks(); _ab = null; }
      @Override public int read() throws IOException {
        return available() == 0 ? -1 : _ab.get1();
      }
      @Override public int read(byte[] b, int off, int len) throws IOException {
        return available() == 0 ? -1 : _ab.read(b,off,len);
      }
    };
  }
}
