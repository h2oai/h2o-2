package water.parser;

import java.util.ArrayList;
import java.util.Arrays;

import water.*;
import water.Job.JobCancelledException;
import water.ValueArray.Column;
import water.parser.ParseDataset.FileInfo;
import water.util.Log;
import water.fvec.ParseTime;

/** Class responsible for actual parsing of the datasets.
 *
 * Works in two phases, first phase collects basic statistics and determines
 * the column encodings of the dataset.
 *
 * Second phase the goes over all data again, encodes them and writes them to
 * the result VA.
 *
 * The parser works distributed for CSV parsing, but is one node only for the
 * XLS and XLSX formats (they are not fully our code).
 */
public class DParseTask extends MRTask<DParseTask> implements CustomParser.DataOut {
  static enum Pass { ONE, TWO }

  // pass 1 types
  private static final byte UCOL = 0;  // unknown
  private static final byte ECOL = 11; // enum column
  private static final byte ICOL = 12; // integer column
  private static final byte FCOL = 13; // float column
  private static final byte DCOL = 14; // double column
  private static final byte TCOL = 15; // time column
  // pass 2 types
  private static final byte BYTE      = 1;
  private static final byte SHORT     = 2;
  private static final byte INT       = 3;
  private static final byte LONG      = 4;
  private static final byte DBYTE     = 5;
  private static final byte DSHORT    = 6;
  private static final byte FLOAT     = 7;
  private static final byte DOUBLE    = 8;
  private static final byte STRINGCOL = 9; // string column (too many enum values)

  private static final int [] COL_SIZES = new int[]{0,1,2,4,8,1,2,-4,-8,1};

  public void addColumns(int ncols){
    _colTypes = Arrays.copyOf(_colTypes, ncols);
    _min = Arrays.copyOf(_min, ncols);
    _max = Arrays.copyOf(_max, ncols);
    for(int i = _ncolumns; i < ncols; ++i){
      _min[i] = Double.POSITIVE_INFINITY;
      _max[i] = Double.NEGATIVE_INFINITY;
    }
    _scale = Arrays.copyOf(_scale, ncols);
    _mean = Arrays.copyOf(_mean, ncols);
    _invalidValues = Arrays.copyOf(_invalidValues, ncols);
    _ncolumns = ncols;
    createEnums();
  }
  // scalar variables
  Pass _phase;
  long _numRows;
  int _nchunks;
  transient int _myrows;
  int _ncolumns;
  int _rpc;
  int _rowsize;
  ParseDataset _job;
  String _error;
  CustomParser _parser;
  boolean _streamMode;
  protected boolean _map;

  // arrays
  byte [] _colTypes;
  int [] _scale;
  int [] _bases;
  long [] _invalidValues;
  double [] _min;
  double [] _max;
  double [] _mean;
  double [] _sigma;
  long [] _nrows;
  Enum [] _enums;

  // transient
  transient int _chunkId;
  // create and used only on the task caller's side
  transient String[][] _colDomains;

  @Override
  public long memOverheadPerChunk(){
   // double memory for phase2 should be safe upper limit here (and too far off from the actual either)
   return (_phase == Pass.TWO)?2*ValueArray.CHUNK_SZ:ValueArray.CHUNK_SZ;
  }

  public DParseTask clone2(){
    DParseTask t = newInstance();
    t._ncolumns = _ncolumns;
    t._parser = (_parser == null)?null:_parser.clone();
    t._sourceDataset = _sourceDataset;
    t._enums = _enums;
    t._colTypes = _colTypes;
    t._nrows = _nrows;
    t._job = _job;
    t._numRows = _numRows;
    t._scale = _scale;
    t._bases = _bases;
    t._ncolumns = _ncolumns;
    t._min = _min;
    t._max = _max;
    t._mean = _mean;
    t._sigma = _sigma;
    t._colNames = _colNames;
    t._error = _error;
    t._invalidValues = _invalidValues;
    t._phase = _phase;
    assert t._colTypes == null || t._ncolumns == t._colTypes.length;
    return t;
  }


  private static final class VAChunkDataIn implements CustomParser.DataIn {
    final Key _key;
    public VAChunkDataIn(Key k) {_key = k;}
    // Fetch chunk data on demand
    public byte[] getChunkData( int cidx ) {
      Key key = _key;
      if( key._kb[0] == Key.ARRAYLET_CHUNK ) { // Chunked data?
        Key aryKey = ValueArray.getArrayKey(key);
        ValueArray ary = DKV.get(aryKey).get();
        if( cidx >= ary.chunks() ) return null;
        key = ary.getChunkKey(cidx); // Return requested chunk
      } else {
        if( cidx > 0 ) return null; // Single chunk?  No next chunk
      }
      Value v = DKV.get(key);
      return v == null ? null : v.memOrLoad();
    }
    @Override public int  getChunkDataStart(int cidx) { return -1; }
    @Override public void setChunkDataStart(int cidx, int offset) { }
  }

  /** Manages the chunk parts of the result hex varray.
   *
   * Second pass parse encodes the data from the source file to the sequence
   * of these stream objects. Each stream object will always go to a single
   * chunk (but a single chunk can contain more stream objects). To manage
   * this situation, the list of stream records is created upfront and then
   * filled automatically.
   *
   * Stream record then knows its chunkIndex, that is which chunk it will be
   * written to, the offset into that chunk it will be written and the number
   * of input rows that will be parsed to it.
   */
  private final class OutputStreamRecord {
    final int _chunkIndex;
    final int _chunkOffset;
    final int _numRows;
    AutoBuffer _abs;

    /** Allocates the stream for the chunk. Streams should only be allocated
     * right before the record will be used and should be stored immediately
     * after that.
     */
    public AutoBuffer initialize() {
      assert  _abs == null;
      return (_abs = new AutoBuffer(_numRows * _rowsize));
    }

    /** Stores the stream to its chunk using the atomic union. After the data
     * from the stream is stored, its memory is freed up.
     */
    public void store() {
      assert _abs.eof():"expected eof, position=" + _abs.position() + ", size=" + _abs._size;
      Key k = ValueArray.getChunkKey(_chunkIndex, _job.dest());
      AtomicUnion u = new AtomicUnion(_abs.bufClose(),_chunkOffset);
      alsoBlockFor(u.fork(k));
      _abs = null; // free mem
    }

    // You are not expected to create record streams yourself, use the
    // createRecords method of the DParseTask.
    protected OutputStreamRecord(int chunkIndex, int chunkOffset, int numRows) {
      _chunkIndex = chunkIndex;
      _chunkOffset = chunkOffset;
      _numRows = numRows;
    }
  }

  public static class AtomicUnion extends Atomic {
    byte [] _bits;
    int _dst_off;
    public AtomicUnion(byte[] buf, int dstOff){
      _dst_off = dstOff;
      _bits = buf;
    }
    @Override public Value atomic( Value val1 ) {
      byte[] mem = _bits;
      int len = Math.max(_dst_off + mem.length,val1==null?0:val1._max);
      byte[] bits2 = MemoryManager.malloc1(len);
      if( val1 != null ) System.arraycopy(val1.memOrLoad(),0,bits2,0,val1._max);
      System.arraycopy(mem,0,bits2,_dst_off,mem.length);
      return new Value(_key,bits2);
    }

    @Override public void onSuccess(Value old){
      _bits = null;             // Do not return the bits
    }
  }

  /** Returns the list of streams that should be used to store the given rows.
   *
   * None of the returned streams is initialized.
   */
  protected OutputStreamRecord[] createRecords(long firstRow, int rowsToParse) {
    assert (_rowsize != 0);
    ArrayList<OutputStreamRecord> result = new ArrayList();
    long rpc = (int) (ValueArray.CHUNK_SZ / _rowsize);
    int rowInChunk = (int)(firstRow % rpc);
    long lastChunk = Math.max(1,this._numRows / rpc) - 1; // index of the last chunk in the VA
    int chunkIndex = (int)(firstRow/rpc); // index of the chunk I am writing to
    if (chunkIndex > lastChunk) { // we can be writing to the second chunk after its real boundary
      assert (chunkIndex == lastChunk + 1);
      rowInChunk += rpc;
      --chunkIndex;
    }
    do {
      // number of rows that go the the current chunk - all remaining rows for the
      // last chunk, or the number of rows that can go to the chunk
      int rowsToChunk = (int)((chunkIndex == lastChunk) ? rowsToParse : Math.min(rowsToParse, rpc - rowInChunk));
      // add the output stream record
      result.add(new OutputStreamRecord(chunkIndex, rowInChunk * _rowsize, rowsToChunk));
      // update the running variables
      if (chunkIndex < lastChunk) {
        rowInChunk = 0;
        ++chunkIndex;
      }
      rowsToParse -= rowsToChunk;
      assert (rowsToParse >= 0);
    } while (rowsToParse > 0);
    _outputIdx = 0;
    _colIdx = _ncolumns; // skip first line
    // return all output streams
    return result.toArray(new OutputStreamRecord[result.size()]);
  }

  transient OutputStreamRecord[] _outputStreams2;
  transient AutoBuffer _ab;
  transient int _outputIdx;
  transient String[] _colNames;
  transient Value _sourceDataset;
  transient int _colIdx;

  public boolean isString(int idx) {
    // in case of idx which is oob, pretend it is a string col (it will be dropped anyways)
    return idx >= _ncolumns || _colTypes[idx]==STRINGCOL;
  }

  // As this is only used for distributed CSV parser we initialize the values
  // for the CSV parser itself.
  public DParseTask() {
    _sourceDataset = null;
    _phase = Pass.ONE;
    _parser = null;
  }

  protected DParseTask makePhase2Clone(FileInfo finfo){
    DParseTask t = clone2();
    t._nrows = finfo._nrows;
    t._sourceDataset = DKV.get(finfo._ikey);
    t._parser._setup._header = finfo._header;
    return t;
  }

  /** Private constructor for phase two, copy constructor from phase one.
   *
   * Use createPhaseTwo() static method instead.
   *
   * @param other
   */
  private DParseTask(DParseTask other) {
    assert (other._phase == Pass.ONE);
    // copy the phase one data
    // don't pass invalid values, we do not need them 2nd pass
    _parser = other._parser;
    _sourceDataset = other._sourceDataset;
    _enums = other._enums;
    _colTypes = other._colTypes;
    _nrows = other._nrows;
    _job = other._job;
    _numRows = other._numRows;
    _scale = other._scale;
    _ncolumns = other._ncolumns;
    _min = other._min;
    _max = other._max;
    _mean = other._mean;
    _sigma = other._sigma;
    _colNames = other._colNames;
    _error = other._error;
    _invalidValues = other._invalidValues;
    _phase = Pass.TWO;
  }

  /** Creates a phase one dparse task.
   *
   * @param dataset Dataset to parse.
   * @return Phase one DRemoteTask object.
   */
  public DParseTask createPassOne(Value dataset, ParseDataset job, CustomParser parser) {
    DParseTask t = clone2();
    t._sourceDataset = dataset;
    t._job = job;
    t._phase = Pass.ONE;
    t._parser = parser;
    if(t._parser._setup != null) {
      t._ncolumns = parser._setup._ncols;
      t._colNames = parser._setup._columnNames;
    }
    return t;
  }

  /** Executes the phase one of the parser.
   *
   * First phase detects the encoding and basic statistics of the parsed
   * dataset.
   *
   * For CSV parsers it detects the parser setup and then launches the
   * distributed computation on per chunk basis.
   *
   * For XLS and XLSX parsers that do not work in distrubuted way parses the
   * whole datasets.
   */
  public void passOne() {
    if((_sourceDataset.isArray())) {
      ValueArray ary = _sourceDataset.get();
      _nchunks = (int)ary.chunks();

    } else
      _nchunks = 1;
    // launch the distributed parser on its chunks.
    if(_parser.parallelParseSupported()){
      dfork(_sourceDataset._key);
    } else {
      _nrows = new long[_nchunks+1];
      createEnums(); // otherwise called in init()
      phaseOneInitialize(); // otherwise called in map
      try{
        _streamMode = true;
        _parser.streamParse(DKV.get(_sourceDataset._key).openStream(), this);
        _numRows = _myrows;
        _map = true;
        // TODO,do  I have to call tryComplete here?
        tryComplete();
      } catch(Exception e){throw new RuntimeException(e);}
    }
  }
  /**
   * Creates the second pass dparse task from a first phase one.
   */
  public DParseTask createPassTwo() {
    assert _map:"creating pass two on an instance which did not have any map result from pass 1 stored in.";
    DParseTask t = clone2();
    t._phase = Pass.TWO;
    // calculate proper numbers of rows for the chunks
    // normalize mean
    for(int i = 0; i < t._ncolumns; ++i)
      t._mean[i] = t._mean[i]/(t._numRows - t._invalidValues[i]);
    // create new data for phase two
    t._colDomains = new String[t._ncolumns][];
    t._bases = new int[t._ncolumns];
    // calculate the column domains
    if(_enums != null)
      for(int i = 0; i < t._enums.length; ++i){
        if(t._colTypes[i] == UCOL && t._enums[i] != null && t._enums[i].size() == 1)
          t._colTypes[i] = ECOL;
        if(t._colTypes[i] == ECOL && t._enums[i] != null && !t._enums[i].isKilled())
          t._colDomains[i] = ValueString.toString(t._enums[i].computeColumnDomain());
        else
          t._enums[i] = null;
      }
    t.calculateColumnEncodings();
    assert t._bases != null;
    return t;
  }




  /** Executes the phase two of the parser task.
   *
   * In phase two the data is encoded to the final VA, which is then created
   * properly at the end.
   *
   * For CSV launches the distributed computation.
   *
   * For XLS and XLSX parsers computes all the chunks itself as there is no
   * option for their distributed processing.
   */
  public void passTwo()  {
    _phase = Pass.TWO;
    _myrows = 0;
    // make sure we delete previous array here, because we insert arraylet header after all chunks are stored in
    // so if we do not delete it now, it will be deleted by UKV automatically later and destroy our values!
    if(_parser.parallelParseSupported())
      this.invoke(_sourceDataset._key);
    else {
      // initialize statistics - invalid rows, sigma and row size
      phaseTwoInitialize();
      // create the output streams
      _outputStreams2 = createRecords(0, (int)_numRows);
      assert (_outputStreams2.length > 0);
      _ab = _outputStreams2[0].initialize();
      // perform the second parse pass
      try{
        _streamMode = true;
        _parser.streamParse(DKV.get(_sourceDataset._key).openStream(),this);
      } catch(Exception e){
        e.printStackTrace();
        throw new RuntimeException(e);
      }
      // store the last stream if not stored during the parse
      if (_ab != null)
        _outputStreams2[_outputIdx].store();
    }
    normalizeSigma();
  }

  public void normalizeSigma() {
    // normalize sigma
    for(int i = 0; i < _ncolumns; ++i)
      _sigma[i] = Math.sqrt(_sigma[i]/(_numRows - _invalidValues[i] - 1));
  }

  /** Creates the value header based on the calculated columns.
   *
   * Also stores the header to its appropriate key. This will be the VA header
   * of the parsed dataset.
   */
  protected void createValueArrayHeader() {
    assert (_phase == Pass.TWO);
    Column[] cols = new Column[_ncolumns];
    int off = 0;
    Log.info("Parse result for " + _job.dest() + " (" + Long.toString(_numRows) + " rows):");
    for( int i = 0; i < cols.length; ++i) {
      cols[i] = new Column();
      cols[i]._n = _numRows - _invalidValues[i];
      cols[i]._base = _bases[i];
      assert (char)pow10i(-_scale[i]) == pow10i(-_scale[i]):"scale out of bounds!, col = " + i + ", scale = " + _scale[i];
      cols[i]._scale = (char)pow10i(-_scale[i]);
      cols[i]._off = off;
      cols[i]._size = (byte)COL_SIZES[_colTypes[i]];
      cols[i]._domain = _colDomains[i];
      cols[i]._max = _max[i];
      cols[i]._min = _min[i];
      cols[i]._mean = _mean[i];
      cols[i]._sigma = _sigma[i];
      cols[i]._name = _colNames != null?_colNames[i]:("C"+(Integer.toString(i+1)));

      try {
        boolean isCategorical = _colDomains[i] != null;
        boolean isConstant = _min[i] == _max[i];
        String CStr = String.format("C%d:", i+1);
        String typeStr = String.format("%s", (isCategorical ? "categorical" : "numeric"));
        String minStr = String.format("min(%f)", _min[i]);
        String maxStr = String.format("max(%f)", _max[i]);
        long numNAs = _invalidValues[i];
        String naStr = (numNAs > 0) ? String.format("na(%d)", numNAs) : "";
        String isConstantStr = isConstant ? "constant" : "";
        String numLevelsStr = isCategorical ? String.format("numLevels(%d)", _colDomains[i].length) : "";
        Log.info(String.format("    %-8s %15s %20s %20s %15s %11s %16s", CStr, typeStr, minStr, maxStr, naStr, isConstantStr, numLevelsStr));
      }
      catch (Exception _) {}

      off += Math.abs(cols[i]._size);
    }
    // let any pending progress reports finish
    DKV.write_barrier();
    // finally make the value array header
    new ValueArray(_job.dest(), _numRows, off, cols).unlock(_job.self());
  }

  protected void createEnums() {
    _enums = new Enum[_ncolumns];
    for(int i = 0; i < _ncolumns; ++i)
      _enums[i] = new Enum();
  }

  @Override public void init(){
    super.init();
    if(_phase == Pass.ONE){
      _nrows = new long[_nchunks+1];
      createEnums();
    }
  }
  /** Sets the column names and creates the array of the enums for each
   * column.
   */
  public void setColumnNames(String[] colNames) {
    if (_phase == Pass.ONE) {
      assert (colNames != null);
      addColumns(colNames.length);
    }
    _colNames = colNames;
  }

  /** Initialize phase one data structures with the appropriate number of
   * columns.
   */
  public void phaseOneInitialize() {
    if (_phase != Pass.ONE)
      assert (_phase == Pass.ONE);
    _invalidValues = new long[_ncolumns];
    _min = new double [_ncolumns];
    Arrays.fill(_min, Double.POSITIVE_INFINITY);
    _max = new double[_ncolumns];
    Arrays.fill(_max, Double.NEGATIVE_INFINITY);
    _mean = new double[_ncolumns];
    _scale = new int[_ncolumns];
    _colTypes = new byte[_ncolumns];
  }

  /** Initializes the phase two data. */
  public void phaseTwoInitialize() {
    assert (_phase == Pass.TWO);
    _invalidValues = new long[_ncolumns];
    _sigma = new double[_ncolumns];
    _rowsize = 0;
    for(byte b:_colTypes) _rowsize += Math.abs(COL_SIZES[b]);
  }

  /** Map function for distributed parsing of the CSV files.
   *
   * In first phase it calculates the min, max, means, encodings and other
   * statistics about the dataset, determines the number of columns.
   *
   * The second pass then encodes the parsed dataset to the result key,
   * splitting it into equal sized chunks.
   */
  @Override public void map(Key key) {
    if(!Job.isRunning(_job.self()))
      throw new JobCancelledException();
    _map = true;
    Key aryKey = null;
    boolean arraylet = key._kb[0] == Key.ARRAYLET_CHUNK;
    if(arraylet) {
      aryKey = ValueArray.getArrayKey(key);
      _chunkId = (int)ValueArray.getChunkIndex(key);
    }
    switch (_phase) {
    case ONE:
      // initialize the column statistics
      phaseOneInitialize();
      // perform the parse
      _parser.clone().parallelParse(_chunkId, new VAChunkDataIn(key), this);
      if(arraylet) {
        long idx = _chunkId+1;
        int idx2 = (int)idx;
        assert idx2 == idx;
        if(idx2 >= _nrows.length){
          System.err.println("incorrect index/array size for key = " + key + ": " + _nrows.length + " <= " + idx2 + ", aryKey = " + aryKey + ", chunks# = " + DKV.get(aryKey).get(ValueArray.class).chunks());
          assert false;
        }
        assert (_nrows[idx2] == 0) : idx+": "+Arrays.toString(_nrows)+" ("+_nrows[idx2]+" -- "+_numRows+")";
        _nrows[idx2] = _myrows;
      } else
        _nrows[1] = _myrows;
      _numRows = _myrows;
      break;
    case TWO:
      assert (_ncolumns != 0);
      assert (_phase == Pass.TWO);
      // initialize statistics - invalid rows, sigma and row size
      phaseTwoInitialize();
      // calculate the first row and the number of rows to parse
      long firstRow = _nrows[_chunkId];
      long lastRow = _nrows[_chunkId+1];
      int rowsToParse = (int)(lastRow - firstRow);
      // create the output streams
      _outputStreams2 = createRecords(firstRow, rowsToParse);
      assert (_outputStreams2.length > 0);
      _ab = _outputStreams2[0].initialize();
      // perform the second parse pass
      _parser.clone().parallelParse(_chunkId, new VAChunkDataIn(key), this);
      // store the last stream if not stored during the parse
      if( _ab != null )
        _outputStreams2[_outputIdx].store();
      getFutures().blockForPending();
      break;
    default:
      assert (false);
    }
    assert _ncolumns == _colTypes.length;
    ParseDataset.onProgress(key,_job._progress);
  }

  @Override
  public void reduce(DParseTask dpt) {
    if(!Job.isRunning(_job.self()))
      return;
    assert dpt._map;
    if(_sigma == null)_sigma = dpt._sigma;
    if(!_map){
      _map = dpt._map;
      _enums = dpt._enums;
      _min = dpt._min;
      _max = dpt._max;
      _mean = dpt._mean;
      _sigma = dpt._sigma;
      _scale = dpt._scale;
      _colTypes = dpt._colTypes;
      _ncolumns = dpt._ncolumns;
      _nrows = dpt._nrows;
      _invalidValues = dpt._invalidValues;
    } else {
      assert _ncolumns >= dpt._ncolumns;
      if (_phase == Pass.ONE) {
        if (_nrows != dpt._nrows)
          for (int i = 0; i < dpt._nrows.length; ++i)
            _nrows[i] |= dpt._nrows[i];
        if(_enums != null) for(int i = 0; i < dpt._ncolumns; ++i)
          if(_enums[i] != dpt._enums[i])
            _enums[i].merge(dpt._enums[i]);
        for(int i = 0; i < dpt._ncolumns; ++i) {
          if(dpt._min[i] < _min[i])_min[i] = dpt._min[i];
          if(dpt._max[i] > _max[i])_max[i] = dpt._max[i];
          if(dpt._scale[i] < _scale[i])_scale[i] = dpt._scale[i];
          if(dpt._colTypes[i] > _colTypes[i])_colTypes[i] = dpt._colTypes[i];
          _mean[i] += dpt._mean[i];
        }
      } else if(_phase == Pass.TWO) {
        for(int i = 0; i < dpt._ncolumns; ++i)
          _sigma[i] += dpt._sigma[i];
      } else
        assert false:"unexpected _phase value:" + _phase;
      for(int i = 0; i < dpt._ncolumns; ++i)
        _invalidValues[i] += dpt._invalidValues[i];
    }
    _numRows += dpt._numRows;
    if(_error == null)_error = dpt._error;
    else if(dpt._error != null) _error = _error + "\n" + dpt._error;
    assert _colTypes.length == _ncolumns;
  }

  static double [] powers10 = new double[]{
    0.0000000001,
    0.000000001,
    0.00000001,
    0.0000001,
    0.000001,
    0.00001,
    0.0001,
    0.001,
    0.01,
    0.1,
    1.0,
    10.0,
    100.0,
    1000.0,
    10000.0,
    100000.0,
    1000000.0,
    10000000.0,
    100000000.0,
    1000000000.0,
    10000000000.0,
  };

  static public long [] powers10i = new long[]{
    1l,
    10l,
    100l,
    1000l,
    10000l,
    100000l,
    1000000l,
    10000000l,
    100000000l,
    1000000000l,
    10000000000l,
    100000000000l,
    1000000000000l,
    10000000000000l,
    100000000000000l,
    1000000000000000l,
    10000000000000000l,
    100000000000000000l,
    1000000000000000000l,
  };

  public static double pow10(int exp){
    return ((exp >= -10 && exp <= 10)?powers10[exp+10]:Math.pow(10, exp));
  }

  public static long pow10i(int exp){
    return powers10i[exp];
  }

  public static final boolean fitsIntoInt(double d){
    return Math.abs((int)d - d) < 1e-8;
  }

  @SuppressWarnings("fallthrough")
  private void calculateColumnEncodings() {
    assert (_bases != null);
    assert _scale != null;
    assert (_min != null);
    assert _ncolumns == _colTypes.length:"ncols=" + _ncolumns + ", colTypes.length=" + _colTypes.length;
    for(int i = 0; i < _ncolumns; ++i){
      // Entirely toss out numeric columns which are largely broken.
      if( (_colTypes[i]==ICOL || _colTypes[i]==DCOL || _colTypes[i]==FCOL ) &&
          (double)_invalidValues[i]/_numRows > 1 ) {
        _enums[i] = null;
        _max[i] = _min[i] = 0;
        _scale[i] = 0;
        _bases[i] = 0;
        _colTypes[i] = STRINGCOL;
        continue;
      }
      if(_colTypes[i] == UCOL && _enums[i] != null && _enums[i].size() == 1)
        _colTypes[i] = ECOL;
      switch(_colTypes[i]){
      case UCOL: // only missing values
        _colTypes[i] = BYTE;
        break;
      case ECOL: // enum
        if(_enums[i] == null || _enums[i].isKilled()){
          _max[i] = 0;
          _min[i] = 0;
          _colTypes[i] = STRINGCOL;
        } else {
          _max[i] = _colDomains[i].length-1;
          _min[i] = 0;
          if(_max[i] < 256)_colTypes[i] = BYTE;
          else if(_max[i] < 65536)_colTypes[i] = SHORT;
          else _colTypes[i] = INT;
        }
        break;
      case ICOL: // number
        if(_min[i] >= Long.MIN_VALUE && _max[i] <= Long.MAX_VALUE){
          if(_min[i] < Integer.MIN_VALUE || _max[i] > Integer.MAX_VALUE){
            _colTypes[i] = LONG;
          } else if (_max[i] - _min[i] < 255) {
            _colTypes[i] = BYTE;
            _bases[i] = (int)_min[i];
          } else if ((_max[i] - _min[i]) < 65535) {
            _colTypes[i] = SHORT;
            _bases[i] = (int)_min[i];
          } else if (_max[i] - _min[i] < (1L << 32) &&
                     _min[i] > Integer.MIN_VALUE && _max[i] < Integer.MAX_VALUE) {
            _colTypes[i] = INT;
            _bases[i] = (int)_min[i];
          } else
            _colTypes[i] = LONG;
          break;
        } else
          _colTypes[i] = DCOL;
        //fallthrough
      case FCOL:
      case DCOL:
        if(_scale[i] >= -4 && (_max[i] <= powers10i[powers10i.length-1]) && (_min[i] >= -powers10i[powers10i.length-1])){
          double s = pow10(-_scale[i]);
          double range = s*(_max[i]-_min[i]);
          double base = s*_min[i];
          if(range < 256){
            if(fitsIntoInt(base)) { // check if base fits into int!
              _colTypes[i] = DBYTE;
              _bases[i] = (int)base;
              break;
            }
          } else if(range < 65535){
            if(fitsIntoInt(base)){
              _colTypes[i] = DSHORT;
              _bases[i] = (int)(base);
              break;
            }
          }
        }
        _scale[i] = 0;
        _bases[i] = 0;
        _colTypes[i] = (_colTypes[i] == FCOL)?FLOAT:DOUBLE;
        break;
      case TCOL:                // Time; millis since jan 1, 1970
        _scale[i] = -1;
        _bases[i] = 0;
        _colTypes[i] = LONG;
        break;

      default: throw H2O.unimpl();
      }
    }
    _invalidValues = null;
  }

  /** Advances to new line. In phase two it also must make sure that the
   *
   */
  public void newLine() {
    ++_myrows;
    if (_phase == Pass.TWO) {
      while (_colIdx < _ncolumns)
        addInvalidCol(_colIdx);
      _colIdx = 0;
      // if we are at the end of current stream, move to the next one
      if (_ab.eof()) {
        assert _outputStreams2[_outputIdx]._abs == _ab;
        _outputStreams2[_outputIdx++].store();
        if (_outputIdx < _outputStreams2.length) {
          _ab = _outputStreams2[_outputIdx].initialize();
        } else {
          _ab = null; // just to be sure we throw a NPE if there is a problem
        }
      }
    }
  }

  /** Rolls back parsed line. Useful for CsvParser when it parses new line
   * that should not be added. It can easily revert it by this.
   */
  public void rollbackLine() {
    --_myrows;
    assert (_phase == Pass.ONE || _ab == null) : "p="+_phase+" ab="+_ab+" oidx="+_outputIdx+" chk#"+_chunkId+" myrow"+_myrows+" ";//+_sourceDataset._key;
  }

  /** Adds double value to the column.
   *
   * @param colIdx
   * @param value
   */
  public void addNumCol(int colIdx, double value) {
    if (Double.isNaN(value)) {
      addInvalidCol(colIdx);
    } else {
      double d= value;
      int exp = 0;
      long number = (long)d;
      while (number != d) {
        d = d * 10;
        --exp;
        number = (long)d;
      }
      addNumCol(colIdx, number, exp);
    }
  }

  /** Adds invalid value to the column.  */
  public void addInvalidCol(int colIdx){
    ++_colIdx;
    if(colIdx >= _ncolumns)
      return;
    ++_invalidValues[colIdx];
    if(_phase == Pass.ONE)
      return;
    switch (_colTypes[colIdx]) {
      case BYTE:
      case DBYTE:
        _ab.put1(-1);
        break;
      case SHORT:
      case DSHORT:
        _ab.put2((short)-1);
        break;
      case INT:
        _ab.put4(Integer.MIN_VALUE);
        break;
      case LONG:
        _ab.put8(Long.MIN_VALUE);
        break;
      case FLOAT:
        _ab.put4f(Float.NaN);
        break;
      case DOUBLE:
        _ab.put8d(Double.NaN);
        break;
      case STRINGCOL:
        // TODO, replace with empty space!
        _ab.put1(-1);
        break;
      default:
        assert false:"illegal case: " + _colTypes[colIdx];
    }
  }

  /** Adds string (enum) value to the column. */
  public void addStrCol( int colIdx, ValueString str ) {
    if( colIdx >= _ncolumns )
      return;
    switch (_phase) {
      case ONE:
        ++_colIdx;
        // If this is a yet unspecified but non-numeric column, attempt a time-parse
        if( _colTypes[colIdx] == UCOL &&
            ParseTime.attemptTimeParse(str) != Long.MIN_VALUE )
          _colTypes[colIdx] = TCOL; // Passed a time-parse, so assume a time-column
        if( _colTypes[colIdx] == TCOL ) {
          long time = ParseTime.attemptTimeParse(str);
          if( time != Long.MIN_VALUE ) {
            time = ParseTime.decodeTime(time); // Get time
            if(time < _min[colIdx])_min[colIdx] = time;
            if(time > _max[colIdx])_max[colIdx] = time;
            _mean[colIdx] += time;
          } else {
            ++_invalidValues[colIdx];
          }
          return;
        }
        // Now attempt to make this an Enum col
        Enum e = _enums[colIdx];
        if( e == null || e.isKilled() ) return;
        e.addKey(str);
        if( _colTypes[colIdx] == UCOL && e.size() > 1)
          _colTypes[colIdx] = ECOL;
        ++_invalidValues[colIdx]; // invalid count in phase0 is in fact number of non-numbers (it is used for mean computation, is recomputed in 2nd pass)
        break;
      case TWO:
        if(_enums[colIdx] != null) {
          ++_colIdx;
          int id = _enums[colIdx].getTokenId(str);
          // we do not expect any misses here
          assert 0 <= id && id < _enums[colIdx].size();
          switch (_colTypes[colIdx]) {
          case BYTE:  _ab.put1(      id); break;
          case SHORT: _ab.put2((char)id); break;
          case INT:   _ab.put4(      id); break;
          default:    assert false:"illegal case: " + _colTypes[colIdx];
          }
        } else if( _colTypes[colIdx] == LONG ) {
          ++_colIdx;
          // Times are strings with a numeric column type of LONG
          long time = ParseTime.attemptTimeParse(str);
          time = ParseTime.decodeTime(time); // Get time
          _ab.put8(time);
          // Update sigma
          if( !Double.isNaN(_mean[colIdx])) {
            double d = time - _mean[colIdx];
            _sigma[colIdx] += d*d;
          }
        } else {
          addInvalidCol(colIdx);
        }
        break;
      default:
        assert (false);
    }
  }

  /** Adds number value to the column parsed with mantissa and exponent.  */
  static final int MAX_FLOAT_MANTISSA = 0x7FFFFF;
  @SuppressWarnings("fallthrough")
  public void addNumCol(int colIdx, long number, int exp) {
    ++_colIdx;
    if(colIdx >= _ncolumns)
      return;
    switch (_phase) {
      case ONE:
        double d = number*pow10(exp);
        if(d < _min[colIdx])_min[colIdx] = d;
        if(d > _max[colIdx])_max[colIdx] = d;
        _mean[colIdx] += d;
        if(exp != 0) {
          if(exp < _scale[colIdx])_scale[colIdx] = exp;
          if(_colTypes[colIdx] != DCOL){
            if(Math.abs(number) > MAX_FLOAT_MANTISSA || exp < -35 || exp > 35)
              _colTypes[colIdx] = DCOL;
            else
              _colTypes[colIdx] = FCOL;
          }
        } else if(_colTypes[colIdx] < ICOL) {
          _colTypes[colIdx] = ICOL;
        }
        break;
      case TWO:
        assert _ab != null;
        switch (_colTypes[colIdx]) {
          case BYTE:
            _ab.put1((byte)(number*pow10i(exp - _scale[colIdx]) - _bases[colIdx]));
            break;
          case SHORT:
            _ab.put2((short)(number*pow10i(exp - _scale[colIdx]) - _bases[colIdx]));
            break;
          case INT:
            _ab.put4((int)(number*pow10i(exp - _scale[colIdx]) - _bases[colIdx]));
            break;
          case LONG:
            _ab.put8(number*pow10i(exp - _scale[colIdx]));
            break;
          case FLOAT:
            _ab.put4f((float)(number * pow10(exp)));
            break;
          case DOUBLE:
            _ab.put8d(number * pow10(exp));
            break;
          case DBYTE:
            _ab.put1((short)(number*pow10i(exp - _scale[colIdx]) - _bases[colIdx]));
            break;
          case DSHORT:
            // scale is computed as negative in the first pass,
            // therefore to compute the positive exponent after scale, we add scale and the original exponent
            _ab.put2((short)(number*pow10i(exp - _scale[colIdx]) - _bases[colIdx]));
            break;
          case STRINGCOL:
            break;
        }
        // update sigma
        if(!Double.isNaN(_mean[colIdx])) {
          d = number*pow10(exp) - _mean[colIdx];
          _sigma[colIdx] += d*d;
        }
        break;
      default:
        assert (false);
    }
  }
  @Override public void invalidLine(String err) {newLine();} //TODO
  @Override public void invalidValue(int line, int col) {} //TODO
}
