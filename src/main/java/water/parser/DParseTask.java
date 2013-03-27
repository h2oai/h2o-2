package water.parser;

import java.util.*;

import water.*;
import water.ValueArray.Column;

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
public final class DParseTask extends MRTask {
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

  final int _startChunk;
  // scalar variables
  boolean _skipFirstLine;
  transient long _chunkId = -1;
  Pass _phase;
  int _myrows;
  int _ncolumns;
  byte _sep = (byte)',';
  byte _decSep = (byte)'.';
  int _rpc;
  int _rowsize;
  int _numRows; // number of rows -- works only in second pass FIXME in first pass object
  // 31 bytes

  ParseDataset _job;
  String _error;

  // arrays
  byte [] _colTypes;
  int [] _scale;
  int [] _bases;
  long [] _invalidValues;
  double [] _min;
  double [] _max;
  double [] _mean;
  double [] _sigma;
  int [] _nrows;
  Enum [] _enums;



  // transients - each map creates and uses it's own, no need to get these back
  // create and used only on the task caller's side
  transient String[][] _colDomains;

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
      assert _abs.eof();
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

    @Override public void onSuccess(){
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
    int rpc = (int) ValueArray.CHUNK_SZ / _rowsize;
    int rowInChunk = (int)firstRow % rpc;
    int lastChunk = Math.max(1,this._numRows / rpc) - 1; // index of the last chunk in the VA
    int chunkIndex = (int)firstRow/rpc; // index of the chunk I am writing to
    if (chunkIndex > lastChunk) { // we can be writing to the second chunk after its real boundary
      assert (chunkIndex == lastChunk + 1);
      rowInChunk += rpc;
      --chunkIndex;
    }
    do {
      // number of rows that go the the current chunk - all remaining rows for the
      // last chunk, or the number of rows that can go to the chunk
      int rowsToChunk = (chunkIndex == lastChunk) ? rowsToParse : Math.min(rowsToParse, rpc - rowInChunk);
      // add the output stream reacord
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
  transient final CustomParser.Type _parserType;
  transient final Value _sourceDataset;
  transient int _colIdx;

  public boolean isString(int idx) {
    // in case of idx which is oob, pretend it is a string col (it will be dropped anyways)
    return idx >= _ncolumns || _colTypes[idx]==STRINGCOL;
  }

  // As this is only used for distributed CSV parser we initialize the values
  // for the CSV parser itself.
  public DParseTask() {
    _startChunk = 0;
    _parserType = CustomParser.Type.CSV;
    _sourceDataset = null;
    _phase = Pass.ONE;
  }

  /** Private constructor for phase one.
   *
   * use createPhaseOne() static method instead.
   */
  private DParseTask(Value dataset, ParseDataset job, CustomParser.Type parserType) {
    _parserType = parserType;
    _sourceDataset = dataset;
    _job = job;
    _phase = Pass.ONE;
    _startChunk = 0;
  }

  protected DParseTask(DParseTask other, Value dataset, int startChunk){
    _phase = other._phase;
    _parserType = other._parserType;
    _sourceDataset = dataset;
    _startChunk = startChunk;
    _enums = other._enums;
    _colTypes = other._colTypes;
    _nrows = other._nrows;
    _skipFirstLine = other._skipFirstLine;
    _myrows = other._myrows; // for simple values, number of rows is kept in the member variable instead of _nrows
    _job = other._job;
    _numRows = other._numRows;
    _sep = other._sep;
    _decSep = other._decSep;
    _scale = other._scale;
    _bases = other._bases;
    _ncolumns = other._ncolumns;
    _min = other._min;
    _max = other._max;
    _mean = other._mean;
    _colNames = other._colNames;
    _error = other._error;
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
    _parserType = other._parserType;
    _sourceDataset = other._sourceDataset;
    _enums = other._enums;
    _colTypes = other._colTypes;
    _nrows = other._nrows;
    _skipFirstLine = other._skipFirstLine;
    _myrows = other._myrows; // for simple values, number of rows is kept in the member variable instead of _nrows
    _job = other._job;
    _numRows = other._numRows;
    _sep = other._sep;
    _decSep = other._decSep;
    _scale = other._scale;
    _ncolumns = other._ncolumns;
    _min = other._min;
    _max = other._max;
    _mean = other._mean;
    _sigma = other._sigma;
    _colNames = other._colNames;
    _error = other._error;
    _invalidValues = other._invalidValues;
    _startChunk = 0;
    _phase = Pass.TWO;
  }

  /** Creates a phase one dparse task.
   *
   * @param dataset Dataset to parse.
   * @param resultKey VA to store results to.
   * @param parserType Parser type to use.
   * @return Phase one DRemoteTask object.
   */
  public static DParseTask createPassOne(Value dataset, ParseDataset job, CustomParser.Type parserType) {
    return new DParseTask(dataset,job,parserType);
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
   *
   * @throws Exception
   */
  public void passOne(CsvParser.Setup setup) throws Exception {
    switch (_parserType) {
    case CSV:
        // precompute the parser setup, column setup and other settings
        if( setup == null ){
          byte [] bits = _sourceDataset.getFirstBytes(); // Can limit to eg 256*1024
          setup = CsvParser.guessCsvSetup(bits);
        }
        if (setup._data == null) {
          _error= "Unable to determine the separator or number of columns on the dataset";
          return;
        }
        _colNames = setup._data[0];
        setColumnNames(_colNames);
        _skipFirstLine = setup._header;
        // set the separator
        this._sep = setup._separator;
        // if parsing value array, initialize the nrows array
        if( _sourceDataset.isArray() ) {
          ValueArray ary = _sourceDataset.get();
          _nrows = new int[(int)ary.chunks()];
        } else
          _nrows = new int[1];
        // launch the distributed parser on its chunks.
        this.invoke(_sourceDataset._key);
        break;
      case XLS:
        // XLS parsing is not distributed, just obtain the value stream and
        // run the parser
        CustomParser p = new XlsParser(this);
        p.parse(_sourceDataset._key);
        --_myrows; // do not count the header
        break;
      case XLSX:
        // XLS parsing is not distributed, just obtain the value stream and
        // run the parser
        CustomParser px = new XlsxParser(this);
        px.parse(_sourceDataset._key);
        break;
      default:
        throw new Error("NOT IMPLEMENTED");
    }
  }

  /** Creates the second pass dparse task from a first phase one.
   */
  public static DParseTask createPassTwo(DParseTask phaseOneTask) {
    DParseTask t = new DParseTask(phaseOneTask);
    // calculate proper numbers of rows for the chunks
    if (t._nrows != null) {
      t._numRows = 0;
      for (int i = 0; i < t._nrows.length; ++i) {
        t._numRows += t._nrows[i];
        t._nrows[i] = t._numRows;
      }
    } else {
      t._numRows = t._myrows;
    }
    // normalize mean
    for(int i = 0; i < t._ncolumns; ++i)
      t._mean[i] = t._mean[i]/(t._numRows - t._invalidValues[i]);
    // create new data for phase two
    t._colDomains = new String[t._ncolumns][];
    t._bases = new int[t._ncolumns];
    // calculate the column domains
    for(int i = 0; i < t._colTypes.length; ++i){
      if(t._colTypes[i] == ECOL && t._enums[i] != null && !t._enums[i].isKilled())
        t._colDomains[i] = t._enums[i].computeColumnDomain();
      else
        t._enums[i] = null;
    }
    t.calculateColumnEncodings();
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
  public void passTwo() throws Exception {
    // make sure we delete previous array here, because we insert arraylet header after all chunks are stored in
    // so if we do not delete it now, it will be deleted by UKV automatically later and destroy our values!
    if(DKV.get(_job.dest()) != null)
      UKV.remove(_job.dest());
    switch (_parserType) {
      case CSV:
        // for CSV parser just launch the distributed parser on the chunks
        // again
        this.invoke(_sourceDataset._key);
        break;
      case XLS:
      case XLSX:
        // initialize statistics - invalid rows, sigma and row size
        phaseTwoInitialize();
        // create the output streams
        _outputStreams2 = createRecords(0, _myrows);
        assert (_outputStreams2.length > 0);
        _ab = _outputStreams2[0].initialize();
        // perform the second parse pass
        CustomParser p = (_parserType == CustomParser.Type.XLS) ? new XlsParser(this) : new XlsxParser(this);
        p.parse(_sourceDataset._key);
        // store the last stream if not stored during the parse
        if (_ab != null)
          _outputStreams2[_outputIdx].store();
        break;
      default:
        throw new Error("NOT IMPLEMENTED");
    }
  }

  public void normalizeSigma() {
    // normalize sigma
    for(int i = 0; i < _ncolumns; ++i)
      _sigma[i] = Math.sqrt(_sigma[i]/(_numRows - _invalidValues[i]));
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
    for( int i = 0; i < cols.length; ++i) {
      cols[i] = new Column();
      cols[i]._n = _numRows - _invalidValues[i];
      cols[i]._base = _bases[i];
      assert (char)pow10i(-_scale[i]) == pow10i(-_scale[i]):"scale out of bounds!, col = " + i + ", scale = " + _scale[i];
      cols[i]._scale = (char)pow10i(-_scale[i]);
      cols[i]._off = (char)off;
      cols[i]._size = (byte)COL_SIZES[_colTypes[i]];
      cols[i]._domain = _colDomains[i];
      cols[i]._max = _max[i];
      cols[i]._min = _min[i];
      cols[i]._mean = _mean[i];
      cols[i]._sigma = _sigma[i];
      cols[i]._name = _colNames[i];
      off += Math.abs(cols[i]._size);
    }
    // let any pending progress reports finish
    DKV.write_barrier();
    // finally make the value array header
    ValueArray ary = new ValueArray(_job.dest(), _numRows, off, cols);
    UKV.put(_job.dest(), ary);
  }

  private void createEnums() {
    if(_enums == null){
      _enums = new Enum[_ncolumns];
      for(int i = 0; i < _ncolumns; ++i)
        _enums[i] = new Enum();
    }
  }

  @Override public void init(){
    super.init();
    createEnums();
  }
  /** Sets the column names and creates the array of the enums for each
   * column.
   */
  public void setColumnNames(String[] colNames) {
    if (_phase == Pass.ONE) {
      assert (colNames != null);
      _colNames = colNames;
      _ncolumns = colNames.length;

      // Initialize the statistics for the XLS parsers. Statistics for CSV
      // parsers are created in the map method - they must be different for
      // each distributed invocation
      if ((_parserType == CustomParser.Type.XLS) || (_parserType == CustomParser.Type.XLSX)) {
        createEnums();
        phaseOneInitialize();
      }
    }
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
    if(_job.cancelled())
      return;
    Key aryKey = null;
    boolean arraylet = key._kb[0] == Key.ARRAYLET_CHUNK;
    boolean skipFirstLine = _skipFirstLine;
    if(arraylet) {
      aryKey = ValueArray.getArrayKey(key);
      _chunkId = ValueArray.getChunkIndex(key);
      skipFirstLine = skipFirstLine || (ValueArray.getChunkIndex(key) != 0);
    }
    switch (_phase) {
    case ONE:
      assert (_ncolumns != 0);
      // initialize the column statistics
      phaseOneInitialize();
      // perform the parse
      CsvParser p = new CsvParser(aryKey, _ncolumns, _sep, _decSep, this,skipFirstLine);
      p.parse(key);
      if(arraylet) {
        long idx = ValueArray.getChunkIndex(key);
        int idx2 = (int)idx;
        assert idx2 == idx;
        assert (_nrows[idx2] == 0) : idx+": "+Arrays.toString(_nrows)+" ("+_nrows[idx2]+" -- "+_myrows+")";
        _nrows[idx2] = _myrows;
      } else
        _nrows[0] = _myrows;
      break;
    case TWO:
      assert (_ncolumns != 0);
      assert (_phase == Pass.TWO);
      // initialize statistics - invalid rows, sigma and row size
      phaseTwoInitialize();
      // calculate the first row and the number of rows to parse
      int firstRow = 0;
      int lastRow = _myrows;
      _myrows = 0;
      long origChunkIdx = _startChunk;
      if( arraylet )
        origChunkIdx += ValueArray.getChunkIndex(key);
      firstRow = (origChunkIdx == 0) ? 0 : _nrows[(int)origChunkIdx-1];
      lastRow = _nrows[(int)origChunkIdx];
      if(lastRow <= firstRow){
        System.err.println("invalid rowsToParse at chunk " + origChunkIdx + ": " + Arrays.toString(_nrows));
      }
      int rowsToParse = lastRow - firstRow;
      // create the output streams
      _outputStreams2 = createRecords(firstRow, rowsToParse);
      assert (_outputStreams2.length > 0);
      _ab = _outputStreams2[0].initialize();
      // perform the second parse pass
      CsvParser p2 = new CsvParser(aryKey, _ncolumns, _sep, _decSep, this,skipFirstLine);
      p2.parse(key);
      // store the last stream if not stored during the parse
      if( _ab != null )
        _outputStreams2[_outputIdx].store();
      getFutures().blockForPending();
      break;
    default:
      assert (false);
    }
    ParseDataset.onProgress(key,_job._progress);
  }

  @Override
  public void reduce(DRemoteTask drt) {
    if(_job.cancelled())
      return;
    try {
      DParseTask other = (DParseTask)drt;
      if(_sigma == null)_sigma = other._sigma;
      if(_invalidValues == null){
        _enums = other._enums;
        _min = other._min;
        _max = other._max;
        _mean = other._mean;
        _sigma = other._sigma;
        _scale = other._scale;
        _colTypes = other._colTypes;
        _nrows = other._nrows;
        _invalidValues = other._invalidValues;
      } else {
        if (_phase == Pass.ONE) {
          if (_nrows != other._nrows)
            for (int i = 0; i < _nrows.length; ++i)
              _nrows[i] |= other._nrows[i];
          for(int i = 0; i < _ncolumns; ++i) {
            if(_enums[i] != other._enums[i])
              _enums[i].merge(other._enums[i]);
            if(other._min[i] < _min[i])_min[i] = other._min[i];
            if(other._max[i] > _max[i])_max[i] = other._max[i];
            if(other._scale[i] < _scale[i])_scale[i] = other._scale[i];
            if(other._colTypes[i] > _colTypes[i])_colTypes[i] = other._colTypes[i];
            _mean[i] += other._mean[i];
          }
        } else if(_phase == Pass.TWO) {
          for(int i = 0; i < _ncolumns; ++i)
              _sigma[i] += other._sigma[i];
        } else
          assert false:"unexpected _phase value:" + _phase;
        for(int i = 0; i < _ncolumns; ++i)
          _invalidValues[i] += other._invalidValues[i];
      }
      _myrows += other._myrows;
      if(_error == null)_error = other._error;
      else if(other._error != null) _error = _error + "\n" + other._error;
    } catch (Exception e) {
      e.printStackTrace();
    }
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

  static long [] powers10i = new long[]{
    1,
    10,
    100,
    1000,
    10000,
    100000,
    1000000,
    10000000,
    100000000,
    1000000000,
    10000000000l
  };

  static double pow10(int exp){
    return ((exp >= -10 && exp <= 10)?powers10[exp+10]:Math.pow(10, exp));
  }

  static long pow10i(int exp){
    assert 10 >= exp && exp >= 0:"unexpected exponent " + exp;
    return powers10i[exp];
  }

  static final boolean fitsIntoInt(double d){
    return Math.abs((int)d - d) < 1e-8;
  }

  @SuppressWarnings("fallthrough")
  private void calculateColumnEncodings() {
    assert (_bases != null);
    assert (_min != null);
    for(int i = 0; i < _ncolumns; ++i){
      // Entirely toss out numeric columns which are largely broken.
      if( (_colTypes[i]==ICOL || _colTypes[i]==DCOL || _colTypes[i]==FCOL ) &&
          (double)_invalidValues[i]/_numRows > 0.2 ) {
        _enums[i] = null;
        _max[i] = _min[i] = 0;
        _scale[i] = 0;
        _bases[i] = 0;
        _colTypes[i] = STRINGCOL;
        continue;
      }

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
        if (_max[i] - _min[i] < 255) {
          _colTypes[i] = BYTE;
          _bases[i] = (int)_min[i];
        } else if ((_max[i] - _min[i]) < 65535) {
          _colTypes[i] = SHORT;
          _bases[i] = (int)_min[i];
        } else if (_max[i] - _min[i] < (1L << 32) &&
                   _min[i] > Integer.MIN_VALUE && _min[i] < Integer.MAX_VALUE) {
          _colTypes[i] = INT;
          _bases[i] = (int)_min[i];
        } else
          _colTypes[i] = LONG;
        break;
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
        _min[i] = 0.0;
        _max[i] = System.currentTimeMillis();
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
  public void addCol(int colIdx, double value) {
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
        if( _colTypes[colIdx] == UCOL ) {
          long time = attemptTimeParse(str);
          if( time != Long.MIN_VALUE )
            _colTypes[colIdx] = TCOL;
        } else if( _colTypes[colIdx] == TCOL ) {
          return;
        }

        // Now attempt to make this an Enum col
        Enum e = _enums[colIdx];
        if( e == null || e.isKilled() ) return;
        if( _colTypes[colIdx] == UCOL )
          _colTypes[colIdx] = ECOL;
        e.addKey(str);
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
          _ab.put8(attemptTimeParse(str));
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

  // Deduce if we are looking at a Date/Time value, or not.
  // If so, return time as msec since Jan 1, 1970 or Long.MIN_VALUE.

  // I tried java.util.SimpleDateFormat, but it just throws too many
  // exceptions, including ParseException, NumberFormatException, and
  // ArrayIndexOutOfBoundsException... and the Piece de resistance: a
  // ClassCastException deep in the SimpleDateFormat code:
  // "sun.util.calendar.Gregorian$Date cannot be cast to sun.util.calendar.JulianCalendar$Date"
  // So I just brutally parse "yyyy-MM-dd HH:mm:ss.SSS"
  private static int digit( int x, int c ) {
    if( x < 0 || c < '0' || c > '9' ) return -1;
    return x*10+(c-'0');
  }
  private long attemptTimeParse( ValueString str ) {
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
}
