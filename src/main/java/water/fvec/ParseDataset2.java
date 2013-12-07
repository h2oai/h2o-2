package water.fvec;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.zip.*;

import jsr166y.CountedCompleter;
import water.*;
import water.H2O.H2OCountedCompleter;
import water.fvec.Vec.VectorGroup;
import water.nbhm.NonBlockingHashMap;
import water.parser.*;
import water.parser.CustomParser.ParserSetup;
import water.parser.CustomParser.ParserType;
import water.parser.CustomParser.StreamDataOut;
import water.parser.ParseDataset.Compression;
import water.parser.Enum;
import water.util.*;
import water.util.Utils.IcedHashMap;
import water.util.Utils.IcedInt;

public final class ParseDataset2 extends Job {
  // Local enum results for all parses
  public static NonBlockingHashMap<Key, Enum[]> ENUMS = new NonBlockingHashMap<Key, Enum[]>();
  public final Key  _progress;  // Job progress Key

  // --------------------------------------------------------------------------
  // Parse an array of csv input/file keys into an array of distributed output Vecs
  public static Frame parse(Key okey, Key [] keys) {return parse(okey,keys,new ParserSetup(),true);}

  public static Frame parse(Key okey, Key [] keys, CustomParser.ParserSetup globalSetup, boolean delete_on_done) {
    Key k = keys[0];
    ByteVec v = (ByteVec)getVec(k);
    byte [] bits = v.elem2BV(0).getBytes();
    Compression cpr = Utils.guessCompressionMethod(bits);
    globalSetup = ParseDataset.guessSetup(Utils.unzipBytes(bits,cpr), globalSetup,true)._setup;
    if( globalSetup._ncols == 0 ) throw new java.lang.IllegalArgumentException(globalSetup.toString());
    return forkParseDataset(okey, keys, globalSetup, delete_on_done).get();
  }
  // Same parse, as a backgroundable Job
  public static ParseDataset2 forkParseDataset(final Key dest, final Key[] keys, final CustomParser.ParserSetup setup, boolean delete_on_done) {
    ParseDataset2 job = new ParseDataset2(dest, keys);
    ParserFJTask fjt = new ParserFJTask(job, keys, setup, delete_on_done);
    job.start(fjt);
    H2O.submitTask(fjt);
    return job;
  }
  // Setup a private background parse job
  private ParseDataset2(Key dest, Key[] fkeys) {
    destination_key = dest;
    // Job progress Key
    _progress = Key.make((byte) 0, Key.JOB);
    UKV.put(_progress, ParseProgress.make(fkeys));
  }

  // Simple internal class doing background parsing, with trackable Job status
  public static class ParserFJTask extends H2OCountedCompleter {
    final ParseDataset2 _job;
    Key [] _keys;
    CustomParser.ParserSetup _setup;
    boolean _delete_on_done;

    public ParserFJTask( ParseDataset2 job, Key [] keys, CustomParser.ParserSetup setup, boolean delete_on_done) {
      _job = job;
      _keys = keys;
      _setup = setup;
      _delete_on_done = delete_on_done;
    }
    @Override public void compute2() {
      parse_impl(_job, _keys, _setup, _delete_on_done);
      tryComplete();
    }

    @Override public boolean onExceptionalCompletion(Throwable ex, CountedCompleter caller){
      if(_job != null){
        _job.cancel(ex.toString());
      }
      ex.printStackTrace();
      return true;
    }
  }

  // --------------------------------------------------------------------------
  // Parser progress
  static class ParseProgress extends Iced {
    final long _total;
    long _value;
    DException _ex;
    ParseProgress(long val, long total){_value = val; _total = total;}
    // Total number of steps is equal to total bytecount across files
    static ParseProgress make( Key[] fkeys ) {
      long total = 0;
      for( Key fkey : fkeys )
        total += getVec(fkey).length();
      return new ParseProgress(0,total);
    }
    public void setException(DException ex){_ex = ex;}
    public DException getException(){return _ex;}
  }
  static final void onProgress(final long len, final Key progress) {
    new TAtomic<ParseProgress>() {
      @Override public ParseProgress atomic(ParseProgress old) {
        if (old == null) return null;
        old._value += len;
        return old;
      }
    }.fork(progress);
  }

  @Override public float progress() {
    ParseProgress progress = UKV.get(_progress);
    if( progress == null || progress._total == 0 ) return 0;
    return progress._value / (float) progress._total;
  }
  @Override public void remove() {
    DKV.remove(_progress);
    super.remove();
  }

  public enum ColType {I, F, E}

  /**
   * Task to update enum values to match the global numbering scheme.  Performs
   * update in place so that values originally numbered using node-local
   * unordered numbering will be numbered using global numbering.
   *
   * @author tomasnykodym
   *
   */
  public static class EnumUpdateTask extends MRTask2<EnumUpdateTask>{
    final Key _key;             // Key to the local enum maps
    final ValueString _vs[][];  // Global mapping
    private transient int[][] _emaps;
    public EnumUpdateTask( Key key, ValueString vs[][]) { _key = key; _vs=vs; }

    // One time per-node
    @Override public void setupLocal() {
      Enum es[] = ENUMS.get(_key);
      _emaps = new int[_vs.length][];
      for( int i = 0; i < _vs.length; ++i ) {
        ValueString ss[] = _vs[i];
        if( ss == null ) continue; // Not an enum
        final Enum e = es[i];
        int [] emap = _emaps[i] = new int[e.lastId()+1];
        Arrays.fill(emap, -1);  // Only used to assert full assignment
        for(int j = 0; j < ss.length; ++j)
          if( e.containsKey(ss[j]) )
            emap[e.getTokenId(ss[j])] = j;
      }
    }

    @Override public void map(Chunk [] chks){
      Enum es[] = ENUMS.get(_key);
      for(int i = 0; i < chks.length; ++i) {
        Chunk chk = chks[i];
        if(_vs[i] == null) { // killed, replace with all NAs
          if( es[i].size() > 0 ) { // Was enum, then killed
            DKV.put(chk._vec.chunkKey(chk.cidx()),new C0DChunk(Double.NaN,chk._len));
            System.out.println("flip col "+i+" to String");
          } // Else was just a numeric (non-string non-enum) column
        } else
          for( int j = 0; j < chk._len; ++j) {
            if( chk.isNA0(j) ) continue;
            int x = _emaps[i][(int)chk.at80(j)];
            assert x != -1;     // The missing enum tag
            chk.set0(j, x);
          }
      }
    }
  }

  // Run once on all nodes; fill in missing zero chunks
  public static class SVFTask extends MRTask<SVFTask> {
    final Frame _f;
    SVFTask( Frame f ) { _f = f; }
    @Override public void map(Key key) {
      Vec v0 = _f.vecs()[0];
      for(int i = 0; i < v0.nChunks(); ++i) {
        if( !v0.chunkKey(i).home() ) continue;
        // First find the nrows as the # rows of non-missing chunks; done on
        // locally-homed chunks only - to keep the data distribution.
        int nlines = 0;
        for( Vec vec : _f.vecs() ) {
          Value val = H2O.get(vec.chunkKey(i)); // Local-get only
          if( val != null ) {
            nlines = ((Chunk)val.get())._len;
            break;
          }
        }

        // Now fill in appropriate-sized zero chunks
        for( Vec vec:_f.vecs() ) {
          Key k = vec.chunkKey(i);
          if( !k.home() ) continue; // Local keys only
          Value val = H2O.get(k);   // Local-get only
          if( val == null )         // Missing?  Fill in w/zero chunk
            H2O.putIfMatch(k, new Value(k,new C0DChunk(0, nlines)), null);
        }
      }
    }
    @Override public void reduce( SVFTask drt ) {}
  }

  private static Vec getVec(Key key) {
    Object o = UKV.get(key);
    return o instanceof Vec ? (ByteVec) o : ((Frame) o).vecs()[0];
  }
  private static String [] genericColumnNames(int ncols){
    String [] res = new String[ncols];
    for(int i = 0; i < res.length; ++i)res[i] = "C" + i;
    return res;
  }

  // --------------------------------------------------------------------------
  // Top-level parser driver
  private static void parse_impl(ParseDataset2 job, Key [] fkeys, CustomParser.ParserSetup setup, boolean delete_on_done) {
    assert setup._ncols > 0;
    // remove any previous instance and insert a sentinel (to ensure no one has
    // been writing to the same keys during our parse)!
    UKV.remove(job.dest());
    if( fkeys.length == 0) {
      job.cancel();
      return;
    }

    // Init Enums locally on each Node
    final int ncols = setup._ncols;
    final Key progress = job._progress;
    new MRTask() {
      @Override public void map( Key key ) {
        Enum es[] = new Enum[ncols]; // Local enums per Node
        for( int i=0; i<ncols; i++ )
          es[i] = new Enum();
        ENUMS.put(progress,es); // Save for 2nd enum-renumbering pass
      }
      @Override public void reduce(DRemoteTask drt) { }
    }.invokeOnAllNodes();

    // Launch the parallel distributed multi-file parse
    Vec v = getVec(fkeys[0]);
    MultiFileParseTask uzpt = new MultiFileParseTask(v.group(),setup,progress,fkeys).invoke(fkeys);

    // Calculate global enum domains; set into AppendableVec headers
    AppendableVec vecs[] = uzpt._dout._vecs;
    ValueString ds[][] = new ValueString[vecs.length][];
    for( int i=0; i<vecs.length; i++ ) // Compute the global Enum domains for all cols
      if( vecs[i].shouldBeEnum() )
        vecs[i]._domain = ValueString.toString(ds[i] = uzpt._dout._enums[i].computeColumnDomain());

    // Close out all the vecs; write them (and domains) into the K/V; get a Frame.
    Frame fr = new Frame(setup._columnNames != null?setup._columnNames:genericColumnNames(uzpt._dout._nCols),uzpt._dout.closeVecs());

    // SVMLight is sparse format, there may be missing chunks with all 0s, fill them in
    new SVFTask(fr).invokeOnAllNodes();

    // Update the Chunks across the cluster, so their enum values agree
    new EnumUpdateTask(progress, ds).doAll(fr);

    // Jam the frame of columns into the K/V store
    Futures fs = new Futures();
    UKV.put(job.dest(),fr,fs);
    // Remove CSV files from H2O memory
    if( delete_on_done ) for( Key k : fkeys ) UKV.remove(k,fs);
    fs.blockForPending();
    job.remove();
  }

  public static ParserSetup guessSetup(Key key, ParserSetup setup, boolean checkHeader){
    ByteVec vec = (ByteVec) getVec(key);
    byte [] bits = vec.elem2BV(0)._mem;
    Compression cpr = Utils.guessCompressionMethod(bits);
    return ParseDataset.guessSetup(Utils.unzipBytes(bits,cpr), setup,checkHeader)._setup;
  }

  public static class ParseProgressMonitor extends Iced implements Job.ProgressMonitor {
    final Key _progressKey;
    private long _progress;
    public ParseProgressMonitor(Key pKey){_progressKey = pKey;}
    @Override public void update(long n) {
      ParseDataset2.onProgress(n, _progressKey);
      _progress += n;
    }
    public long progress() {
      return _progress;
    }

  }
  // --------------------------------------------------------------------------
  // We want to do a standard MRTask with a collection of file-keys (so the
  // files are parsed in parallel across the cluster), but we want to throttle
  // the parallelism on each node.
  public static class MultiFileParseTask extends MRTask<MultiFileParseTask> {
    private final Key _progress;
    public final CustomParser.ParserSetup _setup; // The expected column layout
    final VectorGroup _vg;      // vector group of the target dataset
    final int _vecIdStart;      // Starting vector ID in the group
    // A rollup of the number of Chunks per input file, so that the parse of
    // one file (out of many) knows which Chunk# to start its Input data at.
    // Mapped one-to-one with the _keys[] array.
    final int [] _fileChunkOffsets;
    // OUTPUT fields:
    public String _parserr;     // NULL if parse is OK, else an error string
    FVecDataOut _dout;          // the Vecs that got built

    MultiFileParseTask(VectorGroup vg,  CustomParser.ParserSetup setup, Key progress, Key[] filekeys ) {
      _vg = vg; _setup = setup; _progress = progress;
      _vecIdStart = _vg.reserveKeys(setup._pType == ParserType.SVMLight?100000000:setup._ncols);

      // A rollup of the number of Chunks per input file, so that the parse of
      // one file (out of many)
      _fileChunkOffsets = new int[filekeys.length];
      for(int i = 1; i < filekeys.length; ++i)
        _fileChunkOffsets[i] = _fileChunkOffsets[i-1]+ getVec(filekeys[i-1]).nChunks();
    }

    // Called once per file
    @Override public void map( Key key ) {
      // Get parser setup info for this chunk
      ByteVec vec = (ByteVec) getVec(key);
      byte [] bits = vec.elem2BV(0)._mem;
      int chunkStartIdx = -1;
      for( int i=0; i<_keys.length; i++ )
        if( _keys[i]==key ) { chunkStartIdx=_fileChunkOffsets[i]; break; }
      assert chunkStartIdx != -1;
      Compression cpr = Utils.guessCompressionMethod(bits);
      CustomParser.ParserSetup localSetup = ParseDataset.guessSetup(Utils.unzipBytes(bits,cpr), _setup,false)._setup;
      // Local setup: nearly the same as the global all-files setup, but maybe
      // has the header-flag changed.
      if(!_setup.isCompatible(localSetup)) {
        _parserr = "Conflicting file layouts, expecting: "+_setup+" but found "+localSetup;
        return;
      }
      // Allow dup headers, if they are equals-ignoring-case
      boolean has_hdr = _setup._header && localSetup._header;
      if( has_hdr ) {           // Both have headers?
        for( int i = 0; i < localSetup._columnNames.length; ++i )
          has_hdr = localSetup._columnNames[i].equalsIgnoreCase(_setup._columnNames[i]);
        if( !has_hdr )          // Headers not compatible?
          // Then treat as no-headers, i.e., parse it as a normal row
          localSetup = new CustomParser.ParserSetup(ParserType.CSV,localSetup._separator, false);
      }
      final int ncols = _setup._ncols;

      // Parse the file
      try {
        if( cpr == Compression.NONE && localSetup._pType.parallelParseSupported ) {
          DParse dp = new DParse(_vg,localSetup, _vecIdStart, chunkStartIdx,this);
          addToPendingCount(1);
          dp.setCompleter(this);
          dp.dfork(new Frame(vec));
        } else {                // Single-threaded parsing
          ParseProgressMonitor pmon = new ParseProgressMonitor(_progress);
          InputStream vis = vec.openStream(pmon);
          switch( cpr ) {
          case NONE:
            _dout = streamParse(vis, localSetup, _vecIdStart, chunkStartIdx,pmon);
            break;
          case ZIP: {           // Zipped file; no parallel decompression;
            ZipInputStream zis = new ZipInputStream(vis);
            ZipEntry ze = zis.getNextEntry(); // Get the *FIRST* entry
            // There is at least one entry in zip file and it is not a directory.
            if( ze != null && !ze.isDirectory() ) _dout = streamParse(zis,localSetup, _vecIdStart, chunkStartIdx,pmon);
            else zis.close();   // Confused: which zipped file to decompress
            break;
          }
          case GZIP:            // Zipped file; no parallel decompression;
            _dout = streamParse(new GZIPInputStream(vis),localSetup,_vecIdStart, chunkStartIdx,pmon);
            break;
          }
        }
      } catch( IOException ioe ) {
        throw new RuntimeException(ioe);
      }
    }

    // Reduce: combine errors from across files.
    // Roll-up other meta data
    @Override public void reduce( MultiFileParseTask uzpt ) {
      assert this != uzpt;
      // Combine parse errors from across files
      if( _parserr == null ) _parserr = uzpt._parserr;
      else if( uzpt._parserr != null ) _parserr += uzpt._parserr;
      // Collect & combine columns across files
      if( _dout == null ) _dout = uzpt._dout;
      else _dout.reduce(uzpt._dout);
    }

    // ------------------------------------------------------------------------
    // Zipped file; no parallel decompression; decompress into local chunks,
    // parse local chunks; distribute chunks later.
    private FVecDataOut streamParse( final InputStream is, final CustomParser.ParserSetup localSetup, int vecIdStart, int chunkStartIdx, ParseProgressMonitor pmon) throws IOException {
      // All output into a fresh pile of NewChunks, one per column
      FVecDataOut dout = new FVecDataOut(_vg, chunkStartIdx, localSetup._ncols, vecIdStart, pmon._progressKey);
      CustomParser p = localSetup.parser();
      // assume 2x inflation rate
      if(localSetup._pType.parallelParseSupported)
        try{p.streamParse(is, dout,pmon);}catch(IOException e){throw new RuntimeException(e);}
      else
        try{p.streamParse(is, dout);}catch(Exception e){throw new RuntimeException(e);}
      // Parse all internal "chunks", until we drain the zip-stream dry.  Not
      // real chunks, just flipping between 32K buffers.  Fills up the single
      // very large NewChunk.
      dout.close(_fs);
      return dout;
    }

    private static class DParse extends MRTask2<DParse> {
      final CustomParser.ParserSetup _setup;
      final int _vecIdStart;
      final int _startChunkIdx; // for multifile parse, offset of the first chunk in the final dataset
      final VectorGroup _vg;
      final Key _progress;
      transient final MultiFileParseTask _outerMFPT;
      FVecDataOut _dout;

      DParse(VectorGroup vg, CustomParser.ParserSetup setup, int vecIdstart, int startChunkIdx, MultiFileParseTask mfpt) {
        _vg = vg;
        _setup = setup;
        _vecIdStart = vecIdstart;
        _startChunkIdx = startChunkIdx;
        _outerMFPT = mfpt;
        _progress = mfpt._progress;
      }
      @Override public void map( Chunk in ) {
        // Break out the input & output vectors before the parse loop
        // The Parser
        CustomParser p;
        switch(_setup._pType){
        case CSV:
          p = new CsvParser(_setup);
          _dout = new FVecDataOut(_vg,_startChunkIdx + in.cidx(),_setup._ncols,_vecIdStart,_progress);
          break;
        case SVMLight:
          p = new SVMLightParser(_setup);
          _dout = new SVMLightFVecDataOut(_vg, _startChunkIdx + in.cidx(), _setup._ncols, _vecIdStart, _progress);
          break;
        default:
          throw H2O.unimpl();
        }
        FVecDataIn din = new FVecDataIn(in);
        p.parallelParse(in.cidx(),din,_dout);
        _dout.close(_fs);
        onProgress(in._len, _progress); // Record bytes parsed
      }
      @Override public void reduce(DParse dp) {_dout.reduce(dp._dout); }
      @Override public void postGlobal() {
        super.postGlobal();
        _outerMFPT._dout = _dout;
      }
    }
  }

  /**
   * Parsed data output specialized for fluid vecs.
   *
   * @author tomasnykodym
   *
   */
  public static class FVecDataOut extends Iced implements CustomParser.StreamDataOut {
    transient NewChunk [] _nvs;
    AppendableVec []_vecs;
    final Enum [] _enums;
    final Key _progress;
    final byte [] _ctypes;
    long _nLines;
    int _nCols;
    int _col = -1;
    final int _cidx;
    final int _vecIdStart;
    boolean _closedVecs = false;
    private final VectorGroup _vg;

    static final private byte UCOL = 0;
    static final private byte NCOL = 1;
    static final private byte ECOL = 2;
    static final private byte TCOL = 3;

    public FVecDataOut(VectorGroup vg, int cidx, int ncols, int vecIdStart, Key progress){
      _vecs = new AppendableVec[ncols];
      _nvs = new NewChunk[ncols];
      _nCols = ncols;
      _cidx = cidx;
      _vg = vg;
      _vecIdStart = vecIdStart;
      for(int i = 0; i < ncols; ++i)
        _nvs[i] = (NewChunk)(_vecs[i] = new AppendableVec(vg.vecKey(vecIdStart + i))).elem2BV(cidx);
      _ctypes = MemoryManager.malloc1(ncols);
      _progress = progress;
      _enums = ENUMS.get(progress);
      assert _enums != null && _enums.length == ncols;
    }

    public FVecDataOut reduce(StreamDataOut sdout){
      FVecDataOut dout = (FVecDataOut)sdout;
      if(dout._vecs.length > _vecs.length){
        AppendableVec [] v = _vecs;
        _vecs = dout._vecs;
        dout._vecs = v;
      }
      for(int i = 0; i < dout._vecs.length; ++i)
        _vecs[i].reduce(dout._vecs[i]);

      // Merge the left/right enum sets together.  No-op locally, but merges
      // results globally.
      if( this._enums != dout._enums ) { // Must be a real (global) merge
        if( ENUMS.get(_progress) == this._enums ) {
          // We're about to smash the left & right enum sets together.  This is
          // destructive when merging sets from over the wire.  We also need a
          // private unmerged copy local, to revert the Enum column to a String
          // column, or do to local remapping of enums to match the global
          // ordering.  Keep the local enums private via making a private copy
          // if needed.
          Enum[] es = _enums.clone(); // Deep clone the Enum array
          for( int i=0; i<es.length; i++ ) es[i] = es[i].clone();
          ENUMS.put(_progress,es);
        }
        // Now merge into the Enum rollup
        for( int i=0; i<this._enums.length; i++ )
          this._enums[i].merge(dout._enums[i]);
      }
      return this;
    }
    public FVecDataOut close(){
      Futures fs = new Futures();
      close(fs);
      fs.blockForPending();
      return this;
    }
    public FVecDataOut close(Futures fs){
      for(NewChunk nv:_nvs)nv.close(_cidx, fs);
      return this;
    }
    public FVecDataOut nextChunk(){
      return new FVecDataOut(_vg, _cidx+1, _nCols, _vecIdStart, _progress);
    }

    public Vec [] closeVecs(){
      Futures fs = new Futures();
      Vec [] res = closeVecs(fs);
      fs.blockForPending();
      return res;
    }

    public Vec [] closeVecs(Futures fs){
      _closedVecs = true;
      Vec [] res = new Vec[_vecs.length];
      for(int i = 0; i < _vecs.length; ++i)
        res[i] = _vecs[i].close(fs);
      return res;
    }

    @Override public void newLine() {
      if(_col >= 0){
        ++_nLines;
        for(int i = _col+1; i < _nCols; ++i)
          addInvalidCol(i);
      }
      _col = -1;
    }
    protected long linenum(){return _nLines;}
    @Override public void addNumCol(int colIdx, long number, int exp) {
      if(colIdx < _nCols)_nvs[_col = colIdx].addNum(number,exp);
      // else System.err.println("Additional column ("+ _nvs.length + " < " + colIdx + ":" + number + "," + exp + ") on line " + linenum());
    }

    @Override public final void addInvalidCol(int colIdx) {
      if(colIdx < _nCols) _nvs[_col = colIdx].addNA();
//      else System.err.println("Additional column ("+ _nvs.length + " < " + colIdx + " NA) on line " + linenum());
    }
    @Override public final boolean isString(int colIdx) { return false; }

    @Override public final void addStrCol(int colIdx, ValueString str) {
      if(colIdx < _nvs.length){
        if(_ctypes[colIdx] == NCOL){ // support enforced types
          addInvalidCol(colIdx);
          return;
        }
        if(_ctypes[colIdx] == UCOL && Utils.attemptTimeParse(str) > 0)
          _ctypes[colIdx] = TCOL;
        if(_ctypes[colIdx] == TCOL){
          long l = Utils.attemptTimeParse(str);
          if(l > 0)addNumCol(colIdx, l, 0);
          else addInvalidCol(colIdx);
        } else if(!_enums[_col = colIdx].isKilled()) {
          // store enum id into exponent, so that it will be interpreted as NA if compressing as numcol.
          int id = _enums[colIdx].addKey(str);
          if(_ctypes[colIdx] == UCOL && id > 1)_ctypes[colIdx] = ECOL;
          _nvs[colIdx].addEnum(id);
        } else // turn the column into NAs by adding value overflowing Enum.MAX_SIZE
          _nvs[colIdx].addEnum(Integer.MAX_VALUE);
      } //else System.err.println("additional column (" + colIdx + ":" + str + ") on line " + linenum());
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
    public void setColumnNames(String [] names){}
    @Override public final void rollbackLine() {}
    @Override public void invalidLine(String err) {
      newLine();
    } // TODO
    @Override public void invalidValue(int line, int col) {} // TODO
  }

  /**
   * Parser data in taking data from fluid vec chunk.
   *
   * @author tomasnykodym
   *
   */
  public static class FVecDataIn implements CustomParser.DataIn {
    final Vec _vec;
    Chunk _chk;
    int _idx;
    final long _firstLine;
    public FVecDataIn(Chunk chk){
      _chk = chk;
      _idx = _chk.cidx();
      _firstLine = _chk._start;
      _vec = chk._vec;
    }
    @Override public byte[] getChunkData(int cidx) {
      if(cidx != _idx)
        _chk = cidx < _vec.nChunks()?_vec.elem2BV(_idx=cidx):null;
      return (_chk == null)?null:_chk._mem;
    }
    @Override public int  getChunkDataStart(int cidx) { return -1; }
    @Override public void setChunkDataStart(int cidx, int offset) { }
  }
  public static class ParseException extends RuntimeException {
    public ParseException(String msg) { super(msg); }
  }
}
