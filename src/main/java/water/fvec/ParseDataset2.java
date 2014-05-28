package water.fvec;

import jsr166y.CountedCompleter;
import water.*;
import water.H2O.H2OCountedCompleter;
import water.fvec.Vec.VectorGroup;
import water.nbhm.NonBlockingHashMap;
import water.parser.*;
import water.parser.CustomParser.ParserSetup;
import water.parser.CustomParser.ParserType;
import water.parser.CustomParser.StreamDataOut;
import water.parser.Enum;
import water.parser.ParseDataset.Compression;
import water.util.Log;
import water.util.Utils;
import water.util.Utils.IcedHashMap;
import water.util.Utils.IcedInt;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public final class ParseDataset2 extends Job {
  public final Key  _progress;  // Job progress Key

  // --------------------------------------------------------------------------
  // Parse an array of csv input/file keys into an array of distributed output Vecs
  public static Frame parse(Key okey, Key [] keys) {return parse(okey,keys,new ParserSetup(),true);}

  public static Frame parse(Key okey, Key [] keys, CustomParser.ParserSetup globalSetup, boolean delete_on_done) {
    Key k = keys[0];
    ByteVec v = (ByteVec)getVec(k);
    byte [] bits = v.chunkForChunkIdx(0).getBytes();
    Compression cpr = Utils.guessCompressionMethod(bits);
    globalSetup = ParseDataset.guessSetup(Utils.unzipBytes(bits,cpr), globalSetup,true)._setup;
    if( globalSetup._ncols == 0 ) throw new java.lang.IllegalArgumentException(globalSetup.toString());
    return forkParseDataset(okey, keys, globalSetup, delete_on_done).get();
  }
  // Same parse, as a backgroundable Job
  public static ParseDataset2 forkParseDataset(final Key dest, final Key[] keys, final CustomParser.ParserSetup setup, boolean delete_on_done) {
    setup.checkDupColumnNames();
    // Some quick sanity checks: no overwriting your input key, and a resource check.
    long sum=0;
    for( Key k : keys ) {
      if( dest.equals(k) )
        throw new IllegalArgumentException("Destination key "+dest+" must be different from all sources");
      sum += DKV.get(k).length(); // Sum of all input filesizes
    }
    long memsz=0;               // Cluster memory
    for( H2ONode h2o : H2O.CLOUD._memary )
      memsz += h2o.get_max_mem();
    if( sum > memsz*4 )
      throw new IllegalArgumentException("Total input file size of "+PrettyPrint.bytes(sum)+" is much larger than total cluster memory of "+PrettyPrint.bytes(memsz)+", please use either a larger cluster or smaller data.");

    ParseDataset2 job = new ParseDataset2(dest, keys);
    new Frame(job.dest(),new String[0],new Vec[0]).delete_and_lock(job.self()); // Lock BEFORE returning
    for( Key k : keys ) Lockable.read_lock(k,job.self()); // Lock BEFORE returning
    ParserFJTask fjt = new ParserFJTask(job, keys, setup, delete_on_done); // Fire off background parse
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
    private transient int[][][] _emap;
    final Key _eKey;
    final ValueString [][] _gDomain;
    final Enum [][] _lEnums;
    final int  [] _chunk2Enum;
    final int [] _colIds;
    public EnumUpdateTask(ValueString [][] gDomain, Enum [][]  lEnums, int [] chunk2Enum, Key lDomKey, int [] colIds){
      _gDomain = gDomain; _lEnums = lEnums; _chunk2Enum = chunk2Enum; _eKey = lDomKey;_colIds = colIds;
    }

    public int[][] emap(int nodeId){
      if(_emap == null)_emap = new int[_lEnums.length][][];
      if(_emap[nodeId] == null){
        int [][] emap = _emap[nodeId] = new int[_gDomain.length][];
        for( int i = 0; i < _gDomain.length; ++i ) {
          if(_gDomain[i] != null){
            assert _lEnums[nodeId] != null:"missing lEnum of node "  + nodeId + ", enums = " + Arrays.toString(_lEnums);
            final Enum e = _lEnums[nodeId][_colIds[i]];
            emap[i] = new int[e.maxId()+1];
            Arrays.fill(emap[i], -1);
            for(int j = 0; j < _gDomain[i].length; ++j) {
              ValueString vs = _gDomain[i][j];
              if( e.containsKey(vs) ) {
                assert e.getTokenId(vs) <= e.maxId():"maxIdx = " + e.maxId() + ", got " + e.getTokenId(vs);
                emap[i][e.getTokenId(vs)] = j;
              }
            }
          }
        }
      }
      return _emap[nodeId];
    }

    @Override public void map(Chunk [] chks){
      int [][] emap = emap(_chunk2Enum[chks[0].cidx()]);
      final int cidx = chks[0].cidx();
      for(int i = 0; i < chks.length; ++i) {
        Chunk chk = chks[i];
        if(_gDomain[i] == null) // killed, replace with all NAs
          DKV.put(chk._vec.chunkKey(chk.cidx()),new C0DChunk(Double.NaN,chk._len));
        else for( int j = 0; j < chk._len; ++j){
          if( chk.isNA0(j) )continue;
          long l = chk.at80(j);
          assert l >= 0 && l < emap[i].length : "Found OOB index "+l+" pulling from "+chk.getClass().getSimpleName();
          assert emap[i][(int)l] >= 0: H2O.SELF.toString() + ": missing enum at col:" + i + ", line: " + j + ", val = " + l + ", chunk=" + chk.getClass().getSimpleName();
          chk.set0(j, emap[i][(int)l]);
        }
        chk.close(cidx, _fs);
      }
    }
  }

  public static class EnumFetchTask extends MRTask<EnumFetchTask> {
    final Key _k;
    final int [] _ecols;
    final int _homeNode; // node where the computation started, enum from this node MUST be cloned!
    public EnumFetchTask(int homeNode, Key k, int [] ecols){_homeNode = homeNode; _k = k;_ecols = ecols;}
    Enum [] _gEnums; // global enums per column
    Enum [][] _lEnums; // local enums per node per column
    @Override public void map(Key key) {
      _lEnums = new Enum[H2O.CLOUD.size()][];
      if(MultiFileParseTask._enums.containsKey(_k)){
        _lEnums[H2O.SELF.index()] = _gEnums = MultiFileParseTask._enums.get(_k);
        // if we are the original node (i.e. there will be no sending over wire),
        // we have to clone the enums not to share the same object (causes problems when computing column domain and renumbering maps).
        if(H2O.SELF.index() == _homeNode){
          _gEnums = _gEnums.clone();
          for(int i = 0; i < _gEnums.length; ++i)
            _gEnums[i] = _gEnums[i].clone();
        }
        MultiFileParseTask._enums.remove(_k);
      }
    }

    @Override public void reduce(EnumFetchTask etk) {
      if(_gEnums == null){
        _gEnums = etk._gEnums;
        _lEnums = etk._lEnums;
      } else if (etk._gEnums != null) {
        for(int i:_ecols) _gEnums[i].merge(etk._gEnums[i]);
        for(int i = 0; i < _lEnums.length; ++i)
          if(_lEnums[i] == null) _lEnums[i] = etk._lEnums[i];
          else assert etk._lEnums[i] == null;
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
    for(int i = 0; i < res.length; ++i) res[i] = "C" + String.valueOf(i+1);
    return res;
  }

  // Log information about the dataset we just parsed.
  private static void logParseResults(ParseDataset2 job, Frame fr) {
    try {
      long numRows = fr.anyVec().length();
      Log.info("Parse result for " + job.dest() + " (" + Long.toString(numRows) + " rows):");

      Vec[] vecArr = fr.vecs();
      for (int i = 0; i < vecArr.length; i++) {
        Vec v = vecArr[i];
        boolean isCategorical = v.isEnum();
        boolean isConstant = (v.min() == v.max());
        String CStr = String.format("C%d:", i+1);
        String typeStr = String.format("%s", (isCategorical ? "categorical" : "numeric"));
        String minStr = String.format("min(%f)", v.min());
        String maxStr = String.format("max(%f)", v.max());
        long numNAs = v.naCnt();
        String naStr = (numNAs > 0) ? String.format("na(%d)", numNAs) : "";
        String isConstantStr = isConstant ? "constant" : "";
        String numLevelsStr = isCategorical ? String.format("numLevels(%d)", v.domain().length) : "";

        boolean printLogSeparatorToStdout = false;
        boolean printColumnToStdout = false;
        {
          // Print information to stdout for this many leading columns.
          final int MAX_HEAD_TO_PRINT_ON_STDOUT = 10;

          // Print information to stdout for this many trailing columns.
          final int MAX_TAIL_TO_PRINT_ON_STDOUT = 10;

          if (vecArr.length <= (MAX_HEAD_TO_PRINT_ON_STDOUT + MAX_TAIL_TO_PRINT_ON_STDOUT)) {
            // For small numbers of columns, print them all.
            printColumnToStdout = true;
          } else if (i < MAX_HEAD_TO_PRINT_ON_STDOUT) {
            printColumnToStdout = true;
          } else if (i == MAX_HEAD_TO_PRINT_ON_STDOUT) {
            printLogSeparatorToStdout = true;
            printColumnToStdout = false;
          } else if ((i + MAX_TAIL_TO_PRINT_ON_STDOUT) < vecArr.length) {
            printColumnToStdout = false;
          } else {
            printColumnToStdout = true;
          }
        }

        if (printLogSeparatorToStdout) {
          System.out.println("Additional column information only sent to log file...");
        }

        if (printColumnToStdout) {
          // Log to both stdout and log file.
          Log.info(String.format("    %-8s %15s %20s %20s %15s %11s %16s", CStr, typeStr, minStr, maxStr, naStr, isConstantStr, numLevelsStr));
        }
        else {
          // Log only to log file.
          Log.info_no_stdout(String.format("    %-8s %15s %20s %20s %15s %11s %16s", CStr, typeStr, minStr, maxStr, naStr, isConstantStr, numLevelsStr));
        }
      }
    }
    catch (Exception xe) {}   // Don't fail due to logging issues.  Just ignore them.
  }

  // --------------------------------------------------------------------------
  // Top-level parser driver
  private static void parse_impl(ParseDataset2 job, Key [] fkeys, CustomParser.ParserSetup setup, boolean delete_on_done) {
    assert setup._ncols > 0;
    if( fkeys.length == 0) {
      job.cancel();
      return;
    }
    // Remove any previous instance and insert a sentinel (to ensure no one has
    // been writing to the same keys during our parse)!
    Vec v = getVec(fkeys[0]);
    MultiFileParseTask uzpt = new MultiFileParseTask(v.group(),setup,job._progress).invoke(fkeys);
    EnumUpdateTask eut = null;
    // Calculate enum domain
    int n = 0;
    int [] ecols = new int[uzpt._dout._nCols];
    for(int i = 0; i < ecols.length; ++i)
      if(uzpt._dout._vecs[i].shouldBeEnum())
        ecols[n++] = i;
    ecols =  Arrays.copyOf(ecols, n);
    if( ecols != null && ecols.length > 0 ) {
      EnumFetchTask eft = new EnumFetchTask(H2O.SELF.index(), uzpt._eKey, ecols).invokeOnAllNodes();
      Enum [] enums = eft._gEnums;
      ValueString [][] ds = new ValueString[ecols.length][];
      int j = 0;
      for(int i:ecols)uzpt._dout._vecs[i]._domain = ValueString.toString(ds[j++] = enums[i].computeColumnDomain());
      eut = new EnumUpdateTask(ds, eft._lEnums, uzpt._chunk2Enum, uzpt._eKey, ecols);
    }
    Frame fr = new Frame(job.dest(),setup._columnNames != null?setup._columnNames:genericColumnNames(uzpt._dout._nCols),uzpt._dout.closeVecs());
    // SVMLight is sparse format, there may be missing chunks with all 0s, fill them in
    SVFTask t = new SVFTask(fr);
    t.invokeOnAllNodes();
    if(eut != null){
      Vec [] evecs = new Vec[ecols.length];
      for(int i = 0; i < evecs.length; ++i)evecs[i] = fr.vecs()[ecols[i]];
      eut.doAll(evecs);
    }

    logParseResults(job, fr);

    // Release the frame for overwriting
    fr.unlock(job.self());
    // Remove CSV files from H2O memory
    if( delete_on_done ) for( Key k : fkeys ) Lockable.delete(k,job.self());
    else for( Key k : fkeys ) {
      Lockable l = UKV.get(k);
      l.unlock(job.self());
    }
    job.remove();
  }

  public static ParserSetup guessSetup(Key key, ParserSetup setup, boolean checkHeader){
    ByteVec vec = (ByteVec) getVec(key);
    byte [] bits = vec.chunkForChunkIdx(0)._mem;
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
    public static NonBlockingHashMap<Key, Enum[]> _enums = new NonBlockingHashMap<Key, Enum[]>();
    private final Key _eKey = Key.make();
    private final Key _progress;
    public final CustomParser.ParserSetup _setup; // The expected column layout
    // OUTPUT fields:
    public String _parserr;              // NULL if parse is OK, else an error string
    // All column data for this one file
//    Vec _vecs[];
    IcedHashMap<Key,IcedInt> _fileChunkOffsets;
    FVecDataOut _dout;
    final VectorGroup _vg; // vector group of the target dataset
    final int _vecIdStart;
    int [] _chunk2Enum;

    MultiFileParseTask(VectorGroup vg,  CustomParser.ParserSetup setup, Key progress ) {
      _vg = vg; _setup = setup; _progress = progress;
      _vecIdStart = _vg.reserveKeys(setup._pType == ParserType.SVMLight?100000000:setup._ncols);
      _runSingleThreaded = true;
    }

    @Override
    public MultiFileParseTask dfork(Key... keys){
      _fileChunkOffsets = new IcedHashMap<Key, IcedInt>();
      int len = 0;
      for(int i = 0; i < keys.length; ++i){
        _fileChunkOffsets.put(keys[i],new IcedInt(len));
        Vec v = getVec(keys[i]);
        len += v.nChunks();
      }
      _chunk2Enum = MemoryManager.malloc4(len);
      Arrays.fill(_chunk2Enum, -1);
      return super.dfork(keys);
    }

    // Called once per file
    @Override public void map( Key key ) {
      // Get parser setup info for this chunk
      ByteVec vec = (ByteVec) getVec(key);
      byte [] bits = vec.chunkForChunkIdx(0)._mem;
      final int chunkStartIdx = _fileChunkOffsets.get(key)._val;
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
        switch( cpr ) {
        case NONE:
          if(localSetup._pType.parallelParseSupported){
            DParse dp = new DParse(_vg,localSetup, _vecIdStart, chunkStartIdx,this);
            addToPendingCount(1);
            dp._removeKey = vec._key;
            dp.exec(new Frame(vec));
            for(int i = 0; i < vec.nChunks(); ++i)
              _chunk2Enum[chunkStartIdx + i] = vec.chunkKey(i).home_node().index();
          }else {
            ParseProgressMonitor pmon = new ParseProgressMonitor(_progress);
            _dout = streamParse(vec.openStream(pmon), localSetup, _vecIdStart, chunkStartIdx,pmon);
            for(int i = 0; i < vec.nChunks(); ++i)
              _chunk2Enum[chunkStartIdx + i] = H2O.SELF.index();
          }
          break;
        case ZIP: {
          // Zipped file; no parallel decompression;
          ParseProgressMonitor pmon = new ParseProgressMonitor(_progress);
          ZipInputStream zis = new ZipInputStream(vec.openStream(pmon));
          ZipEntry ze = zis.getNextEntry(); // Get the *FIRST* entry
          // There is at least one entry in zip file and it is not a directory.
          if( ze != null && !ze.isDirectory() ) _dout = streamParse(zis,localSetup, _vecIdStart, chunkStartIdx,pmon);
          else zis.close();       // Confused: which zipped file to decompress
          // set this node as the one which rpocessed all the chunks
          for(int i = 0; i < vec.nChunks(); ++i)
            _chunk2Enum[chunkStartIdx + i] = H2O.SELF.index();
          break;
        }
        case GZIP:
          // Zipped file; no parallel decompression;
          ParseProgressMonitor pmon = new ParseProgressMonitor(_progress);
          _dout = streamParse(new GZIPInputStream(vec.openStream(pmon)),localSetup,_vecIdStart, chunkStartIdx,pmon);
          // set this node as the one which processed all the chunks
          for(int i = 0; i < vec.nChunks(); ++i)
            _chunk2Enum[chunkStartIdx + i] = H2O.SELF.index();
          break;
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
      if(_dout == null)_dout = uzpt._dout;
      else _dout.reduce(uzpt._dout);
      if(_chunk2Enum == null)_chunk2Enum = uzpt._chunk2Enum;
      else if(_chunk2Enum != uzpt._chunk2Enum) { // we're sharing global array!
        for(int i = 0; i < _chunk2Enum.length; ++i){
          if(_chunk2Enum[i] == -1)_chunk2Enum[i] = uzpt._chunk2Enum[i];
          else assert uzpt._chunk2Enum[i] == -1:Arrays.toString(_chunk2Enum) + " :: " + Arrays.toString(uzpt._chunk2Enum);
        }
      }
    }

    private static Enum [] enums(Key eKey, int ncols){
      if(!_enums.containsKey(eKey)){
        Enum [] enums = new Enum[ncols];
        for(int i = 0; i < enums.length; ++i)enums[i] = new Enum();
        _enums.putIfAbsent(eKey, enums);
      }
      return _enums.get(eKey);
    }
    // ------------------------------------------------------------------------
    // Zipped file; no parallel decompression; decompress into local chunks,
    // parse local chunks; distribute chunks later.
    private FVecDataOut streamParse( final InputStream is, final CustomParser.ParserSetup localSetup, int vecIdStart, int chunkStartIdx, ParseProgressMonitor pmon) throws IOException {
      // All output into a fresh pile of NewChunks, one per column
      FVecDataOut dout = new FVecDataOut(_vg, chunkStartIdx, localSetup._ncols, vecIdStart, enums(_eKey,localSetup._ncols));
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
      private transient AppendableVec [] _appendables;
      FVecDataOut _dout;
      final Key _eKey;
      final Key _progress;
      Key _removeKey;
      transient final MultiFileParseTask _outerMFPT;


      DParse(VectorGroup vg, CustomParser.ParserSetup setup, int vecIdstart, int startChunkIdx, MultiFileParseTask mfpt) {
        super(mfpt);
        _vg = vg;
        _setup = setup;
        _vecIdStart = vecIdstart;
        _startChunkIdx = startChunkIdx;
        _outerMFPT = mfpt;
        _eKey = mfpt._eKey;
        _progress = mfpt._progress;
      }
      @Override public void setupLocal(){
        super.setupLocal();
        if(_setup._pType == ParserType.CSV){
          _appendables = new AppendableVec[_setup._ncols];
          for(int i = 0; i < _setup._ncols; ++i)
            _appendables[i] = new AppendableVec(_vg.vecKey(_vecIdStart + i));
        }
      }
      @Override public void map( Chunk in ) {
        Enum [] enums = enums(_eKey,_setup._ncols);
        // Break out the input & output vectors before the parse loop
        // The Parser
        FVecDataIn din = new FVecDataIn(in);
        FVecDataOut dout;
        CustomParser p;
        switch(_setup._pType){
          case CSV:
            p = new CsvParser(_setup);
            dout = new FVecDataOut(_vg,_startChunkIdx + in.cidx(),_setup._ncols,_vecIdStart,enums,_appendables);
            break;
          case SVMLight:
            p = new SVMLightParser(_setup);
            dout = new SVMLightFVecDataOut(_vg, _startChunkIdx + in.cidx(), _setup._ncols, _vecIdStart, enums);
            break;
          default:
            throw H2O.unimpl();
        }
        p.parallelParse(in.cidx(),din,dout);
        (_dout = dout).close(_fs);
        onProgress(in._len, _progress); // Record bytes parsed
      }
      @Override public void reduce(DParse dp){
        if(_dout == null)_dout = dp._dout;
        else _dout.reduce(dp._dout);
      }
      @Override public void postGlobal() {
        super.postGlobal();
        _outerMFPT._dout = _dout;
        _dout = null;
        if(_removeKey != null) UKV.remove(_removeKey);
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
    protected transient NewChunk [] _nvs;
    protected AppendableVec []_vecs;
    private transient final Enum [] _enums;
    protected final byte [] _ctypes;
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

    private static final AppendableVec[] newAppendables(int n, VectorGroup vg, int vecIdStart){
      AppendableVec [] apps = new AppendableVec[n];
      for(int i = 0; i < n; ++i)
        apps[i] = new AppendableVec(vg.vecKey(vecIdStart + i));
      return apps;
    }
    public FVecDataOut(VectorGroup vg, int cidx, int ncols, int vecIdStart, Enum [] enums){
      this(vg,cidx,ncols,vecIdStart,enums,newAppendables(ncols,vg,vecIdStart));
    }

    public FVecDataOut(VectorGroup vg, int cidx, int ncols, int vecIdStart, Enum [] enums, AppendableVec [] appendables){
      _vecs = appendables;
      _nvs = new NewChunk[ncols];
      _enums = enums;
      _nCols = ncols;
      _cidx = cidx;
      _vg = vg;
      _vecIdStart = vecIdStart;
      _ctypes = MemoryManager.malloc1(ncols);
      for(int i = 0; i < ncols; ++i)
        _nvs[i] = (NewChunk)_vecs[i].chunkForChunkIdx(_cidx);
    }

    public FVecDataOut reduce(StreamDataOut sdout){
      FVecDataOut dout = (FVecDataOut)sdout;
      if(_vecs != dout._vecs){
        _nCols = Math.max(_nCols,dout._nCols);
        if(dout._vecs.length > _vecs.length){
          AppendableVec [] v = _vecs;
          _vecs = dout._vecs;
          dout._vecs = v;
        }
        for(int i = 0; i < dout._vecs.length; ++i)
          _vecs[i].reduce(dout._vecs[i]);
      }
      return this;
    }
    @Override public FVecDataOut close(){
      Futures fs = new Futures();
      close(fs);
      fs.blockForPending();
      return this;
    }
     public void check(){
       if(_nvs != null) for(NewChunk nv:_nvs)
        assert (nv._len2 == _nLines):"unexpected number of lines in NewChunk, got " + nv._len2 + ", but expected " + _nLines;
    }
    @Override public FVecDataOut close(Futures fs){
      if( _nvs == null ) return this; // Might call close twice
      for(NewChunk nv:_nvs) nv.close(_cidx, fs);
      _nvs = null;  // Free for GC
      return this;
    }
    @Override public FVecDataOut nextChunk(){
      return  new FVecDataOut(_vg, _cidx+1, _nCols, _vecIdStart, _enums);
    }

    private Vec [] closeVecs(){
      Futures fs = new Futures();
      _closedVecs = true;
      Vec [] res = new Vec[_vecs.length];
      for(int i = 0; i < _vecs[0]._espc.length; ++i){
        int j = 0;
        while(j < _vecs.length && _vecs[j]._espc[i] == 0)++j;
        if(j == _vecs.length)break;
        final long clines = _vecs[j]._espc[i];
        for(AppendableVec v:_vecs) {
          if(v._espc[i] == 0)v._espc[i] = clines;
          else assert v._espc[i] == clines:"incompatible number of lines: " +  v._espc[i] +  " != " + clines;
        }
      }
      for(int i = 0; i < _vecs.length; ++i)
        res[i] = _vecs[i].close(fs);
      _vecs = null;  // Free for GC
      fs.blockForPending();
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
      if( colIdx < _nCols ) _nvs[_col = colIdx].addNum(number,exp);
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
        if(_ctypes[colIdx] == UCOL && ParseTime.attemptTimeParse(str) > 0)
          _ctypes[colIdx] = TCOL;
        if(_ctypes[colIdx] == TCOL){
          long l = ParseTime.attemptTimeParse(str);
          if( l == Long.MIN_VALUE ) addInvalidCol(colIdx);
          else {
            int time_pat = ParseTime.decodePat(l); // Get time pattern
            l = ParseTime.decodeTime(l);           // Get time
            addNumCol(colIdx, l, 0);               // Record time in msec
            _nvs[_col]._timCnt[time_pat]++; // Count histo of time parse patterns
          }
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
        _chk = cidx < _vec.nChunks()?_vec.chunkForChunkIdx(_idx = cidx):null;
      return (_chk == null)?null:_chk._mem;
    }
    @Override public int  getChunkDataStart(int cidx) { return -1; }
    @Override public void setChunkDataStart(int cidx, int offset) { }
  }
  public static class ParseException extends RuntimeException {
    public ParseException(String msg) { super(msg); }
  }
}
