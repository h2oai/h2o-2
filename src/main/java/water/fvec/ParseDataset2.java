package water.fvec;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.UUID;
import java.util.zip.*;

import jsr166y.CountedCompleter;
import water.*;
import water.H2O.H2OCountedCompleter;
import water.nbhm.NonBlockingHashMap;
import water.parser.*;
import water.parser.CustomParser.ParserSetup;
import water.parser.CustomParser.ParserType;
import water.parser.ParseDataset.Compression;
import water.parser.Enum;
import water.util.Utils;

public final class ParseDataset2 extends Job {
  public final Key  _progress;  // Job progress Key

  // --------------------------------------------------------------------------
  // Parse an array of csv input/file keys into an array of distributed output Vecs
  public static Frame parse(Key okey, Key [] keys) {
    // TODO, get global setup from all files!
    Key k = keys[0];
    ByteVec v = (ByteVec)getVec(k);
    byte [] bits = v.elem2BV(0)._mem;
    Compression cpr = Utils.guessCompressionMethod(bits);
    CustomParser.ParserSetup globalSetup = ParseDataset.guessSetup(Utils.unzipBytes(bits,cpr), new ParserSetup(),true)._setup;
    return forkParseDataset(okey, keys, globalSetup).get();
  }

  public static Frame parse(Key okey, Key [] keys, CustomParser.ParserSetup globalSetup) {
    return forkParseDataset(okey, keys, globalSetup).get();
  }
  // Same parse, as a backgroundable Job
  public static ParseDataset2 forkParseDataset(final Key dest, final Key[] keys, final CustomParser.ParserSetup setup) {
    ParseDataset2 job = new ParseDataset2(dest, keys);
    H2O.submitTask(job.start(new ParserFJTask(job, keys, setup)));
    return job;
  }
  // Setup a private background parse job
  private ParseDataset2(Key dest, Key[] fkeys) {
    super("Parsing", dest);
    // Job progress Key
    _progress = Key.make(UUID.randomUUID().toString(), (byte) 0, Key.JOB);
    UKV.put(_progress, ParseProgress.make(fkeys));
  }

  // Simple internal class doing background parsing, with trackable Job status
  public static class ParserFJTask extends H2OCountedCompleter {
    final ParseDataset2 _job;
    Key [] _keys;
    CustomParser.ParserSetup _setup;

    public ParserFJTask( ParseDataset2 job, Key [] keys, CustomParser.ParserSetup setup) {
      _job = job;
      _keys = keys;
      _setup = setup;
    }
    @Override public void compute2() {
      parse_impl(_job, _keys, _setup);
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
    private transient int[][] _emap;
    final Key _eKey;
    final String [][] _gDomain;
    final int [] _colIds;
    boolean _run;
    public EnumUpdateTask(String [][] gDomain,Key lDomKey, int [] colIds){_gDomain = gDomain; _eKey = lDomKey;_colIds = colIds;}

    @Override public void init(){
      // compute the emap
      if((_run = MultiFileParseTask._enums.containsKey(_eKey))){
        Enum [] enums = MultiFileParseTask._enums.get(_eKey);
        MultiFileParseTask._enums.remove(_eKey);
        _emap = new int[_gDomain.length][];
        for( int i = 0; i < _gDomain.length; ++i ) {
          final Enum e = enums[_colIds[i]];
          _emap[i] = new int[e.maxId()+1];
          Arrays.fill(_emap[i], -1);
          for(int j = 0; j < _gDomain[i].length; ++j) {
            ValueString vs = new ValueString(_gDomain[i][j].getBytes());
            if( e.containsKey(vs) ) _emap[i][e.getTokenId(vs)] = j;
          }
        }
      }
    }

    @Override public void map(Chunk [] chks){
      if(_run)
      for(int i = 0; i < chks.length; ++i){
        for( int j = 0; j < chks[i]._len; ++j){
          long l = chks[i].at80(j);
          if( chks[i].valueIsNA(l) ) continue;
          assert _emap[i][(int)l] >= 0:H2O.SELF.toString() + ": missing enum at col:" + i + ", line: " + j + ", val = " + l + "chunk=" + chks[i].getClass().getSimpleName();
          chks[i].set80(j, _emap[i][(int)l]);
        }
      }
    }
  }

  static class LocalEnumRecord extends Iced{
    Enum [] _enums;
    public LocalEnumRecord(int ncols){
      _enums = new Enum[ncols];
      for(int i = 0; i < ncols; ++i)
        _enums[i] = new Enum();
    }
  }

  public static class EnumFetchTask extends MRTask<EnumFetchTask> {
    final Key _k;
    final int [] _ecols;
    final int _homeNode; // node where the computation started, enum from this node MUST be cloned!
    public EnumFetchTask(int homeNode, Key k, int [] ecols){_homeNode = homeNode; _k = k;_ecols = ecols;}
    Enum [] _enums;
    @Override public void map(Key key) {
      if(MultiFileParseTask._enums.containsKey(_k)){
        _enums = MultiFileParseTask._enums.get(_k);
        // if we are the original node (i.e. there will be no sending over wire),
        // we have to clone the enums not to share the same object (causes problems when computing column domain and renumbering maps).
        if(H2O.SELF.index() == _homeNode){
          _enums = _enums.clone();
          for(int i = 0; i < _enums.length; ++i)
            _enums[i] = _enums[i].clone();
        }
      }
    }

    @Override public void reduce(EnumFetchTask etk) {
      if(_enums == null) _enums = etk._enums;
      else if (etk._enums != null)
        for(int i:_ecols) _enums[i].merge(etk._enums[i]);
    }
    public static Enum [] fetchEnums(Key k, int [] ecols){
      return new EnumFetchTask(H2O.SELF.index(), k, ecols).invokeOnAllNodes()._enums;
    }
  }

  // Run once on all nodes; fill in missing zero chunks
  public static class SVFTask extends MRTask<SVFTask> {
    final Frame _f;
    SVFTask( Frame f ) { _f = f; }
    @Override public void map(Key key) {
      Vec v0 = _f._vecs[0];
      for(int i = 0; i < v0.nChunks(); ++i) {
        if( !v0.chunkKey(i).home() ) continue;
        // First find the nrows as the # rows of non-missing chunks; done on
        // locally-homed chunks only - to keep the data distribution.
        int nlines = 0;
        for( Vec vec : _f._vecs ) {
          Value val = H2O.get(vec.chunkKey(i)); // Local-get only
          if( val != null ) {
            nlines = ((Chunk)val.get())._len;
            break;
          }
        }

        // Now fill in appropriate-sized zero chunks
        for( Vec vec:_f._vecs ) {
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
    return o instanceof Vec ? (ByteVec) o : ((Frame) o)._vecs[0];
  }

  // --------------------------------------------------------------------------
  // Top-level parser driver
  private static void parse_impl(ParseDataset2 job, Key [] fkeys, CustomParser.ParserSetup setup) {
    // remove any previous instance and insert a sentinel (to ensure no one has
    // been writing to the same keys during our parse)!
    UKV.remove(job.dest());
    if( fkeys.length == 0) {
      job.cancel();
      return;
    }
    MultiFileParseTask uzpt = new MultiFileParseTask(setup,job._progress).invoke(fkeys);
    int [] ecols = uzpt.enumCols();
    String[] names = new String[uzpt._vecs.length];
    for( int i=0; i<names.length; i++ )
      names[i] = setup._header ? setup._columnNames[i] : (""+i);

    // Rollup all the enum columns; uniformly renumber enums per chunk, etc.
    if( ecols != null && ecols.length > 0 ) {
      Enum [] enums = EnumFetchTask.fetchEnums(uzpt._eKey, ecols);
      for(int i:ecols) uzpt._vecs[i]._domain = enums[i].computeColumnDomain();
      String [][] ds = new String[ecols.length][];
      for(int i = 0; i < ecols.length; ++i)ds[i] = uzpt._vecs[ecols[i]]._domain;
      Vec [] evecs = new Vec[ecols.length];
      for(int i = 0; i < evecs.length; ++i)evecs[i] = uzpt._vecs[ecols[i]];
      new EnumUpdateTask(ds, uzpt._eKey, ecols).doAll(evecs);
    }
    // Jam the frame of columns into the K/V store
    UKV.put(job.dest(),new Frame(names,uzpt._vecs));
    job.remove();
  }

  public static ParserSetup guessSetup(Key key, ParserSetup setup, boolean checkHeader){

    ByteVec vec = (ByteVec) getVec(key);
    byte [] bits = vec.elem2BV(0)._mem;
    Compression cpr = Utils.guessCompressionMethod(bits);
    return ParseDataset.guessSetup(Utils.unzipBytes(bits,cpr), setup,checkHeader)._setup;
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
    Vec _vecs[];

    MultiFileParseTask( CustomParser.ParserSetup setup, Key progress ) {_setup = setup; _progress = progress; }

    // Called once per file
    @Override public void map( Key key ) {
      // Get parser setup info for this chunk
      ByteVec vec = (ByteVec) getVec(key);
      byte [] bits = vec.elem2BV(0)._mem;
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
      _vecs = new Vec[ncols];
      Key [] keys = vec.group().addVecs(ncols);
      for( int i=0; i<ncols; i++ )
        _vecs[i] = new AppendableVec(keys[i]);
      // Parse the file
      try {
        switch( cpr ) {
        case NONE:
          if( !localSetup._pType.parallelParseSupported ) {
            // XLS types end up here
            streamParse(vec.openStream(_progress), localSetup);
          } else if( localSetup._pType == ParserType.CSV ) {
            // Parallel decompress of CSV
            Vec bvs[] = Arrays.copyOf(_vecs,_vecs.length+1,Vec[].class);
            bvs[bvs.length-1] = vec;
            DParse dp = new DParse(localSetup).doAll(bvs);
            // After the MRTask2, the input Vec array has all the AppendableVecs
            // closed() and rewritten as plain Vecs.  Copy those back into the _cols
            // array.
            for( int i=0; i<_vecs.length; i++ ) _vecs[i] = dp.vecs(i);
          } else {
            // Parallel decompress of SVMLight
            SVMLightDParse sdp = new SVMLightDParse().doAll(vec);
            // svmlight is sparse, some chunks might be missing, fill them with all 0s
            Vec [] newVecs = sdp._dout.closeVecs(_fs);
            new SVFTask(new Frame(null,newVecs)).invokeOnAllNodes();
            _vecs = newVecs;
          }
          break;
        case ZIP: {
          // Zipped file; no parallel decompression;
          ZipInputStream zis = new ZipInputStream(vec.openStream(_progress));
          ZipEntry ze = zis.getNextEntry(); // Get the *FIRST* entry
          // There is at least one entry in zip file and it is not a directory.
          if( ze != null && !ze.isDirectory() ) streamParse(zis,localSetup);
          else zis.close();       // Confused: which zipped file to decompress
          break;
        }
        case GZIP:
          // Zipped file; no parallel decompression;
          streamParse(new GZIPInputStream(vec.openStream(_progress)),localSetup);
          break;
        }
      } catch( IOException ioe ) {
        _parserr = ioe.toString();
        return;
      }
    }

    // Reduce: combine errors from across files.
    // Roll-up other meta data
    @Override public void reduce( MultiFileParseTask uzpt ) {
      // Combine parse errors from across files
      if( _parserr == null ) _parserr = uzpt._parserr;
      else if( uzpt._parserr != null ) _parserr += uzpt._parserr;
      // Collect & combine columns across files
      if( _vecs == null ) { _vecs = uzpt._vecs; return; }
      // Reduce multiple AppendableVecs together
      throw H2O.unimpl();
    }

    private Enum [] enums(){
      if(!_enums.containsKey(_eKey)){
        Enum [] enums = new Enum[_vecs.length];
        for(int i = 0; i < enums.length; ++i)enums[i] = new Enum();
        _enums.putIfAbsent(_eKey, enums);
      }
      return _enums.get(_eKey);
    }
    // ------------------------------------------------------------------------
    // Zipped file; no parallel decompression; decompress into local chunks,
    // parse local chunks; distribute chunks later.
    private void streamParse( final InputStream is, final CustomParser.ParserSetup localSetup ) throws IOException {
      // All output into a fresh pile of NewChunks, one per column
      final NewChunk nvs[] = new NewChunk[_vecs.length];
      for( int i=0; i<nvs.length; i++ )
        nvs[i] = new NewChunk(_vecs[i],0/*starting chunk#*/);
      FVecDataOut dout = new FVecDataOut(nvs, enums());
      CustomParser p = localSetup.parser();
      try{p.streamParse(is, dout);}catch(Exception e){throw new RuntimeException(e);}
      // Parse all internal "chunks", until we drain the zip-stream dry.  Not
      // real chunks, just flipping between 32K buffers.  Fills up the single
      // very large NewChunk.
      for( int i=0; i<_vecs.length; i++ ) {
        nvs[i].close(0/*actual chunk number*/,_fs);
        _vecs[i] = ((AppendableVec)_vecs[i]).close(_fs);
      }
    }

    // ------------------------------------------------------------------------
    private class SVMLightDParse extends MRTask2<SVMLightDParse> {
      SVMLightFVecDataOut _dout;
      @Override public void map( Chunk in ) { // svm light version, we do not know how many cols we're gonna have, keep them separate from MRTask2!
        Enum [] enums = enums();
        FVecDataIn din = new FVecDataIn(in);
        SVMLightFVecDataOut dout = new SVMLightFVecDataOut(in,enums);
        SVMLightParser p = new SVMLightParser();
        p.parallelParse(in.cidx(), din, dout);
        dout.close(_fs);
        _dout = dout;
      }

      @Override public void reduce(SVMLightDParse dp){
        if(_dout == null)
          _dout = dp._dout;
        else
          _dout.reduce(dp._dout);
      }
    }

    private class DParse extends MRTask2<DParse> {
      final CustomParser.ParserSetup _setup;
      DParse(CustomParser.ParserSetup setup) {_setup = setup;}

      @Override public void map( Chunk[] bvs ) {
        Enum [] enums = enums();
        // Break out the input & output vectors before the parse loop
        final Chunk in = bvs[bvs.length-1];
        final NewChunk[] nvs = Arrays.copyOf(bvs,bvs.length-1, NewChunk[].class);
        // The Parser
        FVecDataIn din = new FVecDataIn(in);
        FVecDataOut dout = new FVecDataOut(nvs,enums);
        CsvParser p = new CsvParser(_setup,in._start > 0);
        p.parallelParse(in.cidx(),din,dout);
        onProgress(in._len, _progress); // Record bytes parsed
      }
    }
    // Collect the string columns
    public int [] enumCols (){
      int [] res = new int[_vecs.length];
      int n = 0;
      for(int i = 0; i < _vecs.length; ++i)
        if(_vecs[i].dtype() == Vec.DType.S)
          res[n++] = i;
      return Arrays.copyOf(res, n);
    }
  }


  /**
   * Parsed data output specialized for fluid vecs.
   *
   * @author tomasnykodym
   *
   */
  public static class FVecDataOut extends Iced implements CustomParser.DataOut {
    transient NewChunk [] _nvs;
    final Enum [] _enums;
    long _nLines;
    int _nCols;
    int _col = -1;

    public FVecDataOut(NewChunk [] nvs, Enum [] enums){
      _nvs = nvs;
      _enums = enums;
      _nCols = nvs.length;
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
      if(colIdx >= _nCols)
        System.err.println("Additional column ("+ _nvs.length + " < " + colIdx + ":" + number + "," + exp + ") on line " + linenum());
      else
        _nvs[_col = colIdx].addNum(number,exp);
    }

    @Override public final void addInvalidCol(int colIdx) {
      if(colIdx >= _nCols)
        System.err.println("Additional column ("+ _nvs.length + " < " + colIdx + " NA) on line " + linenum());
      else
        _nvs[_col = colIdx].addNA();
    }
    @Override public final boolean isString(int colIdx) { return false; }

    @Override public final void addStrCol(int colIdx, ValueString str) {
      if(colIdx >= _nvs.length){
        System.err.println("additional column (" + colIdx + ":" + str + ") on line " + linenum());
        return;
      }
      if(!_enums[_col = colIdx].isKilled()) {
        // store enum id into exponent, so that it will be interpreted as NA if compressing as numcol.
        int id = _enums[colIdx].addKey(str);
        _nvs[colIdx].addEnum(id);
        assert _enums[colIdx].getTokenId(str) == id;
        assert id <= _enums[colIdx].maxId();
      } else // turn the column into NAs by adding value overflowing Enum.MAX_SIZE
        _nvs[colIdx].addEnum(Integer.MAX_VALUE);
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
  }
  public static class ParseException extends RuntimeException {
    public ParseException(String msg) { super(msg); }
  }
}
