package water.fvec;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.UUID;
import java.util.zip.*;

import water.*;
import water.H2O.H2OCountedCompleter;
import water.fvec.FVecParser.ChunkParser;
import water.fvec.FVecParser.StreamParser;
import water.nbhm.NonBlockingHashMap;
import water.parser.*;
import water.parser.Enum;
import water.util.Log;

public final class ParseDataset2 extends Job {
  public final Key  _progress;  // Job progress Key

  // --------------------------------------------------------------------------
  // Parse an array of csv input/file keys into an array of distributed output Vecs.
  public static Frame parse(Key okey, Key [] keys) {
    return forkParseDataset(okey, keys, null).get();
  }
  // Same parse, as a backgroundable Job
  public static ParseDataset2 forkParseDataset(final Key dest, final Key[] keys, final CsvParser.Setup setup) {
    ParseDataset2 job = new ParseDataset2(dest, keys);
    H2O.submitTask(job.start(new ParserFJTask(job, keys, setup)));
    return job;
  }
  // Setup a private background parse job
  private ParseDataset2(Key dest, Key[] fkeys) {
    super("Parse", dest);
    // Job progress Key
    _progress = Key.make(UUID.randomUUID().toString(), (byte) 0, Key.JOB);
    UKV.put(_progress, ParseProgress.make(fkeys));
  }

  // Simple internal class doing background parsing, with trackable Job status
  public static class ParserFJTask extends H2OCountedCompleter {
    final ParseDataset2 _job;
    Key [] _keys;
    CsvParser.Setup _setup;

    public ParserFJTask( ParseDataset2 job, Key [] keys, CsvParser.Setup setup) {
      _job = job;
      _keys = keys;
      _setup = setup;
    }
    @Override public void compute2() {
      parse_impl(_job, _keys, _setup);
      tryComplete();
    }
  }

  // --------------------------------------------------------------------------
  // Parser progress
  static class ParseProgress extends Iced {
    long _total;
    long _value;
    ParseProgress(long val, long total){_value = val; _total = total;}
    // Total number of steps is equal to total bytecount across files
    static ParseProgress make( Key[] fkeys ) {
      long total = 0;
      for( Key fkey : fkeys )
        total += ((ByteVec)UKV.get(fkey)).length();
      return new ParseProgress(0,total);
    }
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
   * Task to update enum values to match the global numbering scheme.
   * Performs update in place so that values originally numbered using node-local unordered numbering will be numbered using global numbering.
   *
   * @author tomasnykodym
   *
   */
  public static class EnumUpdateTask extends MRTask2<EnumUpdateTask>{
    private transient int[][] _emap;
    final Key _eKey;
    final String [][] _gDomain;
    final int [] _colIds;


    public EnumUpdateTask(String [][] gDomain,Key lDomKey, int [] colIds){_gDomain = gDomain; _eKey = lDomKey;_colIds = colIds;}

    @Override public void init(){
      // compute the emap
      Enum [] enums = MultiFileParseTask._enums.get(_eKey);
      MultiFileParseTask._enums.remove(_eKey);
      _emap = new int[_gDomain.length][];
      for(int i = 0; i < _gDomain.length; ++i){
        _emap[i] = new int[enums[_colIds[i]].maxId()+1];
        Arrays.fill(_emap[i], -1);
        final Enum e = enums[_colIds[i]];
        for(int j = 0; j < _gDomain[i].length; ++j){
          ValueString vs = new ValueString(_gDomain[i][j].getBytes());
          if(enums[_colIds[i]].containsKey(vs))_emap[i][e.getTokenId(vs)] = j;
        }
        System.out.println(H2O.SELF + ": "+ e.toString() + ": " + i + ": " + Arrays.toString(_emap[i]));
      }
    }

    @Override public void map(Chunk [] chks){
      for(int i = 0; i < chks.length; ++i){
        for( int j = 0; j < chks[i]._len; ++j){
          long l = chks[i].at80(j);
          if(chks[i].valueIsNA(l))continue;
          if(_emap[i].length <= chks[i].at80(j) || _emap[i][(int)chks[i].at80(j)] < 0)
            System.err.println(H2O.SELF + ": haha");

          assert _emap[i][(int)chks[i].at80(j)] >= 0:H2O.SELF.toString() + ": missing enum at col:" + i + ", line: " + j + ", val = " + chks[i].at80(j);
          chks[i].set80(j, _emap[i][(int)chks[i].at80(j)]);
        }
        chks[i].close(chks[i].cidx(), _fs);
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

  public static class EnumFetchTask extends MRTask {
    final Key _k;
    final int [] _ecols;
    public EnumFetchTask(Key k, int [] ecols){_k = k;_ecols = ecols;}
    Enum [] _enums;
    @Override public void map(Key key) {_enums = MultiFileParseTask._enums.get(_k);}

    @Override public void reduce(DRemoteTask drt) {
      EnumFetchTask etk = (EnumFetchTask)drt;
      if(_enums == null) _enums = etk._enums;
      else if (etk._enums != null){
        for(int i:_ecols)_enums[i].merge(etk._enums[i]);
      }
    }
    public static Enum [] fetchEnums(Key k, int [] ecols){
      EnumFetchTask tsk = new EnumFetchTask(k, ecols);
      Key [] keys = new Key[H2O.CLOUD.size()];
      for(int i = 0; i < keys.length; ++i)
        keys[i] = Key.make("aaa",(byte)1, Key.DFJ_INTERNAL_USER,H2O.CLOUD._memary[i]);
      tsk.invoke(keys);
      return tsk._enums;
    }
  }

  // --------------------------------------------------------------------------
  // Top-level parser driver
  private static void parse_impl(ParseDataset2 job, Key [] fkeys, CsvParser.Setup setup) {
    // remove any previous instance and insert a sentinel (to ensure no one has
    // been writing to the same keys during our parse)!
    UKV.remove(job.dest());
    if( fkeys.length == 0) {
      job.cancel();
      return;
    }
    // Guess column layout.  For multiple files, the caller is supposed to
    // guarantee they have equal & compatible columns and/or headers.
    ByteVec vec = UKV.get(fkeys[0]);
    Compression compression = guessCompressionMethod(vec);
    byte sep = setup == null ? CsvParser.NO_SEPARATOR : setup._separator;
    if( setup == null || setup._data == null || setup._data[0] == null )
      setup = csvGuessValue(vec, sep, compression);
    // Parallel file parse launches across the cluster
    MultiFileParseTask uzpt = new MultiFileParseTask(setup).invoke(fkeys);
    if( uzpt._parserr != null )
      throw new ParseException(uzpt._parserr);
    int [] ecols = uzpt.enumCols();
    String[] names = new String[uzpt._cols.length];
    for( int i=0; i<names.length; i++ )
      names[i] = setup._header ? setup._data[0][i] : (""+i);

    if(ecols != null && ecols.length > 0){
      Enum [] enums = EnumFetchTask.fetchEnums(uzpt._eKey, ecols);
      for(int i:ecols) {
        uzpt._cols[i]._domain = enums[i].computeColumnDomain();
        System.out.println(Arrays.toString(uzpt._cols[i]._domain));
      }
      String [][] ds = new String[ecols.length][];
      for(int i = 0; i < ecols.length; ++i)ds[i] = uzpt._cols[ecols[i]]._domain;
      Vec [] evecs = new Vec[ecols.length];
      for(int i = 0; i < evecs.length; ++i)evecs[i] = uzpt._cols[ecols[i]];
      EnumUpdateTask t = new EnumUpdateTask(ds, uzpt._eKey, ecols);
      t.invoke(evecs);
    }
    // Jam the frame of columns into the K/V store
    UKV.put(job.dest(),new Frame(job.dest(),names,uzpt._cols));
  }

  // --------------------------------------------------------------------------
  // We want to do a standard MRTask with a collection of file-keys (so the
  // files are parsed in parallel across the cluster), but we want to throttle
  // the parallelism on each node.
  public static class MultiFileParseTask extends MRTask<MultiFileParseTask> {
    public static NonBlockingHashMap<Key, Enum[]> _enums = new NonBlockingHashMap<Key, Enum[]>();
    private final  Key _eKey = Key.make();
    public final CsvParser.Setup _setup; // The expected column layout
    // OUTPUT fields:
    public String _parserr;              // NULL if parse is OK, else an error string
    // All column data for this one file
    Vec _cols[];
    MultiFileParseTask( CsvParser.Setup setup ) {_setup = setup;}

    public int [] enumCols (){
      int [] res = new int[_cols.length];
      int n = 0;
      for(int i = 0; i < _cols.length; ++i){
        if(_cols[i].dtype() == Vec.DType.S)
          res[n++] = i;
      }
      return Arrays.copyOf(res, n);
    }
    // Called once per file
    @Override public void map( Key key ) {
      // Get parser setup info for this chunk
      ByteVec vec = UKV.get(key);
      Compression cpr = guessCompressionMethod(vec);
      // Local setup: nearly the same as the global all-files setup, but maybe
      // has the header-flag changed.
      CsvParser.Setup setup = csvGuessValue(vec,_setup._separator,cpr);
      if( !_setup.equals(setup) ) {
        _parserr = "Conflicting file layouts, expecting: "+_setup+" but found "+setup;
        return;
      }
      // Allow dup headers, if they are equals-ignoring-case
      boolean has_hdr = _setup._header && setup._header;
      if( has_hdr ) {           // Both have headers?
        for( int i = 0; i < setup._data[0].length; ++i )
          has_hdr &= setup._data[0][i].equalsIgnoreCase(_setup._data[0][i]);
        if( !has_hdr )          // Headers not compatible?
          // Then treat as no-headers, i.e., parse it as a normal row
          setup = new CsvParser.Setup(setup,false);
      }

      final int ncols = _setup._data[0].length;
      _cols = new Vec[ncols];
      Key [] keys = vec.group().addVecs(ncols);
      for( int i=0; i<ncols; i++ )
        _cols[i] = new AppendableVec(keys[i]);
      // Parse the file
      try {
        switch( cpr ) {
        case NONE:
          // Parallel decompress
          distroParse(vec,setup);
          break;
        case ZIP: {
          // Zipped file; no parallel decompression;
          ZipInputStream zis = new ZipInputStream(vec.openStream());
          ZipEntry ze = zis.getNextEntry(); // Get the *FIRST* entry
          // There is at least one entry in zip file and it is not a directory.
          if( ze != null && !ze.isDirectory() ) streamParse(zis,setup);
          else zis.close();       // Confused: which zipped file to decompress
          break;
        }
        case GZIP:
          // Zipped file; no parallel decompression;
          streamParse(new GZIPInputStream(vec.openStream()),setup);
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
      if( _cols == null ) { _cols = uzpt._cols; return; }
      // Reduce multiple AppendableVecs together
      throw H2O.unimpl();
    }

    private Enum [] enums(){
      if(!_enums.containsKey(_eKey)){
        Enum [] enums = new Enum[_cols.length];
        for(int i = 0; i < enums.length; ++i)enums[i] = new Enum();
        _enums.putIfAbsent(_eKey, enums);
      }
      return _enums.get(_eKey);
    }
    // ------------------------------------------------------------------------
    // Zipped file; no parallel decompression; decompress into local chunks,
    // parse local chunks; distribute chunks later.
    private void streamParse( final InputStream is, final CsvParser.Setup localSetup ) throws IOException {
      // All output into a fresh pile of NewChunks, one per column
      final NewChunk nvs[] = new NewChunk[_cols.length];
      for( int i=0; i<nvs.length; i++ )
        nvs[i] = new NewChunk(_cols[i],0/*starting chunk#*/);

      StreamParser parser = new StreamParser(is, nvs, localSetup, enums());
      // Parse all internal "chunks", until we drain the zip-stream dry.  Not
      // real chunks, just flipping between 32K buffers.  Fills up the single
      // very large NewChunk.
      int cidx=0;
      while( is.available() > 0 )
        parser.parse(cidx++);
      // Close & compress all the NewChunks for this one file.
      for( int i=0; i<_cols.length; i++ ) {
        nvs[i].close(0/*actual chunk number*/,_fs);
        _cols[i] = ((AppendableVec)_cols[i]).close(_fs);
      }
    }

    // ------------------------------------------------------------------------
    // Distributed parse of an unzipped raw text file.
    private void distroParse( ByteVec vec, final CsvParser.Setup localSetup ) throws IOException {
      Vec bvs[] = Arrays.copyOf(_cols,_cols.length+1,Vec[].class);
      bvs[bvs.length-1] = vec;
      DParse dp = new DParse(localSetup).invoke(bvs);
      // After the MRTask2, the input Vec array has all the AppendableVecs
      // closed() and rewritten as plain Vecs.  Copy those back into the _cols
      // array.
      for( int i=0; i<_cols.length; i++ ) _cols[i] = dp.vecs(i);


    }
    private class DParse extends MRTask2<DParse> {
      final CsvParser.Setup _setup;
      DParse(CsvParser.Setup setup) {_setup = setup;}
      @Override public void map( Chunk[] bvs ) {
        Enum [] enums = enums();
        // Break out the input & output vectors before the parse loop
        final Chunk in = bvs[bvs.length-1];
        final NewChunk[] nvs = new NewChunk[bvs.length-1];
        for( int i=0; i<nvs.length; i++ ) nvs[i] = (NewChunk)bvs[i];
        // The Parser
        ChunkParser parser = new ChunkParser(in,nvs,_setup,enums);
        parser.parse(0);
      }
    }
  }

  // --------------------------------------------------------------------------
  // Heuristics

  public static enum Compression { NONE, ZIP, GZIP }
  public static Compression guessCompressionMethod( ByteVec vec) {
    C1Chunk bv = vec.elem2BV(0); // First chunk of bytes
    // Look for ZIP magic
    if( vec.length() > ZipFile.LOCHDR && bv.get4(0) == ZipFile.LOCSIG )
      return Compression.ZIP;
    if( vec.length() > 2 && (0xFFFF&bv.get2(0)) == GZIPInputStream.GZIP_MAGIC )
      return Compression.GZIP;
    return Compression.NONE;
  }

  public static CsvParser.Setup csvGuessValue( ByteVec vec, byte separator, Compression compression ) {
    // Since this data is all bytes, we know each chunk is just raw text.
    C1Chunk bv = vec.elem2BV(0);
    // See if we can make sense of the first few rows.
    byte[] bs = bv._mem;
    int off = 0;                   // Offset of read/decompressed bytes
    // First decrypt compression
    InputStream is = null;
    try {
      switch( compression ) {
      case NONE: off = bs.length; break; // All bytes ready already
      case GZIP: is = new GZIPInputStream(vec.openStream()); break;
      case ZIP: {
        ZipInputStream zis = new ZipInputStream(vec.openStream());
        ZipEntry ze = zis.getNextEntry(); // Get the *FIRST* entry
        // There is at least one entry in zip file and it is not a directory.
        if( ze != null && !ze.isDirectory() ) is = zis;
        else zis.close();       // Confused: which zipped file to decompress
        break;
      }
      }

      // If reading from a compressed stream, estimate we can read 2x uncompressed
      if( is != null )
        bs = new byte[bs.length * 2];
      // Now read from the (possibly compressed) stream expanding into bs
      while( off < bs.length ) {
        int len = is.read(bs, off, bs.length - off);
        if( len < 0 )
          break;
        off += len;
        if( off == bs.length ) { // Dataset is uncompressing alot! Need more space...
          if( bs.length >= Vec.CHUNK_SZ )
            break; // Already got enough
          bs = Arrays.copyOf(bs, bs.length * 2);
        }
      }
    } catch( IOException ioe ) { // Stop at any io error
      Log.err(ioe);
    } finally {
      if( is != null ) try { is.close(); } catch( IOException ioe ) {}
    }
    if( off < bs.length )
      bs = Arrays.copyOf(bs, off); // Trim array to length read

    // Now try to interpret the unzipped data as a CSV
    return CsvParser.inspect(bs, separator);
  }

  public static class ParseException extends RuntimeException {
    public ParseException(String msg) { super(msg); }
  }
}


