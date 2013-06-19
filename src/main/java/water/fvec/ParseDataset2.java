package water.fvec;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.zip.*;
import water.*;
import water.H2O.H2OCountedCompleter;
import water.parser.CsvParser;
import water.parser.ValueString;
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

    Vec[] vecs = new Vec[uzpt._cols.length];
    String[] names = new String[uzpt._cols.length];
    for( int i=0; i<vecs.length; i++ ) {
      vecs[i] = uzpt._cols[i].close();
      names[i] = setup._header ? setup._data[0][i] : (""+i);
    }
    // Jam the frame of columns into the K/V store
    UKV.put(job.dest(),new Frame(job.dest(),names,vecs));
  }

  // --------------------------------------------------------------------------
  // We want to do a standard MRTask with a collection of file-keys (so the
  // files are parsed in parallel across the cluster), but we want to throttle
  // the parallelism on each node.
  private static class MultiFileParseTask extends MRTask<MultiFileParseTask> {
    public final CsvParser.Setup _setup; // The expected column layout
    // OUTPUT fields: 
    public String _parserr;              // NULL if parse is OK, else an error string
    // All column data for this one file
    AppendableVec _cols[];

    MultiFileParseTask( CsvParser.Setup setup ) { _setup = setup; }

    //// Total memory on this Node
    //static long MEM = MemoryManager.MEM_MAX;
    //// Node-local desired file parallelism level
    //static int PLEVEL=2;
    //// Bogus estimate of memory used; cranked really high to force MRTask
    //// to throttle parallelism level.
    //@Override public long memOverheadPerChunk() { 
    //  System.out.println("MEM_MAX="+MEM+", will reserve "+(MEM/PLEVEL));
    //  return (MEM/PLEVEL); 
    //}

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

      // Setup result columns named: filekeyC0, filekeyC1, etc...
      final int ncols = _setup._data[0].length;
      _cols = new AppendableVec[ncols];
      for( int i=0; i<ncols; i++ )
        _cols[i] = new AppendableVec(Key.make(key.toString()+"C"+i));

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

    // ------------------------------------------------------------------------
    // Zipped file; no parallel decompression; decompress into local chunks,
    // parse local chunks; distribute chunks later.
    private void streamParse( final InputStream is, final CsvParser.Setup localSetup ) throws IOException {

      // All output into a fresh pile of NewVectors, one per column
      final NewVector nvs[] = new NewVector[_cols.length];
      for( int i=0; i<nvs.length; i++ )
        nvs[i] = new NewVector(_cols[i],0/*starting chunk#*/);

      // The parser for fluid vecs
      CsvParser parser = new CsvParser(localSetup,false) {
          // A 2-entry cache of 32K chunks.
          // no real chunks; just double-pump decompression buffers
          private byte[] _bits0 = new byte[32*1024];
          private byte[] _bits1 = new byte[32*1024];
          private int cidx0=-1, cidx1=-1; // Chunk #s
          private int _col=0;             // Column #

          // Read a next chunk of data: arbitrarily split into 32K chunks.
          // Chunk size is small enough to allow double-buffered chunks to live
          // in L2 cache, but large enough to handle all reasonable row sizes.
          // Not distributed since we're reading from a stream; not in the K/V
          // store; just chunked.
          @Override public byte[] getChunkData( int cidx ) {
            // Check the 2-entry cache
            if( cidx==cidx0 ) return _bits0;
            if( cidx==cidx1 ) return _bits1;
            // Replace the oldest chunk (smallest chunk#)
            if( _parserr != null ) return null;
            assert cidx==cidx0+1 || cidx==cidx1+1;
            byte[] bits = cidx0<cidx1 ? _bits0 : _bits1;
            if( cidx0<cidx1 ) cidx0 = cidx;
            else              cidx1 = cidx;
            // Read as much as the buffer will hold
            int off=0;
            try {
              while( off < bits.length ) {
                int len = is.read(bits,off,bits.length-off);
                if( len == -1 ) break;
                off += len;
              }
            } catch( IOException ioe ) { _parserr = ioe.toString(); }
            if( off == bits.length ) return bits;
            if( off == 0 ) return null;
            return Arrays.copyOf(bits,off);
          }

          // Handle a newLine action from the parser
          @Override public void newLine() {
            if( _col > 0 )
              while( _col < _cols.length )
                addInvalidCol(_col++);
            _col=0;
          }

          // Handle a new number column in the parser
          @Override public void addNumCol(int colIdx, long number, int exp) {
            assert colIdx==_col;
            nvs[colIdx].append2(number,exp);
            _col++;             // Next column filled in
          }
          
          @Override public void addStrCol(int colIdx, ValueString str) { 
            Log.unwrap(System.err,"colIdx="+colIdx+" str="+str);
            assert colIdx==_col;
            _col++;             // Next column filled in
            throw H2O.unimpl(); 
          }
          @Override public void addInvalidCol(int colIdx) { throw H2O.unimpl(); }
          @Override public void rollbackLine() { }
          @Override public boolean isString(int colIdx) { return false; }
        };

      // Parse all internal "chunks", until we drain the zip-stream dry.  Not
      // real chunks, just flipping between 32K buffers.  Fills up the single
      // very large NewVector.
      int cidx=0;
      while( is.available() > 0 )
        parser.parse(cidx++);

      // Close & compress all the NewVectors for this one file.
      for( int i=0; i<nvs.length; i++ )
        nvs[i].close(_fs);
    }

    // ------------------------------------------------------------------------
    // Distributed parse of an unzipped raw text file.
    private void distroParse( ByteVec vec, final CsvParser.Setup localSetup ) throws IOException {
      Vec bvs[] = Arrays.copyOf(_cols,_cols.length+1,Vec[].class);
      bvs[bvs.length-1] = vec;
      new DParse(localSetup).invoke(bvs);
    }
    private class DParse extends MRTask2<DParse> {
      final CsvParser.Setup _setup;
      DParse(CsvParser.Setup setup) { _setup = setup; }
      @Override public void map( BigVector[] bvs ) {
        // Break out the input & output vectors before the parse loop
        final BigVector in = bvs[bvs.length-1];
        final NewVector[] nvs = new NewVector[bvs.length-1];
        for( int i=0; i<nvs.length; i++ ) nvs[i] = (NewVector)bvs[i];

        // The Parser
        CsvParser parser = new CsvParser(_setup,false) {
            private byte[] _mem2; // Chunk following this one
            private int _col=0; // Column #
            @Override public byte[] getChunkData( int cidx ) {
              if( cidx==0 ) return in._mem;
              if( _mem2 != null ) return _mem2;
              BigVector in2 = in._vec.nextBV(in);
              return in2 == null ? null : (_mem2=in2._mem);
            }
            // Handle a newLine action from the parser
            @Override public void newLine() {
              if( _col > 0 )
                while( _col < _cols.length )
                  addInvalidCol(_col++);
              _col=0;
            }

            // Handle a new number column in the parser
            @Override public void addNumCol(int colIdx, long number, int exp) {
              assert colIdx==_col;
              nvs[colIdx].append2(number,exp);
              _col++;             // Next column filled in
            }
            
            @Override public void addStrCol(int colIdx, ValueString str) { 
              Log.unwrap(System.err,"colIdx="+colIdx+" str='"+str+"'");
              assert colIdx==_col;
              _col++;             // Next column filled in
              throw H2O.unimpl(); 
            }
            @Override public void addInvalidCol(int colIdx) { throw H2O.unimpl(); }
            @Override public void rollbackLine() { }
            @Override public boolean isString(int colIdx) { return false; }
          };
        parser.parse(0);
      }
    }

  }

  // --------------------------------------------------------------------------
  // Heuristics

  public static enum Compression { NONE, ZIP, GZIP }
  public static Compression guessCompressionMethod( ByteVec vec) {
    C1Vector bv = vec.elem2BV(0); // First chunk of bytes
    // Look for ZIP magic
    if( vec.length() > ZipFile.LOCHDR && bv.get4(0) == ZipFile.LOCSIG )
      return Compression.ZIP;
    if( vec.length() > 2 && (0xFFFF&bv.get2(0)) == GZIPInputStream.GZIP_MAGIC )
      return Compression.GZIP;
    return Compression.NONE;
  }

  public static CsvParser.Setup csvGuessValue( ByteVec vec, byte separator, Compression compression ) {
    // Since this data is all bytes, we know each chunk is just raw text.
    C1Vector bv = vec.elem2BV(0);
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
          if( bs.length >= ValueArray.CHUNK_SZ )
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
