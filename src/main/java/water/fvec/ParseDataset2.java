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
  public static void parse(Key okey, Key [] keys) {
    forkParseDataset(okey, keys, null).get();
  }
  // Same parse, as a backgroundable Job
  public static Job forkParseDataset(final Key dest, final Key[] keys, final CsvParser.Setup setup) {
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
    System.out.println("Setup="+setup);

    // Parallel file parse launches across the cluster
    MultiFileParseTask uzpt = new MultiFileParseTask(setup).invoke(fkeys);
    
    if( uzpt._parserr != null )
      throw new ParseException(uzpt._parserr);

    throw H2O.unimpl();
  }

  // --------------------------------------------------------------------------
  // We want to do a standard MRTask with a collection of file-keys (so the
  // files are parsed in parallel across the cluster), but we want to throttle
  // the parallelism on each node.
  private static class MultiFileParseTask extends MRTask<MultiFileParseTask> {
    public final CsvParser.Setup _setup; // The expected column layout
    public String _parserr;              // NULL if parse is OK, else an error string

    MultiFileParseTask( CsvParser.Setup setup ) {
      _setup = setup;
    }

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

      // Parse the file
      try {
        switch( cpr ) {
        case NONE:
          // Parallel decompress
          throw H2O.unimpl();
        case ZIP: {
          // Zipped file; no parallel decompression;
          ZipInputStream zis = new ZipInputStream(vec.openStream());
          ZipEntry ze = zis.getNextEntry(); // Get the *FIRST* entry
          // There is at least one entry in zip file and it is not a directory.
          if( ze != null && !ze.isDirectory() ) streamParse(key,zis);
          else zis.close();       // Confused: which zipped file to decompress
          break;
        }
        case GZIP: 
          // Zipped file; no parallel decompression;
          streamParse(key,new GZIPInputStream(vec.openStream())); 
          break;
        }
      } catch( IOException ioe ) {
        _parserr = ioe.toString();
        return;
      }

      // Close any AppendableVec
      //for( int i=0; i<ncols; i++ ) {
      //}
      throw H2O.unimpl();
          
    }

    @Override public void reduce( MultiFileParseTask uzpt ) {
      // Combine parse errors from across files
      if( _parserr == null ) _parserr = uzpt._parserr;
      else if( uzpt._parserr != null ) _parserr += uzpt._parserr;
    }

    // Zipped file; no parallel decompression; decompress into local chunks,
    // parse local chunks; distribute chunks later.
    private void streamParse( Key fkey, final InputStream is ) throws IOException {

      // All output into a fresh pile of AppendableVecs, one per column
      final int ncols = _setup._data[0].length;
      final AppendableVec cols[] = new AppendableVec[ncols];
      final NewVector nvs[] = new NewVector[ncols];
      for( int i=0; i<ncols; i++ ) {
        cols[i] = new AppendableVec(Key.make(fkey.toString()+"C"+i));
        nvs[i] = new NewVector(cols[i],0/*chunk#*/);
      }

      // The parser for fluid vecs
      CsvParser parser = new CsvParser(_setup,null) {
          // A 2-entry cache of 32K chunks.
          // no real chunks; just double-pump decompression buffers
          private byte[] _bits0 = new byte[32*1024];
          private byte[] _bits1 = new byte[32*1024];
          private int cidx0=-1, cidx1=-1; // Chunk #s
          private int _col=0;             // Column #

          // Read a next chunk of data: arbirtrarily split into 32K chunks.
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
              while( _col < cols.length )
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
          void addInvalidCol(int colIdx) { throw H2O.unimpl(); }
          @Override public void rollbackLine() { }
          @Override public boolean isString(int colIdx) { return false; }
        };

      // Parse all internal "chunks", until we drain the zip-stream dry.
      // Not real chunks, just flipping between 32K buffers.
      int cidx=0;
      while( is.available() > 0 )
        parser.parse(cidx++);

      // Close all the NewVectors
      for( int i=0; i<ncols; i++ ) {
        nvs[i].close(null/*futures?*/);
      }

    }
  }

  // --------------------------------------------------------------------------
  // Heuristics

  public static enum Compression { NONE, ZIP, GZIP }
  public static Compression guessCompressionMethod( ByteVec vec) {
    C0Vector bv = vec.elem2BV(0); // First chunk of bytes
    // Look for ZIP magic
    if( vec.length() > ZipFile.LOCHDR && bv.get4(0) == ZipFile.LOCSIG )
      return Compression.ZIP;
    if( vec.length() > 2 && (0xFFFF&bv.get2(0)) == GZIPInputStream.GZIP_MAGIC )
      return Compression.GZIP;
    return Compression.NONE;
  }

  public static CsvParser.Setup csvGuessValue( ByteVec vec, byte separator, Compression compression ) {
    // Since this data is all bytes, we know each chunk is just raw text.
    C0Vector bv = vec.elem2BV(0);
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
