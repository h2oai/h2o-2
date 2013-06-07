package water.fvec;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.zip.*;
import water.*;
import water.H2O.H2OCountedCompleter;
import water.api.Inspect;
import water.parser.CsvParser;
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
      ByteVec vec = UKV.get(key);
      Compression cpr = guessCompressionMethod(vec);
      CsvParser.Setup setup = csvGuessValue(vec,_setup._separator,cpr);
      final int ncol1 = _setup._data[0].length;
      final int ncol2 =  setup._data[0].length;
      if( ncol1 != ncol2 ) {
        _parserr = "Found conflicting number of columns (using separator '" +_setup._separator + "' when parsing multiple files.  Found " + ncol2 + " columns  in " + key + " , but expected " + ncol1;
        return;
      }
      throw H2O.unimpl();
    //_fileInfo[_idx]._header = setup._header;
    //if(_fileInfo[_idx]._header && _headers != null) // check if we have the header, it should be the same one as we got from the head
    //  for(int i = 0; i < setup._data[0].length; ++i)
    //    _fileInfo[_idx]._header = _fileInfo[_idx]._header && setup._data[0][i].equalsIgnoreCase(_headers[i]);
    //setup = new CsvParser.Setup(_sep, _fileInfo[_idx]._header, setup._data, setup._numlines, setup._bits);
    //_p1 = DParseTask.createPassOne(v, _job, _pType);
    //_p1.setCompleter(this);
    //_p1.passOne(setup);
    //// DO NOT call tryComplete here, _p1 calls it!
    }

    @Override public void reduce( MultiFileParseTask uzpt ) {
      // Combine parse errors from across files
      if( _parserr == null ) _parserr = uzpt._parserr;
      else if( uzpt._parserr != null ) _parserr += uzpt._parserr;
    }
  }

  // --------------------------------------------------------------------------
  // Heuristics

  public static enum Compression { NONE, ZIP, GZIP }
  public static Compression guessCompressionMethod( ByteVec vec) {
    C0Vector bv = vec.elem2BV(0,0); // First chunk of bytes
    // Look for ZIP magic
    if( vec.length() > ZipFile.LOCHDR && bv.get4(0) == ZipFile.LOCSIG )
      return Compression.ZIP;
    if( vec.length() > 2 && bv.get2(0) == GZIPInputStream.GZIP_MAGIC )
      return Compression.GZIP;
    return Compression.NONE;
  }

  public static CsvParser.Setup csvGuessValue( ByteVec vec, byte separator, Compression compression ) {
    // Since this data is all bytes, we know each chunk is just raw text.
    C0Vector bv = vec.elem2BV(0,0);
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
