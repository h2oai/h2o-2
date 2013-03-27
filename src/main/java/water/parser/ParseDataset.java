package water.parser;

import java.io.IOException;
import java.util.*;
import java.util.zip.*;

import water.*;
import water.H2O.H2OCountedCompleter;
import water.api.Inspect;
import water.parser.DParseTask.Pass;

import com.google.common.base.Throwables;
import com.google.common.io.Closeables;

/**
 * Helper class to parse an entire ValueArray data, and produce a structured ValueArray result.
 *
 * @author <a href="mailto:cliffc@0xdata.com"></a>
 */
@SuppressWarnings("fallthrough")
public final class ParseDataset extends Job {
  public static enum Compression { NONE, ZIP, GZIP }

  private final long _total;
  public final Key  _progress;

  private ParseDataset(Key dest, Key[] keys) {
    super("Parse", dest);
    //if( keys.length > 1 ) throw H2O.unimpl();
    Value dataset = DKV.get(keys[0]);
    _total = dataset.length() * Pass.values().length;
    _progress = Key.make(UUID.randomUUID().toString(), (byte) 0, Key.JOB);
    UKV.put(_progress, new Progress());
  }

  private ParseDataset(Key dest, Value [] dataset) {
    super("Parse", dest);
    long t = dataset[0].length();
    for(int i = 1; i < dataset.length; ++i)
      t += dataset[i].length();
    _total = t * Pass.values().length;
    _progress = Key.make(UUID.randomUUID().toString(), (byte) 0, Key.JOB);
    UKV.put(_progress, new Progress());
  }

  // Guess
  public static Compression guessCompressionMethod(Value dataset) {
    byte[] b = dataset.getFirstBytes(); // First chunk
    AutoBuffer ab = new AutoBuffer(b);

    // Look for ZIP magic
    if( b.length > ZipFile.LOCHDR && ab.get4(0) == ZipFile.LOCSIG )
      return Compression.ZIP;
    if( b.length > 2 && ab.get2(0) == GZIPInputStream.GZIP_MAGIC )
      return Compression.GZIP;
    return Compression.NONE;
  }

  // Parse the dataset (uncompressed, zippped) as a CSV-style thingy and
  // produce a structured dataset as a result.
  private static void parse(ParseDataset job, Key[] keys, CsvParser.Setup setup) {
    Value [] dataset = new Value[keys.length];
    for(int i = 0; i < keys.length; ++i)
      dataset[i] = DKV.get(keys[i]);

    if( dataset[0].isHex() )
      throw new IllegalArgumentException("This is a binary structured dataset; "
          + "parse() only works on text files.");

    try {
      // try if it is XLS file first
      try {
        parseUncompressed(job, dataset, CustomParser.Type.XLS, setup);
        return;
      } catch( Exception e ) {
        // pass
      }
      Compression compression = guessCompressionMethod(dataset[0]);
      if( compression == Compression.ZIP ) {
        try {
          parseUncompressed(job, dataset, CustomParser.Type.XLSX, setup);
          return;
        } catch( Exception e ) {
          // pass
        }
      }
      switch( compression ) {
      case NONE: parseUncompressed(job, dataset, CustomParser.Type.CSV, setup); break;
      case ZIP:  parseZipped(job, dataset, setup); break;
      case GZIP: parseGZipped(job, dataset, setup); break;
      default:   throw new Error("Unknown compression of dataset!");
      }
    } catch( java.io.EOFException eof ) {
      // Unexpected EOF?  Assume its a broken file, and toss the whole parse out
      UKV.put(job.dest(), new Fail(eof.getMessage()));
    } catch( Exception e ) {
      UKV.put(job.dest(), new Fail(e.getMessage()));
      throw Throwables.propagate(e);
    } finally {
      job.remove();
    }
  }

  public static void parse(Key dest, Key[] keys) {
    ParseDataset job = new ParseDataset(dest, keys);
    job.start();
    parse(job, keys, null);
  }

  public static Job forkParseDataset( final Key dest, final Key[] keys, final CsvParser.Setup setup ) {
    final ParseDataset job = new ParseDataset(dest, keys);
    job.start();
    H2O.submitTask(new H2OCountedCompleter() {
        @Override public void compute2() { parse(job, keys, setup); tryComplete(); }
      });
    return job;
  }

  public static class ParseException extends Exception{
    public ParseException(String msg){super(msg);}
  }
  public static void parseUncompressed(ParseDataset job, Value [] dataset, CustomParser.Type parserType, CsvParser.Setup setup) throws Exception{
    if(setup == null)
      setup = Inspect.csvGuessValue(dataset[0]);
    CsvParser.Setup headerSetup = setup;
    setup = new CsvParser.Setup(setup._separator,false,setup._data,setup._numlines,setup._bits);
    long nchunks = 0;
    // count the total number of chunks
    for(int i = 0; i < dataset.length; ++i){
      if(dataset[i].isArray()){
        ValueArray ary = dataset[i].get();
        nchunks += ary.chunks();
      } else
        nchunks += 1;
    }
    int chunks = (int)nchunks;
    assert chunks == nchunks;
    // parse the first value
    DParseTask phaseOne = DParseTask.createPassOne(dataset[0], job, parserType);
    int [] startchunks = new int[dataset.length+1];
    phaseOne.passOne(headerSetup);
    if( (phaseOne._error != null) && !phaseOne._error.isEmpty() ) {
      System.err.println(phaseOne._error);
      throw new Exception("The dataset format is not recognized/supported");
    }
    if(dataset.length > 1){     // parse the rest
      startchunks[1] = phaseOne._nrows.length;
      phaseOne._nrows = Arrays.copyOf(phaseOne._nrows, chunks);
      for(int i = 1; i < dataset.length; ++i){
        DParseTask tsk = DParseTask.createPassOne(dataset[i], job, CustomParser.Type.CSV);
        assert(!setup._header);
        tsk.passOne(setup);
        if( (tsk._error != null) && !tsk._error.isEmpty() ) {
          System.err.println(phaseOne._error);
          throw new Exception("The dataset format is not recognized/supported");
        }
        startchunks[i+1] = startchunks[i] + tsk._nrows.length;
        // modified reduction step, compute the compression scheme and the nrows array
        for (int j = 0; j < tsk._nrows.length; ++j)
          phaseOne._nrows[j+startchunks[i]] = tsk._nrows[j];
        assert tsk._ncolumns == phaseOne._ncolumns;
        for(int j = 0; j < tsk._ncolumns; ++j) {
          if(phaseOne._enums[j] != tsk._enums[j])
            phaseOne._enums[j].merge(tsk._enums[j]);
          if(tsk._min[j] < phaseOne._min[j])phaseOne._min[j] = tsk._min[j];
          if(tsk._max[j] > phaseOne._max[j])phaseOne._max[j] = tsk._max[j];
          if(tsk._scale[j] < phaseOne._scale[j])phaseOne._scale[j] = tsk._scale[j];
          if(tsk._colTypes[j] > phaseOne._colTypes[j])phaseOne._colTypes[j] = tsk._colTypes[j];
          phaseOne._mean[j] += tsk._mean[j];
          phaseOne._invalidValues[j] += tsk._invalidValues[j];
        }
      }
    }
    // now do the pass 2
    DParseTask phaseTwo = DParseTask.createPassTwo(phaseOne);
    phaseTwo.passTwo();
    if((phaseTwo._error != null) && !phaseTwo._error.isEmpty()) {
      System.err.println(phaseTwo._error);
      throw new Exception("The dataset format is not recognized/supported");
    }
    for(int i = 1; i < dataset.length; ++i){
      DParseTask tsk = new DParseTask(phaseTwo,dataset[i],startchunks[i]);
      tsk._skipFirstLine = false;
      tsk.passTwo();
      for(int j = 0; j < phaseTwo._ncolumns; ++j){
        phaseTwo._sigma[j] += tsk._sigma[j];
        phaseTwo._invalidValues[j] += tsk._invalidValues[j];
      }
      if( (tsk._error != null) && !tsk._error.isEmpty() ) {
        System.err.println(phaseTwo._error);
        throw new Exception("The dataset format is not recognized/supported");
      }
      UKV.remove(dataset[i]._key);
    }
    phaseTwo.normalizeSigma();
    phaseTwo.createValueArrayHeader();
    job.remove();
  }

  // Unpack zipped CSV-style structure and call method parseUncompressed(...)
  // The method exepct a dataset which contains a ZIP file encapsulating one file.
  public static void parseZipped(ParseDataset job, Value [] dataset, CsvParser.Setup setup) throws IOException {
    // Dataset contains zipped CSV
    ZipInputStream zis = null;
    Key keys [] = new Key[dataset.length];
    try{
      for(int i = 0; i < dataset.length; ++i){
        try {
          // Create Zip input stream and uncompress the data into a new key <ORIGINAL-KEY-NAME>_UNZIPPED
          zis = new ZipInputStream(dataset[i].openStream());
          // Get the *FIRST* entry
          ZipEntry ze = zis.getNextEntry();
          // There is at least one entry in zip file and it is not a directory.
          if( ze != null && !ze.isDirectory() ) {
            keys[i] = Key.make(new String(dataset[i]._key._kb) + "_UNZIPPED");
            ValueArray.readPut(keys[i], zis, job);
          }
        } finally {
          Closeables.closeQuietly(zis);
        }
        // else it is possible to dive into a directory but in this case I would
        // prefer to return error since the ZIP file has not expected format
      }
      for(int i = 0; i < keys.length; ++i)
        if( keys[i] == null )
          throw new Error("Cannot uncompressed ZIP-compressed dataset!");
      parse(job, keys, setup);
    } finally {
      for(int i = 0; i < keys.length; ++i)
        if(keys[i] != null)
          UKV.remove(keys[i]);
    }
  }

  public static void parseGZipped(ParseDataset job, Value [] dataset, CsvParser.Setup setup) throws IOException {
    GZIPInputStream gzis = null;
    Key [] keys = new Key [dataset.length];
    try{
      try {
        for(int i = 0; i < dataset.length; ++i){
          gzis = new GZIPInputStream(dataset[i].openStream());
          keys[i] = ValueArray.readPut(new String(dataset[i]._key._kb) + "_UNZIPPED", gzis);
        }
      } finally {
        Closeables.closeQuietly(gzis);
      }
      for(int i = 0; i < keys.length; ++i)
        if( keys[i] == null )
          throw new Error("Cannot uncompressed ZIP-compressed dataset!");
      parse(job, keys, setup);
    }finally {
      for(int i = 0; i < keys.length; ++i)
        if(keys[i] != null)UKV.remove(keys[i]);
    }
  }

  // True if the array is all NaNs
  static boolean allNaNs(double ds[]) {
    for( double d : ds )
      if( !Double.isNaN(d) )
        return false;
    return true;
  }

  // Progress (TODO count chunks in VA, unify with models?)

  static class Progress extends Iced {
    long _value;
  }

  @Override
  public float progress() {
    if(_total == 0) return 0;
    Progress progress = UKV.get(_progress);
    return (progress != null ? progress._value : 0) / (float) _total;
  }

  @Override public void remove() {
    DKV.remove(_progress);
    super.remove();
  }

  static final void onProgress(final Key chunk, final Key progress) {
    assert progress != null;
    new TAtomic<Progress>() {
      @Override public Progress atomic(Progress old) {
        if( old == null ) return null;
        Value val = DKV.get(chunk);
        if( val == null ) return null;
        old._value += val.length();
        return old;
      }
    }.fork(progress);
  }
}
