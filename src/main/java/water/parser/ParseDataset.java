package water.parser;

import com.google.common.base.Throwables;
import com.google.common.io.Closeables;
import java.io.IOException;
import java.util.zip.*;
import jsr166y.CountedCompleter;
import water.*;
import water.Jobs.Fail;
import water.Jobs.Job;
import water.Jobs.Progress;
import water.parser.DParseTask.Pass;

/**
 * Helper class to parse an entire ValueArray data, and produce a structured
 * ValueArray result.
 *
 * @author <a href="mailto:cliffc@0xdata.com"></a>
 */
@SuppressWarnings("fallthrough")
public final class ParseDataset {
  public static enum Compression { NONE, ZIP, GZIP }

  // Guess
  public static Compression guessCompressionMethod(Value dataset) {
    byte[] b = dataset.getFirstBytes(); // First chunk
    AutoBuffer ab = new AutoBuffer(b);

    // Look for ZIP magic
    if (b.length > ZipFile.LOCHDR && ab.get4(0) == ZipFile.LOCSIG)
      return Compression.ZIP;
    if (b.length > 2 && ab.get2(0) == GZIPInputStream.GZIP_MAGIC)
      return Compression.GZIP;
    return Compression.NONE;
  }

  // Parse the dataset (uncompressed, zippped) as a CSV-style thingy and
  // produce a structured dataset as a result.
  private static void parse( Job job, Value dataset, CsvParser.Setup setup ) {
    if( dataset.isHex() )
      throw new IllegalArgumentException("This is a binary structured dataset; "
          + "parse() only works on text files.");
    try {
      // try if it is XLS file first
      try {
        parseUncompressed(job,dataset,CustomParser.Type.XLS, setup);
        return;
      } catch (Exception e) {
        // pass
      }
      Compression compression = guessCompressionMethod(dataset);
      if (compression == Compression.ZIP) {
        try {
          parseUncompressed(job,dataset,CustomParser.Type.XLSX, setup);
          return;
        } catch (Exception e) {
          // pass
        }
      }
      switch (compression) {
      case NONE: parseUncompressed(job, dataset,CustomParser.Type.CSV, setup); break;
      case ZIP : parseZipped (job, dataset, setup); break;
      case GZIP: parseGZipped(job, dataset, setup); break;
      default : throw new Error("Unknown compression of dataset!");
      }
    } catch( Exception e ) {
      UKV.put(job._dest, new Fail(e.getMessage()));
      throw Throwables.propagate(e);
    } finally {
      Jobs.remove(job._key);
    }
  }

  public static void parse( Key dest, Value dataset ) {
    Job job = Jobs.start("Parse", dest);
    parse(job, dataset, null);
  }

  public static Job forkParseDataset( final Key dest, final Value dataset, final CsvParser.Setup setup ) {
    final Job job = Jobs.start("Parse", dest);
    H2O.FJP_NORM.submit(new CountedCompleter() {
        @Override public void compute() { parse(job, dataset, setup); tryComplete(); }
        public boolean onExceptionalCompletion( Throwable ex, CountedCompleter caller ) {
          ex.printStackTrace();
          return true;
        }
      });
    return job;
  }

  // Parse the uncompressed dataset as a CSV-style structure and produce a structured dataset
  // result. This does a distributed parallel parse.
  public static void parseUncompressed( Job job, Value dataset, CustomParser.Type parserType, CsvParser.Setup setup ) throws Exception {
    UKV.put(job._progress, new Progress(0, dataset.length() * Pass.values().length));
    DParseTask phaseOne = DParseTask.createPassOne(dataset, job, parserType);
    phaseOne.passOne(setup);
    if ((phaseOne._error != null) && !phaseOne._error.isEmpty()) {
      System.err.println(phaseOne._error);
      throw new Exception("The dataset format is not recognized/supported");
    }
    DParseTask phaseTwo = DParseTask.createPassTwo(phaseOne);
    phaseTwo.passTwo();
    if ((phaseTwo._error != null) && !phaseTwo._error.isEmpty()) {
      System.err.println(phaseTwo._error);
      throw new Exception("The dataset format is not recognized/supported");
    }
  }

  // Unpack zipped CSV-style structure and call method parseUncompressed(...)
  // The method exepct a dataset which contains a ZIP file encapsulating one file.
  public static void parseZipped( Job job, Value dataset, CsvParser.Setup setup ) throws IOException {
    // Dataset contains zipped CSV
    ZipInputStream zis = null;
    Key key = null;
    try {
      // Create Zip input stream and uncompress the data into a new key <ORIGINAL-KEY-NAME>_UNZIPPED
      zis = new ZipInputStream(dataset.openStream());
      // Get the *FIRST* entry
      ZipEntry ze = zis.getNextEntry();
      // There is at least one entry in zip file and it is not a directory.
      if (ze != null && !ze.isDirectory()) {
        key = Key.make(new String(dataset._key._kb) + "_UNZIPPED");
        ValueArray.readPut(key, zis, job);
      }
      // else it is possible to dive into a directory but in this case I would
      // prefer to return error since the ZIP file has not expected format
    } finally { Closeables.closeQuietly(zis); }
    if( key == null ) throw new Error("Cannot uncompressed ZIP-compressed dataset!");
    Value uncompressedDataset = DKV.get(key);
    parse(job, uncompressedDataset,setup);
    UKV.remove(key);
  }

  public static void parseGZipped( Job job, Value dataset, CsvParser.Setup setup ) throws IOException {
    GZIPInputStream gzis = null;
    Key key = null;
    try {
      gzis = new GZIPInputStream(dataset.openStream());
      key = ValueArray.readPut(new String(dataset._key._kb) + "_UNZIPPED", gzis);
    } finally { Closeables.closeQuietly(gzis); }

    if( key == null ) throw new Error("Cannot uncompressed GZIP-compressed dataset!");
    Value uncompressedDataset = DKV.get(key);
    parse(job, uncompressedDataset,setup);
    UKV.remove(key);
  }

  // True if the array is all NaNs
  static boolean allNaNs( double ds[] ) {
    for( double d : ds )
      if( !Double.isNaN(d) )
        return false;
    return true;
  }
}
