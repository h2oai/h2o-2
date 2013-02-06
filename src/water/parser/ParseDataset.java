package water.parser;
import java.io.IOException;
import java.util.zip.*;

import jsr166y.RecursiveAction;
import water.*;

import com.google.common.base.Throwables;
import com.google.common.io.Closeables;

/**
 * Helper class to parse an entire ValueArray data, and produce a structured
 * ValueArray result.
 *
 * @author <a href="mailto:cliffc@0xdata.com"></a>
 */
@SuppressWarnings("fallthrough")
public final class ParseDataset {
  static enum Compression { NONE, ZIP, GZIP }

  // Guess
  private static Compression guessCompressionMethod(Value dataset) {
    byte[] b = dataset.getFirstBytes(); // First chunk
    AutoBuffer ab = new AutoBuffer(b);

    // Look for ZIP magic
    if (b.length > ZipFile.LOCHDR && ab.get4(0) == ZipFile.LOCSIG)
      return Compression.ZIP;
    if (b.length > 2 && ab.get2(0) == GZIPInputStream.GZIP_MAGIC)
      return Compression.GZIP;
    return Compression.NONE;
  }

  // Parse the dataset (uncompressed, zippped) as a CSV-style thingy and produce a structured dataset as a
  // result.
  private static void parseImpl( Key result, Value dataset ) {
    if( dataset.isHex() )
      throw new IllegalArgumentException("This is a binary structured dataset; "
          + "parse() only works on text files.");
    try {
      // try if it is XLS file first
      try {
        parseUncompressed(result,dataset,CustomParser.Type.XLS);
        return;
      } catch (Exception e) {
        // pass
      }
      Compression compression = guessCompressionMethod(dataset);
      if (compression == Compression.ZIP) {
        try {
          parseUncompressed(result,dataset,CustomParser.Type.XLSX);
          return;
        } catch (Exception e) {
          // pass
        }
      }
      switch (compression) {
        case NONE: parseUncompressed(result, dataset,CustomParser.Type.CSV); break;
        case ZIP : parseZipped (result, dataset); break;
        case GZIP: parseGZipped (result, dataset); break;
        default : throw new Error("Uknown compression of dataset!");
      }
    } catch( Exception e ) {
      ParseStatus.error(result, e.getMessage());
      throw Throwables.propagate(e);
    }
  }
  public static void parse( Key result, Value dataset ) {
    ParseStatus.initialize(result, dataset.length());
    parseImpl(result, dataset);
  }

  public static void forkParseDataset( final Key result, final Value dataset ) {
    ParseStatus.initialize(result, dataset.length());
    H2O.FJP_NORM.submit(new RecursiveAction() {
      @Override
      protected void compute() {
        parseImpl(result, dataset);
      }
    });
  }

 // Parse the uncompressed dataset as a CSV-style structure and produce a structured dataset
 // result. This does a distributed parallel parse.
  public static void parseUncompressed( Key result, Value dataset, CustomParser.Type parserType ) throws Exception {
    DParseTask phaseOne = DParseTask.createPassOne(dataset, result, parserType);
    phaseOne.passOne();
    if ((phaseOne._error != null) && !phaseOne._error.isEmpty()) {
      System.err.println(phaseOne._error);
      throw new Exception("The dataset format is not recognized/supported");
    }
    DParseTask phaseTwo = DParseTask.createPassTwo(phaseOne);
    phaseTwo.passTwo();
    if ((phaseTwo._error != null) && !phaseTwo._error.isEmpty()) {
      System.err.println(phaseTwo._error);
      UKV.remove(result); // delete bad stuff if any
      throw new Exception("The dataset format is not recognized/supported");
    }
  }

  // Unpack zipped CSV-style structure and call method parseUncompressed(...)
  // The method exepct a dataset which contains a ZIP file encapsulating one file.
  public static void parseZipped( Key result, Value dataset ) throws IOException {
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
        key = ValueArray.readPut(new String(dataset._key._kb) + "_UNZIPPED", zis);
      }
      // else it is possible to dive into a directory but in this case I would
      // prefer to return error since the ZIP file has not expected format
    } finally { Closeables.closeQuietly(zis); }
    if( key == null ) throw new Error("Cannot uncompressed ZIP-compressed dataset!");
    Value uncompressedDataset = DKV.get(key);
    parse(result, uncompressedDataset);
    UKV.remove(key);
  }

  public static void parseGZipped( Key result, Value dataset ) throws IOException {
    GZIPInputStream gzis = null;
    Key key = null;
    try {
      gzis = new GZIPInputStream(dataset.openStream());
      key = ValueArray.readPut(new String(dataset._key._kb) + "_UNZIPPED", gzis);
    } finally { Closeables.closeQuietly(gzis); }

    if( key == null ) throw new Error("Cannot uncompressed GZIP-compressed dataset!");
    Value uncompressedDataset = DKV.get(key);
    parse(result, uncompressedDataset);
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
