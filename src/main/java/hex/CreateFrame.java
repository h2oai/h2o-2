package hex;

import water.*;
import water.api.DocGen;
import water.fvec.*;
import water.util.FrameUtils;
import water.util.Log;
import water.util.RString;

import java.util.Random;

/**
 * Create a Frame from scratch
 * If randomize = true, then the frame is filled with Random values.
 *
 */
public class CreateFrame extends Request2 {
  static final int API_WEAVER=1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  @API(help = "Name (Key) of frame to be created", required = true, filter = Default.class, json=true)
  public String key;

  @API(help = "Number of rows", required = true, filter = Default.class, lmin = 1, json=true)
  public long rows = 10000;

  @API(help = "Number of data columns (in addition to the first response column)", required = true, filter = Default.class, lmin = 1, json=true)
  public int cols = 10;

  @API(help = "Random number seed", filter = Default.class, json=true)
  public long seed = new Random().nextLong();

  @API(help = "Whether frame should be randomized", filter = Default.class, json=true)
  public boolean randomize = true;

  @API(help = "Constant value (for randomize=false)", filter = Default.class, json=true)
  public long value = 0;

  @API(help = "Range for real variables (-range ... range)", filter = Default.class, json=true)
  public long real_range = 100;

  @API(help = "Fraction of categorical columns (for randomize=true)", filter = Default.class, dmin = 0, dmax = 1, json=true)
  public double categorical_fraction = 0.2;

  @API(help = "Factor levels for categorical variables", filter = Default.class, lmin = 2, json=true)
  public int factors = 100;

  @API(help = "Fraction of integer columns (for randomize=true)", filter = Default.class, dmin = 0, dmax = 1, json=true)
  public double integer_fraction = 0.2;

  @API(help = "Range for integer variables (-range ... range)", filter = Default.class, json=true)
  public long integer_range = 100;

  @API(help = "Fraction of binary columns (for randomize=true)", filter = Default.class, dmin = 0, dmax = 1, json=true)
  public double binary_fraction = 0.1;

  @API(help = "Fraction of 1's in binary columns", filter = Default.class, dmin = 0, dmax = 1, json=true)
  public double binary_ones_fraction = 0.02;

  @API(help = "Fraction of missing values", filter = Default.class, dmin = 0, dmax = 1, json=true)
  public double missing_fraction = 0.01;

  @API(help = "Number of factor levels of the first column (1=real, 2=binomial, N=multinomial)", filter = Default.class, lmin = 1, json=true)
  public int response_factors = 2;

  public boolean positive_response; // only for response_factors=1

  @API(help = "Whether an additional response column should be generated", filter = Default.class, json=true)
  public boolean has_response = false;

  @Override public Response serve() {
    try {
      if (integer_fraction + binary_fraction + categorical_fraction > 1) throw new IllegalArgumentException("Integer, binary and categorical fractions must add up to <= 1.");
      if (Math.abs(missing_fraction) > 1) throw new IllegalArgumentException("Missing fraction must be between 0 and 1.");
      if (Math.abs(integer_fraction) > 1) throw new IllegalArgumentException("Integer fraction must be between 0 and 1.");
      if (Math.abs(binary_fraction) > 1) throw new IllegalArgumentException("Binary fraction must be between 0 and 1.");
      if (Math.abs(binary_ones_fraction) > 1) throw new IllegalArgumentException("Binary ones fraction must be between 0 and 1.");
      if (Math.abs(categorical_fraction) > 1) throw new IllegalArgumentException("Categorical fraction must be between 0 and 1.");
      if (categorical_fraction > 0 && factors <= 1) throw new IllegalArgumentException("Factors must be larger than 2 for categorical data.");
      if (response_factors < 1) throw new IllegalArgumentException("Response factors must be either 1 (real-valued response), or >=2 (factor levels).");
      if (cols <= 0 || rows <= 0) throw new IllegalArgumentException("Must have number of rows > 0 and columns > 1.");
      if (key.length() == 0) throw new IllegalArgumentException("Output key must be provided.");

      if (!randomize) {
        if (integer_fraction != 0 || categorical_fraction != 0)
          throw new IllegalArgumentException("Cannot have integer or categorical fractions > 0 unless randomize=true.");
      } else {
        if (value != 0)
          throw new IllegalArgumentException("Cannot set data to a constant value if randomize=true.");
      }

      final FrameCreator fct = new FrameCreator(this);
      H2O.submitTask(fct);
      fct.join();

      Log.info("Created frame '" + key + "'.");
      Log.info(FrameUtils.chunkSummary((Frame)UKV.get(Key.make(key))).toString());
      return Response.done(this);
    } catch( Throwable t ) {
      return Response.error(t);
    }
  }

  @Override public boolean toHTML( StringBuilder sb ) {
    Frame fr = UKV.get(Key.make(key));
    if (fr==null) {
      return false;
    }
    RString aft = new RString("<a href='Inspect2.html?src_key=%$key'>%key</a>");
    aft.replace("key", key);
    DocGen.HTML.section(sb, "Frame creation done.<br/>Frame '" + aft.toString()
            + "' now has " + fr.numRows() + " rows and " + (fr.numCols()-1)
            + " data columns, as well as a " + (response_factors == 1 ? "real-valued" : (response_factors == 2 ? "binomial" : "multi-nomial"))
            + " response variable as the first column.<br/>Number of chunks: " + fr.anyVec().nChunks() + ".");
    DocGen.HTML.paragraph(sb, FrameUtils.chunkSummary((Frame)UKV.get(Key.make(key))).toString().replace("\n","<br/>"));
    return true;
  }

}
