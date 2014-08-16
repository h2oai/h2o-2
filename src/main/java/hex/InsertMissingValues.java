package hex;

import water.Key;
import water.Request2;
import water.UKV;
import water.api.DocGen;
import water.fvec.Frame;
import water.fvec.FrameCreator;
import water.util.Log;
import water.util.RString;

import java.util.Random;

/**
 * Insert missing values into an existing frame (overwrite in-place).
 * Useful to test algorithm's ability to cope with missing values.
 */
public class InsertMissingValues extends Request2 {
  static final int API_WEAVER=1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  @API(help = "Key of frame to add missing values to", required = true, filter = Default.class, json=true)
  public Key key;

  @API(help = "Random number seed", filter = Default.class, json=true)
  public long seed = new Random().nextLong();

  @API(help = "Fraction of missing values", filter = Default.class, dmin = 1e-10, dmax = 1, json=true)
  public double missing_fraction = 0.01;

  @Override public Response serve() {
    try {
      if (missing_fraction == 0) throw new IllegalArgumentException("Missing fraction must be larger than 0.");
      if (Math.abs(missing_fraction) > 1) throw new IllegalArgumentException("Missing fraction must be between 0 and 1.");
      if (key == null) throw new IllegalArgumentException("A valid key must be provided.");

      Frame fr = UKV.get(key);
      if (fr == null) throw new IllegalArgumentException("Frame " + key + " not found.");

      new FrameCreator.MissingInserter(seed, missing_fraction).doAll(fr);

      Log.info("Modified frame '" + key + "' : added " + missing_fraction * 100 + "% missing values.");
      return Response.done(this);
    } catch( Throwable t ) {
      return Response.error(t);
    }
  }

  @Override public boolean toHTML( StringBuilder sb ) {
    Frame fr = UKV.get(key);
    if (fr==null) {
      return false;
    }
    RString aft = new RString("<a href='Inspect2.html?src_key=%$key'>%key</a>");
    aft.replace("key", key);
    DocGen.HTML.section(sb, "Inserted missing values into frame '" + aft.toString() + " done.");
    return true;
  }

}
