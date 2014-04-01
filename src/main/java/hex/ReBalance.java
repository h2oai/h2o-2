package hex;

import water.H2O;
import water.Key;
import water.Request2;
import water.UKV;
import water.api.DocGen;
import water.api.Request;
import water.api.RequestBuilders;
import water.fvec.Frame;
import water.fvec.RebalanceDataSet;
import water.util.RString;

import java.util.Random;

/**
 * Rebalance a Frame
 */
public class ReBalance extends Request2 {
  static final int API_WEAVER=1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  @Request.API(help = "Frame to rebalance", required = true, filter = Request.Default.class, json=true)
  public Frame before;

  @Request.API(help = "Random number seed", filter = Request.Default.class, json=true)
  public long seed = new Random().nextLong();

  @Request.API(help = "Key for rebalanced frame", filter = Request.Default.class, json=true)
  public String after = before != null ? before._key.toString() + ".balanced" : null;

  @Request.API(help = "Number of chunks", filter = Request.Default.class, json=true)
  public int chunks = H2O.CLOUD.size() * H2O.NUMCPUS * 4;

  @Override public RequestBuilders.Response serve() {
    if( before==null ) throw new IllegalArgumentException("Missing frame to rebalance!");
    try {
      if (chunks > before.numRows()) throw new IllegalArgumentException("Cannot create more than " + before.numRows() + " chunks.");
      if( after==null ) after = before._key.toString() + ".balanced";
      final Key newKey = Key.make(after);
      RebalanceDataSet rb = new RebalanceDataSet(before, newKey, chunks);
      H2O.submitTask(rb);
      rb.join();
      return RequestBuilders.Response.done(this);
    } catch( Throwable t ) {
      return RequestBuilders.Response.error(t);
    }
  }

  @Override public boolean toHTML( StringBuilder sb ) {
    if (UKV.get(Key.make(after))==null) {
      return false;
    }
    RString aft = new RString("<a href='Inspect2.html?src_key=%$key'>%key</a>");
    aft.replace("key", after);
    DocGen.HTML.section(sb, "Rebalancing done. Frame '" + aft.toString()
            + "' now has " + ((Frame)UKV.get(Key.make(after))).anyVec().nChunks()
            + " chunks (before: " + before.anyVec().nChunks() + ").");
    return true;
  }

}
