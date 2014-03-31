package hex;

import water.Key;
import water.Request2;
import water.UKV;
import water.api.DocGen;
import water.api.Request;
import water.api.RequestBuilders;
import water.fvec.Frame;
import water.fvec.Vec;
import water.util.MRUtils;

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

  @Override public RequestBuilders.Response serve() {
    Vec va = null, vp;
    // Input handling
    if( before==null )
      throw new IllegalArgumentException("Missing frame to rebalance!");
    if( after==null )
      throw new IllegalArgumentException("Missing name (key for the rebalanced Frame!");

    try {
      before.read_lock(null);
      Frame balanced = MRUtils.shuffleAndBalance(before, seed, false, false);
      before.unlock(null);

      Frame balanced2 = new Frame(Key.make(after), balanced.names(), balanced.vecs());
      balanced2.delete_and_lock(null);
      balanced2.unlock(null);

      return RequestBuilders.Response.done(this);
    } catch( Throwable t ) {
      return RequestBuilders.Response.error(t);
    } finally {       // Delete adaptation vectors
      if (va!=null) UKV.remove(va._key);
    }
  }

  @Override public boolean toHTML( StringBuilder sb ) {
    if (UKV.get(Key.make(after))==null) {
      return false;
    }
    DocGen.HTML.section(sb, "Rebalancing done.");
    return true;
  }

}
