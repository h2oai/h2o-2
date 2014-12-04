package water.api;

import water.DKV;
import water.Futures;
import water.Request2;
import water.fvec.*;


/**
 * Created by tomasnykodym on 11/19/14.
 */
public class RemoveVec extends Request2 {
  static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  @API(help = "", required = true, filter = Default.class, gridable = false)
  Frame source;

  @API(help = "Input columns (Indexes start at 0)", filter=colsFilter.class, hide=false)
  public int[] cols;
  class colsFilter extends MultiVecSelect { public colsFilter() { super("source"); } }

  @Override
  protected Response serve() {
    Futures fs = new Futures();
    for(Vec v:source.remove(cols))
      v.remove(fs);
    DKV.put(source._key, source,fs);
    fs.blockForPending();
    return Inspect2.redirect(this, source._key.toString());
  }
}
