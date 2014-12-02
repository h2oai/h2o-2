package water.api;

import hex.OrderTsk;
import water.*;
import water.H2O.H2OCallback;
import water.H2O.H2OCountedCompleter;
import water.H2O.H2OEmptyCompleter;
import water.fvec.*;
import water.util.Utils;


/**
 * Created by tomasnykodym on 11/19/14.
 *
 * Get indexes and values of n smallest (largest if rev=true) elements in each of the vecs.
 * Store result as a frame.
 */
public class Order extends Request2 {
  static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  @API(help = "", required = true, filter = Default.class, gridable = false)
  Frame source;

  @Request.API(help = "Key for frame conatining the result", filter = Request.Default.class, json=true)
  public Key destination_key = source != null ? Key.make(source._key.toString() + ".order") : null;

  @API(help = "Input columns (Indexes start at 0)", filter=colsFilter.class, hide=false)
  public int[] cols;
  class colsFilter extends MultiVecSelect { public colsFilter() { super("source"); } }

  @API(help="",filter=Default.class, required = true, gridable = false)
  public int n = 5;

  @API(help="",filter=Default.class, required = true)
  public boolean rev = true;

  @API(help="",filter=Default.class, required=false)
  boolean add_one = true;

  @Override
  protected Response serve() {
    if(n > 10000) // global order not supported
      throw H2O.unimpl();
    long [] espc = new long[]{0,n};
    final Vec [] dst = new Vec(Vec.newKey(),espc).makeZeros(cols.length);
    H2OEmptyCompleter cmp = new H2OEmptyCompleter();
    cmp.setPendingCount(cols.length-1);
    final int addOne = (add_one?1:0);
    for(int i = 0; i < cols.length; ++i) {
      final int fi = i;
      new OrderTsk(new H2OCallback<OrderTsk>(cmp) {
        @Override
        public void callback(OrderTsk ot) {
          Vec.Writer w = dst[fi].open();
          for (int j = 0; j < ot._ids.length; ++j)
            w.set(j, ot._ids[j] + addOne);
          w.close();
        }
      }, n, rev).asyncExec(source.vec(cols[i]));
    }
    cmp.join();
    Futures fs = new Futures();
    if(destination_key == null)
      destination_key = Key.make(source._key.toString() + ".order");
    DKV.put(destination_key, new Frame(destination_key, Utils.select(source.names(), cols),dst),fs);
    fs.blockForPending();
    return Inspect2.redirect(this, destination_key.toString());
  }
}
