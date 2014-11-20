package water.api;

import hex.la.DMatrix;
import water.Futures;
import water.Key;
import water.Request2;
import water.fvec.*;


/**
 * Created by tomasnykodym on 11/19/14.
 */
public class MatrixMultiply extends Request2 {
  static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  @API(help = "", required = true, filter = Default.class, gridable = false)
  Frame x;
  @API(help = "", required = true, filter = Default.class, gridable = false)
  Frame y;

  @API(help="",required = true, filter = Default.class, gridable = false)
  boolean skip_first_col = true;
  @Request.API(help = "Key for rebalanced frame", filter = Request.Default.class, json=true)
  public Key after = (x != null && y != null) ? Key.make(x._key + "%*%" + y._key) : null;
  @Override
  protected Response serve() {
    Key jk = Key.make(after + "_job");
    Futures fs = new Futures();
    if(skip_first_col && x.numCols() == (y.numRows()+1)) {
      // hack to remove columns added during smvlight conversion
      x.remove(0).remove(fs);
      y.remove(0).remove(fs);
    }
    new DMatrix.MatrixMulJob(jk,after,x,y).fork();
    return Response.redirect(this, "/2/Inspector", "src_key",after);
  }
}
