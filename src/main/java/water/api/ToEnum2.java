package water.api;

import water.Request2;
import water.fvec.Frame;
import water.fvec.Vec;
import water.util.Log;

public class ToEnum2 extends Request2 {
  @Override public RequestServer.API_VERSION[] supportedVersions() { return SUPPORTS_ONLY_V2; }
  @Override protected void registered(RequestServer.API_VERSION version) { super.registered(version); }

  @API(help="An existing H2O Frame key.", required=true, filter=Default.class)
  public Frame src_key;

  @API(help="The column index to perform the factorization on.", required = true, filter=Default.class)
  public int column_index;

  @Override
  protected Response serve() {
    try {
      if (column_index <= 0) throw new IllegalArgumentException("Column index is 1 based. Please supply a valid column index in the range [1,"+ src_key.numCols()+"]");
      Log.info("Factorizing column " + column_index);
      Vec nv =  src_key.vecs()[column_index - 1].toEnum();
      src_key.replace(column_index - 1, nv);
    } catch( Throwable e ) {
      return Response.error(e);
    }
    return Inspect2.redirect(this, src_key._key.toString());
  }
}
