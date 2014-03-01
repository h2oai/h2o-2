package water.api;

import com.google.gson.JsonObject;
import water.*;
import water.util.Log;
import hex.TypeChange;

public class ToEnum extends Request {
  protected final H2OHexKey      _key       = new H2OHexKey(KEY);
  protected final Int            _col_index = new Int(COL_INDEX, -1);
  protected final Bool           _to_enum   = new Bool(TO_ENUM,true,"");

////  @Override protected void registered(RequestServer.API_VERSION version) { super.registered(version); }
//
  @Override
  protected Response serve() {
    try {
      int column_index = _col_index.value();
      boolean to_enum = _to_enum.value();
      String colname =  _col_index.toString();
      Log.info("Factorizing column " + colname);
//      Value v = _key.value();
//      String key = v._key.toString();
      new TypeChange(column_index, to_enum, _key.value()._key).invoke();

    } catch( Throwable e ) {
      return Response.error(e);
    }

    JsonObject response = new JsonObject();
    return Response.done(response);
  }
}
