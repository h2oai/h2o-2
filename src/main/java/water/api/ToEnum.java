//package water.api;
//
//import com.google.gson.JsonObject;
//import water.Value;
//import water.util.Log;
//
//public class ToEnum extends Request {
//  protected final H2OExistingKey _key       = new H2OExistingKey(KEY);
//  protected final Int            _col_index = new Int(COL_INDEX, -1);

////  @Override protected void registered(RequestServer.API_VERSION version) { super.registered(version); }
//
//  @Override
//  protected Response serve() {
//    try {
//      String colname =  _col_index.toString();
//      Log.info("Factorizing column " + colname);
//      Value v = _key.value();
//      String key = v._key.toString();
//
//
//
//    } catch( Throwable e ) {
//      return Response.error(e);
//    }
//
//    JsonObject response = new JsonObject();
//    return Response.done(response);
//  }
//}
