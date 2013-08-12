package water.api;

import water.Key;
import water.ValueArray;

import com.google.gson.JsonObject;

public class SetColumnNames extends Request {
  protected final H2OHexKey _tgtKey = new H2OHexKey("target");
  protected final H2OHexKey _srcKey = new H2OHexKey("source");
  @Override protected Response serve() {
    ValueArray tgt = _tgtKey.value();
    tgt.setColumnNames(_srcKey.value().colNames());
    return Inspect.redirect(new JsonObject(), tgt._key);
  }
  public static String link(Key k, String s){
    return "<a href='SetColumnNames.query?target="+k+"'>" + s + "</a>";
  }
}
