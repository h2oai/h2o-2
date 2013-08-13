package water.api;

import water.Key;
import water.ValueArray;

import com.google.gson.JsonObject;

public class SetColumnNames extends Request {


  protected final H2OHexKey _tgtKey = new H2OHexKey("target");

  private class HeaderKey extends H2OHexKey {
    public HeaderKey(){super("source");}
    @Override protected ValueArray parse(String input) throws IllegalArgumentException {
      ValueArray res = super.parse(input);
      if(res.numCols() != _tgtKey.value().numCols())
        throw new IllegalArgumentException("number of columns don't match!");
      return res;
    }
  }
  protected final HeaderKey _srcKey = new HeaderKey();

  @Override protected Response serve() {
    ValueArray tgt = _tgtKey.value();
    tgt.setColumnNames(_srcKey.value().colNames());
    return Inspect.redirect(new JsonObject(), tgt._key);
  }
  public static String link(Key k, String s){
    return "<a href='SetColumnNames.query?target="+k+"'>" + s + "</a>";
  }
}
