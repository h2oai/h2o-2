package water.api;

import water.*;

import com.google.gson.JsonObject;

public class SetColumnNames extends Request {
  protected final H2OHexKey _tgtKey = new H2OHexKey("target");
  // Pick either another Key to dup columns, or pass in a String
  protected final HeaderKey _srcKey = new HeaderKey();

  protected final HeaderStr _srcStr = new HeaderStr();

  private class HeaderKey extends H2OHexKey {
    public HeaderKey(){super("copy_from",(Key)null); addPrerequisite(_tgtKey); }
    @Override protected ValueArray parse(String input) throws IllegalArgumentException {
      ValueArray res = super.parse(input);
      if(res.numCols() != _tgtKey.value().numCols())
        throw new IllegalArgumentException("number of columns don't match!");
      return res;
    }
  }

  private class HeaderStr extends Str {
    public HeaderStr(){super("comma_separated_list",null); addPrerequisite(_tgtKey); }
    @Override protected String parse(String input) throws IllegalArgumentException {
      String ss[] = input.split(",");
      if( ss.length != _tgtKey.value().numCols())
        throw new IllegalArgumentException("number of columns don't match!");
      return input;
    }
  }

  @Override protected Response serve() {
    ValueArray tgt = _tgtKey.value();
    String[] names;
    if( _srcKey.value() != null ) names = _srcKey.value().colNames();
    else if( _srcStr.value() != null ) names = _srcStr.value().split(",");
    else throw new IllegalArgumentException("No column names given");
    tgt.setColumnNames(names);
    // Must write in the new header.  Must use DKV instead of UKV, because do
    // not want to delete the underlying data.
    Futures fs = new Futures();
    DKV.put(tgt._key,tgt,fs);
    fs.blockForPending();
    return Inspect.redirect(new JsonObject(), tgt._key);
  }
  public static String link(Key k, String s){
    return "<a href='SetColumnNames.query?target="+k+"'>" + s + "</a>";
  }
}
