package water.api;

import water.Value;
import water.hdfs.PersistHdfs;
import water.util.Log;

import com.google.gson.JsonObject;

public class ExportHdfs extends Request {
  protected final H2OExistingKey _source = new H2OExistingKey(SOURCE_KEY);
  protected final Str _path = new Str(PATH);

  public ExportHdfs() {
    _requestHelp = "Exports JSON to the given HDFS path. The Web server node "
        + "must have write permission to the HDFS path.";
    _path._requestHelp = "HDFS path to export to.";
  }

  @Override protected Response serve() {
    Value value = _source.value();
    String path = _path.value();
    try {
      if( value == null ) throw new IllegalArgumentException("Unknown key: " + _source.record()._originalValue);
      PersistHdfs.store(path, value);
    } catch( Exception e ) {
      Log.err(e);
      return Response.error(e.getMessage());
    }
    JsonObject json = new JsonObject();
    Response r = Response.done(json);
    return r;
  }
}
