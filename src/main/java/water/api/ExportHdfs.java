package water.api;

import org.apache.hadoop.fs.Path;

import water.*;
import water.persist.PersistHdfs;
import water.util.Log;

import dontweave.gson.JsonObject;

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
      byte[] data = null;
      Model model = getAsModel(value);
      if( model != null ) {
        // Add extension, used during import
        if( !path.endsWith(Extensions.JSON) ) path += Extensions.JSON;
        data = model.writeJSON(new AutoBuffer()).buf();
      }
      if( data != null ) PersistHdfs.store(new Path(path), data);
      else throw new UnsupportedOperationException("Only models can be exported");
    } catch( Throwable e ) {
      return Response.error(e);
    }
    JsonObject json = new JsonObject();
    Response r = Response.done(json);
    return r;
  }

  private static Model getAsModel(Value v) {
    if( v.type() == TypeMap.PRIM_B ) return null;
    Iced iced = v.get();
    if( iced instanceof Model ) return (Model) iced;
    return null;
  }
}
