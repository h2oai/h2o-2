package water.api;

import hex.ScoreTask;
import water.*;
import water.util.RString;

import com.google.gson.JsonObject;

/**
 *
 * @author tomasnykodym
 *
 */
public class GeneratePredictionsPage extends Request {
  protected final H2OModelKey _modelKey = new H2OModelKey(new TypeaheadModelKeyRequest(),MODEL_KEY,true);
  protected final H2OHexKey   _dataKey  = new H2OHexKey(DATA_KEY);
  protected final H2OKey      _dest     = new H2OKey(DEST_KEY, null); // destination key is not compulsory, if not specified random name is created.

  public static String link(Key k, String content) {
    // RString rs = new RString("<a href='GeneratePredictionsPage.query?model_key=%key'>%content</a>");
    RString rs = new RString("<a href='GeneratePredictionsPage.query?%key_param=%$key'>%content</a>");
    rs.replace("key_param", MODEL_KEY);
    rs.replace("key", k.toString());
    rs.replace("content", content);
    return rs.toString();
  }

  @Override protected Response serve() {
    try {
      JsonObject res = new JsonObject();
      ValueArray ary = _dataKey.value();
      OldModel m = (OldModel)_modelKey.value();
      Key dest = _dest.value()!=null ? _dest.value() : Key.make("__Prediction_" + Key.make());
      return Inspect.redirect(res, ScoreTask.score(m, ary, dest));
    } catch( Throwable t ) {
      return Response.error(t);
    }
  }
}
