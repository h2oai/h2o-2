package water.api;

import hex.DGLM.GLMException;
import hex.ScoreTask;
import water.*;
import water.api.RequestArguments.H2OHexKey;
import water.api.RequestArguments.H2OKey;
import water.util.Log;
import water.util.RString;

import com.google.gson.JsonObject;

/**
 *
 * @author tomasnykodym
 *
 */
public class GeneratePredictionsPage extends Request {
  protected final H2OModelKey _modelKey = new H2OModelKey(new TypeaheadModelKeyRequest(),MODEL_KEY,true);
  // protected final H2OHexKey _dataKey = new H2OHexKey(KEY);
  protected final H2OHexKey _dataKey = new H2OHexKey(DATA_KEY);
  // protected final H2OKey _dest = new H2OKey(DEST_KEY, true);
  protected final H2OKey _dest = new H2OKey(DEST_KEY, Key.make("__Prediction_" + Key.make()));


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
      Model m = (Model)_modelKey.value();
      Key dest = _dest.value();
      return Inspect.redirect(res, ScoreTask.score(m, ary, dest));
    }catch(GLMException e){
      Log.err(e);
      return Response.error(e.getMessage());
    } catch (Throwable t) {
      Log.err(t);
      return Response.error(t.getMessage());
    }
  }
}
