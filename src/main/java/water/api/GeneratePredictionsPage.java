package water.api;

import hex.DGLM.GLMException;
import hex.ScoreTask;
import water.*;
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
  protected final H2OHexKey _dataKey = new H2OHexKey(KEY);


  public static String link(Key k, String content) {
    RString rs = new RString("<a href='GeneratePredictionsPage.query?model_key=%key'>%content</a>");
    rs.replace("key", k.toString());
    rs.replace("content", content);
    return rs.toString();
  }


  @Override protected Response serve() {
    try {
      JsonObject res = new JsonObject();
      ValueArray ary = _dataKey.value();
      Model m = (Model)_modelKey.value();
      return Inspect.redirect(res, ScoreTask.score(m, ary,Key.make("prediction.hex")));
    }catch(GLMException e){
      Log.err(e);
      return Response.error(e.getMessage());
    } catch (Throwable t) {
      Log.err(t);
      return Response.error(t.getMessage());
    }
  }
}
