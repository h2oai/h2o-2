package water.api;

import hex.rf.ConfusionTask;
import hex.rf.RFModel;
import water.Key;
import water.UKV;
import water.util.RString;

import com.google.gson.JsonObject;

/**
 * Page for random forest scoring.
 *
 * Redirect directly to RF view.
 */
public class RFScore extends Request {

  protected final H2OHexKey          _dataKey  = new H2OHexKey(DATA_KEY);
  protected final RFModelKey         _modelKey = new RFModelKey(MODEL_KEY);
  protected final HexKeyClassCol     _classCol = new HexKeyClassCol(CLASS, _dataKey);
  protected final Int                _numTrees = new NTree(NUM_TREES, _modelKey);
  protected final H2OCategoryWeights _weights  = new H2OCategoryWeights(WEIGHTS, _modelKey, _dataKey, _classCol, 1);

  RFScore() {
    _numTrees._readOnly = true;
  }

  public static String link(Key k, String content) {
    return link(DATA_KEY, k, content);
  }

  /** Generates a link to this page - param is {DATA_KEY, MODEL_KEY} with respect to specified key. */
  public static String link(String keyParam, Key k, String content) {
    RString rs = new RString("<a href='RFScore.query?%key_param=%$key'>%content</a>");
    rs.replace("key_param", keyParam);
    rs.replace("key", k.toString());
    rs.replace("content", content);
    return rs.toString();
  }

  private void clearCachedCM() {
    UKV.remove(ConfusionTask.keyForCM(_modelKey.value()._key,_numTrees.value(),Key.make(_dataKey.originalValue()),_classCol.value(),true));
    UKV.remove(ConfusionTask.keyForCM(_modelKey.value()._key,_numTrees.value(),Key.make(_dataKey.originalValue()),_classCol.value(),false));
  }

  @Override protected Response serve() {
    RFModel model = _modelKey.value();
    JsonObject response = new JsonObject();
    response.addProperty(DATA_KEY, _dataKey.originalValue());
    response.addProperty(MODEL_KEY, _modelKey.originalValue());
    response.addProperty(CLASS, _classCol.value());
    response.addProperty(NUM_TREES, model._totalTrees);
    response.addProperty(OOBEE, 0);
    if (_weights.specified())
      response.addProperty(WEIGHTS, _weights.originalValue());

    // Always clear CM matrix and recompute them to be sure that no stalled CM is in the system
    clearCachedCM();

    return RFView.redirect(response, null, _modelKey.value()._key, _dataKey.value()._key, model._totalTrees, _classCol.value(), _weights.originalValue(), false, false);
  }
}
