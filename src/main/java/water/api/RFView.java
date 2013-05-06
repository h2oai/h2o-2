package water.api;

import java.util.Arrays;

import hex.rf.Confusion;
import hex.rf.RFModel;
import water.Key;
import water.util.RString;

import com.google.gson.*;

/**
 * RFView shows a progress of random forest building and data scoring.
 */
public class RFView extends /* Progress */ Request {

  /** The number specifies confusion matrix refresh threshold (in percent of trees). */
  public static final int DEFAULT_CM_REFRESH_TRESHOLD   = 25; // = 25% - means the CM will be generated each 25% of trees has been built

  protected final H2OHexKey          _dataKey  = new H2OHexKey(DATA_KEY);
  protected final RFModelKey         _modelKey = new RFModelKey(MODEL_KEY);
  protected final HexKeyClassCol     _classCol = new HexKeyClassCol(CLASS, _dataKey);
  protected final Int                _numTrees = new NTree(NUM_TREES, _modelKey);
  protected final H2OCategoryWeights _weights  = new H2OCategoryWeights(WEIGHTS, _modelKey, _dataKey, _classCol, 1);
  protected final Bool               _oobee    = new Bool(OOBEE,false,"Out of bag errors");
  protected final Bool               _noCM     = new Bool(NO_CM, false,"Do not produce confusion matrix");
  protected final Bool               _clearCM  = new Bool(JSON_CLEAR_CM, false, "Clear cache of model confusion matrices");
  protected final Bool               _iterativeCM       = new Bool(ITERATIVE_CM, true, "Compute confusion matrix on-the-fly");
  protected final Int                _refreshTresholdCM = new Int(JSON_REFRESH_TRESHOLD_CM, DEFAULT_CM_REFRESH_TRESHOLD);
  /** RFView specific parameters names */
  public static final String JSON_CONFUSION_KEY   = "confusion_key";
  public static final String JSON_CLEAR_CM        = "clear_confusion_matrix";
  public static final String JSON_REFRESH_TRESHOLD_CM = "refresh_treshold_cm";

  // JSON keys
  public static final String JSON_CM              = "confusion_matrix";
  public static final String JSON_CM_TYPE         = "type";
  public static final String JSON_CM_HEADER       = "header";
  public static final String JSON_CM_MATRIX       = "scores";
  public static final String JSON_CM_TREES        = "used_trees";
  public static final String JSON_CM_CLASS_ERR    = "classification_error";
  public static final String JSON_CM_ROWS         = "rows";
  public static final String JSON_CM_ROWS_SKIPPED = "rows_skipped";
  public static final String JSON_CM_CLASSES_ERRORS = "classes_errors";

  private final static String[] PARAMS_LIST  = new String[] {DATA_KEY, MODEL_KEY, CLASS, NUM_TREES, WEIGHTS, OOBEE, NO_CM, JSON_CLEAR_CM};
  RFView() {
    // hide in generated query page
    _oobee._hideInQuery = true;
    _numTrees._readOnly = true;
  }

  public static Response redirect(JsonObject fromPageResponse, Key job, Key model) {
    JsonObject destPageParams = new JsonObject();
    destPageParams.addProperty(JOB, job.toString());
    destPageParams.addProperty(DEST_KEY, model.toString());
    for (String param : PARAMS_LIST) {
      if (fromPageResponse.has(param)) destPageParams.addProperty(param, fromPageResponse.get(param).getAsString());
    }
    return Response.redirect(fromPageResponse, RFView.class, destPageParams);
  }

  public static String link(Key k, String content) {
    return link(k, DATA_KEY, content);
  }

  public static String link(Key k, String keyParam, String content) {
    RString rs = new RString("<a href='RFView.query?%key_param=%$key'>%content</a>");
    rs.replace("key_param", keyParam);
    rs.replace("key", k.toString());
    rs.replace("content", content);
    return rs.toString();
  }

  public static Response redirect(JsonObject fromPageResponse, Key rfModel) {
    JsonObject redir = new JsonObject();
    redir.addProperty(MODEL_KEY, rfModel.toString());
    return Response.redirect(null, RFView.class, redir);
  }

  public static Response redirect(JsonObject fromPageResponse, Key rfModel, Key dataKey, boolean oobee) {
    JsonObject redir = new JsonObject();
    redir.addProperty(MODEL_KEY, rfModel.toString());
    redir.addProperty(DATA_KEY, dataKey.toString());
    redir.addProperty(OOBEE, oobee);
    return Response.redirect(fromPageResponse, RFView.class, redir);
  }

  protected JsonObject defaultJsonResponse() {
    JsonObject r = new JsonObject();
    RFModel model = _modelKey.value();
    r.addProperty(  DATA_KEY, _dataKey.originalValue());
    r.addProperty( MODEL_KEY, _modelKey.originalValue());
    r.addProperty(     CLASS, _classCol.value());
    r.addProperty( NUM_TREES, model._totalTrees);
    r.addProperty(      MTRY, model._splitFeatures);
    r.addProperty(MTRY_NODES, Arrays.toString(model._nodesSplitFeatures));
    r.addProperty(     OOBEE, _oobee.value());
    // CM specific options
    r.addProperty(NO_CM, _noCM.value());
    r.addProperty(JSON_REFRESH_TRESHOLD_CM, _refreshTresholdCM.value());

    return r;
  }

  protected Response jobDone(JsonObject jsonResp) {
    return Response.done(jsonResp);
  }

  @Override protected Response serve() {
    int tasks        = 0;
    int finished     = 0;
    RFModel model    = _modelKey.value();
    double[] weights = _weights.value();

    tasks    = model._totalTrees;
    finished = model.size();

    JsonObject response = defaultJsonResponse();
    // CM return and possible computation is requested
    if (!_noCM.value() && (finished==tasks || _iterativeCM.value()) && finished > 0) {
      // Compute the highest number of trees which is less then a threshold
      int modelSize = tasks * _refreshTresholdCM.value()/100;
      modelSize     = modelSize == 0 || finished==tasks ? finished : modelSize * (finished/modelSize);

      // Get the confusion matrix
      Confusion confusion = Confusion.make(model, modelSize, _dataKey.value()._key, _classCol.value(), weights, _oobee.value());
      response.addProperty(JSON_CONFUSION_KEY, confusion.keyFor().toString());
      // if the matrix is valid, report it in the JSON
      if (confusion.isValid() && modelSize > 0) {
        //finished += 1;
        JsonObject cm = new JsonObject();
        JsonArray cmHeader = new JsonArray();
        JsonArray matrix = new JsonArray();
        cm.addProperty(JSON_CM_TYPE, _oobee.value() ? "OOB error estimate" : "full scoring");
        cm.addProperty(JSON_CM_CLASS_ERR, confusion.classError());
        cm.addProperty(JSON_CM_ROWS_SKIPPED, confusion.skippedRows());
        cm.addProperty(JSON_CM_ROWS, confusion.rows());
        // create the header
        for (String s : cfDomain(confusion, 1024))
          cmHeader.add(new JsonPrimitive(s));
        cm.add(JSON_CM_HEADER,cmHeader);
        // add the matrix
        final int nclasses = confusion.dimension();
        JsonArray classErrors = new JsonArray();
        for (int crow = 0; crow < nclasses; ++crow) {
          JsonArray row  = new JsonArray();
          int classHitScore = 0;
          for (int ccol = 0; ccol < nclasses; ++ccol) {
            row.add(new JsonPrimitive(confusion._matrix[crow][ccol]));
            if (crow!=ccol) classHitScore += confusion._matrix[crow][ccol];
          }
          // produce infinity members in case of 0.f/0
          classErrors.add(new JsonPrimitive((float)classHitScore / (classHitScore + confusion._matrix[crow][crow])));
          matrix.add(row);
        }
        cm.add(JSON_CM_CLASSES_ERRORS, classErrors);
        cm.add(JSON_CM_MATRIX,matrix);
        cm.addProperty(JSON_CM_TREES,confusion._treesUsed);
        response.add(JSON_CM,cm);
      }
    }

    // Trees
    JsonObject trees = new JsonObject();
    trees.addProperty(Constants.TREE_COUNT,  model.size());
    if( model.size() > 0 ) {
      trees.add(Constants.TREE_DEPTH,  model.depth().toJson());
      trees.add(Constants.TREE_LEAVES, model.leaves().toJson());
    }
    response.add(Constants.TREES,trees);

    // Build a response
    Response r;
    if (finished == tasks) {
      r = jobDone(response);
      r.addHeader("<div class='alert'>" + RFScore.link(MODEL_KEY, model._selfKey, "Use this model for scoring.") + " </div>");
    } else { r = Response.poll(response, finished, tasks);  }
    r.setBuilder(JSON_CM, new ConfusionMatrixBuilder());
    r.setBuilder(TREES, new TreeListBuilder());
    return r;
  }

  static String[] cfDomain(final Confusion cm, int maxClasses) {
    String[] dom = cm.domain();
    if (dom.length > maxClasses)
      throw new IllegalArgumentException("The column has more than "+maxClasses+" values. Are you sure you have that many classes?");
    return dom;
  }

  private StringBuilder stats(StringBuilder sb, JsonElement json) {
    if( json == null ) {
      return sb.append(" / / ");
    } else {
      JsonObject obj = json.getAsJsonObject();
      return sb.append(String.format("%4.1f / %4.1f / %4.1f",
          obj.get(MIN).getAsDouble(),
          obj.get(MAX).getAsDouble(),
          obj.get(MEAN).getAsDouble()));
    }
  }

  public class TreeListBuilder extends ObjectBuilder {
    @Override public String build(Response response, JsonObject t, String contextName) {
      int n = t.get(Constants.TREE_COUNT).getAsInt();
      StringBuilder sb = new StringBuilder();
      if (n > 0) {
        sb.append("<h3>Trees</h3>");
        sb.append(t.get(Constants.TREE_COUNT)).append(" trees with min/max/mean depth of ");
        stats(sb, t.get(TREE_DEPTH )).append(" and leaf of ");
        stats(sb, t.get(TREE_LEAVES)).append(".<br>");
        for( int i = 0; i < n; ++i ) {
          sb.append(RFTreeView.link(_modelKey.value(), i,
              _dataKey.value(), _classCol.value(),
              Integer.toString(i+1))).append(" ");
        }
      } else {
        sb.append("<h3>No trees yet...</h3>");
      }
      return sb.toString();
    }
  }

  public static class ConfusionMatrixBuilder extends ObjectBuilder {
    @Override public String build(Response response, JsonObject cm, String contextName) {
      StringBuilder sb = new StringBuilder();
      if (cm.has(JSON_CM_MATRIX)) {
        sb.append("<h3>Confusion matrix - ").append(cm.get(JSON_CM_TYPE).getAsString()).append("</h3>");
        sb.append("<dl class='dl-horizontal'>");
        sb.append("<dt>classification error</dt><dd>").append(String.format("%5.3f %%", 100*cm.get(JSON_CM_CLASS_ERR).getAsFloat())).append("</dd>");
        long rows = cm.get(JSON_CM_ROWS).getAsLong();
        long skippedRows = cm.get(JSON_CM_ROWS_SKIPPED).getAsLong();
        sb.append("<dt>used / skipped rows </dt><dd>").append(String.format("%d / %d (%3.1f %%)", rows, skippedRows, (double)skippedRows*100/(skippedRows+rows))).append("</dd>");
        sb.append("</dl>");
        sb.append("<table class='table table-striped table-bordered table-condensed'>");
        sb.append("<tr><th>Actual \\ Predicted</th>");
        JsonArray header = (JsonArray) cm.get(JSON_CM_HEADER);
        for (JsonElement e: header)
          sb.append("<th>"+e.getAsString()+"</th>");
        sb.append("<th>Error</th></tr>");
        int classes = header.size();
        long[] totals = new long[classes];
        JsonArray matrix = (JsonArray) cm.get(JSON_CM_MATRIX);
        long sumTotal = 0;
        long sumError = 0;
        for (int crow = 0; crow < classes; ++crow) {
          JsonArray row = (JsonArray) matrix.get(crow);
          long total = 0;
          long error = 0;
          sb.append("<tr><th>"+header.get(crow).getAsString()+"</th>");
          for (int ccol = 0; ccol < classes; ++ccol) {
            long num = row.get(ccol).getAsLong();
            total += num;
            totals[ccol] += num;
            if (ccol == crow) {
              sb.append("<td style='background-color:LightGreen'>");
            } else {
              sb.append("<td>");
              error += num;
            }
            sb.append(num);
            sb.append("</td>");
          }
          sb.append("<td>");
          sb.append(String.format("%5.3f = %d / %d", (double)error/total, error, total));
          sb.append("</td></tr>");
          sumTotal += total;
          sumError += error;
        }
        sb.append("<tr><th>Totals</th>");
        for (int i = 0; i < totals.length; ++i)
          sb.append("<td>"+totals[i]+"</td>");
        sb.append("<td><b>");
        sb.append(String.format("%5.3f = %d / %d", (double)sumError/sumTotal, sumError, sumTotal));
        sb.append("</b></td></tr>");
        sb.append("</table>");
        sb.append("Trees used: "+cm.get(JSON_CM_TREES).getAsInt());
      } else {
        sb.append("<div class='alert alert-info'>");
        sb.append("Confusion matrix is being computed into the key:</br>");
        sb.append(cm.get(JSON_CONFUSION_KEY).getAsString());
        sb.append("</div>");
      }
      return sb.toString();
    }
  }
}
