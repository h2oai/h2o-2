package water.api;

import water.Key;
import water.api.RequestBuilders.Response;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import hex.rf.Confusion;
import hex.rf.RFModel;

public class RFView extends Request {

  protected final H2OHexKey _dataKey = new H2OHexKey(DATA_KEY);
  protected final RFModelKey _modelKey = new RFModelKey(MODEL_KEY);
  protected final HexKeyClassCol _classCol = new HexKeyClassCol(CLASS, _dataKey);
  protected final Int _numTrees = new Int(NUM_TREES,50,0,Integer.MAX_VALUE);
  protected final H2OCategoryWeights _weights = new H2OCategoryWeights(WEIGHTS, _dataKey, _classCol, 1);
  protected final Bool _oobee = new Bool(OOBEE,false,"Out of bag errors");
  protected final Bool _noCM = new Bool(NO_CM, false,"Do not produce confusion matrix");
  protected final Bool _clearCM = new Bool(JSON_CLEAR_CM, false, "Clear cache of model confusion matrices");

  public static final String JSON_CONFUSION_KEY = "confusion_key";
  public static final String JSON_CLEAR_CM = "clear_confusion_matrix";

  public static final String JSON_CM        = "confusion_matrix";
  public static final String JSON_CM_TYPE   = "type";
  public static final String JSON_CM_HEADER = "header";
  public static final String JSON_CM_MATRIX = "scores";
  public static final String JSON_CM_TREES  = "used_trees";
  public static final String JSON_CM_CLASS_ERR    = "classification_error";
  public static final String JSON_CM_ROWS         = "rows";
  public static final String JSON_CM_ROWS_SKIPPED = "rows_skipped";

  public static Response redirect(JsonObject resp, Key dest) {
    JsonObject redir = new JsonObject();
    redir.addProperty(MODEL_KEY, dest.toString());
    return Response.redirect(resp, RFView.class, redir);
  }

  @Override protected Response serve() {
    int tasks = 0;
    int finished = 0;
    RFModel model = _modelKey.value();
    double[] weights = _weights.value();
    JsonObject response = new JsonObject();

    response.addProperty(DATA_KEY, _dataKey.originalValue());
    response.addProperty(MODEL_KEY, _modelKey.originalValue());
    response.addProperty(CLASS, _classCol.value());
    response.addProperty(NUM_TREES, model._totalTrees);
    response.addProperty(MTRY, -1);

    tasks += model._totalTrees;
    finished += model.size();

    // CM return and possible computation is requested
    if (!_noCM.value()) {
      tasks += 1;
      // get the confusion matrix
      Confusion confusion = Confusion.make(model, _dataKey.value()._key, _classCol.value(), weights, _oobee.value());
      response.addProperty(JSON_CONFUSION_KEY, confusion.keyFor().toString());
      // if the matrix is valid, report it in the JSON
      if (confusion.isValid() && finished > 0) {
        finished += 1;
        JsonObject cm = new JsonObject();
        JsonArray cmHeader = new JsonArray();
        JsonArray matrix = new JsonArray();
        cm.addProperty(JSON_CM_TYPE, _oobee.value() ? "OOB error estimate" : "full scoring");
        cm.addProperty(JSON_CM_CLASS_ERR, confusion.classError());
        cm.addProperty(JSON_CM_ROWS_SKIPPED, confusion.skippedRows());
        cm.addProperty(JSON_CM_ROWS, confusion.rows());
        // create the header
        for (String s : vaCategoryNames(_dataKey.value()._cols[_classCol.value()],1024))
          cmHeader.add(new JsonPrimitive(s));
        cm.add(JSON_CM_HEADER,cmHeader);
        // add the matrix
        final int nclasses = model.classes();
        for (int crow = 0; crow < nclasses; ++crow) {
          JsonArray row = new JsonArray();
          for (int ccol = 0; ccol < nclasses; ++ccol)
            row.add(new JsonPrimitive(confusion._matrix[crow][ccol]));
          matrix.add(row);
        }
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

    Response r = (finished == tasks) ? Response.done(response) : Response.poll(response, finished, tasks);
    r.setBuilder(JSON_CM, new ConfusionMatrixBuilder());
    r.setBuilder(Constants.TREES, new TreeListBuilder());
    return r;
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
        sb.append("<dt>used / skipped rows </dt><dd>").append(String.format("%d / %d (%3.1f %%)", rows, skippedRows, (double)skippedRows/(skippedRows+rows))).append("</dd>");
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
