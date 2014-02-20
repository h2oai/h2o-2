package water.api;

import hex.rf.*;
import hex.rf.ConfusionTask.CMFinal;
import hex.rf.ConfusionTask.CMJob;

import java.util.Arrays;

import water.*;
import water.api.RequestBuilders.Response;
import water.util.RString;

import com.google.gson.*;

/**
 * RFView shows a progress of random forest building and data scoring.
 */
public class RFView extends /* Progress */ Request {

  /** The number specifies confusion matrix refresh threshold (in percent of trees). */
  public static final int DEFAULT_CM_REFRESH_THRESHOLD = 25; // = 25% - means the CM will be generated each 25% of trees has been built

  protected final H2OHexKey          _dataKey  = new H2OHexKey(DATA_KEY);
  protected final RFModelKey         _modelKey = new RFModelKey(MODEL_KEY);
  protected final HexKeyClassCol     _classCol = new HexKeyClassCol(CLASS, _dataKey);
  protected final Int                _numTrees = new NTree(NUM_TREES, _modelKey);
  protected final H2OCategoryWeights _weights  = new H2OCategoryWeights(WEIGHTS, _modelKey, _dataKey, _classCol, 1);
  protected final Bool               _oobee    = new Bool(OOBEE,false,"Out of bag errors");
  protected final Bool               _noCM     = new Bool(NO_CM, false,"Do not produce confusion matrix");
  protected final Bool               _clearCM  = new Bool(JSON_CLEAR_CM, true, "Clear cache of model confusion matrices");
  protected final Bool               _iterativeCM        = new Bool(ITERATIVE_CM, true, "Compute confusion matrix on-the-fly");
  protected final Int                _refreshThresholdCM = new Int(JSON_REFRESH_THRESHOLD_CM, DEFAULT_CM_REFRESH_THRESHOLD);
  // Dummy parameters
  protected final H2OKey              _job     = new H2OKey(JOB,false);
  protected final H2OKey              _dest    = new H2OKey(DEST_KEY,false);
  /** RFView specific parameters names */
  public static final String JSON_CONFUSION_KEY   = "confusion_key";
  public static final String JSON_CLEAR_CM        = "clear_confusion_matrix";
  public static final String JSON_REFRESH_THRESHOLD_CM = "refresh_threshold_cm";

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

  RFView() {
    // hide in generated query page
    _oobee._hideInQuery = true;
    _numTrees._readOnly = true;
    _job._hideInQuery   = true;
    _dest._hideInQuery  = true;
  }

  public static Response redirect(JsonObject fromPageResponse, Key jobKey, Key modelKey, Key dataKey, int ntree, int classCol, String weights, boolean oobee, boolean iterativeCM) {
    JsonObject redirect = new JsonObject();
    if (jobKey!=null) redirect.addProperty(JOB, jobKey.toString());
    redirect.addProperty(MODEL_KEY, modelKey.toString());
    redirect.addProperty(DEST_KEY, modelKey.toString());
    redirect.addProperty(DATA_KEY, dataKey.toString());
    redirect.addProperty(NUM_TREES, ntree);
    redirect.addProperty(CLASS, classCol);
    if (weights != null)
      redirect.addProperty(WEIGHTS, weights);
    redirect.addProperty(OOBEE, oobee);
    redirect.addProperty(ITERATIVE_CM, iterativeCM);

    return Response.redirect(fromPageResponse, RFView.class, redirect);
  }

  public static Response redirect(JsonObject fromPageResponse, Key rfModelKey) {
    RFModel rfModel = DKV.get(rfModelKey).get();
    ValueArray data = DKV.get(rfModel._dataKey).get();
    return redirect(fromPageResponse, null, rfModelKey, rfModel._dataKey, rfModel._totalTrees, data.numCols()-1, null, true, false );
  }

  public static Response redirect(JsonObject fromPageResponse, Key rfModel, Key dataKey, boolean oobee) {
    JsonObject redir = new JsonObject();
    redir.addProperty(MODEL_KEY, rfModel.toString());
    redir.addProperty(DATA_KEY, dataKey.toString());
    redir.addProperty(OOBEE, oobee);
    return Response.redirect(fromPageResponse, RFView.class, redir);
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

  protected JsonObject defaultJsonResponse() {
    // This will be shown every request
    JsonObject r = new JsonObject();
    RFModel model = _modelKey.value();
    r.addProperty(  DATA_KEY, _dataKey.originalValue());
    r.addProperty( MODEL_KEY, _modelKey.originalValue());
    r.addProperty(     CLASS, _classCol.specified() ? _classCol.value() : findResponseIdx(model));
    r.addProperty( NUM_TREES, model._totalTrees);
    r.addProperty(      MTRY, model._splitFeatures);
    r.addProperty(MTRY_NODES, Arrays.toString(model._nodesSplitFeatures));
    r.addProperty(     OOBEE, _oobee.value());
    // CM specific options
    r.addProperty(NO_CM, _noCM.value());
    r.addProperty(JSON_REFRESH_THRESHOLD_CM, _refreshThresholdCM.value());

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
    // Finish refresh after rf model is done and confusion matrix for all trees is computed
    boolean done = false;
    int classCol = _classCol.specified() ? _classCol.value() : findResponseIdx(model);

    tasks    = model._totalTrees;
    finished = model.size();

    // Handle cancelled/aborted jobs
    if (_job.value()!=null) {
      Job jjob = Job.findJob(_job.value());
      if (jjob!=null && jjob.isCancelled())
        return Response.error(jjob.exception == null ? "Job was cancelled by user!" : jjob.exception);
    }

    JsonObject response = defaultJsonResponse();
    // CM return and possible computation is requested
    if (!_noCM.value() && (finished==tasks || _iterativeCM.value()) && finished > 0) {
      // Compute the highest number of trees which is less then a threshold
      int modelSize = tasks * _refreshThresholdCM.value()/100;
      modelSize     = modelSize == 0 || finished==tasks ? finished : modelSize * (finished/modelSize);

      // Get the computing the matrix - if no job is computing, then start a new job
      Job cmJob       = ConfusionTask.make(model, modelSize, _dataKey.value()._key, classCol, weights, _oobee.value());
      // Here the the job is running - it saved a CM which can be already finished or in invalid state.
      CMFinal confusion = UKV.get(cmJob.dest());
      // if the matrix is valid, report it in the JSON
      if (confusion!=null && confusion.valid() && modelSize > 0) {
        //finished += 1;
        JsonObject cm       = new JsonObject();
        JsonArray  cmHeader = new JsonArray();
        JsonArray  matrix   = new JsonArray();
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
            row.add(new JsonPrimitive(confusion.matrix(crow,ccol)));
            if (crow!=ccol) classHitScore += confusion.matrix(crow,ccol);
          }
          // produce infinity members in case of 0.f/0
          classErrors.add(new JsonPrimitive((float)classHitScore / (classHitScore + confusion.matrix(crow,crow))));
          matrix.add(row);
        }
        cm.add(JSON_CM_CLASSES_ERRORS, classErrors);
        cm.add(JSON_CM_MATRIX,matrix);
        cm.addProperty(JSON_CM_TREES,modelSize);
        response.add(JSON_CM,cm);
        // Signal end only and only if all trees were generated and confusion matrix is valid
        done = finished == tasks;
      }
    } else if (_noCM.value() && finished == tasks) done = true;

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
    if (done) {
      r = jobDone(response);
      r.addHeader("<div class='alert'>" + /*RFScore.link(MODEL_KEY, model._key, "Use this model for scoring.") */ GeneratePredictionsPage.link(model._key, "Predict!")+ " </div>");
    } else { r = Response.poll(response, finished, tasks);  }
    r.setBuilder(JSON_CM, new ConfusionMatrixBuilder());
    r.setBuilder(TREES, new TreeListBuilder());
    return r;
  }

  static String[] cfDomain(final CMFinal cm, int maxClasses) {
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
              _dataKey.value(),
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
        sb.append("<dt>trees used</dt><dd>"+cm.get(JSON_CM_TREES).getAsInt()).append("</dd>");
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
      } else {
        sb.append("<div class='alert alert-info'>");
        sb.append("Confusion matrix is being computed into the key:</br>");
        sb.append(cm.get(JSON_CONFUSION_KEY).getAsString());
        sb.append("</div>");
      }
      return sb.toString();
    }
  }

  static final int findResponseIdx(RFModel model) {
    String nresponse = model.responseName();
    ValueArray ary = UKV.get(model._dataKey);
    int idx = 0;
    for (ValueArray.Column cols : ary._cols) if (nresponse.equals(cols._name)) return idx; else idx++;
    return -1;
  }
}
