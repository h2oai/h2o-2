package water.api;

import java.util.*;

import com.google.gson.*;
import water.*;
import water.util.Log;

public class Models extends Request2 {

  ///////////////////////
  // Request2 boilerplate
  ///////////////////////
  static final int API_WEAVER=1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  // This Request supports the HTML 'GET' command, and this is the help text
  // for GET.
  static final String DOC_GET = "Return the list of models.";

  public static String link(Key k, String content){
    return  "<a href='/2/models'>" + content + "</a>";
  }
  ///////////////////////


  public static final Gson gson = new GsonBuilder().serializeSpecialFloatingPointValues().setPrettyPrinting().create();

  private class ModelSummary {
    public String model_type = "unknown";
    public String family = "unknown";
    public String state = "unknown";
    public List<String> input_column_names = new ArrayList<String>();
    public String response_column_name = null;
  }

  /**
   * Summarize fields which are generic to water.Model.
   */
  private void summarizeModel(ModelSummary summary, Value value, water.Model model) {
    String[] names = model._names;

    summary.model_type = model.getClass().toString();

    summary.response_column_name = names[names.length - 1];

    for (int i = 0; i < names.length - 1; i++)
      summary.input_column_names.add(names[i]);
  }

  /**
   * Summarize fields which are specific to hex.glm.GLMModel.
   */
  private void summarizeGLMModel(ModelSummary summary, Value value, hex.glm.GLMModel model) {
    // add generic fields such as column names
    summarizeModel(summary, value, model);

    summary.model_type = "GLM";
    summary.family = model.getParams().getFamily().toString();

    // Job.JobHandle = (Job)DKV.get(model.getJobKey());
    // summary.state = job.state.toString());
  }

  /**
   * Summarize fields which are specific to hex.drf.DRF.DRFModel.
   */
  private void summarizeDRFModel(ModelSummary summary, Value value, hex.drf.DRF.DRFModel model) {
    // add generic fields such as column names
    summarizeModel(summary, value, model);

    summary.model_type = "DRF";
    // summary.family = model.getParams().getFamily().toString();

    // Job.JobHandle = (Job)DKV.get(model.getJobKey());
    // summary.state = job.state.toString());
  }

  /**
   * Summarize fields which are specific to hex.deeplearning.DeepLearningModel.
   */
  private void summarizeDeepLearningModel(ModelSummary summary, Value value, hex.deeplearning.DeepLearningModel model) {
    // add generic fields such as column names
    summarizeModel(summary, value, model);

    summary.model_type = "DeepLearning";
    // summary.family = model.getParams().getFamily().toString();

    // Job.JobHandle = (Job)DKV.get(model.getJobKey());
    // summary.state = job.state.toString());
  }

  /**
   * Summarize fields which are specific to hex.gbm.GBM.GBMModel.
   */
  private void summarizeGBMModel(ModelSummary summary, Value value, hex.gbm.GBM.GBMModel model) {
    // add generic fields such as column names
    summarizeModel(summary, value, model);

    summary.model_type = "GBM";
    // summary.family = model.getParams().getFamily().toString();

    // Job.JobHandle = (Job)DKV.get(model.getJobKey());
    // summary.state = job.state.toString());
  }

  @Override
  protected Response serve() {

    // Get all the model keys.  Right now it's a hack to determine which values are models.
    Set<Key> keySet = H2O.globalKeySet(null);

    Map modelsMap = new TreeMap(); // Sort for pretty display and reliable ordering.
    for (Key key : keySet) {
      String keyString = key.toString();
      ModelSummary summary = new ModelSummary();

      Value value = DKV.get(key);
      // TODO: we don't have a way right now of getting the type without deserializing to a POJO.
      // This is going to deserialize the enture KV store.  We need a less brute-force way.
      Iced pojo = value.get();

      if (pojo instanceof hex.glm.GLMModel) {
        summarizeGLMModel(summary, value, (hex.glm.GLMModel) pojo);
      } else if (pojo instanceof hex.drf.DRF.DRFModel) {
        summarizeDRFModel(summary, value, (hex.drf.DRF.DRFModel) pojo);
      } else if (pojo instanceof hex.deeplearning.DeepLearningModel) {
        summarizeDeepLearningModel(summary, value, (hex.deeplearning.DeepLearningModel) pojo);
      } else if (pojo instanceof hex.gbm.GBM.GBMModel) {
        summarizeGBMModel(summary, value, (hex.gbm.GBM.GBMModel) pojo);
      } else if (pojo instanceof water.Model) {
        // catch-all
        summarizeModel(summary, value, (water.Model) pojo);
      } else {
        // skip
        continue;
      }

      modelsMap.put(keyString, summary);
    }

    Map resultsMap = new HashMap();
    resultsMap.put("models", modelsMap);

    // TODO: temporary hack to get things going
    String json = gson.toJson(resultsMap);
    Log.info("Json for results: " + json);

    JsonObject result = gson.fromJson(json, JsonElement.class).getAsJsonObject();
    return Response.done(result);
  } // serve()

}
