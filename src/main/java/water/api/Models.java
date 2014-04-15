package water.api;

import java.lang.reflect.Field;
import java.util.*;

import com.google.gson.*;
import hex.deeplearning.DeepLearning;
import hex.drf.DRF;
import hex.gbm.GBM;
import hex.glm.GLM2;
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
    return  "<a href='/2/Models'>" + content + "</a>";
  }


  ////////////////
  // Query params:
  ////////////////
  @API(help="An existing H2O Model key.", required=false, filter=Default.class)
  Model key = null;


  /////////////////
  // The Code (tm):
  /////////////////
  public static final Gson gson = new GsonBuilder().serializeSpecialFloatingPointValues().setPrettyPrinting().create();

  public static final class ModelSummary {
    public String model_algorithm = "unknown";
    public Model.ModelCategory model_category = Model.ModelCategory.Unknown;
    public Job.JobState state = Job.JobState.CREATED;
    public List<String> input_column_names = new ArrayList<String>();
    public String response_column_name = "unknown";
    public Map parameters = new HashMap<String, Object>();
  }

  private static Map whitelistJsonObject(JsonObject unfiltered, Set<String> whitelist) {
    // If we create a new JsonObject here and serialize it the key/value pairs are inside
    // a superflouous "members" object, so create a Map instead.
    JsonObject filtered = new JsonObject();

    Set<Map.Entry<String,JsonElement>> entries = unfiltered.entrySet();
    for (Map.Entry<String,JsonElement> entry : entries) {
      String key = entry.getKey();

      if (whitelist.contains(key))
        filtered.add(key, entry.getValue());
    }
    return gson.fromJson(gson.toJson(filtered), Map.class);
  }


  public static Map<String, ModelSummary> generateModelSummaries(Set<String>keys, Map<String, Model> models) {
      Map<String, ModelSummary> modelSummaries = new TreeMap<String, ModelSummary>();

      if (null == keys) {
        keys = models.keySet();
      }

      for (String key : keys) {
        ModelSummary summary = new ModelSummary();
        Models.summarizeAndEnhanceModel(summary, models.get(key));
        modelSummaries.put(key, summary);
      }

      return modelSummaries;
  }


  /**
   * Summarize subclasses of water.Model.
   */
  protected static void summarizeAndEnhanceModel(ModelSummary summary, Model model) {
    if (model instanceof hex.glm.GLMModel) {
      summarizeGLMModel(summary, (hex.glm.GLMModel) model);
    } else if (model instanceof hex.drf.DRF.DRFModel) {
      summarizeDRFModel(summary, (hex.drf.DRF.DRFModel) model);
    } else if (model instanceof hex.deeplearning.DeepLearningModel) {
      summarizeDeepLearningModel(summary, (hex.deeplearning.DeepLearningModel) model);
    } else if (model instanceof hex.gbm.GBM.GBMModel) {
      summarizeGBMModel(summary, (hex.gbm.GBM.GBMModel) model);
    } else {
      // catch-all
      summarizeModelCommonFields(summary, (Model) model);
    }
  }


  /**
   * Summarize fields which are generic to water.Model.
   */
  private static void summarizeModelCommonFields(ModelSummary summary, Model model) {
    String[] names = model._names;

    summary.model_algorithm = model.getClass().toString(); // fallback only

    summary.state = ((Job)model.job()).getState();
    summary.model_category = model.getModelCategory();

    summary.response_column_name = names[names.length - 1];

    for (int i = 0; i < names.length - 1; i++)
      summary.input_column_names.add(names[i]);
  }


  /******
   * GLM2
   ******/
  private static final String[] GLM_whitelist_array = {
    "max_iter",
    "standardize",
    "n_folds",
    "family",
    "_wgiven",
    "_proximalPenalty",
    "_beta",
    "_runAllLambdas",
    "link",
    "tweedie_variance_power",
    "tweedie_link_power",
    "alpha",
    "lambda_max",
    "lambda",
    "beta_epsilon"
  };
  private static final Set<String> GLM_whitelist = new HashSet<String>(Arrays.asList(GLM_whitelist_array));

  /**
   * Summarize fields which are specific to hex.glm.GLMModel.
   */
  private static void summarizeGLMModel(ModelSummary summary, hex.glm.GLMModel model) {
    // add generic fields such as column names
    summarizeModelCommonFields(summary, model);

    summary.model_algorithm = "GLM";

    JsonObject all_params = ((GLM2)model.get_params()).toJSON();
    summary.parameters = whitelistJsonObject(all_params, GLM_whitelist);
  }


  /******
   * DRF
   ******/
  private static final String[] DRF_whitelist_array = {
    "build_tree_one_node",
    "ntrees",
    "max_depth",
    "min_rows",
    "nbins",
    "score_each_iteration",
    "_mtry",
    "_seed"
  };
  private static final Set<String> DRF_whitelist = new HashSet<String>(Arrays.asList(DRF_whitelist_array));

  /**
   * Summarize fields which are specific to hex.drf.DRF.DRFModel.
   */
  private static void summarizeDRFModel(ModelSummary summary, hex.drf.DRF.DRFModel model) {
    // add generic fields such as column names
    summarizeModelCommonFields(summary, model);

    summary.model_algorithm = "DRF";

    JsonObject all_params = ((DRF)model.get_params()).toJSON();
    summary.parameters = whitelistJsonObject(all_params, DRF_whitelist);
  }

  /***************
   * DeepLearning
   ***************/
  private static final String[] DL_whitelist_array = { // "checkpoint",
    // "expert_mode",
    "activation",
    "hidden",
    "epochs",
    "train_samples_per_iteration",
    "seed",
    "adaptive_rate",
    "rho",
    "epsilon",
    "rate",
    "rate_annealing",
    "rate_decay",
    "momentum_start",
    "momentum_ramp",
    "momentum_stable",
    "nesterov_accelerated_gradient",
    "input_dropout_ratio",
    "hidden_dropout_ratios;",
    "l1",
    "l2",
    "max_w2",
    "initial_weight_distribution",
    "initial_weight_scale",
    "loss",
    "score_interval",
    "score_training_samples",
    "score_validation_samples",
    "score_duty_cycle",
    "classification_stop",
    "regression_stop",
    // "quiet_mode",
    // "max_confusion_matrix_size",
    "max_hit_ratio_k",
    "balance_classes",
    "max_after_balance_size",
    "score_validation_sampling",
    // "diagnostics",
    // "variable_importances",
    "fast_mode",
    "ignore_const_cols",
    // "force_load_balance",
    // "replicate_training_data",
    // "single_node_mode",
    "shuffle_training_data"
  };
  private static final Set<String> DL_whitelist = new HashSet<String>(Arrays.asList(DL_whitelist_array));

  /**
   * Summarize fields which are specific to hex.deeplearning.DeepLearningModel.
   */
  private static void summarizeDeepLearningModel(ModelSummary summary, hex.deeplearning.DeepLearningModel model) {
    // add generic fields such as column names
    summarizeModelCommonFields(summary, model);

    summary.model_algorithm = "DeepLearning";

    JsonObject all_params = ((DeepLearning)model.get_params()).toJSON();
    summary.parameters = whitelistJsonObject(all_params, DL_whitelist);
  }

  /******
   * GBM
   ******/
  private static final String[] GBM_whitelist_array = {
    "ntrees",
    "max_depth",
    "min_rows",
    "nbins",
    "score_each_iteration",
    "learn_rate",
    "grid_parallelism",
  };
  private static final Set<String> GBM_whitelist = new HashSet<String>(Arrays.asList(GBM_whitelist_array));

  /**
   * Summarize fields which are specific to hex.gbm.GBM.GBMModel.
   */
  private static void summarizeGBMModel(ModelSummary summary, hex.gbm.GBM.GBMModel model) {
    // add generic fields such as column names
    summarizeModelCommonFields(summary, model);

    summary.model_algorithm = "GBM";

    JsonObject all_params = ((GBM)model.get_params()).toJSON();
    summary.parameters = whitelistJsonObject(all_params, GBM_whitelist);

  }


  /**
   * Fetch all Models from the KV store.
   */
  protected Map<String, Model>fetchAll() {
    // Get all the water.model keys.
    //
    // NOTE: globalKeySet filters by class when it pulls stuff from other nodes,
    // but still returns local keys of all types so we need to filter below.
    Set<Key> keySet = H2O.globalKeySet("water.Model"); // filter by class, how cool is that?

    Map<String, Model> modelsMap = new TreeMap(); // Sort for pretty display and reliable ordering.

    for (Key key : keySet) {
      if( !key.user_allowed() ) // Also filter out for user-keys
        continue;
      if( H2O.get(key) == null )
        continue;

      String keyString = key.toString();

      Value value = DKV.get(key);
      Iced pojo = value.get();

      if (! (pojo instanceof Model))
        continue;
      Model model = (Model)pojo;

      modelsMap.put(keyString, model);
    }

    return modelsMap;
  }


  /**
   * Fetch all the Models from the KV store, sumamrize and enhance them, and return a map of them.
   */
  private Response serveOneOrAll(Map<String, Model> modelsMap) {
    Map<String, ModelSummary> modelSummaries = Models.generateModelSummaries(null, modelsMap);

    Map resultsMap = new LinkedHashMap();
    resultsMap.put("models", modelSummaries);

    // TODO: temporary hack to get things going
    String json = gson.toJson(resultsMap);

    JsonObject result = gson.fromJson(json, JsonElement.class).getAsJsonObject();
    return Response.done(result);
  }


  @Override
  protected Response serve() {

    if (null == this.key) {
      return serveOneOrAll(fetchAll());
    } else {
      Model model = this.key;
      Map<String, Model> modelsMap = new TreeMap(); // Sort for pretty display and reliable ordering.
      modelsMap.put(model._key.toString(), model);
      return serveOneOrAll(modelsMap);
    }

  } // serve()

}
