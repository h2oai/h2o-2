package water.api;

import dontweave.gson.Gson;
import dontweave.gson.GsonBuilder;
import dontweave.gson.JsonElement;
import dontweave.gson.JsonObject;
import hex.VarImp;
import hex.deeplearning.DeepLearning;
import hex.drf.DRF;
import hex.gbm.GBM;
import hex.glm.GLM2;
import hex.glm.GLMModel;
import hex.singlenoderf.SpeeDRF;
import hex.nb.NaiveBayes;
import hex.nb.NBModel;
import org.apache.commons.math3.util.Pair;
import water.*;
import water.api.Frames.FrameSummary;
import water.fvec.Frame;

import java.util.*;

import static water.util.ParamUtils.*;

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

  @API(help="Find Frames that are compatible with the Model.", required=false, filter=Default.class)
  boolean find_compatible_frames = false;

  @API(help="An existing H2O Frame key to score with the Model which is specified by the key parameter.", required=false, filter=Default.class)
  Frame score_frame = null;

  @API(help="Should we adapt() the Frame to the Model?", required=false, filter=Default.class)
  boolean adapt = true;


  /////////////////
  // The Code (tm):
  /////////////////
  public static final Gson gson = new GsonBuilder().serializeSpecialFloatingPointValues().setPrettyPrinting().create();

  public static final class ModelSummary {
    public String[] warnings = new String[0];
    public String model_algorithm = "unknown";
    public Model.ModelCategory model_category = Model.ModelCategory.Unknown;
    public Job.JobState state = Job.JobState.CREATED;
    public String id = null;
    public String key = null;
    public long creation_epoch_time_millis = -1;
    public long training_duration_in_ms = -1;
    public List<String> input_column_names = new ArrayList<String>();
    public String response_column_name = "unknown";
    public Map critical_parameters = new HashMap<String, Object>();
    public Map secondary_parameters = new HashMap<String, Object>();
    public Map expert_parameters = new HashMap<String, Object>();
    public Map variable_importances = null;
    public Set<String> compatible_frames = new HashSet<String>();
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


  /**
   * Fetch all the Frames so we can see if they are compatible with our Model(s).
   */
  private Pair<Map<String, Frame>, Map<String, Set<String>>> fetchFrames() {
    Map<String, Frame> all_frames = null;
    Map<String, Set<String>> all_frames_cols = null;

    if (this.find_compatible_frames) {
      // caches for this request
      all_frames = Frames.fetchAll();
      all_frames_cols = new TreeMap<String, Set<String>>();

      for (Map.Entry<String, Frame> entry : all_frames.entrySet()) {
        all_frames_cols.put(entry.getKey(), new TreeSet<String>(Arrays.asList(entry.getValue()._names)));
      }
    }
    return new Pair<Map<String, Frame>, Map<String, Set<String>>>(all_frames, all_frames_cols);
  }


  private static Map<String, Frame> findCompatibleFrames(Model model, Map<String, Frame> all_frames, Map<String, Set<String>> all_frames_cols) {
    Map<String, Frame> compatible_frames = new TreeMap<String, Frame>();

    Set<String> model_column_names = new HashSet(Arrays.asList(model._names));

    for (Map.Entry<String, Set<String>> entry : all_frames_cols.entrySet()) {
      Set<String> frame_cols = entry.getValue();

      if (frame_cols.containsAll(model_column_names)) {
        /// See if adapt throws an exception or not.
        try {
          Frame frame = all_frames.get(entry.getKey());
          Frame[] outputs = model.adapt(frame, false); // TODO: this does too much work; write canAdapt()
          Frame adapted = outputs[0];
          Frame trash = outputs[1];
          // adapted.delete();  // TODO: shouldn't we clean up adapted vecs?  But we can't delete() the frame as a whole. . .
          trash.delete();

          // A-Ok
          compatible_frames.put(entry.getKey(), frame);
        }
        catch (Exception e) {
          // skip
        }
      }
    }

    return compatible_frames;
  }


  public static Map<String, ModelSummary> generateModelSummaries(Set<String>keys, Map<String, Model> models, boolean find_compatible_frames, Map<String, Frame> all_frames, Map<String, Set<String>> all_frames_cols) {
      Map<String, ModelSummary> modelSummaries = new TreeMap<String, ModelSummary>();

      if (null == keys) {
        keys = models.keySet();
      }

      for (String key : keys) {
        ModelSummary summary = new ModelSummary();
        Models.summarizeAndEnhanceModel(summary, models.get(key), find_compatible_frames, all_frames, all_frames_cols);
        modelSummaries.put(key, summary);
      }

      return modelSummaries;
  }


  /**
   * Summarize subclasses of water.Model.
   */
  protected static void summarizeAndEnhanceModel(ModelSummary summary, Model model, boolean find_compatible_frames, Map<String, Frame> all_frames, Map<String, Set<String>> all_frames_cols) {
    if (model instanceof GLMModel) {
      summarizeGLMModel(summary, (GLMModel) model);
    } else if (model instanceof DRF.DRFModel) {
      summarizeDRFModel(summary, (DRF.DRFModel) model);
    } else if (model instanceof hex.deeplearning.DeepLearningModel) {
      summarizeDeepLearningModel(summary, (hex.deeplearning.DeepLearningModel) model);
    } else if (model instanceof hex.gbm.GBM.GBMModel) {
      summarizeGBMModel(summary, (hex.gbm.GBM.GBMModel) model);
    } else if (model instanceof hex.singlenoderf.SpeeDRFModel) {
      summarizeSpeeDRFModel(summary, (hex.singlenoderf.SpeeDRFModel) model);
    } else if (model instanceof NBModel) {
      summarizeNBModel(summary, (NBModel) model);
    } else {
      // catch-all
      summarizeModelCommonFields(summary, model);
    }

    if (find_compatible_frames) {
      Map<String, Frame> compatible_frames = findCompatibleFrames(model, all_frames, all_frames_cols);
      summary.compatible_frames = compatible_frames.keySet();
    }
  }


  /**
   * Summarize fields which are generic to water.Model.
   */
  private static void summarizeModelCommonFields(ModelSummary summary, Model model) {
    String[] names = model._names;

    summary.warnings = model.warnings;

    summary.model_algorithm = model.getClass().toString(); // fallback only

    // model.job() is a local copy; on multinode clusters we need to get from the DKV
    Key job_key = ((Job)model.job()).self();
    if (null == job_key) throw H2O.fail("Null job key for model: " + (model == null ? "null model" : model._key)); // later when we deserialize models from disk we'll relax this constraint
    Job job = DKV.get(job_key).get();
    summary.state = job.getState();
    summary.model_category = model.getModelCategory();

    UniqueId unique_id = model.getUniqueId();
    summary.id = unique_id.getId();
    summary.key = unique_id.getKey();
    summary.creation_epoch_time_millis = unique_id.getCreationEpochTimeMillis();
    summary.training_duration_in_ms = model.training_duration_in_ms;

    summary.response_column_name = names[names.length - 1];

    for (int i = 0; i < names.length - 1; i++)
      summary.input_column_names.add(names[i]);

    // Ugh.
    VarImp vi = model.varimp();
    if (null != vi) {
      summary.variable_importances = new LinkedHashMap();
      summary.variable_importances.put("varimp", vi.varimp);
      summary.variable_importances.put("variables", vi.getVariables());
      summary.variable_importances.put("method", vi.method);
      summary.variable_importances.put("max_var", vi.max_var);
      summary.variable_importances.put("scaled", vi.scaled());
    }
  }


  /******
   * GLM2
   ******/
  private static final Set<String> GLM_critical_params = getCriticalParamNames(GLM2.DOC_FIELDS);
  private static final Set<String> GLM_secondary_params = getSecondaryParamNames(GLM2.DOC_FIELDS);
  private static final Set<String> GLM_expert_params = getExpertParamNames(GLM2.DOC_FIELDS);

  /**
   * Summarize fields which are specific to hex.glm.GLMModel.
   */
  private static void summarizeGLMModel(ModelSummary summary, hex.glm.GLMModel model) {
    // add generic fields such as column names
    summarizeModelCommonFields(summary, model);

    summary.model_algorithm = "GLM";

    JsonObject all_params = (model.get_params()).toJSON();
    summary.critical_parameters = whitelistJsonObject(all_params, GLM_critical_params);
    summary.secondary_parameters = whitelistJsonObject(all_params, GLM_secondary_params);
    summary.expert_parameters = whitelistJsonObject(all_params, GLM_expert_params);
  }


  /******
   * DRF
   ******/
  private static final Set<String> DRF_critical_params = getCriticalParamNames(DRF.DOC_FIELDS);
  private static final Set<String> DRF_secondary_params = getSecondaryParamNames(DRF.DOC_FIELDS);
  private static final Set<String> DRF_expert_params = getExpertParamNames(DRF.DOC_FIELDS);

  /**
   * Summarize fields which are specific to hex.drf.DRF.DRFModel.
   */
  private static void summarizeDRFModel(ModelSummary summary, hex.drf.DRF.DRFModel model) {
    // add generic fields such as column names
    summarizeModelCommonFields(summary, model);

    summary.model_algorithm = "BigData RF";

    JsonObject all_params = (model.get_params()).toJSON();
    summary.critical_parameters = whitelistJsonObject(all_params, DRF_critical_params);
    summary.secondary_parameters = whitelistJsonObject(all_params, DRF_secondary_params);
    summary.expert_parameters = whitelistJsonObject(all_params, DRF_expert_params);
  }

  /******
   * SpeeDRF
   ******/
  private static final Set<String> SpeeDRF_critical_params = getCriticalParamNames(SpeeDRF.DOC_FIELDS);
  private static final Set<String> SpeeDRF_secondary_params = getSecondaryParamNames(SpeeDRF.DOC_FIELDS);
  private static final Set<String> SpeeDRF_expert_params = getExpertParamNames(SpeeDRF.DOC_FIELDS);

  /**
   * Summarize fields which are specific to hex.drf.DRF.SpeeDRFModel.
   */
  private static void summarizeSpeeDRFModel(ModelSummary summary, hex.singlenoderf.SpeeDRFModel model) {
    // add generic fields such as column names
    summarizeModelCommonFields(summary, model);

    summary.model_algorithm = "Random Forest";

    JsonObject all_params = (model.get_params()).toJSON();
    summary.critical_parameters = whitelistJsonObject(all_params, SpeeDRF_critical_params);
    summary.secondary_parameters = whitelistJsonObject(all_params, SpeeDRF_secondary_params);
    summary.expert_parameters = whitelistJsonObject(all_params, SpeeDRF_expert_params);
  }

  /***************
   * DeepLearning
   ***************/
  private static final Set<String> DL_critical_params = getCriticalParamNames(DeepLearning.DOC_FIELDS);
  private static final Set<String> DL_secondary_params = getSecondaryParamNames(DeepLearning.DOC_FIELDS);
  private static final Set<String> DL_expert_params =getExpertParamNames(DeepLearning.DOC_FIELDS);

  /**
   * Summarize fields which are specific to hex.deeplearning.DeepLearningModel.
   */
  private static void summarizeDeepLearningModel(ModelSummary summary, hex.deeplearning.DeepLearningModel model) {
    // add generic fields such as column names
    summarizeModelCommonFields(summary, model);

    summary.model_algorithm = "DeepLearning";

    JsonObject all_params = (model.get_params()).toJSON();
    summary.critical_parameters = whitelistJsonObject(all_params, DL_critical_params);
    summary.secondary_parameters = whitelistJsonObject(all_params, DL_secondary_params);
    summary.expert_parameters = whitelistJsonObject(all_params, DL_expert_params);
  }

  /******
   * GBM
   ******/
  private static final Set<String> GBM_critical_params = getCriticalParamNames(GBM.DOC_FIELDS);
  private static final Set<String> GBM_secondary_params = getSecondaryParamNames(GBM.DOC_FIELDS);
  private static final Set<String> GBM_expert_params = getExpertParamNames(GBM.DOC_FIELDS);

  /**
   * Summarize fields which are specific to hex.gbm.GBM.GBMModel.
   */
  private static void summarizeGBMModel(ModelSummary summary, hex.gbm.GBM.GBMModel model) {
    // add generic fields such as column names
    summarizeModelCommonFields(summary, model);

    summary.model_algorithm = "GBM";

    JsonObject all_params = (model.get_params()).toJSON();
    summary.critical_parameters = whitelistJsonObject(all_params, GBM_critical_params);
    summary.secondary_parameters = whitelistJsonObject(all_params, GBM_secondary_params);
    summary.expert_parameters = whitelistJsonObject(all_params, GBM_expert_params);
  }

  /******
   * NB
   ******/
  private static final Set<String> NB_critical_params = getCriticalParamNames(NaiveBayes.DOC_FIELDS);
  private static final Set<String> NB_secondary_params = getSecondaryParamNames(NaiveBayes.DOC_FIELDS);
  private static final Set<String> NB_expert_params = getExpertParamNames(NaiveBayes.DOC_FIELDS);

  /**
   * Summarize fields which are specific to hex.nb.NBModel.
   */
  private static void summarizeNBModel(ModelSummary summary, hex.nb.NBModel model) {
    // add generic fields such as column names
    summarizeModelCommonFields(summary, model);

    summary.model_algorithm = "Naive Bayes";

    JsonObject all_params = (model.get_params()).toJSON();
    summary.critical_parameters = whitelistJsonObject(all_params, NB_critical_params);
    summary.secondary_parameters = whitelistJsonObject(all_params, NB_secondary_params);
    summary.expert_parameters = whitelistJsonObject(all_params, NB_expert_params);
  }

  /**
   * Fetch all Models from the KV store.
   */
  protected Map<String, Model> fetchAll() {
    return H2O.KeySnapshot.globalSnapshot().fetchAll(water.Model.class);
  }


  /**
   * Score a frame with the given model.
   */
  protected static Response scoreOne(Frame frame, Model score_model, boolean adapt) {
    return Frames.scoreOne(frame, score_model);
  }


  /**
   * Fetch all the Models from the KV store, sumamrize and enhance them, and return a map of them.
   */
  private Response serveOneOrAll(Map<String, Model> modelsMap) {
    // returns empty sets if !this.find_compatible_frames
    Pair<Map<String, Frame>, Map<String, Set<String>>> frames_info = fetchFrames();
    Map<String, Frame> all_frames = frames_info.getFirst();
    Map<String, Set<String>> all_frames_cols = frames_info.getSecond();

    Map<String, ModelSummary> modelSummaries = Models.generateModelSummaries(null, modelsMap, find_compatible_frames, all_frames, all_frames_cols);

    Map resultsMap = new LinkedHashMap();
    resultsMap.put("models", modelSummaries);

    // If find_compatible_frames then include a map of the Frame summaries.  Should we put this on a separate switch?
    if (this.find_compatible_frames) {
      Set<String> all_referenced_frames = new TreeSet<String>();

      for (Map.Entry<String, ModelSummary> entry: modelSummaries.entrySet()) {
        ModelSummary summary = entry.getValue();
        all_referenced_frames.addAll(summary.compatible_frames);
      }

      Map<String, FrameSummary> frameSummaries = Frames.generateFrameSummaries(all_referenced_frames, all_frames, false, null, null);
      resultsMap.put("frames", frameSummaries);
    }

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
      if (null == this.score_frame) {
        Model model = this.key;
        Map<String, Model> modelsMap = new TreeMap(); // Sort for pretty display and reliable ordering.
        modelsMap.put(model._key.toString(), model);
        return serveOneOrAll(modelsMap);
      } else {
        return scoreOne(this.score_frame, this.key, this.adapt);
      }
    }
  } // serve()

}
