package water.api;

import dontweave.gson.*;
import org.apache.commons.math3.util.Pair;
import water.*;
import water.api.Models.ModelSummary;
import water.fvec.Frame;

import java.util.*;

public class Frames extends Request2 {

  ///////////////////////
  // Request2 boilerplate
  ///////////////////////
  static final int API_WEAVER=1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  // This Request supports the HTML 'GET' command, and this is the help text
  // for GET.
  static final String DOC_GET = "Return the list of dataframes.";

  public static String link(Key k, String content){
    return  "<a href='/2/Frames'>" + content + "</a>";
  }


  ////////////////
  // Query params:
  ////////////////
  @API(help="An existing H2O Frame key.", required=false, filter=Default.class)
  Frame key = null;

  @API(help="Find Models that are compatible with the Frame.", required=false, filter=Default.class)
  boolean find_compatible_models = false;

  @API(help="An existing H2O Model key to score with the Frame which is specified by the key parameter.", required=false, filter=Default.class)
  Model score_model = null;


  /////////////////
  // The Code (tm):
  /////////////////
  public static final Gson gson = new GsonBuilder().serializeSpecialFloatingPointValues().setPrettyPrinting().create();

  public static final class FrameSummary {
    public String id = null;
    public String key = null;
    public long creation_epoch_time_millis = -1;

    public String[] column_names = { };
    public Set<String> compatible_models = new HashSet<String>();
    public boolean is_raw_frame = true; // guilty until proven innocent
  }

  // TODO: refactor, since this is duplicated
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
   * Fetch all the Models so we can see if they are compatible with our Frame(s).
   */
  private Pair<Map<String, Model>, Map<String, Set<String>>> fetchModels() {
    Map<String, Model> all_models = null;
    Map<String, Set<String>> all_models_cols = null;

    if (this.find_compatible_models) {
      // caches for this request
      all_models = (new Models()).fetchAll();
      all_models_cols = new TreeMap<String, Set<String>>();

      for (Map.Entry<String, Model> entry : all_models.entrySet()) {
        all_models_cols.put(entry.getKey(), new TreeSet<String>(Arrays.asList(entry.getValue()._names)));
      }
    }
    return new Pair<Map<String, Model>, Map<String, Set<String>>>(all_models, all_models_cols);
  }


  private static Map<String, Model> findCompatibleModels(Frame frame, Map<String, Model> all_models, Map<String, Set<String>> all_models_cols) {
    Map<String, Model> compatible_models = new TreeMap<String, Model>();

    Set<String> frame_column_names = new HashSet(Arrays.asList(frame._names));

    for (Map.Entry<String, Set<String>> entry : all_models_cols.entrySet()) {
      Set<String> model_cols = entry.getValue();

      if (frame_column_names.containsAll(model_cols)) {
        /// See if adapt throws an exception or not.
        try {
          Model model = all_models.get(entry.getKey());
          Frame[] outputs = model.adapt(frame, false); // TODO: this does too much work; write canAdapt()
          Frame adapted = outputs[0];
          Frame trash = outputs[1];
          // adapted.delete();  // TODO: shouldn't we clean up adapted vecs?  But we can't delete() the frame as a whole. . .
          trash.delete();

          // A-Ok
          compatible_models.put(entry.getKey(), model);
        }
        catch (Exception e) {
          // skip
        }
      }
    }

    return compatible_models;
  }


  public static Map<String, FrameSummary> generateFrameSummaries(Set<String>keys, Map<String, Frame> frames, boolean find_compatible_models, Map<String, Model> all_models, Map<String, Set<String>> all_models_cols) {
      Map<String, FrameSummary> frameSummaries = new TreeMap<String, FrameSummary>();

      if (null == keys) {
        keys = frames.keySet();
      }

      for (String key : keys) {
        FrameSummary summary = new FrameSummary();
        Frames.summarizeAndEnhanceFrame(summary, frames.get(key), find_compatible_models, all_models, all_models_cols);
        frameSummaries.put(key, summary);
      }

      return frameSummaries;
  }


  /**
   * Summarize fields in water.fvec.Frame.
   */
  private static void summarizeAndEnhanceFrame(FrameSummary summary, Frame frame, boolean find_compatible_models, Map<String, Model> all_models, Map<String, Set<String>> all_models_cols) {
    UniqueId unique_id = frame.getUniqueId();
    summary.id = unique_id.getId();
    summary.key = unique_id.getKey();
    summary.creation_epoch_time_millis = unique_id.getCreationEpochTimeMillis();

    summary.column_names = frame._names;
    summary.is_raw_frame = frame.isRawData();

    if (find_compatible_models) {
      Map<String, Model> compatible_models = findCompatibleModels(frame, all_models, all_models_cols);
      summary.compatible_models = compatible_models.keySet();
    }
  }


  /**
   * Fetch all Frames from the KV store.
   */
  protected static Map<String, Frame>fetchAll() {
    // Get all the fvec frame keys.
    return H2O.KeySnapshot.globalSnapshot().fetchAll(Frame.class); // Sort for pretty display and reliable ordering.
  }

  /**
   * For one or more Frame from the KV store, sumamrize and enhance them and Response containing a map of them.
   */
  private Response serveOneOrAll(Map<String, Frame> framesMap) {
    // returns empty sets if !this.find_compatible_models
    Pair<Map<String, Model>, Map<String, Set<String>>> models_info = fetchModels();
    Map<String, Model> all_models = models_info.getFirst();
    Map<String, Set<String>> all_models_cols = models_info.getSecond();

    Map<String, FrameSummary> frameSummaries = Frames.generateFrameSummaries(null, framesMap, find_compatible_models, all_models, all_models_cols);

    Map resultsMap = new LinkedHashMap();
    resultsMap.put("frames", frameSummaries);

    // If find_compatible_models then include a map of the Model summaries.  Should we put this on a separate switch?
    if (this.find_compatible_models) {
      Set<String> all_referenced_models = new TreeSet<String>();

      for (Map.Entry<String, FrameSummary> entry: frameSummaries.entrySet()) {
        FrameSummary summary = entry.getValue();
        all_referenced_models.addAll(summary.compatible_models);
      }

      Map<String, ModelSummary> modelSummaries = Models.generateModelSummaries(all_referenced_models, all_models, false, null, null);
      resultsMap.put("models", modelSummaries);
    }

    // TODO: temporary hack to get things going
    String json = gson.toJson(resultsMap);

    JsonObject result = gson.fromJson(json, JsonElement.class).getAsJsonObject();
    return Response.done(result);
  }


  /**
   * Score a frame with the given model.
   */
  protected static Response scoreOne(Frame frame, Model score_model) {

    water.ModelMetrics metrics = water.ModelMetrics.getFromDKV(score_model, frame);

    if (null == metrics) {
      // have to compute
      water.util.Log.debug("Cache miss: computing ModelMetrics. . .");
      long before = System.currentTimeMillis();
      Frame predictions = score_model.score(frame, true); // TODO: for now we're always calling adapt inside score
      long after = System.currentTimeMillis();

      ConfusionMatrix cm = new ConfusionMatrix(); // for regression this computes the MSE
      AUC auc = null;
      HitRatio hr = null;

      if (score_model.isClassifier()) {
        auc = new AUC();
//      hr = new HitRatio();
        score_model.calcError(frame, frame.vec(score_model.responseName()), predictions, predictions, "Prediction error:",
                                      true, 20, cm, auc, hr);
      } else {
        score_model.calcError(frame, frame.vec(score_model.responseName()), predictions, predictions, "Prediction error:",
                                      true, 20, cm, null, null);
      }

      // Now call AUC and ConfusionMatrix and maybe HitRatio
      metrics = new water.ModelMetrics(score_model.getUniqueId(),
                                       score_model.getModelCategory(),
                                       frame.getUniqueId(),
                                       after - before,
                                       after,
                                       (auc == null ? null : auc.data()),
                                       cm);

      // Put the metrics into the KV store
      metrics.putInDKV();
    } else {
      // it's already cached in the DKV
      water.util.Log.debug("using ModelMetrics from the cache. . .");
    }

    JsonObject metricsJson = metrics.toJSON();
    JsonArray metricsArray = new JsonArray();
    metricsArray.add(metricsJson);
    JsonObject result = new JsonObject();
    result.add("metrics", metricsArray);
    return Response.done(result);
  }


  @Override
  protected Response serve() {

    if (null == this.key) {
      return serveOneOrAll(fetchAll());
    } else {
      if (null == this.score_model) {
        // just serve it
        Frame frame = this.key;
        Map<String, Frame> framesMap = new TreeMap(); // Sort for pretty display and reliable ordering.
        framesMap.put(frame._key.toString(), frame);
        return serveOneOrAll(framesMap);
      } else {
        // score it
        return scoreOne(this.key, this.score_model);
      }
    }
  } // serve()

} // class Frames
