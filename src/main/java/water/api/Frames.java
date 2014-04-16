package water.api;

import org.apache.commons.math3.util.Pair;

import java.lang.reflect.Field;
import java.util.*;

import com.google.gson.*;
import hex.deeplearning.DeepLearning;
import hex.drf.DRF;
import hex.gbm.GBM;
import hex.glm.GLM2;
import water.*;
import water.fvec.Frame;
import water.util.Log;

import water.api.Models.ModelSummary;

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

  @API(help="Should we adapt() the Frame to the Model?", required=false, filter=Default.class)
  boolean adapt = true;


  /////////////////
  // The Code (tm):
  /////////////////
  public static final Gson gson = new GsonBuilder().serializeSpecialFloatingPointValues().setPrettyPrinting().create();

  public static final class FrameSummary {
    public String[] column_names = { };
    public Set<String> compatible_models = new HashSet<String>();
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
          // TODO: we have to free the vecTrash vectors?
          // Frame vecTrash = inputs[1];

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
    summary.column_names = frame._names;

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
    //
    // NOTE: globalKeySet filters by class when it pulls stuff from other nodes,
    // but still returns local keys of all types so we need to filter below.
    Set<Key> keySet = H2O.globalKeySet("water.fvec.Frame"); // filter by class, how cool is that?

    Map<String, Frame> framesMap = new TreeMap(); // Sort for pretty display and reliable ordering.

    for (Key key : keySet) {
      if( !key.user_allowed() ) // Also filter out for user-keys
        continue;
      if( H2O.get(key) == null )
        continue;

      String keyString = key.toString();

      Value value = DKV.get(key);
      Iced pojo = value.get();

      if (! (pojo instanceof Frame))
        continue;
      Frame frame = (Frame)pojo;

      framesMap.put(keyString, frame);
    }

    return framesMap;
  }


  /**
   * Fetch all the Frames from the KV store, sumamrize and enhance them, and return a map of them.
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
  protected static Response scoreOne(Frame frame, Model score_model, boolean adapt) {
    Frame input = frame;

    if (adapt) {
      Frame[] inputs = score_model.adapt(frame, false);
      input = inputs[0];
      // TODO: we have to free the vecTrash vectors?
      Frame vecTrash = inputs[1];
    }

    long before = System.currentTimeMillis();
    Frame predictions = score_model.score(frame, false);
    long after = System.currentTimeMillis();

    ConfusionMatrix cm = new ConfusionMatrix(); // for regression this computes the MSE
    AUC auc = null;
    HitRatio hr = null;
    double error = 0.0d;

    if (score_model.isClassifier()) {
      auc = new AUC();
//      hr = new HitRatio();
      error = score_model.calcError(input, input.vec(score_model.responseName()), predictions, predictions, "Prediction error:",
                                    true, 20, cm, auc, hr);
    } else {
      error = score_model.calcError(input, input.vec(score_model.responseName()), predictions, predictions, "Prediction error:",
                                    true, 20, cm, null, null);
    }

    // Now call AUC and ConfusionMatrix and maybe HitRatio
    Map metrics = new LinkedHashMap();
    metrics.put("model", score_model._key.toString());
    metrics.put("frame", frame._key.toString());

    metrics.put("duration_in_ms", after - before);

    metrics.put("error", error);
    metrics.put("cm", cm.toJSON());
    metrics.put("auc", auc.toJSON());
    metrics.put("hr", hr);

    Map resultsMap = new LinkedHashMap();
    resultsMap.put("metrics", metrics);

    // TODO: temporary hack to get things going
    String json = gson.toJson(resultsMap);
    // Log.info("Json for results: " + json);

    JsonObject result = gson.fromJson(json, JsonElement.class).getAsJsonObject();
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
        return scoreOne(this.key, this.score_model, this.adapt);
      }
    }

  } // serve()

}
