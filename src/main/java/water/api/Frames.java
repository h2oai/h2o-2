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
        compatible_models.put(entry.getKey(), all_models.get(entry.getKey()));
      }
    }

    return compatible_models;
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
  protected Map<String, Frame>fetchAll() {
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
  private Response serveAll() {
    Map<String, FrameSummary> frameSummariesMap = new TreeMap<String, FrameSummary>(); // Sort for pretty display and reliable ordering.
    Map<String, Frame> framesMap = fetchAll();

    // returns empty sets if !this.find_compatible_models
    Pair<Map<String, Model>, Map<String, Set<String>>> models_info = fetchModels();
    Map<String, Model> all_models = models_info.getFirst();
    Map<String, Set<String>> all_models_cols = models_info.getSecond();

    Set<String> all_referenced_models = new TreeSet<String>();
    for (Map.Entry<String, Frame> entry : framesMap.entrySet()) {
      String keyString = entry.getKey();
      FrameSummary summary = new FrameSummary();
      Frame frame = entry.getValue();

      summarizeAndEnhanceFrame(summary, frame, this.find_compatible_models, all_models, all_models_cols);
      all_referenced_models.addAll(summary.compatible_models);
      frameSummariesMap.put(keyString, summary);
    }

    Map resultsMap = new HashMap();
    resultsMap.put("frames", frameSummariesMap);

    // If find_compatible_models then include a map of the model summaries.  Should we put this on a separate switch?
    if (this.find_compatible_models) {
      Map<String, ModelSummary> modelSummaries = Models.generateModelSummaries(all_referenced_models, all_models);
      resultsMap.put("models", modelSummaries);
    }

    // TODO: temporary hack to get things going
    String json = gson.toJson(resultsMap);

    JsonObject result = gson.fromJson(json, JsonElement.class).getAsJsonObject();
    return Response.done(result);
  }


  private Response serveOne(Frame frame) {
    Map frameSummariesMap = new TreeMap(); // Sort for pretty display and reliable ordering.
    FrameSummary summary = new FrameSummary();

    // returns empty sets if !this.find_compatible_models
    Pair<Map<String, Model>, Map<String, Set<String>>> models_info = fetchModels();
    Map<String, Model> all_models = models_info.getFirst();
    Map<String, Set<String>> all_models_cols = models_info.getSecond();

    summarizeAndEnhanceFrame(summary, frame, this.find_compatible_models, all_models, all_models_cols);
    frameSummariesMap.put(frame._key.toString(), summary);

    Set<String> all_referenced_models = new TreeSet<String>();
    all_referenced_models.addAll(summary.compatible_models);

    Map resultsMap = new HashMap();
    resultsMap.put("frames", frameSummariesMap);

    // If find_compatible_models then include a map of the model summaries.  Should we put this on a separate switch?
    if (this.find_compatible_models) {
      Map<String, ModelSummary> modelSummaries = Models.generateModelSummaries(all_referenced_models, all_models);
      resultsMap.put("models", modelSummaries);
    }

    // TODO: temporary hack to get things going
    String json = gson.toJson(resultsMap);
    // Log.info("Json for results: " + json);

    JsonObject result = gson.fromJson(json, JsonElement.class).getAsJsonObject();
    return Response.done(result);

  }

  @Override
  protected Response serve() {

    if (null == this.key) {
      return serveAll();
    } else {
      return serveOne(this.key);
    }

  } // serve()

}
