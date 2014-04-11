package water.api;

// import org.apache.commons.lang3.tuple.ImmutablePair;

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
  boolean find_matching_models = false;


  /////////////////
  // The Code (tm):
  /////////////////
  public static final Gson gson = new GsonBuilder().serializeSpecialFloatingPointValues().setPrettyPrinting().create();

  private class FrameSummary {
    public String[] column_names = { };
  }

  // TODO: refactor, since this is duplicated
  private Map whitelistJsonObject(JsonObject unfiltered, Set<String> whitelist) {
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
   * Summarize fields in water.fvec.Frame.
   */
  private void summarizeFrame(FrameSummary summary, Frame frame) {
    summary.column_names = frame._names;
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

    for (Map.Entry<String, Frame> entry : framesMap.entrySet()) {
      String keyString = entry.getKey();
      FrameSummary summary = new FrameSummary();
      Frame frame = entry.getValue();
      summarizeFrame(summary, frame);
      frameSummariesMap.put(keyString, summary);
    }

    Map resultsMap = new HashMap();
    resultsMap.put("frames", frameSummariesMap);

    // TODO: temporary hack to get things going
    String json = gson.toJson(resultsMap);

    JsonObject result = gson.fromJson(json, JsonElement.class).getAsJsonObject();
    return Response.done(result);
  }


  private Response serveOne(Frame frame) {
    Map frameSummariesMap = new TreeMap(); // Sort for pretty display and reliable ordering.
    FrameSummary summary = new FrameSummary();

    summarizeFrame(summary, frame);
    frameSummariesMap.put(frame._key.toString(), summary);

    Map resultsMap = new HashMap();
    resultsMap.put("frames", frameSummariesMap);

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
