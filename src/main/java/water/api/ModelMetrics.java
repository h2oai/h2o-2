package water.api;

import java.util.*;

import dontweave.gson.*;

import water.DKV;
import water.Key;
import water.Request2;
import water.H2O;
import water.Value;
import water.Iced;

import water.fvec.Frame;
import water.util.Log;

public class ModelMetrics extends Request2 {

  ///////////////////////
  // Request2 boilerplate
  ///////////////////////
  static final int API_WEAVER=1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  // This Request supports the HTML 'GET' command, and this is the help text
  // for GET.
  static final String DOC_GET = "Return the list of model metrics.";

  public static String link(Key k, String content){
    return  "<a href='/2/ModelMetrics'>" + content + "</a>";
  }


  ////////////////
  // Query params:
  ////////////////
  @API(help="An existing H2O ModelMetrics key.", required=false, filter=Default.class)
  water.ModelMetrics key = null;

  @API(help="Expand the referenced Model and Frame objects.", required=false, filter=Default.class)
  boolean expand_references = false;


  /////////////////
  // The Code (tm):
  /////////////////
  public static final Gson gson = new GsonBuilder().serializeSpecialFloatingPointValues().setPrettyPrinting().create();

  /**
   * Fetch all ModelMetrics from the KV store.
   */
  protected static List<water.ModelMetrics>fetchAll() {
    return new ArrayList<water.ModelMetrics>(H2O.KeySnapshot.globalSnapshot().fetchAll(water.ModelMetrics.class).values());
  }


  /**
   * For one or more water.ModelMetrics from the KV store return Response containing a map of them.
   */
  private Response serveOneOrAll(List<water.ModelMetrics> list) {
    JsonArray metricsArray = new JsonArray();

    for (water.ModelMetrics metrics : list) {
      JsonObject metricsJson = metrics.toJSON();
      metricsArray.add(metricsJson);
    }

    JsonObject result = new JsonObject();
    result.add("metrics", metricsArray);
    return Response.done(result);
  }


  @Override
  protected Response serve() {
    if (null == this.key) {
      return serveOneOrAll(fetchAll());
    } else {
      // just serve it
      water.ModelMetrics mm = this.key;
      List<water.ModelMetrics> list = new ArrayList();
      list.add(mm);
      return serveOneOrAll(list);
    }
  } // serve()

} // class Frames
