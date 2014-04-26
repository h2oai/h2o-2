package water;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import water.Model;
import water.Model.ModelCategory;
import water.api.AUC;
import water.api.ConfusionMatrix;
import water.api.DocGen;
import water.api.Request.API;
import water.api.Request.Default;
import water.fvec.Frame;


/**
 * Container to hold the metric for a model as scored on a specific frame.
 */

public final class ModelMetrics extends Iced {
  static final int API_WEAVER = 1;
  static public DocGen.FieldDoc[] DOC_FIELDS;

  @API(help="The unique ID (key / uuid / creation timestamp) for the model used for this scoring run.", required=false, filter=Default.class, json=true)
  public UniqueId model = null;
  @API(help="The category (e.g., Clustering) for the model used for this scoring run.", required=false, filter=Default.class, json=true)
  public Model.ModelCategory model_category = null;
  @API(help="The unique ID (key / uuid / creation timestamp) for the frame used for this scoring run.", required=false, filter=Default.class, json=true)
  public UniqueId frame = null;

  @API(help="The error measure for this scoring run.", required=false, filter=Default.class, json=true)
  public double error_measure = Double.MAX_VALUE;
  @API(help="The duration in mS for this scoring run.", required=false, filter=Default.class, json=true)
  public long duration_in_ms =-1L;
  @API(help="The time in mS since the epoch for the start of this scoring run.", required=false, filter=Default.class, json=true)
  public long scoring_time = -1L;

  @API(help="The AUC object for this scoring run.", required=false, filter=Default.class, json=true)
  public AUC auc = null;
  @API(help="The ConfusionMatrix object for this scoring run.", required=false, filter=Default.class, json=true)
  public ConfusionMatrix cm = null;

  public ModelMetrics(UniqueId model, ModelCategory model_category, UniqueId frame, double error_measure, long duration_in_ms, long scoring_time, AUC auc, ConfusionMatrix cm) {
    this.model = model;
    this.model_category = model_category;
    this.frame = frame;
    this.error_measure = (float)error_measure;
    this.duration_in_ms = duration_in_ms;
    this.scoring_time = scoring_time;

    this.auc = auc;
    this.cm = cm;
  }

  public static String keyName(Model model, Frame frame) {
    return "modelmetrics_" + model.getUniqueId().getUuid() + "_on_" + frame.getUniqueId().getUuid();
  }

  public static String keyName(UniqueId model, UniqueId frame) {
    return "modelmetrics_" + model.getUuid() + "_on_" + frame.getUuid();
  }

  public String keyName() {
    return "modelmetrics_" + this.model.getUuid() + "_on_" + this.frame.getUuid();
  }

  public void putInDKV() {
    String keyname = this.keyName();
    Key metricsKey = Key.makeUserHidden(Key.make(keyname));
    DKV.put(metricsKey, this);

  }

  public static ModelMetrics getFromDKV(Model model, Frame frame) {
    Value v = DKV.get(Key.make(keyName(model, frame)));

    if (null == v)
      return null;

    return (ModelMetrics)v.get();
  }

  public static ModelMetrics getFromDKV(UniqueId model, UniqueId frame) {
    Value v = DKV.get(Key.make(keyName(model, frame)));

    if (null == v)
      return null;

    return (ModelMetrics)v.get();
  }

  public JsonObject toJSON() {
    final String json = new String(writeJSON(new AutoBuffer()).buf());
    if (json.length() == 0) return new JsonObject();
    JsonObject jo = (JsonObject)new JsonParser().parse(json);

    if (jo.has("model"))
      jo.getAsJsonObject("model").addProperty("model_category", this.model_category.toString());

    return jo;
  }
}
