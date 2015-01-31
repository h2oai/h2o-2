package water.api;

import hex.deeplearning.DeepLearning;
import hex.deeplearning.DeepLearningModel;
import hex.deeplearning.Neurons;
import water.Key;
import water.Request2;
import water.UKV;

public class DeepLearningModelView extends Request2 {
  static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  @API(help="Deep Learning Model Key", required=true, filter=DeepLearningModelKeyFilter.class)
  Key _modelKey;
  class DeepLearningModelKeyFilter extends H2OKey { public DeepLearningModelKeyFilter() { super("model_key",true); } }

  @API(help="Deep Learning Model")
  DeepLearningModel deeplearning_model;

  public static String link(String txt, Key model) {
    return "<a href='DeepLearningModelView.html?_modelKey=" + model + "'>" + txt + "</a>";
  }

  public static Response redirect(Request req, Key modelKey) {
    return Response.redirect(req, "/2/DeepLearningModelView", "_modelKey", modelKey);
  }

  @Override public boolean toHTML(StringBuilder sb){
    if (deeplearning_model != null)
      deeplearning_model.generateHTML("Deep Learning Model", sb);
    return true;
  }

  @Override protected Response serve() {
    deeplearning_model = UKV.get(_modelKey);
    if (deeplearning_model == null) return Response.error("Model '" + _modelKey + "' not found!");
    else return Response.done(this);
  }

  @Override public void toJava(StringBuilder sb) {
    deeplearning_model.toJavaHtml(sb);
  }
  @Override protected String serveJava() {
    deeplearning_model = UKV.get(_modelKey);
    if (deeplearning_model!=null)
      return deeplearning_model.toJava();
    else
      return "";
  }
}
