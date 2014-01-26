package water.api;

import hex.NeuralNet.NeuralNetModel;
import water.Key;
import water.Request2;
import water.UKV;

public class NeuralNetModelView extends Request2 {
  static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  @API(help="NeuralNet Model Key", required=true, filter=NeuralNetModelKeyFilter.class)
  Key _modelKey;
  class NeuralNetModelKeyFilter extends H2OKey { public NeuralNetModelKeyFilter() { super("model_key",true); } }

  @API(help="NeuralNet Model")
  NeuralNetModel neuralnet_model;

  public static String link(String txt, Key model) {
    return "<a href='NeuralNetModelView.html?_modelKey=" + model + "'>" + txt + "</a>";
  }

  public static Response redirect(Request req, Key modelKey) {
    return Response.redirect(req, "/2/NeuralNetModelView", "_modelKey", modelKey);
  }

  @Override public boolean toHTML(StringBuilder sb){
    neuralnet_model.generateHTML("NeuralNet Model", sb);
    return true;
  }

  @Override protected Response serve() {
    neuralnet_model = UKV.get(_modelKey);
    if (neuralnet_model == null) return Response.error("Model '" + _modelKey + "' not found!");
    else return Response.done(this);
  }

  @Override public void toJava(StringBuilder sb) { neuralnet_model.toJavaHtml(sb); }
  @Override protected String serveJava() {
    NeuralNetModel m = UKV.get(_modelKey);
    if (m!=null)
      return m.toJava();
    else
      return "";
  }
}
