package hex.glm;

import water.*;
import water.api.DocGen;
import water.api.Request;

public class GLMModelUpdate extends Request2 {

  static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  @API(help="GLM Model Key", required=true, filter=GLMModelKeyFilter.class)
  Key _modelKey;

  class GLMModelKeyFilter extends H2OKey { public GLMModelKeyFilter() { super("",true); } }

  @API(help="GLM Model")
  GLMModel glm_model;

  @API(help = "decision threshold",filter=Default.class)
  double threshold;

  @API(help="lambda_value to be used in scoring",filter=Default.class)
  double lambda = Double.NaN;

  public static String link(String txt, Key model) {return link(txt,model,Double.NaN);}
  public static String link(String txt, Key model, double lambda) {
    return "<a href='GLMModelUpdate.html?_modelKey=" + model + "&lambda=" + lambda + "'>" + txt + "</a>";
  }
  public static Response redirect(Request req, Key modelKey) {
    return Response.redirect(req, "/2/GLMModelUpdate", "_modelKey", modelKey);
  }
  public static Response redirect(Request req, Key modelKey, double threshold, int lambdaId) {
    return Response.redirect(req, "/2/GLMModelUpdate", "_modelKey", modelKey,"threshold",threshold, "lambda_id", lambdaId);
  }
  @Override public boolean toHTML(StringBuilder sb){
    new GLMModelView(glm_model).toHTML(sb);
    return true;
  }

  @Override protected Response serve() {
    Value v = DKV.get(_modelKey);
    if(v != null){
      glm_model = v.get();
      glm_model.write_lock(null);
      int id = 0;
      for(; id < glm_model.submodels.length; ++id)
        if(glm_model.submodels[id].lambda_value == lambda){
          threshold = glm_model.submodels[id].validation.best_threshold;
          break;
        }
      glm_model.setSubmodelIdx(id);
      glm_model.update(null);
      glm_model.unlock(null);
    }
    return GLMModelView.redirect(this,_modelKey);
  }

//  @Override protected Response serve() {
//    Value v = DKV.get(_modelKey);
//    if(v == null)
//      return Response.poll(this, 0, 100, "_modelKey", _modelKey.toString());
//    glm_model = v.get();
//    if(Double.isNaN(lambda_value))lambda_value = glm_model.lambdas[glm_model.best_lambda_idx];
//    Job j;
//    if((j = Job.findJob(glm_model.job_key)) != null && j.exception != null)
//      return Response.error(j.exception);
//    if(DKV.get(glm_model.job_key) != null && j != null)
//      return Response.poll(this, (int) (100 * j.progress()), 100, "_modelKey", _modelKey.toString());
//    else
//      return Response.done(this);
//  }
}

