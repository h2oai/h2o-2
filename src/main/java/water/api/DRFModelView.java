package water.api;

import hex.gbm.DRF.DRFModel;
import water.*;
import water.api.RequestBuilders.Response;

public class DRFModelView extends Request2 {
  static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  @API(help="DRF Model Key", required=true, filter=DRFModelKeyFilter.class)
  Key _modelKey;
  class DRFModelKeyFilter extends H2OKey { public DRFModelKeyFilter() { super("model_key",true); } }

  public static Response redirect(Request req, Key modelKey) {
    return new Response(Response.Status.redirect, req, -1, -1, "DRFModelView", "_modelKey", modelKey);
  }

  public boolean toHTML(StringBuilder sb){
    DRFModel m = DKV.get(_modelKey).get();
    m.generateHTML("DRF Model", sb);
    return true;
  }

  @Override protected Response serve() {
    return new Response(Response.Status.done,this,-1,-1,null);
  }
}
