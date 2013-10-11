package water.api;

import hex.gbm.GBM.GBMModel;
import water.*;

import com.google.gson.JsonObject;

public class GBMProgressPage extends Progress2 {
  /** Return {@link Response} for finished job. */
  @Override protected Response jobDone(Job job, Key dst) {
    JsonObject args = new JsonObject();
    args.addProperty("model_key", job.dest().toString());
    return GBMModelView.redirect(this, job.dest());
  }

  public static Response redirect(Request req, Key jobkey, Key dest) {
    return new Response(Response.Status.redirect, req, -1, -1, "GBMProgressPage", "job", jobkey, "dst_key", dest);
  }

  @Override public boolean toHTML(StringBuilder sb) {
    Value value = DKV.get(job.dest());
    if( value == null ) DocGen.HTML.paragraph(sb, "Pending...");
    else ((GBMModel)value.get()).generateHTML("GBM Model", sb);
    return true;
  }
}
