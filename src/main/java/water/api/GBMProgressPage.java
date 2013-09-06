package water.api;

import com.google.gson.JsonObject;

import hex.gbm.GBM.GBMModel;
import water.*;
import water.api.RequestBuilders.Response;

public class GBMProgressPage extends Progress2 {
  /** Return {@link Response} for finished job. */
  protected Response jobDone(final Job job, final String dst) {
    JsonObject args = new JsonObject();
    args.addProperty("model_key", job.dest().toString());
    return GBMModelView.redirect(this, job.dest());
  }

  public static Response redirect(Request req, Key jobkey, Key dest) {
    return new Response(Response.Status.redirect, req, -1, -1, "GBMProgressPage", "job", jobkey, "dst_key", dest );
  }

  @Override public boolean toHTML( StringBuilder sb ) {
    Job jjob = Job.findJob(Key.make(job.value()));
    DocGen.HTML.title(sb,jjob.description);
    DocGen.HTML.section(sb,dst_key.value());
    GBMModel m = DKV.get(jjob.dest()).get();
    GBMModelView.generateHTML(m, sb);
    return true;
  }
}
