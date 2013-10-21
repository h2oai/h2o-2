package water.api;

import com.google.gson.JsonObject;

import hex.gbm.DRF.DRFModel;
import water.*;
import water.api.RequestBuilders.Response;

public class DRFProgressPage extends Progress2 {
  /** Return {@link Response} for finished job. */
  @Override protected Response jobDone(final Job job, final Key dst) {
    JsonObject args = new JsonObject();
    args.addProperty("model_key", job.dest().toString());
    return DRFModelView.redirect(this, job.dest());
  }

  public static Response redirect(Request req, Key jobkey, Key dest) {
    return new Response(Response.Status.redirect, req, -1, -1, "DRFProgressPage", "job", jobkey, "dst_key", dest );
  }

  @Override public boolean toHTML( StringBuilder sb ) {
    Job jjob = Job.findJob(job_key);
    DRFModel m = DKV.get(jjob.dest()).get();
    m.generateHTML("DRF Model", sb);
    return true;
  }
}
