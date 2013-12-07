package water.api;

import com.google.gson.JsonObject;
import hex.drf.DRF.DRFModel;
import water.*;
import water.api.RequestBuilders.Response;
import water.api.RequestServer.API_VERSION;

public class DRFProgressPage extends Progress2 {
  /** Return {@link Response} for finished job. */
  @Override protected Response jobDone(final Job job, final Key dst) {
    JsonObject args = new JsonObject();
    args.addProperty(MODEL_KEY, job.dest().toString());
    return DRFModelView.redirect(this, job.dest());
  }

  public static Response redirect(Request req, Key jobkey, Key dest) {
    return Response.redirect(req, "/2/DRFProgressPage", JOB_KEY, jobkey, DEST_KEY, dest);
  }

  @Override public boolean toHTML( StringBuilder sb ) {
    Job jjob = Job.findJob(job_key);
    DRFModel m = UKV.get(jjob.dest());
    if (m!=null) m.generateHTML("DRF Model", sb);
    else DocGen.HTML.paragraph(sb, "Pending...");
    return true;
  }
  @Override public API_VERSION[] supportedVersions() { return SUPPORTS_V1_V2; }
}
