package hex.pca;

import water.*;
import water.api.Progress2;
import water.api.Request;
import water.api.RequestBuilders.Response;

import com.google.gson.JsonObject;

public class PCAProgressPage extends Progress2 {
  /** Return {@link Response} for finished job. */
  @Override protected Response jobDone(final Job job, final String dst) {
    JsonObject args = new JsonObject();
    args.addProperty("model_key", job.dest().toString());
    return PCAModelView.redirect(this, job.dest());
  }

  public static Response redirect(Request req, Key jobkey, Key dest) {
    return new Response(Response.Status.redirect, req, -1, -1, "PCAProgressPage", "job", jobkey, "dst_key", dest );
  }

  @Override public boolean toHTML(StringBuilder sb) {
    Job jjob = Job.findJob(Key.make(job.value()));
    Value v = DKV.get(jjob.dest());
    PCAModel m = v != null ? (PCAModel) v.get() : null;
    if(v != null)
      m.generateHTML("GLM Model", sb);
    else
      sb.append("<b>No model yet.</b>");
    return true;
  }
}

