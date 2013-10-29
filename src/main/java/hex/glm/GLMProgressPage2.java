package hex.glm;


import water.*;
import water.api.Progress2;
import water.api.Request;
import water.api.RequestBuilders.Response;

import com.google.gson.JsonObject;

public class GLMProgressPage2 extends Progress2 {
  /** Return {@link Response} for finished job. */
  @Override
  protected Response jobDone(final Job job, final Key dst) {
    JsonObject args = new JsonObject();
    args.addProperty(MODEL_KEY, job.dest().toString());
    return GLMModelView.redirect(this, job.dest());
  }
  public static Response redirect(Request req, Key jobkey, Key dest) {
    return new Response(Response.Status.redirect, req, -1, -1, "GLMProgressPage2", JOB_KEY, jobkey, DEST_KEY, dest );
  }
  @Override public boolean toHTML( StringBuilder sb ) {
    Job jjob = Job.findJob(job_key);
    Value v = DKV.get(jjob.dest());
    if(v != null){
      GLMModel m = v.get();
      m.generateHTML("GLM Model", sb);
    } else
      sb.append("<b>No model yet.</b>");
    return true;
  }
}
