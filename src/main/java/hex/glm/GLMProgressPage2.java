package hex.glm;


import water.*;
import water.api.Progress2;
import water.api.Request;

import com.google.gson.JsonObject;

public class GLMProgressPage2 extends Progress2 {
  /** Return {@link Response} for finished job. */
  protected Response jobDone(final Job job, final String dst) {
    JsonObject args = new JsonObject();
    args.addProperty("model_key", job.dest().toString());
    return GLMModelView.redirect(this, job.dest());
  }

  public static Response redirect(Request req, Key jobkey, Key dest) {
    return new Response(Response.Status.redirect, req, -1, -1, "GLMProgressPage2", "job", jobkey, "dst_key", dest );
  }

  @Override public boolean toHTML( StringBuilder sb ) {
    Job jjob = Job.findJob(job);
    Value v = DKV.get(jjob.dest());
    if(v != null){
      GLMModel m = v.get();
      m.generateHTML("GLM Model", sb);
    } else
      sb.append("<b>No model yet.</b>");
    return true;
  }

}
