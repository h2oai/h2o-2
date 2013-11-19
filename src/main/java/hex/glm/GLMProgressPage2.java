package hex.glm;


import hex.GridSearch;
import hex.glm.GLM2.GLMGridSearch;
import water.*;
import water.api.*;
import water.api.RequestServer.API_VERSION;

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
    return Response.redirect(req, "/2/GLMProgressPage2", JOB_KEY, jobkey, DEST_KEY, dest );
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


  public static class GLMGrid extends Progress2 {
    API_VERSION _v;
    boolean _isDone;
    public static Response redirect(Request req, Key jobkey, Key dest,API_VERSION v) {
      return Response.redirect(req, href2(v), JOB_KEY, jobkey, DEST_KEY, dest);
    }

    private static String href2(API_VERSION v) {
      return v.prefix() + "GLMGrid";
    }
    @Override protected String href(API_VERSION v) {
      return href2(v);
    }
    @Override public boolean toHTML(StringBuilder sb) {
      try{
        GLM2.GLMGrid grid = UKV.get(destination_key);
        grid.toHTML(sb);
      }catch(Throwable t){
//        t.printStackTrace();
      }
      return true;
    }
    @Override protected Response jobDone(final Job job, final Key dst) {
      _isDone = true;
      return Response.done(this);
    }
  }
}
