package hex.glm;

import hex.glm.GLM2.GLMGrid;
import water.*;
import water.api.*;

public class GLMGridProgress extends Progress2 {

  @Override public boolean toHTML(StringBuilder sb){
    Value v = DKV.get(destination_key);
    if(v == null)return true;
    GLMGrid g = v.get();
    return new GLMGridView(g).toHTML(sb);
  }

  /** Return {@link Response} for finished job. */
  @Override
  protected Response jobDone(final Key dst) {
    return GLMGridView.redirect(this, dst);
  }

  /** Return default progress {@link Response}. */
  @Override
  protected Response jobInProgress(final Job job, final Key dst) {
    progress = job.progress();
    return Response.poll(this, (int) (100 * progress), 100, "job_key", job_key.toString(), "destination_key",
        destination_key.toString());
  }
  public static Response redirect(Request req, Key jobkey, Key dest) {
    return Response.redirect(req, "/2/GLMProgress", "job_key", jobkey, "destination_key", dest);
  }
}
