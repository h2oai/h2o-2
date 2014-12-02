package hex.singlenoderf;

import water.DKV;
import water.Job;
import water.Key;
import water.Value;
import water.api.Progress2;
import water.api.Request;

public class SpeeDRFProgressPage extends Progress2 {
  /** Return {@link water.api.RequestBuilders.Response} for finished job. */
  @Override protected Response jobDone(final Key dst) {
    return SpeeDRFModelView.redirect(this, dst);
  }

  public static Response redirect(Request req, Key jobkey, Key dest) {
    return Response.redirect(req, "/2/SpeeDRFProgressPage", JOB_KEY, jobkey, DEST_KEY, dest);
  }

  @Override public boolean toHTML( StringBuilder sb ) {
    Job jjob = Job.findJob(job_key);
    if (jjob ==null) return true;
    Value v = DKV.get(jjob.dest());
    if(v != null){
      SpeeDRFModel m = v.get();
      m.generateHTML("SpeeDRF", sb);
    } else
      sb.append("<b>No model yet.</b>");
    return true;
  }
}
