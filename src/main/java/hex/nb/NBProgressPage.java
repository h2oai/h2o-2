package hex.nb;

import water.*;
import water.api.*;
import water.api.RequestBuilders.Response;

public class NBProgressPage extends Progress2 {
  /** Return {@link Response} for finished job. */
  @Override protected Response jobDone(final Key dst) {
    return NBModelView.redirect(this, dst);
  }

  public static Response redirect(Request req, Key jobkey, Key dest) {
    return Response.redirect(req, "/2/NBProgressPage", JOB_KEY, jobkey, DEST_KEY, dest );
  }

  @Override public boolean toHTML( StringBuilder sb ) {
    Job jjob = Job.findJob(job_key);
    if (jjob ==null) return true;
    Value v = DKV.get(jjob.dest());
    if(v != null){
      NBModel m = v.get();
      m.generateHTML("Naive Bayes Model", sb);
    } else
      sb.append("<b>No model yet.</b>");
    return true;
  }
}
