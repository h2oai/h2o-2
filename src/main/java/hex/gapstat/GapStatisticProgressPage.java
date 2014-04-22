package hex.gapstat;

import water.DKV;
import water.Job;
import water.Key;
import water.Value;
import water.api.Progress2;
import water.api.Request;

public class GapStatisticProgressPage extends Progress2 {
  /** Return {@link Response} for finished job. */
  @Override protected Response jobDone(final Key dst) {
    return GapStatisticModelView.redirect(this, dst);
  }

  public static Response redirect(Request req, Key jobkey, Key dest) {
    return Response.redirect(req, "/2/GapStatisticProgressPage", JOB_KEY, jobkey, DEST_KEY, dest );
  }

  @Override public boolean toHTML( StringBuilder sb ) {
    Job jjob = Job.findJob(job_key);
    if (jjob ==null) return true;
    Value v = DKV.get(jjob.dest());
    if(v != null){
      GapStatisticModel m = v.get();
      m.generateHTML("Gap Statistic", sb);
    } else
      sb.append("<b>No model yet.</b>");
    return true;
  }
}