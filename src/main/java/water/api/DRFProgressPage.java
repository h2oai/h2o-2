package water.api;

import hex.drf.DRF.DRFModel;
import water.Job;
import water.Key;
import water.UKV;

public class DRFProgressPage extends Progress2 {
  /** Return {@link Response} for finished job. */
  @Override protected Response jobDone(final Key dst) {
    return DRFModelView.redirect(this, dst);
  }

  public static Response redirect(Request req, Key jobkey, Key dest) {
    return Response.redirect(req, "/2/DRFProgressPage", JOB_KEY, jobkey, DEST_KEY, dest);
  }

  @Override public boolean toHTML( StringBuilder sb ) {
    Job jjob = Job.findJob(job_key);
    if (jjob ==null) return true;
    DRFModel m = UKV.get(jjob.dest());
    if (m!=null) m.generateHTML("DRF Model", sb);
    else DocGen.HTML.paragraph(sb, "Pending...");
    return true;
  }
}
