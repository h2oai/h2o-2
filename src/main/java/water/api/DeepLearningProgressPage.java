package water.api;

import hex.deeplearning.DeepLearningModel;
import water.Job;
import water.Key;
import water.UKV;

public class DeepLearningProgressPage extends Progress2 {
  /** Return {@link water.api.RequestBuilders.Response} for finished job. */
  @Override protected Response jobDone(final Key dst) {
    return DeepLearningModelView.redirect(this, dst);
  }

  public static Response redirect(Request req, Key jobkey, Key dest) {
    return Response.redirect(req, "/2/DeepLearningProgressPage", JOB_KEY, jobkey, DEST_KEY, dest);
  }

  @Override public boolean toHTML( StringBuilder sb ) {
    Job jjob = Job.findJob(job_key);
    if (jjob ==null) return true;
    DeepLearningModel m = UKV.get(jjob.dest());
    if (m!=null) m.generateHTML("Deep Learning Model", sb);
    else DocGen.HTML.paragraph(sb, "Pending...");
    return true;
  }
}

