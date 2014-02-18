package water.api;

import hex.nn.NNModel;
import water.Job;
import water.Key;
import water.UKV;

public class NNProgressPage extends Progress2 {
  /** Return {@link water.api.RequestBuilders.Response} for finished job. */
  @Override protected Response jobDone(final Key dst) {
    return NNModelView.redirect(this, dst);
  }

  public static Response redirect(Request req, Key jobkey, Key dest) {
    return Response.redirect(req, "/2/NNProgressPage", JOB_KEY, jobkey, DEST_KEY, dest);
  }

  @Override public boolean toHTML( StringBuilder sb ) {
    Job jjob = Job.findJob(job_key);
    if (jjob ==null) return true;
    NNModel m = UKV.get(jjob.dest());
    if (m!=null) m.generateHTML("NeuralNet Model", sb);
    else DocGen.HTML.paragraph(sb, "Pending...");
    return true;
  }
}

