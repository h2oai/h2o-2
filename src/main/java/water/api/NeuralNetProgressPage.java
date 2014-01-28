package water.api;

import com.google.gson.JsonObject;
import hex.NeuralNet;
import water.Job;
import water.Key;
import water.UKV;

public class NeuralNetProgressPage extends Progress2 {
  /** Return {@link Response} for finished job. */
  @Override protected Response jobDone(final Job job, final Key dst) {
    JsonObject args = new JsonObject();
    args.addProperty(MODEL_KEY, job.dest().toString());
    return NeuralNetModelView.redirect(this, job.dest());
  }

  public static Response redirect(Request req, Key jobkey, Key dest) {
    return Response.redirect(req, "/2/NeuralNetProgressPage", JOB_KEY, jobkey, DEST_KEY, dest);
  }

  @Override public boolean toHTML( StringBuilder sb ) {
    Job jjob = Job.findJob(job_key);
    NeuralNet.NeuralNetModel m = UKV.get(jjob.dest());
    if (m!=null) m.generateHTML("NeuralNet Model", sb);
    else DocGen.HTML.paragraph(sb, "Pending...");
    return true;
  }
}

