package water.api;

import hex.gbm.GBM.GBMModel;
import water.Job;
import water.Key;
import water.UKV;

public class GBMProgressPage extends Progress2 {
  /** Return {@link Response} for finished job. */
  @Override protected Response jobDone(final Key dst) {
    return GBMModelView.redirect(this, dst);
  }

  public static Response redirect(Request req, Key jobkey, Key dest) {
    return Response.redirect(req, "/2/GBMProgressPage", JOB_KEY, jobkey, DEST_KEY, dest);
  }

  @Override public boolean toHTML( StringBuilder sb ) {
    GBMModel m = UKV.get(destination_key);
    if (m!=null) m.generateHTML("GBM Model", sb);
    else DocGen.HTML.paragraph(sb, "Pending...");
    return true;
  }
}
