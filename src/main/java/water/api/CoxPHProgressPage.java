package water.api;

import hex.CoxPH.CoxPHModel;
import water.Key;
import water.UKV;

public class CoxPHProgressPage extends Progress2 {
  /** Return {@link water.api.RequestBuilders.Response} for finished job. */
  @Override protected Response jobDone(final Key dest) {
    return CoxPHModelView.redirect(this, dest);
  }

  public static Response redirect(Request req, Key jobkey, Key dest) {
    return Response.redirect(req, "/2/CoxPHProgressPage", JOB_KEY, jobkey, DEST_KEY, dest);
  }

  @Override public boolean toHTML(StringBuilder sb) {
    CoxPHModel m = UKV.get(destination_key);
    if (m != null)
      m.generateHTML("Cox Proportional Hazards Model", sb);
    else
      DocGen.HTML.paragraph(sb, "Pending...");
    return true;
  }
}
