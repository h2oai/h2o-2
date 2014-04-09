//package hex.speedrf;
//
//import water.*;
//import water.api.*;
//
//public class SPDRFProgress extends Progress2 {
//
//
//  @Override public boolean toHTML(StringBuilder sb){
//    Value v = DKV.get(destination_key);
//    if(v == null)return true;
//    RFModel m = v.get();
//    return new SPDRFView(m).toHTML(sb);
//  }
//
//  /** Return {@link water.api.RequestBuilders.Response} for finished job. */
//  @Override
//  protected Response jobDone(final Key dst) {
//    return SPDRFView.redirect(this, dst);
//  }
//
//  /** Return default progress {@link water.api.RequestBuilders.Response}. */
//  @Override
//  protected Response jobInProgress(final Job job, final Key dst) {
//    progress = job.progress();
//    return Response.poll(this, (int) (100 * progress), 100, "job_key", job_key.toString(), "destination_key",
//            destination_key.toString());
//  }
//  public static Response redirect(Request req, Key jobkey, Key dest) {
//    return Response.redirect(req, "/2/SPDRFProgress", "job_key", jobkey, "destination_key", dest);
//  }
//}
