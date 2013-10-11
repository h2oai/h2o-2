package water.api;

import water.*;

public class Progress2 extends Request2 {
  static final int API_WEAVER = 1;
  static public DocGen.FieldDoc[] DOC_FIELDS;
  static final String DOC_GET = "Track progress of an ongoing Job";

  @API(help = "The Job being tracked", filter = Default.class)
  public Job job;

  @API(help = "The destination key being produced", filter = Default.class)
  public Key dst_key;

  public static Response redirect(Request req, Key jobkey, Key dest) {
    return new Response(Response.Status.redirect, req, -1, -1, "Progress2", "job", jobkey, "dst_key", dest);
  }

  @Override protected Response serve() {
    if( job != null && job.exception != null )
      return Response.error(job.exception);
    if( job == null || job.end_time > 0 || job.cancelled() )
      return jobDone(job, dst_key);
    return jobInProgress(job, dst_key);
  }

  /** Return {@link Response} for finished job. */
  protected Response jobDone(Job job, Key dst) {
    return Inspect2.redirect(this, dst);
  }

  /** Return default progress {@link Response}. */
  protected Response jobInProgress(Job job, Key dst) {
    return new Response(Response.Status.poll, this, (int) (100 * job.progress()), 100, null);
  }

  @Override public boolean toHTML(StringBuilder sb) {
    DocGen.HTML.title(sb, job != null ? job.description : null);
    DocGen.HTML.section(sb, dst_key.toString());
    return true;
  }

  @Override protected boolean log() {
    return false;
  }
}
