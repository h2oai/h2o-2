package water.api;

import water.*;
import water.api.Request.Default;
import water.api.RequestServer.API_VERSION;

public class Progress2 extends Request2 {
  static final int API_WEAVER=1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  // This Request supports the HTML 'GET' command, and this is the help text
  // for GET.
  static final String DOC_GET = "Track progress of an ongoing Job";

  @API(help="The Job id being tracked.", required = true, filter = Default.class)
  public Key job;

  @API(help="The destination key being produced.", required = true, filter = Default.class)
  public Key dst_key;

  public static Response redirect(Request req, Key jobkey, Key dest) {
    return new Response(Response.Status.redirect, req, -1, -1, "Progress2", "job", jobkey, "dst_key", dest );
  }

  @Override protected Response serve() {
    Job jjob = Job.findJob(job);
    if(jjob != null && jjob.exception != null)return Response.error(jjob.exception);
    if(jjob == null || jjob.end_time > 0 || jjob.cancelled()) return jobDone(jjob, dst_key);
    return jobInProgress(jjob, dst_key);
  }

  /** Return {@link Response} for finished job. */
  protected Response jobDone(final Job job, final Key dst) {
    return Inspect2.redirect(this,dst.toString());
  }

  /** Return default progress {@link Response}. */
  protected Response jobInProgress(final Job job, final Key dst) {
    return new Response(Response.Status.poll, this, (int)(100*job.progress()), 100, null);
  }

  @Override public boolean toHTML( StringBuilder sb ) {
    Job jjob = Job.findJob(job);
    DocGen.HTML.title(sb,jjob!=null?jjob.description:null);
    DocGen.HTML.section(sb,dst_key.toString());
    return true;
  }

  @Override protected boolean log() { return false; }
  @Override public API_VERSION[] supportedVersions() { return  SUPPORTS_ONLY_V2; }
}
