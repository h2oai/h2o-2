package water.api;

import water.Job;
import water.Key;

import com.google.gson.JsonObject;

public class Progress extends Request {
  final Str _job  = new Str(JOB);
  final Str _dest = new Str(DEST_KEY);

  public static Response redirect(JsonObject fromPageResponse, Key job, Key dest) {
    JsonObject destPageParams = new JsonObject();
    destPageParams.addProperty(JOB, job.toString());
    destPageParams.addProperty(DEST_KEY, dest.toString());
    return Response.redirect(fromPageResponse, Progress.class, destPageParams);
  }

  @Override
  protected Response serve() {
    Job job = findJob();
    JsonObject jsonResponse = defaultJsonResponse();

    if( job == null || job._endTime != 0 )
      return jobDone(job, jsonResponse);

    return jobInProgress(job, jsonResponse);
  }

  /** Find job key for this request */
  protected Job findJob() {
    Key key = Key.make(_job.value());
    return Job.findJob(key);
  }

  /** Create default Json response with destination key */
  protected JsonObject defaultJsonResponse() {
    JsonObject response = new JsonObject();
    response.addProperty(RequestStatics.DEST_KEY, _dest.value());
    return response;
  }

  /** Return {@link Response} for finished job. */
  protected Response jobDone(final Job job,final JsonObject jsonResp) {
    return Inspect.redirect(jsonResp, job, Key.make(_dest.value()));
  }

  /** Return default progress {@link Response}. */
  protected Response jobInProgress(final Job job, JsonObject jsonResp) {
    Response r = Response.poll(jsonResp, job.progress());

    final String description = job._description;
    r.setBuilder(ROOT_OBJECT, defaultProgressBuilder(description));
    r.setBuilder(RequestStatics.DEST_KEY, new KeyElementBuilder());
    return r;
  }

  @Override protected boolean log() {
    return false;
  }

  static final ObjectBuilder defaultProgressBuilder(final String description) {
    return new ObjectBuilder() {
      @Override
      public String caption(JsonObject object, String objectName) {
        return "<h3>" + description + "</h3>";
      }
    };
  }
}
