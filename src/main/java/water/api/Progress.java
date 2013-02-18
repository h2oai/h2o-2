package water.api;

import water.*;
import water.Jobs.Job;
import water.Jobs;

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
    Key key = Key.make(_job.value());
    Job job = null;
    for( Job current : Jobs.get() ) {
      if( current._key.equals(key) ) {
        job = current;
        break;
      }
    }

    JsonObject response = new JsonObject();
    response.addProperty(RequestStatics.DEST_KEY, _dest.value());

    if( job == null )
      return Inspect.redirect(response, Key.make(_dest.value()));

    Jobs.Progress progress = UKV.get(job._progress, new Jobs.Progress());
    Response r = Response.poll(response, progress != null ? progress.get() : 1f);
    final String description = job._description;
    r.setBuilder(ROOT_OBJECT, new ObjectBuilder() {
      @Override
      public String caption(JsonObject object, String objectName) {
        return "<h3>" + description + "</h3>";
      }
    });
    r.setBuilder(RequestStatics.DEST_KEY, new KeyElementBuilder());
    return r;
  }
}
