package water.api;

import water.Key;
import water.Value;
import water.store.s3.MultipartUpload;

import com.google.gson.JsonObject;

public class ExportS3Progress extends Request {
  protected final H2OExistingKey _dest = new H2OExistingKey(DEST_KEY);

  public static Response redirect(JsonObject fromPageResponse, Key dest) {
    JsonObject destPageParams = new JsonObject();
    destPageParams.addProperty(DEST_KEY, dest.toString());
    return Response.redirect(fromPageResponse, ExportS3Progress.class, destPageParams);
  }

  @Override
  protected Response serve() {
    Value v = _dest.value();
    MultipartUpload.Progress progress = v.get(new MultipartUpload.Progress());
    JsonObject response = new JsonObject();
    response.addProperty(RequestStatics.DEST_KEY, v._key.toString());
    Response r;

    if( progress._error != null ) {
      r = Response.error(progress._error);
    } else {
      if( !progress._confirmed )
        return Response.poll(response, progress._done / (float) progress._todo);

      response.addProperty(STATUS, SUCCEEDED);
      r = Response.done(response);
    }

    return r;
  }
}
