package water.api;

import water.*;
import water.RReader.RModel;

import com.google.gson.JsonObject;

public class RReaderProgress extends Request {
  protected final H2OExistingKey _dest = new H2OExistingKey(DEST_KEY);

  public static Response redirect(JsonObject fromPageResponse, Key dest) {
    JsonObject destPageParams = new JsonObject();
    destPageParams.addProperty(DEST_KEY, dest.toString());
    return Response.redirect(fromPageResponse, RReaderProgress.class, destPageParams);
  }

  @Override
  protected Response serve() {
    Value v = _dest.value();
    JsonObject response = new JsonObject();
    Response r;
    if( v == null ) { // Not found? Maybe a race with polling progress before 1st status object appears
      r = Response.poll(response, 0);
    } else {
      response.addProperty(RequestStatics.DEST_KEY, v._key.toString());
      RModel model = UKV.get(v._key, new RModel());
      if( model._progress == 1f ) {
        r = RFView.redirect(response, v._key);
      } else {
        RModel ps = v.get(new RModel());
        if( ps._error != null ) {
          r = Response.error(ps._error);
        } else {
          r = Response.poll(response, 1f);
        }
      }
    }
    r.setBuilder(RequestStatics.DEST_KEY, new KeyElementBuilder());
    return r;
  }
}
