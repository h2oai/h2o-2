package water.api;

import com.google.gson.JsonObject;
import hex.rf.RFModel;
import water.*;

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
      Iced ps = v.get();
      if( ps instanceof Job.Fail ) {
        r = Response.error(((Job.Fail)ps)._message);
      } else {
        r = RFView.redirect(response, v._key);
      }
    }
    r.setBuilder(RequestStatics.DEST_KEY, new KeyElementBuilder());
    return r;
  }
}
