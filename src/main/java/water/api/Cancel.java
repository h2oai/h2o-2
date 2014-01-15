package water.api;

import water.Job;
import water.Key;
import water.util.RString;

import com.google.gson.JsonObject;

public class Cancel extends Request {
  // TODO use ExistingJobKey (check other places)
  protected final Str _key = new Str(KEY);

  public static String link(Key k, String content) {
    RString rs = new RString("<a href='Cancel.html?key=%key'>%content</a>");
    rs.replace("key", k.toString());
    rs.replace("content",content);
    return rs.toString();
  }

  @Override
  protected Response serve() {
    String key = _key.value();
    try {
      Job.findJob(Key.make(key)).cancel();
    } catch( Exception e ) {
      return Response.error(e.getMessage());
    }
    JsonObject response = new JsonObject();
    return Response.redirect(response, Jobs.class, null);
  }
}
