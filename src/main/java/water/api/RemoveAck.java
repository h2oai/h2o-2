package water.api;

import water.Value;

import dontweave.gson.JsonObject;

public class RemoveAck extends Request {
  protected final H2OExistingKey _key = new H2OExistingKey(KEY);

  @Override
  protected Response serve() {
    Value v = _key.value();
    String key = v._key.toString();
    JsonObject response = new JsonObject();
    response.addProperty(RequestStatics.KEY, key);
    Response r = Response.done(new JsonObject());
    r.addHeader("" //
        + "<div class='alert alert-error'>Are you sure you want to delete key <strong>" + key + "</strong>?<br/>" //
        + "There is no way back!" //
        + "</div>" //
        + "<div style='text-align:center'>" //
        + "<a href='javascript:history.back()'><button class='btn btn-primary'>No, go back</button></a>" //
        + "&nbsp;&nbsp;&nbsp;" //
        + "<a href='Remove.html?" + KEY + "=" + key + "'><button class='btn btn-danger'>Yes!</button></a>" //
        + "</div>");
    return r;
  }
}
