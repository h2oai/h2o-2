
package water.api;

import dontweave.gson.JsonObject;

public class TestRedirect extends Request {
  @Override protected Response serve() {
    JsonObject resp = new JsonObject();
    resp.addProperty("hoho","hehe");
    return Response.redirect(resp, TestPoll.class,resp);
  }
}
