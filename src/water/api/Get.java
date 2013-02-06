package water.api;

import com.google.gson.JsonObject;
import java.util.Properties;
import water.*;

public class Get extends Request {
  protected H2OExistingKey _key = new H2OExistingKey(KEY);

  @Override public NanoHTTPD.Response serve(NanoHTTPD server, Properties args, RequestType type) {
    if( type == RequestType.json ) {
      JsonObject resp = new JsonObject();
      resp.addProperty(ERROR,"This request is only provided for browser connections");
      return wrap(server, resp);
    } else if( type != RequestType.www ) {
      return super.serve(server, args, type);
    }

    String query = checkArguments(args, type);
    if (query != null) return wrap(server,query,type);
    try {
      Value val = _key.value();
      Key key = val._key;
      if (!key.user_allowed())
        return wrap(server,build(Response.error("Not a user key: " + key)));
      // HTML file save of Value
      NanoHTTPD.Response res = server.new Response(NanoHTTPD.HTTP_OK,NanoHTTPD.MIME_DEFAULT_BINARY,val.openStream());
      res.addHeader("Content-Length", Long.toString(val.length()));
      res.addHeader("Content-Disposition", "attachment; filename="+key.toString());
      return res;
    } catch (Exception e) {
      return wrap(server,build(Response.error(e.getMessage())));
    }
  }

  @Override protected Response serve() {
    throw new Error("Get should not be called from this context");
  }
}
