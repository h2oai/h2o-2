
package water.api;

import java.util.Properties;

import water.NanoHTTPD;

import com.google.gson.JsonObject;

/** Request that only supports browser (html, query, help, wiki) request types.
 *
 * When accessed from JSON throws.
 *
 * @author peta
 */
public abstract class HTMLOnlyRequest extends Request {

  @Override final public NanoHTTPD.Response serve(NanoHTTPD server, Properties args, RequestType type) {
    if (type == RequestType.json) {
      JsonObject resp = new JsonObject();
      resp.addProperty(ERROR,"This request is only provided for browser connections");
      return wrap(server, resp);
    }
    return super.serve(server,args,type);
  }

  @Override protected Response serve() {
    return Response.doneEmpty();
  }
}
