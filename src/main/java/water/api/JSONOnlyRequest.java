
package water.api;

import com.google.gson.JsonObject;
import java.util.Properties;
import water.NanoHTTPD;

/** JSON only request. Throws in any other access mode.
 *
 * @author peta
 */
public abstract class JSONOnlyRequest extends Request {

  public NanoHTTPD.Response serve(NanoHTTPD server, Properties args, RequestType type) {
    if (type == RequestType.json) {
      return super.serve(server,args,type);
    } else {
      JsonObject resp = new JsonObject();
      resp.addProperty(ERROR,"This request is only provided for browser connections");
      return wrap(server, resp);
    }
  }

}
