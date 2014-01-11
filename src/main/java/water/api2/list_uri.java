package water.api2;

import water.NanoHTTPD;
import water.api.RequestServer;

/**
 *
 */
public class list_uri extends AbstractSimpleRequestHandler {
  public list_uri() {
    super(HML_GET, SINCE_V3, UNTIL_FOREVER, "/list_uri\\.json");
  }

  public NanoHTTPD.Response serve(NanoHTTPD.IHTTPSession session) throws Exception {
    String message = "{ message: \"hi\" }\n";
    NanoHTTPD.Response response = new NanoHTTPD.Response(NanoHTTPD.Response.Status.OK, RequestServer.MIME_JSON, message);
    return response;
  }
}
