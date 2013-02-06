
package water.api;

import water.web.RString;

/**
 *
 * @author peta
 */
public class HTTP500 extends Request {

  private final Str _error = new Str(ERROR,"Unknown error");

  public HTTP500() {
    _requestHelp = "Displays the HTTP 500 page with error specified in JSON"
            + " argument error. This page is displayed when any unexpected"
            + " exception is returned from the request processing at any level.";
    _error._requestHelp = "Error description for the 500. Generally the exception message.";
  }

  @Override public Response serve() {
    return Response.error(_error.value());
  }

  private static final String _html =
            "<h3>HTTP 500 - Internal Server Error</h3>"
          + "<div class='alert alert-error'>%ERROR</div>"
          ;

  @Override protected String build(Response response) {
    StringBuilder sb = new StringBuilder();
    sb.append("<div class='container'>");
    sb.append("<div class='row-fluid'>");
    sb.append("<div class='span12'>");
    sb.append(buildResponseHeader(response));
    RString str = new RString(_html);
    str.replace("ERROR", response.error());
    sb.append(str.toString());
    sb.append("</div></div></div>");
    return sb.toString();
  }


}
