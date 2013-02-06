package water.web;

import java.util.Properties;

import water.NanoHTTPD;

public class Logoff extends Page {

  private static final String logoff =
      "<div class=\"alert alert-success\">"
    + "Thank you, you are now logged off."
    + "</div>";

  @Override public Object serve(Server server, Properties args, String sessionID) {
      RString r = new RString(Page.html);
      r.replace("contents",logoff);
      NanoHTTPD.Response resp = server.new Response(NanoHTTPD.HTTP_OK, NanoHTTPD.MIME_HTML, r.toString());
      resp.addHeader("Set-cookie", SessionManager.SESSION_COOKIE+"=__void__");
      return resp;
  }

}
