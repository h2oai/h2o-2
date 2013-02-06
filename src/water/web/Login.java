/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package water.web;

import java.util.Properties;
import water.NanoHTTPD;
import water.NanoHTTPD.Response;

/**
 *
 * @author peta
 */
public class Login extends Page {

  private static final String unauthorized =
      "<div class=\"alert alert-error\">"
    + "Sorry, the provided username and password are not valid. Please try to <a href=\"loginQuery\">login again</a>"
    + "</div>";

  private static final String authorized =
      "<div class=\"alert alert-success\">"
    + "Thank you, you are now authorized. You may now proceeed to <a href=\"/\">H2O</a>"
    + "</div>";
  
  @Override public Object serve(Server server, Properties args, String sessionID) {
    String inputUsername = args.getProperty("username");
    String inputPassword = args.getProperty("password");
    String sessID = Server._sessionManager.verifyAuthentication(inputUsername, inputPassword);
    if (sessID == null) {
      RString r = new RString(Page.html);
      r.replace("contents",unauthorized);
      return server.new Response(NanoHTTPD.HTTP_OK, NanoHTTPD.MIME_HTML,r.toString());      
    } else {
      RString r = new RString(Page.html);
      r.replace("contents",authorized);
      Response resp = server.new Response(NanoHTTPD.HTTP_OK, NanoHTTPD.MIME_HTML, r.toString());
      resp.addHeader("Set-cookie", SessionManager.SESSION_COOKIE+"="+sessID);
      return resp;
    }
  }

  /** Anyone can view the login page. 
   * 
   * @param username
   * @return 
   */
  @Override public boolean authenticate(String username) {
    return true;
  }

  @Override public String[] requiredArguments() {
    return new String[] { "username", "password" };
  }
  
  
}
