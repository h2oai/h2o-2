/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package water.web;

import java.util.Properties;
import water.NanoHTTPD;

/** A login page that simply prompts for a username and a password.
 * 
 * At the moment, the login and 
 * 
 *
 * @author peta
 */
public class LoginQuery extends Page {
  private static final String contents =
          "<form class=\"form-horizontal\" method=\"POST\" encoding=\"multipart/form-data\" action=\"login\">"
          + "  <div class=\"control-group\">"
          + "    <label class=\"control-label\" for=\"username\">Username</label>"
          + "    <div class=\"controls\">"
          + "      <input type=\"text\" name =\"username\" id=\"username\" placeholder=\"Username\">"
          + "    </div>"
          + "  </div>"
          + "  <div class=\"control-group\">"
          + "    <label class=\"control-label\" for=\"password\">Password</label>"
          + "    <div class=\"controls\">"
          + "      <input type=\"password\" name=\"password\" id=\"password\" placeholder=\"Password\">"
          + "    </div>"
          + "  </div>"
          + "  <div class=\"control-group\">"
          + "    <div class=\"controls\">"
          + "      <button type=\"submit\" class=\"btn\">Sign in</button>"
          + "    </div>"
          + "  </div>"
          + "</form>";

  @Override public Object serve(Server server, Properties args, String sessionID) {
    RString response = new RString(html);
    response.replace("contents",contents);

    return server.new Response(NanoHTTPD.HTTP_OK, NanoHTTPD.MIME_HTML,response.toString());
    
  }

  /** Anyone can view the loginQuery page. 
   * 
   * @param username
   * @return 
   */
  @Override public boolean authenticate(String username) {
    return true;
  }
  
}
