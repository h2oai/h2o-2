/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package water.web;

/** A very simple class that manages the session for the H2O pages. 
 * 
 * I am a simple guy and we do not need anything extra now. Simply looks for
 * cookie sessionID and if found to be the magic value, sets as authenticated.
 *
 * @author peta
 */
public class SessionManager {
  
  
  public static final String SESSION_COOKIE = "sessionID";
  
  /** Returns the name of the user associated with the given session ID, or
   * null if no such session exists. 
   * 
   * NOTE As of now, only recognizes the magic session ID value upon which
   * returns the "H2O" as the username. When we need fancier logging and
   * security control, this can be easily changed to our needs. 
   * 
   * @param sessionID
   * @return 
   */
  public String authenticate(String sessionID) {
    return "magicSessionID".equals(sessionID) ? "H2O" : null;
  }
  
  
  /** Verifies that the given username and password are correct and if so issues
   * a sessionID.
   * 
   * If not, returns null. 
   * 
   * NOTE As of now, the authentication process is very simple. If the user is
   * H2O and the password is H2SO4, returns the magic cookie that determines
   * the valid session, otherwise returns null. 
   * 
   * @param username
   * @param password
   * @return 
   */  
  public String verifyAuthentication(String username, String password) {
    return ("H2O".equals(username) && "H2SO4".equals(password)) ? "magicSessionID" : null;
  }
  
}
