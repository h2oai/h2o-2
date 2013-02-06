
package water.web;

import java.util.Properties;

/**
 *
 * @author peta
 */
public class JSONPage extends H2OPage {

  @Override protected String serveImpl(Server server, Properties args, String sessionID) throws PageError {
    return error("This URL is only available for programming language interfaces and has no meaning for web browser access.");
  }

}
