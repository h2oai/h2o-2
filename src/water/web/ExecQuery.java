package water.web;

import java.util.Properties;

/**
 *
 * @author peta
 */
public class ExecQuery extends H2OPage {
  static final String html =
    "<p>What do you want to exec?:</p>"
    + "<form class='well form-inline' action='Exec'>"
    + "  <button type='submit' class='btn btn-primary'>Exec!</button>"
    + "  <input type='text' class='input-small span8' placeholder='R like expression' value = '%expr' name='Expr' id='Expr'>"
    + "</form> "
    ;

  @Override protected String serveImpl(Server server, Properties args, String sessionID) {
    RString str = new RString(html);
    str.replace("expr","");
    return str.toString();
  }
  
}
