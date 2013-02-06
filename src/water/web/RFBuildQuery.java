
package water.web;

import java.util.Properties;

import water.ValueArray;

public class RFBuildQuery extends H2OPage {
  
  static String html = ""
          + "<p>Select the data on which the random forest should be build and then click on the <i>Next</i> button:</p>"
          + "<form class='well form-inline' action='RFBuildQuery1'>"
          + "  <input type='text' class='input-small span4' placeholder='key' name='dataKey' id='dataKey'>"
          + "  <button type='submit' class='btn btn-primary'>Next</button>"
          + "</form> "
          ;
  
  @Override protected String serveImpl(Server server, Properties p, String sessionID) throws PageError {
    return html;
  }
}
