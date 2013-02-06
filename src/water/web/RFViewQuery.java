package water.web;

import java.util.Properties;

/**
 *
 * @author peta
 */
public class RFViewQuery extends H2OPage {

  final String html = "Select the model & data and other arguments for the Random Forest View to look at:<br/>"
          + "<form class='well form-inline' action='RFViewQuery1'>"
          + "  <button class='btn btn-primary' type='submit'>View</button>"
          + "  <input class='input-small span4' type='text' name='dataKey' id='dataKey' placeholder='Hex key for data' value=\"%dataKey\">"
          + "  <input style='display:none' type='text' name='modelKey' id='modelKey' placeholder='Hex key for model' value=\"%modelKey\">"
          + "  <input style='display:none' type='text' name='class' id='class' placeholder='Class' value=\"%class\">"
          + "</form>"
          ;

  @Override protected String serveImpl(Server server, Properties args, String sessionID) throws PageError {
    RString result = new RString(html);
    result.replace("dataKey",args.getProperty("dataKey",""));
    result.replace("modelKey",args.getProperty("modelKey",""));
    result.replace("class",args.getProperty("class",""));
    return result.toString();
    
  }

}
