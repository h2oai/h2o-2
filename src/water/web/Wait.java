
package water.web;

import java.util.Properties;

/**
 *
 * @author peta
 */
public class Wait extends H2OPage {

  public static final String html="<form id='rfw' name='rfw' action='%target' method='get'>%arg{<input type='hidden' name='%name' value='%value'/>}</form>"
          + "<div class='alert alert-info'>%message</div>"
          + "<script>\n"
          + "document.forms['rfw'].submit()\n"
          + "</script>";


  @Override
  protected String serveImpl(Server server, Properties args, String sessionID) throws PageError {
    RString result = new RString(html);
    result.replace("message", args.getProperty("_WAIT_MESSAGE"));
    result.replace("target", args.getProperty("_WAIT_TARGET"));
    args.remove("_WAIT_MESSAGE");
    args.remove("_WAIT_TARGET");
    for (String s: args.stringPropertyNames()) {
      RString x = result.restartGroup("arg");
      x.replace("name",s);
      x.replace("value",args.getProperty(s));
      x.append();
    }
    return result.toString();
  }

}
