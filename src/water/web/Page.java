package water.web;

import java.util.Properties;

import com.google.gson.JsonObject;
import water.H2O;

public abstract class Page {

  public static final String html =
      "<!DOCTYPE html>"
    + "<html lang=\"en\">"
    + "  <head>"
    + "    <meta charset=\"utf-8\">"
    + "    %refresh"
    + "    <title>H2O, from 0xdata</title>"
    + "    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\">"
    + "    <link href=\"bootstrap/css/bootstrap.css\" rel=\"stylesheet\">"
    + "    <style>"
    + "      body {"
    + "        padding-top: 60px; /* 60px to make the container go all the way to the bottom of the topbar */"
    + "      }"
    + "    </style>"
    + "    <link href=\"bootstrap/css/bootstrap-responsive.css\" rel=\"stylesheet\">"
    + "    <!-- Le HTML5 shim, for IE6-8 support of HTML5 elements -->"
    + "    <!--[if lt IE 9]>"
    + "      <script src=\"http://html5shim.googlecode.com/svn/trunk/html5.js\"></script>"
    + "    <![endif]-->"
    + "    %styles"
    + "    <!-- Le fav and touch icons -->"
    + "    <link rel=\"shortcut icon\" href=\"favicon.ico\">"
    + "    <script src='bootstrap/js/jquery.js'></script>"
    + "    %scripts"
    + "  </head>"
    + "  <body>"
    + "    <div class=\"navbar navbar-fixed-top\">"
    + "      <div class=\"navbar-inner\">"
    + "        <div class=\"container\">"
    + "          <a class=\"btn btn-navbar\" data-toggle=\"collapse\" data-target=\".nav-collapse\">"
    + "            <span class=\"icon-bar\"></span>"
    + "            <span class=\"icon-bar\"></span>"
    + "            <span class=\"icon-bar\"></span>"
    + "          </a>"
    + "          <a class=\"brand\" href=\"/\">H<sub>2</sub>O</a>"
    + "          <div class=\"nav\">"
    + "            <ul class=\"nav\">"
    + "              %navbar"
    + "            </ul>"
    + "          </div><!--/.nav-collapse -->"
    + "        </div>"
    + "      </div>"
    + "    </div>"
    + "    <div class=\"container\" style=\"margin: 0px auto\">"
    + "      %contents"
    + "    </div>"
    + "    <div class=\"container\" style=\"margin: 0px auto; text-align:center\">"
    + "      %footer"
    + "    </div>"
    + "  </body>"
    + "</html>";

  public static class PageError extends Exception {
    public PageError(String msg) { super(msg); }
  }

  public String[] requiredArguments() {
    return null;
  }

  /** Returns true, if the page can be viewed by the given user. By default all
   * pages can only be viewed by the H2O user.
   *
   * NOTE temporarily disabled to allow nice debugging. re-enable when you
   * actually want to use the feature.
   *
   * @param username
   * @return
   */
  public boolean authenticate(String username) {
    if (H2O.OPT_ARGS.auth == null)
      return true; // sessions disabled, everyone is allowed to view everything
    else
      return "H2O".equals(username);
  }

  public abstract Object serve(Server server, Properties args, String sessionID);

  public JsonObject serverJson(Server server, Properties parms, String sessionID) throws PageError {
    return null;
  }
}
