package water.api;


import java.io.IOException;
import java.io.InputStream;
import java.util.*;

import water.*;
import water.util.*;

import com.google.common.base.Objects;
import com.google.common.io.ByteStreams;
import com.google.common.io.Closeables;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

/** A basic class for a JSON request.
 */
public abstract class Request extends RequestBuilders {
  public String _requestHelp;

  protected abstract Response serve();

  protected Response serve_debug() { throw H2O.unimpl(); }

  public NanoHTTPD.Response serve(NanoHTTPD server, Properties args, RequestType type) {
    switch (type) {
      case help:
        return wrap(server, build(Response.done(serveHelp())));
      case json:
      case www:
        String query = checkArguments(args, type);
        if (query != null)
          return wrap(server,query,type);
        long time = System.currentTimeMillis();
        Response response = serve();
        response.setTimeStart(time);
        if (type == RequestType.json)
          return wrap(server, response.toJson());
        return wrap(server,build(response));
      case debug:
        response = serve_debug();
        return wrap(server,build(response));
      case query:
        query = checkArguments(args, type);
        return wrap(server,query);
      default:
        throw new RuntimeException("Invalid request type "+type.toString());
    }
  }

  protected JsonObject serveHelp() {
    JsonObject r = new JsonObject();
    r.addProperty(NAME, getClass().getSimpleName());
    r.addProperty(DESCRIPTION, _requestHelp);
    JsonArray args = new JsonArray();
    for( Argument arg : _arguments ) {
      args.add(arg.requestHelp());
    }
    r.add(ARGUMENTS, args);
    return r;
  }

  protected void buildPython(IndentingAppender ia) throws IOException {
    String name = getClass().getSimpleName();

    ia.append("def ").append(name).append("(self");
    ia.incrementIndent().incrementIndent();
    for( Argument arg : _arguments ) {
      ia.append(", ").append(arg._name);
      if( !arg._required ) ia.append("=None");
    }
    ia.appendln("):");
    ia.decrementIndent().decrementIndent();
    ia.incrementIndent();
    ia.appendln("'''");
    ia.appendln(Objects.firstNonNull(_requestHelp, "MISSING HELP STRING"));

    if( !_arguments.isEmpty() ) ia.appendln("Arguments:");
    ia.incrementIndent();
    for( Argument arg : _arguments ) {
      ia.append(arg._name).append(" -- ");
      if( arg._required ) ia.append("required -- ");
      ia.appendln(arg.queryDescription());
      ia.incrementIndent();
      ia.appendln(Objects.firstNonNull(arg._requestHelp, "MISSING HELP STRING"));
      ia.decrementIndent();
    }
    ia.decrementIndent();
    ia.appendln("'''");
    ia.appendln("pass");
    ia.decrementIndent();
  }

  protected NanoHTTPD.Response wrap(NanoHTTPD server, String response) {
    RString html = new RString(_htmlTemplate);
    html.replace("CONTENTS",response);
    return server.new Response(NanoHTTPD.HTTP_OK, NanoHTTPD.MIME_HTML, html.toString());
  }

  protected NanoHTTPD.Response wrap(NanoHTTPD server, JsonObject response) {
    return server.new Response(NanoHTTPD.HTTP_OK, NanoHTTPD.MIME_JSON,
        response.toString());
  }

  protected NanoHTTPD.Response wrap(NanoHTTPD server, String value, RequestType type) {
    if (type == RequestType.json)
      return server.new Response(NanoHTTPD.HTTP_OK, NanoHTTPD.MIME_JSON, value);
    return wrap(server,value);
  }

  // html template and navbar handling -----------------------------------------

  private static String _htmlTemplate;

  static {
    InputStream resource = Boot._init.getResource2("/page.html");
    try {
      _htmlTemplate = new String(ByteStreams.toByteArray(resource)).replace("%cloud_name",H2O.NAME);
    } catch (NullPointerException e) {
      Log.die("page.html not found in resources.");
    } catch (Exception e) {
      Log.die(e.getMessage());
    } finally {
      Closeables.closeQuietly(resource);
    }
  }

  private static class MenuItem {
    public final Request _request;
    public final String _name;
    public MenuItem(Request request, String name) {
      _request = request;
      _name = name;
    }
    public void toHTML(StringBuilder sb) {
      sb.append("<li><a href='");
      sb.append(_request.getClass().getSimpleName()+".html");
      sb.append("'>");
      sb.append(_name);
      sb.append("</a></li>");
    }

  }

  private static HashMap<String, ArrayList<MenuItem> > _navbar = new HashMap();
  private static ArrayList<String> _navbarOrdering = new ArrayList();

  public static void initializeNavBar() {
    StringBuilder sb = new StringBuilder();
    for (String s : _navbarOrdering) {
      ArrayList<MenuItem> arl = _navbar.get(s);
      if ((arl.size() == 1) && arl.get(0)._name.equals(s)) {
        arl.get(0).toHTML(sb);
      } else {
        sb.append("<li class='dropdown'>");
        sb.append("<a href='#' class='dropdown-toggle' data-toggle='dropdown'>");
        sb.append(s);
        sb.append("<b class='caret'></b>");
        sb.append("</a>");
        sb.append("<ul class='dropdown-menu'>");
        for (MenuItem i : arl)
          i.toHTML(sb);
        sb.append("</ul></li>");
      }
    }
    RString str = new RString(_htmlTemplate);
    str.replace("NAVBAR",sb.toString());
    str.replace("CONTENTS","%CONTENTS");
    _htmlTemplate = str.toString();
  }

  public static Request addToNavbar(Request r, String name) {
    assert (! _navbar.containsKey(name));
    ArrayList<MenuItem> arl = new ArrayList();
    arl.add(new MenuItem(r,name));
    _navbar.put(name,arl);
    _navbarOrdering.add(name);
    return r;
  }

  public static Request addToNavbar(Request r, String name, String category) {
    ArrayList<MenuItem> arl = _navbar.get(category);
    if (arl == null) {
      arl = new ArrayList();
      _navbar.put(category,arl);
      _navbarOrdering.add(category);
    }
    arl.add(new MenuItem(r,name));
    return r;
  }

}
