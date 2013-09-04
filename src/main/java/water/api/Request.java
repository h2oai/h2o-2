package water.api;

import java.io.InputStream;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.util.*;
import java.util.Map.Entry;

import water.*;
import water.util.*;
import water.util.Log.Tag.Sys;

import com.google.common.io.ByteStreams;
import com.google.common.io.Closeables;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public abstract class Request extends RequestBuilders {
  @Retention(RetentionPolicy.RUNTIME)
  public @interface API {
    String help();

    boolean required() default false;

    int since() default 1;

    int until() default Integer.MAX_VALUE;

    Class<? extends Filter> filter() default Filter.class;

    Class<? extends Filter>[] filters() default {};
  }

  public interface Filter {
    boolean run(Object value);
  }

  /**
   * NOP filter, use to define a field as input.
   */
  public class Default implements Filter {
    @Override public boolean run(Object value) {
      return true;
    }
  }

  public class TypedKey extends H2OKey {
    Class _type;
    public TypedKey(Class type) { super("",true); _type = type; }
    @Override protected Key parse(String input) {
      Key k = Key.make(input);
      Value v = DKV.get(k);
      if( v == null )
        throw new IllegalArgumentException(input+":"+errors()[0]);
      if( v.type() != TypeMap.onLoad(_type.getName()) )
        throw new IllegalArgumentException(input+":"+errors()[1]);
      return k;
    }
    @Override protected String queryDescription() { return "An existing H2O Frame key."; }
    @Override protected String[] errors() { return new String[] { "Key not found", "Key is not a " + _type.getSimpleName() }; }
  }

  public class ColumnSelect implements Filter {
    public final String _key;
    protected ColumnSelect(String key) {
      _key = key;
    }
    @Override public boolean run(Object value) {
      return true;
    }
  }

  public class VecSelect implements Filter {
    public final String _key;
    protected VecSelect(String key) { _key = key; }
    @Override public boolean run(Object value) { return true; }
  }

  //

  public String _requestHelp;

  protected Request(String help) {
    _requestHelp = help;
  }

  protected Request() {
  }

  protected String href() {
    return getClass().getSimpleName();
  }

  protected RequestType hrefType() {
    return RequestType.www;
  }

  protected void registered() {
  }

  protected Request create(Properties parms) {
    return this;
  }

  protected abstract Response serve();

  protected Response serve_debug() {
    throw H2O.unimpl();
  }

  protected boolean log() {
    return true;
  }

  public NanoHTTPD.Response serve(NanoHTTPD server, Properties args, RequestType type) {
    // Needs to be done also for help to initialize or argument records
    String query = checkArguments(args, type);
    onArgumentsParsed();
    switch( type ) {
      case help:
        return wrap(server, HTMLHelp());
      case json:
      case www:
      case png:
        if( log() ) {
          String log = getClass().getSimpleName();
          for( Object arg : args.keySet() ) {
            String value = args.getProperty((String) arg);
            if( value != null && value.length() != 0 )
              log += " " + arg + "=" + value;
          }
          Log.debug(Sys.HTTPD, log);
        }
        if( query != null )
          return wrap(server, query, type);
        long time = System.currentTimeMillis();
        Response response = serve();
        response.setTimeStart(time);
        if( type == RequestType.json )
          return response._req == null ? wrap(server, response.toJson()) : wrap(server, new String(response._req
              .writeJSON(new AutoBuffer()).buf()), RequestType.json);
        return wrap(server, build(response));
      case debug:
        response = serve_debug();
        return wrap(server, build(response));
      case query:
        return wrap(server, query);
      default:
        throw new RuntimeException("Invalid request type " + type.toString());
    }
  }

  protected void onArgumentsParsed() {
  }

  protected NanoHTTPD.Response wrap(NanoHTTPD server, String response) {
    RString html = new RString(_htmlTemplate);
    html.replace("CONTENTS", response);
    return server.new Response(NanoHTTPD.HTTP_OK, NanoHTTPD.MIME_HTML, html.toString());
  }

  protected NanoHTTPD.Response wrap(NanoHTTPD server, JsonObject response) {
    return server.new Response(NanoHTTPD.HTTP_OK, NanoHTTPD.MIME_JSON, response.toString());
  }

  protected NanoHTTPD.Response wrap(NanoHTTPD server, String value, RequestType type) {
    if( type == RequestType.json )
      return server.new Response(NanoHTTPD.HTTP_OK, NanoHTTPD.MIME_JSON, value);
    return wrap(server, value);
  }

  // html template and navbar handling -----------------------------------------

  private static String _htmlTemplate;

  static {
    InputStream resource = Boot._init.getResource2("/page.html");
    try {
      _htmlTemplate = new String(ByteStreams.toByteArray(resource)).replace("%cloud_name", H2O.NAME);
    } catch( NullPointerException e ) {
      if( !Log._dontDie ) {
        Log.err(e);
        Log.die("page.html not found in resources.");
      }
    } catch( Exception e ) {
      Log.err(e);
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
      sb.append(_request.href() + _request.hrefType()._suffix);
      sb.append("'>");
      sb.append(_name);
      sb.append("</a></li>");
    }
  }

  private static HashMap<String, ArrayList<MenuItem>> _navbar = new HashMap();
  private static ArrayList<String> _navbarOrdering = new ArrayList();

  public static void initializeNavBar() {
    StringBuilder sb = new StringBuilder();
    for( String s : _navbarOrdering ) {
      ArrayList<MenuItem> arl = _navbar.get(s);
      if( (arl.size() == 1) && arl.get(0)._name.equals(s) ) {
        arl.get(0).toHTML(sb);
      } else {
        sb.append("<li class='dropdown'>");
        sb.append("<a href='#' class='dropdown-toggle' data-toggle='dropdown'>");
        sb.append(s);
        sb.append("<b class='caret'></b>");
        sb.append("</a>");
        sb.append("<ul class='dropdown-menu'>");
        for( MenuItem i : arl )
          i.toHTML(sb);
        sb.append("</ul></li>");
      }
    }
    RString str = new RString(_htmlTemplate);
    str.replace("NAVBAR", sb.toString());
    str.replace("CONTENTS", "%CONTENTS");
    _htmlTemplate = str.toString();
  }

  public static Request addToNavbar(Request r, String name) {
    assert (!_navbar.containsKey(name));
    ArrayList<MenuItem> arl = new ArrayList();
    arl.add(new MenuItem(r, name));
    _navbar.put(name, arl);
    _navbarOrdering.add(name);
    return r;
  }

  public static Request addToNavbar(Request r, String name, String category) {
    ArrayList<MenuItem> arl = _navbar.get(category);
    if( arl == null ) {
      arl = new ArrayList();
      _navbar.put(category, arl);
      _navbarOrdering.add(category);
    }
    arl.add(new MenuItem(r, name));
    return r;
  }

  public static JsonObject execSync(Request request) {
    for( ;; ) {
      Response r = request.serve();
      switch( r._status ) {
        case error:
          throw new RuntimeException(r.error());
        case redirect:
          request = RequestServer.requests().get(r._redirectName);
          Properties args = new Properties();
          for( Entry<String, JsonElement> entry : r._redirectArgs.entrySet() )
            args.put(entry.getKey(), entry.getValue().getAsString());
          request.checkArguments(args, RequestType.json);
          break;
        case poll:
          // Not a FJ thread, just wait
          try {
            Thread.sleep(100);
          } catch( InterruptedException e ) {
            throw new RuntimeException(e);
          }
          break;
        case done:
          return r._response;
      }
    }
  }

  // ==========================================================================

  public boolean toHTML(StringBuilder sb) {
    return false;
  }

  public String toDocGET() {
    return null;
  }

  /**
   * Example of passing & failing request. Will be prepended with
   * "curl -s localhost:54321/Request.json". Return param/value pairs that will be used to build up
   * a URL, and the result from serving the URL will show up as an example.
   */
  public String[] DocExampleSucc() {
    return null;
  }

  public String[] DocExampleFail() {
    return null;
  }

  public String HTMLHelp() {
    return DocGen.HTML.genHelp(this);
  }

  public String ReSTHelp() {
    return DocGen.ReST.genHelp(this);
  }
}
