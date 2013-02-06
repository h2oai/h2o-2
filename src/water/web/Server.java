package water.web;

import H2OInit.Boot;

import com.google.common.io.ByteStreams;
import com.google.common.io.Closeables;
import com.google.gson.JsonObject;
import java.io.*;
import java.net.ServerSocket;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import water.H2O;
import water.NanoHTTPD;
import water.api.RequestServer;
import water.web.Page.PageError;

/** This is a simple web server. */
public class Server extends NanoHTTPD {

  // cache of all loaded resources
  private static final ConcurrentHashMap<String,byte[]> _cache = new ConcurrentHashMap();
  private static final HashMap<String,Page> _pages = new HashMap();

  static final SessionManager _sessionManager;

  // initialization ------------------------------------------------------------
  static {
    _sessionManager = new SessionManager();
    // initialize pages
    _pages.put("",new Cloud());
    _pages.put("Cloud",new Cloud());
    _pages.put("API",new ApiTest());
    _pages.put("COV",new Covariance());
    _pages.put("Covariance",new Covariance());
    _pages.put("cor",new Covariance());
    _pages.put("cov",new Covariance());
    _pages.put("var",new Covariance());
    _pages.put("DebugView",new DebugView());
    _pages.put("Debug",new Debug());
    _pages.put("Exec",new ExecWeb());
    _pages.put("ExecQuery",new ExecQuery());
    _pages.put("Get",new Get());
    //_pages.put("GetQuery",new GetQuery());
    //_pages.put("GetVector",new GetVector());
    _pages.put("ImportFolder",new ImportFolder());
    _pages.put("ImportQuery",new ImportQuery());
    _pages.put("ImportUrl",new ImportUrl());
    _pages.put("Inspect",new Inspect());
    _pages.put("LR",new LinearRegression());
    _pages.put("GLM",new GLM());
    _pages.put("glm",new GLM());
    _pages.put("Glm",new GLM());
    _pages.put("Parse",new Parse());
    //_pages.put("PR",new ProgressReport());
    //_pages.put("ProgressReport",new ProgressReport());
    //_pages.put("ProgressView",new ProgressView());
    _pages.put("PutFile",new PutFile());
    //_pages.put("PutHDFS",new PutHDFS());
    _pages.put("Put",new PutQuery());
    _pages.put("PutValue",new PutValue());
    //_pages.put("PutVector",new PutVector());
    _pages.put("RFView",new RFView());
    _pages.put("Wait",new Wait());
    _pages.put("RFViewQuery",new RFViewQuery());
    _pages.put("RFViewQuery1",new RFViewQuery1());
    _pages.put("RFBuildQuery",new RFBuildQuery());
    _pages.put("RFBuildQuery1",new RFBuildQuery1());
    _pages.put("RFBuildQuery2",new RFBuildQuery2());
    _pages.put("RFTreeView",new RFTreeView());
    _pages.put("RF",new RandomForestPage());
    _pages.put("RandomForest",new RandomForestPage());
    _pages.put("Remote",new Remote());
    _pages.put("Remove",new Remove());
    _pages.put("RemoveAck",new RemoveAck());
    _pages.put("Shutdown",new Shutdown());
    _pages.put("StoreView",new StoreView());
    //_pages.put("Test",new Test());
    _pages.put("Timeline",new TimelinePage());
    _pages.put("Store2HDFS",new Store2HDFS());
    _pages.put("loginQuery", new LoginQuery());
    _pages.put("login", new Login());
    _pages.put("logoff", new Logoff());
    _pages.put("nop", new NOP());
    _pages.put("NOP", new NOP());
    _pages.put("JStack", new DebugJStackView());
    _pages.put("DbgJStack", new DebugJStackView());
  }


  // Keep spinning until we get to launch the NanoHTTPD
  public static void start() {
    new Thread( new Runnable() {
        public void run() {
          while( true ) {
            try {
              // Try to get the NanoHTTP daemon started
              new Server(H2O._webSocket);
              break;
            } catch ( Exception ioe ) {
              System.err.println("Launching NanoHTTP server got "+ioe);
              try { Thread.sleep(1000); } catch( InterruptedException e ) { } // prevent denial-of-service
            }
          }
        }
      }, "HTTP Server Launcher").start();
  }

  /** Returns the sessionID stored in the cookie, or null if no such cookie was
   * found.
   *
   * Only a very simple cookie parser.
   */
  private String getSessionIDFromCookie(Properties header) {
    String cks = header.getProperty("cookie","");
    String[] parts = cks.split(" ");
    for (String s: parts) {
      s = s.trim();
      if (s.startsWith(SessionManager.SESSION_COOKIE+"=")) {
        s = s.substring(SessionManager.SESSION_COOKIE.length()+1, s.endsWith(";") ? s.length()-1 : s.length());
        return s;
      }
    }
    return null;
  }

  // uri serve -----------------------------------------------------------------
  @Override public Response serve( String uri, String method, Properties header, Properties parms, Properties files ) {
    Thread.currentThread().setPriority(Thread.MAX_PRIORITY-1); // Jack priority for user-visible requests
    if (uri.isEmpty()) uri = "/";

    Page page = _pages.get(uri.substring(1));
    boolean json = uri.endsWith(".json");
    if (json && page == null) page = _pages.get(uri.substring(1, uri.length()-5));

    String mime = json ? MIME_JSON : MIME_HTML;

    // if we cannot handle it, then it might be a resource
    if (page==null) return getResource(uri);

    // authenticate and display an error if not authorized to view

    String sessionID = getSessionIDFromCookie(header);
    String username = _sessionManager.authenticate(sessionID);
    if (!page.authenticate(username))
      return http401(uri);

    // unify GET and POST arguments
    parms.putAll(files);
    // check that required arguments are present
    String[] reqArgs = page.requiredArguments();
    if (reqArgs!=null) {
      for (String s : reqArgs) {
        if (!parms.containsKey(s) || parms.getProperty(s).isEmpty()) {
          if (json) {
            JsonObject r = new JsonObject();
            r.addProperty("Error", "Not all required parameters were supplied: argument "+s+" is missing.");
            return new Response(HTTP_OK, mime, r.toString());
          } else {
            return new Response(HTTP_OK, mime,
              H2OPage.wrap(H2OPage.error("Not all required parameters were supplied to page <strong>"+uri+"</strong><br/>Argument <strong>"+s+"</strong> is missing.")));
          }
        }
      }
    }
    Object result;
    try {
      result = json ? page.serverJson(this,parms,sessionID) : page.serve(this,parms,sessionID);
    } catch( PageError e ) {
      if (json) {
        JsonObject r = new JsonObject();
        r.addProperty("Error", e.getMessage());
        result = r;
      } else {
        result = e.getMessage();
      }
    }
    if (result == null) return http404(uri);
    if (result instanceof Response) return (Response)result;
    if (result instanceof InputStream)
      return new Response(NanoHTTPD.HTTP_OK, mime, (InputStream) result);
    return new Response(NanoHTTPD.HTTP_OK, mime, result.toString());
  }

  public static Page getPage(String uri) {
    return _pages.get(uri);
  }

  private Server( ServerSocket socket ) throws IOException {
    super(socket,null);
  }

  // Resource loading ----------------------------------------------------------

  // Returns the response containing the given uri with the appropriate mime
  // type.
  private Response getResource(String uri) {
    byte[] bytes = _cache.get(uri);
    if( bytes == null ) {
      InputStream resource = Boot._init.getResource2(uri);
      if (resource != null) {
        try {
          bytes = ByteStreams.toByteArray(resource);
        } catch( IOException e ) { }
        byte[] res = _cache.putIfAbsent(uri,bytes);
        if( res != null ) bytes = res; // Racey update; take what is in the _cache
      }
      Closeables.closeQuietly(resource);
    }
    if (bytes==null)
      return http404(uri);
    String mime = NanoHTTPD.MIME_DEFAULT_BINARY;
    if (uri.endsWith(".css"))
      mime = "text/css";
    else if (uri.endsWith(".html"))
      mime = "text/html";
    return new Response(NanoHTTPD.HTTP_OK,mime,new ByteArrayInputStream(bytes));
  }

  // others --------------------------------------------------------------------

  private Response http404(String uri) {
    RString r = new RString(Page.html);
    r.replace("contents","Location "+uri+" not found.");
    return new Response(NanoHTTPD.HTTP_NOTFOUND,NanoHTTPD.MIME_HTML,r.toString());
  }

  private Response http401(String uri) {
    RString r = new RString(Page.html);
    r.replace("contents","You are not authorized to view "+uri+" try to <a href=\"loginQuery\">login</a> first.");
    return new Response(NanoHTTPD.HTTP_OK,NanoHTTPD.MIME_HTML,r.toString()); // we do not use HTTP authorization for the webpage access
  }

}
