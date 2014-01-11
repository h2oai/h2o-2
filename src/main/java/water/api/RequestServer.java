package water.api;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import hex.*;
import hex.GridSearch.GridSearchProgress;
import hex.KMeans2.KMeans2ModelView;
import hex.KMeans2.KMeans2Progress;
import hex.NeuralNet.NeuralNetProgress;
import hex.NeuralNet.NeuralNetScore;
import hex.drf.DRF;
import hex.gbm.GBM;
import hex.glm.*;
import hex.pca.*;

import java.io.*;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import water.*;
import water.api.Script.RunScript;
import water.api.Upload.PostFile;
import water.deploy.LaunchJar;
import water.fvec.UploadFileVec;
import water.util.*;
import water.util.Log.Tag.Sys;
import water.util.Utils.ExpectedExceptionForDebug;

import water.api2.*;

import com.google.common.io.ByteStreams;
import com.google.common.io.Closeables;

/** This is a simple web server. */
public class RequestServer extends NanoHTTPD {
  /**
   * Some HTTP response status codes
   */
  public static final String
          HTTP_OK = "200 OK",
          HTTP_PARTIALCONTENT = "206 Partial Content",
          HTTP_RANGE_NOT_SATISFIABLE = "416 Requested Range Not Satisfiable",
          HTTP_REDIRECT = "301 Moved Permanently",
          HTTP_NOTMODIFIED = "304 Not Modified",
          HTTP_FORBIDDEN = "403 Forbidden",
          HTTP_UNAUTHORIZED = "401 Unauthorized",
          HTTP_NOTFOUND = "404 Not Found",
          HTTP_BADREQUEST = "400 Bad Request",
          HTTP_TOOLONGREQUEST = "414 Request-URI Too Long",
          HTTP_INTERNALERROR = "500 Internal Server Error",
          HTTP_NOTIMPLEMENTED = "501 Not Implemented";

  /**
   * Common mime types for dynamic content
   */
  public static final String
          MIME_PLAINTEXT = "text/plain",
          MIME_HTML = "text/html",
          MIME_JSON = "application/json",
          MIME_DEFAULT_BINARY = "application/octet-stream",
          MIME_XML = "text/xml";

  public enum API_VERSION {
    V_1(1, "/"),
    V_2(2, "/2/"); // FIXME: better should be /v2/
    final private int _version;
    final private String _prefix;
    public final String prefix() { return _prefix; }
    private API_VERSION(int version, String prefix) { _version = version; _prefix = prefix; }
  }
  static RequestServer SERVER;

  // cache of all loaded resources
  private static final ConcurrentHashMap<String,byte[]> _cache = new ConcurrentHashMap();
  protected static final HashMap<String,Request> _requests = new HashMap();
  public static HashMap<String,Request> requests() {
    return _requests;
  }

  static final Request _http404;
  static final Request _http500;

  // initialization ------------------------------------------------------------
  static {
    _http404 = registerRequest(new HTTP404());
    _http500 = registerRequest(new HTTP500());

    Request.addToNavbar(registerRequest(new Inspect4UX()),  "NEW Inspect",                "Data");
    Request.addToNavbar(registerRequest(new Inspect()),     "Inspect",                    "Data");
    Request.addToNavbar(registerRequest(new StoreView()),   "View All",                   "Data");
    Request.addToNavbar(registerRequest(new Parse()),       "Parse",                      "Data");
    Request.addToNavbar(registerRequest(new ImportFiles()), "Import Files",               "Data");
    Request.addToNavbar(registerRequest(new ImportUrl()),   "Import URL",                 "Data");
    Request.addToNavbar(registerRequest(new ImportS3()),    "Import S3",                  "Data");
    Request.addToNavbar(registerRequest(new ExportS3()),    "Export S3",                  "Data");
    Request.addToNavbar(registerRequest(new ImportHdfs()),  "Import HDFS",                "Data");
    Request.addToNavbar(registerRequest(new ExportHdfs()),  "Export HDFS",                "Data");
    Request.addToNavbar(registerRequest(new Upload()),      "Upload",                     "Data");
    Request.addToNavbar(registerRequest(new Get()),         "Download",                   "Data");

    Request.addToNavbar(registerRequest(new SummaryPage()), "Summary",                    "Model");
    Request.addToNavbar(registerRequest(new GLM()),         "GLM",                        "Model");
    Request.addToNavbar(registerRequest(new GLMGrid()),     "GLM Grid",                   "Model");
    Request.addToNavbar(registerRequest(new PCA()),         "PCA",                        "Model");
    Request.addToNavbar(registerRequest(new KMeans()),      "KMeans",                     "Model");
    Request.addToNavbar(registerRequest(new GBM()),         "GBM",                        "Model");
    Request.addToNavbar(registerRequest(new RF()),          "Single Node RF",             "Model");
    Request.addToNavbar(registerRequest(new DRF()),         "Distributed RF (Beta)",      "Model");
    Request.addToNavbar(registerRequest(new GLM2()),        "GLM2 (Beta)",                "Model");
    Request.addToNavbar(registerRequest(new KMeans2()),     "KMeans2 (Beta)",             "Model");
    Request.addToNavbar(registerRequest(new NeuralNet()),   "Neural Network (Beta)",      "Model");

    Request.addToNavbar(registerRequest(new RFScore()),     "Random Forest",              "Score");
    Request.addToNavbar(registerRequest(new GLMScore()),    "GLM",                        "Score");
    Request.addToNavbar(registerRequest(new KMeansScore()), "KMeans",                     "Score");
    Request.addToNavbar(registerRequest(new KMeansApply()), "KMeans Apply",               "Score");
    Request.addToNavbar(registerRequest(new PCAScore()),    "PCA (Beta)",                 "Score");
    Request.addToNavbar(registerRequest(new NeuralNetScore()), "Neural Network (Beta)",   "Score");
    Request.addToNavbar(registerRequest(new GeneratePredictionsPage()),  "Predict",       "Score");
    Request.addToNavbar(registerRequest(new Predict()),     "Predict2",      "Score");
    Request.addToNavbar(registerRequest(new Score()),       "Apply Model",                "Score");
    Request.addToNavbar(registerRequest(new ConfusionMatrix()), "Confusion Matrix",       "Score");

    Request.addToNavbar(registerRequest(new Jobs()),        "Jobs",            "Admin");
    Request.addToNavbar(registerRequest(new Cloud()),       "Cluster Status",  "Admin");
    Request.addToNavbar(registerRequest(new IOStatus()),    "Cluster I/O",     "Admin");
    Request.addToNavbar(registerRequest(new Timeline()),    "Timeline",        "Admin");
    Request.addToNavbar(registerRequest(new JStack()),      "Stack Dump",      "Admin");
    Request.addToNavbar(registerRequest(new Debug()),       "Debug Dump",      "Admin");
    Request.addToNavbar(registerRequest(new LogView()),     "Inspect Log",     "Admin");
    Request.addToNavbar(registerRequest(new Script()),      "Get Script",      "Admin");
    Request.addToNavbar(registerRequest(new Shutdown()),    "Shutdown",        "Admin");

    Request.addToNavbar(registerRequest(new Documentation()),       "H2O Documentation",      "Help");
    Request.addToNavbar(registerRequest(new Tutorials()),           "Tutorials Home",         "Help");
    Request.addToNavbar(registerRequest(new TutorialRFIris()),      "Random Forest Tutorial", "Help");
    Request.addToNavbar(registerRequest(new TutorialGLMProstate()), "GLM Tutorial",           "Help");
    Request.addToNavbar(registerRequest(new TutorialKMeans()),      "KMeans Tutorial",        "Help");

    // Beta things should be reachable by the API and web redirects, but not put in the menu.
    if(H2O.OPT_ARGS.beta == null) {
      registerRequest(new ImportFiles2());
      registerRequest(new Parse2());
      registerRequest(new Inspect2());
      registerRequest(new SummaryPage2());
      registerRequest(new hex.LR2());
    } else {
      Request.addToNavbar(registerRequest(new ImportFiles2()),   "Import Files2",        "Beta (FluidVecs!)");
      Request.addToNavbar(registerRequest(new Parse2()),         "Parse2",               "Beta (FluidVecs!)");
      Request.addToNavbar(registerRequest(new Upload2()),        "Upload2",              "Beta (FluidVecs!)");
      Request.addToNavbar(registerRequest(new Inspect2()),       "Inspect2",             "Beta (FluidVecs!)");
      Request.addToNavbar(registerRequest(new hex.LR2()),        "Linear Regression2",   "Beta (FluidVecs!)");
      Request.addToNavbar(registerRequest(new SummaryPage2()),   "Summary2",             "Beta (FluidVecs!)");
      Request.addToNavbar(registerRequest(new Console()),        "Console",              "Beta (FluidVecs!)");
      Request.addToNavbar(registerRequest(new ExportModel()),    "Export Model",         "Beta (FluidVecs!)");
      Request.addToNavbar(registerRequest(new ImportModel()),    "Import Model",         "Beta (FluidVecs!)");
    }

    //Column Expand
    registerRequest(new OneHot());
    // internal handlers
    //registerRequest(new StaticHTMLPage("/h2o/CoefficientChart.html","chart"));
    registerRequest(new Cancel());
    registerRequest(new DRFModelView());
    registerRequest(new DRFProgressPage());
    registerRequest(new DownloadDataset());
    registerRequest(new Exec2());
    registerRequest(new ExportS3Progress());
    registerRequest(new GBMModelView());
    registerRequest(new GBMProgressPage());
    registerRequest(new GLMGridProgress());
    registerRequest(new GLMProgressPage());
    registerRequest(new GridSearchProgress());
    registerRequest(new LogView.LogDownload());
    registerRequest(new RPackage.RDownload());
    registerRequest(new NeuralNetProgress());
    registerRequest(new KMeans2Progress());
    registerRequest(new KMeans2ModelView());
    registerRequest(new PCAProgressPage());
    registerRequest(new PCAModelView());
    registerRequest(new PostFile());
    registerRequest(new water.api.Upload2.PostFile());
    registerRequest(new Progress());
    registerRequest(new Progress2());
    registerRequest(new PutValue());
    registerRequest(new RFTreeView());
    registerRequest(new RFView());
    registerRequest(new RPackage());
    registerRequest(new RReaderProgress());
    registerRequest(new Remove());
    registerRequest(new RemoveAll());
    registerRequest(new RemoveAck());
    registerRequest(new RunScript());
    registerRequest(new SetColumnNames());
    registerRequest(new LogAndEcho());
    registerRequest(new GLMProgress());
    registerRequest(new hex.glm.GLMGridProgress());
    // Typeahead
    registerRequest(new TypeaheadModelKeyRequest());
    registerRequest(new TypeaheadGLMModelKeyRequest());
    registerRequest(new TypeaheadRFModelKeyRequest());
    registerRequest(new TypeaheadKMeansModelKeyRequest());
    registerRequest(new TypeaheadPCAModelKeyRequest());
    registerRequest(new TypeaheadHexKeyRequest());
    registerRequest(new TypeaheadFileRequest());
    registerRequest(new TypeaheadHdfsPathRequest());
    registerRequest(new TypeaheadKeysRequest("Existing H2O Key", "", null));
    registerRequest(new TypeaheadS3BucketRequest());
    // testing hooks
    registerRequest(new TestPoll());
    registerRequest(new TestRedirect());
//    registerRequest(new GLMProgressPage2());
    registerRequest(new GLMModelView());
    registerRequest(new GLMGridView());
//    registerRequest(new GLMValidationView());
    registerRequest(new FrameSplit());
    registerRequest(new LaunchJar());
    Request.initializeNavBar();
  }

  /**
   * Registers the request with the request server.
   */
  public static Request registerRequest(Request req) {
    assert req.supportedVersions().length > 0;
    for (API_VERSION ver : req.supportedVersions()) {
      String href = req.href(ver);
      assert (! _requests.containsKey(href)) : "Request with href "+href+" already registered";
      _requests.put(href,req);
      req.registered(ver);
    }
    return req;
  }

  public static void unregisterRequest(Request req) {
    for (API_VERSION ver : req.supportedVersions()) {
      String href = req.href(ver);
      _requests.remove(href);
    }
  }

  // Keep spinning until we get to launch the NanoHTTPD
  public static void mystart() {
    new Thread( new Runnable() {
        @Override public void run()  {
          while( true ) {
            try {
              // Try to get the NanoHTTP daemon started
              SERVER = new RequestServer(H2O._apiSocket);
              SERVER.start();
              break;
            } catch ( Exception ioe ) {
              Log.err(Sys.HTTPD,"Launching NanoHTTP server got ",ioe);
              try { Thread.sleep(1000); } catch( InterruptedException e ) { } // prevent denial-of-service
            }
          }
        }
      }, "Request Server launcher").start();
  }

  @Override public Response serve(IHTTPSession session) {
    // Jack priority for user-visible requests
    Thread.currentThread().setPriority(Thread.MAX_PRIORITY - 1);

    Map<String, String> files = new HashMap<String, String>();
    Method method = session.getMethod();
    if (Method.PUT.equals(method) || Method.POST.equals(method)) {
      try {
        session.parseBody(files);
      } catch (IOException ioe) {
        return new Response(Response.Status.INTERNAL_ERROR, MIME_PLAINTEXT, "SERVER INTERNAL ERROR: IOException: " + ioe.getMessage());
      } catch (ResponseException re) {
        return new Response(re.getStatus(), MIME_PLAINTEXT, re.getMessage());
      }
    }

    String uri = session.getUri();
    boolean va_postfile = uri.matches("/PostFile.*");
    boolean fvec_postfile = uri.matches("/\\d+/PostFile.*");
    Map<String, String> parmsMap = new HashMap<String, String>();
    if (va_postfile || fvec_postfile) {
      // RequestArguments path is very sensitive and cannot tolerate the extra argument
      // that triggers the file inclusion, which is a weird API bug.  Tolerate this for now.
      //
      // e.g.  curl -v -F "file=@allyears2k_headers.zip" "http://localhost:54321/PostFile.json?key=a.zip"
      //
      // The 'key' argument is used to specify the key name.  The 'file' argument is used to
      // provide the file, but the RequestArguments flow rejects it.  So forcibly purge all
      // arguments not named 'key' here.
      String key = "key";
      String value = session.getParms().get(key);
      if (value != null) {
        parmsMap.put(key, value);
      }
    }
    else {
      parmsMap = session.getParms();
    }
    Properties parms = new Properties();
    parms.putAll(parmsMap);

    if (Method.POST.equals(method)) {
      if (va_postfile || fvec_postfile) {
        if (files.size() > 0) {
          String fileName = (String) files.values().toArray()[0];
          String key = parms.getProperty("key");
          if (key != null) {
            FileInputStream fis = null;
            try {
              fis = new FileInputStream(fileName);
              if (va_postfile) {
                ValueArray.readPut(key, fis);
              }
              else {
                assert (fvec_postfile);
                UploadFileVec.readPut(key, fis);
              }
            }
            catch (Exception e) {
              // This should never happen, since the file is created by NanoHTTPD.
              Log.err(e);
              Log.err("NanoHTTPD POST failed to write file " + fileName + " (" + e.getMessage() + ")");
            }
            finally {
              if (fis != null) {
                try { fis.close(); } catch (Exception _) {}
              }
            }
          }
        }
      }
    }

    //----------------------------------------------------------------------
    // New simple AbstractSimpleRequestHandler handlers.
    //----------------------------------------------------------------------

    AbstractSimpleRequestHandler srh = findSimpleRequestHandler(method.name(), uri);
    if (srh != null) {
      try {
        Response response = srh.serve(session);
        return response;
      }
      catch (ASRIllegalArgumentException e) {
        ASRArgumentErrorInfo ei = e.getErrorInfo();
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        String json = gson.toJson(ei);
        Response response = new Response(Response.Status.BAD_REQUEST, RequestServer.MIME_JSON, json);
        return response;
      }
      catch (Exception e) {
        StringWriter sw = new StringWriter();
        e.printStackTrace(new PrintWriter(sw));
        String st = sw.toString();
        ASRInternalServerErrorInfo ei = new ASRInternalServerErrorInfo(e.getMessage() != null ? e.getMessage() : "", st);
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        String json = gson.toJson(ei);
        Response response = new Response(Response.Status.INTERNAL_ERROR, RequestServer.MIME_JSON, json);
        return response;
      }
    }

    //----------------------------------------------------------------------
    // Legacy RequestArguments handlers.
    //----------------------------------------------------------------------

    // update arguments and determine control variables
    if( uri.isEmpty() || uri.equals("/") ) uri = "/Tutorials.html";
    // determine the request type
    Request.RequestType type = Request.RequestType.requestType(uri);
    String requestName = type.requestName(uri);
    try {
      // determine if we have known resource
      Request request = _requests.get(requestName);
      // if the request is not know, treat as resource request, or 404 if not
      // found
      if (request == null)
        return getResource(uri);
      // Some requests create an instance per call
      request = request.create(parms);
      // call the request
      return request.serve(this,parms,type);
    } catch (Exception e) {
      if(!(e instanceof ExpectedExceptionForDebug))
        e.printStackTrace();
      // make sure that no Exception is ever thrown out from the request
      parms.put(Request.ERROR, e.getClass().getSimpleName() + ": " + e.getMessage());
      return _http500.serve(this,parms,type);
    }
  }

  private RequestServer( ServerSocket socket ) throws IOException {
    super(socket);
    _srhList = new ArrayList<AbstractSimpleRequestHandler>();
    addInitialSimpleRequestHandlers();
  }

  // Resource loading ----------------------------------------------------------

  // Returns the response containing the given uri with the appropriate mime
  // type.
  private NanoHTTPD.Response getResource(String uri) {
    byte[] bytes = _cache.get(uri);
    if( bytes == null ) {
      InputStream resource = Boot._init.getResource2(uri);
      if (resource != null) {
        try {
          bytes = ByteStreams.toByteArray(resource);
        } catch( IOException e ) { Log.err(e); }
        byte[] res = _cache.putIfAbsent(uri,bytes);
        if( res != null ) bytes = res; // Racey update; take what is in the _cache
      }
      Closeables.closeQuietly(resource);
    }
    if ((bytes == null) || (bytes.length == 0)) {
      // make sure that no Exception is ever thrown out from the request
      Properties parms = new Properties();
      parms.setProperty(Request.ERROR,uri);
      return _http404.serve(this,parms,Request.RequestType.www);
    }
    String mime = RequestServer.MIME_DEFAULT_BINARY;
    if (uri.endsWith(".css"))
      mime = "text/css";
    else if (uri.endsWith(".html"))
      mime = "text/html";
    return new NanoHTTPD.Response(Response.Status.OK,mime,new ByteArrayInputStream(bytes));
  }

  //----------------------------------------------------------------------
  // SimpleRequestHandler state.
  //----------------------------------------------------------------------

  private ArrayList<AbstractSimpleRequestHandler> _srhList;

  public void addSimpleRequestHandler(AbstractSimpleRequestHandler srh) {
    _srhList.add(srh);
  }

  private void addInitialSimpleRequestHandlers() {
    addSimpleRequestHandler(new list_uri());
  }

  private AbstractSimpleRequestHandler findSimpleRequestHandler(String method, String uri) {
    for (AbstractSimpleRequestHandler srh : _srhList) {
      if (srh.matches(method, uri)) {
        return srh;
      }
    }

    return null;
  }
}
