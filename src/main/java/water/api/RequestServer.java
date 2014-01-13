package water.api;

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
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import water.*;
import water.api.Script.RunScript;
import water.api.Upload.PostFile;
import water.deploy.LaunchJar;
import water.util.*;
import water.util.Log.Tag.Sys;
import water.util.Utils.ExpectedExceptionForDebug;

import com.google.common.io.ByteStreams;
import com.google.common.io.Closeables;

/** This is a simple web server. */
public class RequestServer extends NanoHTTPD {
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
    registerRequest(new water.api.Levels());    // Temporary hack to get factor levels efficiently
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
  public static void start() {
    new Thread( new Runnable() {
        @Override public void run()  {
          while( true ) {
            try {
              // Try to get the NanoHTTP daemon started
              SERVER = new RequestServer(H2O._apiSocket);
              break;
            } catch ( Exception ioe ) {
              Log.err(Sys.HTTPD,"Launching NanoHTTP server got ",ioe);
              try { Thread.sleep(1000); } catch( InterruptedException e ) { } // prevent denial-of-service
            }
          }
        }
      }, "Request Server launcher").start();
  }

  // uri serve -----------------------------------------------------------------
  @Override public NanoHTTPD.Response serve( String uri, String method, Properties header, Properties parms ) {
    // Jack priority for user-visible requests
    Thread.currentThread().setPriority(Thread.MAX_PRIORITY-1);
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
      parms.setProperty(Request.ERROR,e.getClass().getSimpleName()+": "+e.getMessage());
      return _http500.serve(this,parms,type);
    }
  }

  private RequestServer( ServerSocket socket ) throws IOException {
    super(socket,null);
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
    String mime = NanoHTTPD.MIME_DEFAULT_BINARY;
    if (uri.endsWith(".css"))
      mime = "text/css";
    else if (uri.endsWith(".html"))
      mime = "text/html";
    return new NanoHTTPD.Response(NanoHTTPD.HTTP_OK,mime,new ByteArrayInputStream(bytes));
  }

}
