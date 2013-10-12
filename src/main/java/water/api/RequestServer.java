package water.api;

import hex.*;
import hex.GridSearch.GridSearchProgress;
import hex.NeuralNet.NeuralNetProgress;
import hex.NeuralNet.NeuralNetScore;
import hex.gbm.GBM;
import hex.glm.*;

import java.io.*;
import java.net.ServerSocket;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import water.*;
import water.api.Script.RunScript;
import water.api.Upload.PostFile;
import water.util.*;
import water.util.Log.Tag.Sys;
import water.util.Utils.ExpectedExceptionForDebug;

import com.google.common.io.ByteStreams;
import com.google.common.io.Closeables;

/** This is a simple web server. */
public class RequestServer extends NanoHTTPD {
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
    _http404 = register(new HTTP404());
    _http500 = register(new HTTP500());

    Request.addToNavbar(register(new Inspect()),     "Inspect",                    "Data");
    Request.addToNavbar(register(new StoreView()),   "View All",                   "Data");
    Request.addToNavbar(register(new Parse()),       "Parse",                      "Data");
    Request.addToNavbar(register(new RReader()),     "Parse R Data",               "Data");
    Request.addToNavbar(register(new ImportFiles()), "Import Files",               "Data");
    Request.addToNavbar(register(new ImportUrl()),   "Import URL",                 "Data");
    Request.addToNavbar(register(new ImportS3()),    "Import S3",                  "Data");
    Request.addToNavbar(register(new ExportS3()),    "Export S3",                  "Data");
    Request.addToNavbar(register(new ImportHdfs()),  "Import HDFS",                "Data");
    Request.addToNavbar(register(new ExportHdfs()),  "Export HDFS",                "Data");
    Request.addToNavbar(register(new Upload()),      "Upload",                     "Data");
    Request.addToNavbar(register(new Get()),         "Download",                   "Data");
    Request.addToNavbar(register(new SummaryPage()), "Summary",                    "Data");

    Request.addToNavbar(register(new RF()),          "Random Forest",              "Model");
    Request.addToNavbar(register(new GLM()),         "GLM",                        "Model");
    Request.addToNavbar(register(new GLMGrid()),     "GLM Grid",                   "Model");
    Request.addToNavbar(register(new KMeans()),      "KMeans",                     "Model");
    Request.addToNavbar(register(new PCA()),         "PCA (Beta)",                 "Model");
    Request.addToNavbar(register(new GBM()),         "GBM (Beta)",                 "Model");
    Request.addToNavbar(register(new GLM2()),        "GLM2 (Beta)",                "Model");
    Request.addToNavbar(register(new NeuralNet()),   "Neural Network (Beta)",      "Model");

    Request.addToNavbar(register(new RFScore()),     "Random Forest",              "Score");
    Request.addToNavbar(register(new GLMScore()),    "GLM",                        "Score");
    Request.addToNavbar(register(new KMeansScore()), "KMeans",                     "Score");
    Request.addToNavbar(register(new KMeansApply()), "KMeans Apply",               "Score");
    Request.addToNavbar(register(new PCAScore()),    "PCA (Beta)",                 "Score");
    Request.addToNavbar(register(new NeuralNetScore()), "Neural Network (Beta)",   "Score");
    Request.addToNavbar(register(new GeneratePredictionsPage()),  "Predict",       "Score");
    Request.addToNavbar(register(new Predict()),     "Predict2",      "Score");
    Request.addToNavbar(register(new Score()),       "Apply Model",                "Score");
    Request.addToNavbar(register(new ConfusionMatrix()), "Confusion Matrix",       "Score");

    //Request.addToNavbar(registerRequest(new Plot()),        "Basic",         "Plot");
    register(new Plot());

    Request.addToNavbar(register(new Jobs()),        "Jobs",            "Admin");
    Request.addToNavbar(register(new Cloud()),       "Cluster Status",  "Admin");
    Request.addToNavbar(register(new IOStatus()),    "Cluster I/O",     "Admin");
    Request.addToNavbar(register(new Timeline()),    "Timeline",        "Admin");
    Request.addToNavbar(register(new JStack()),      "Stack Dump",      "Admin");
    Request.addToNavbar(register(new Debug()),       "Debug Dump",      "Admin");
    Request.addToNavbar(register(new LogView()),     "Inspect Log",     "Admin");
    Request.addToNavbar(register(new Script()),      "Get Script",      "Admin");
    Request.addToNavbar(register(new Shutdown()),    "Shutdown",        "Admin");

    Request.addToNavbar(register(new Documentation()),       "H2O Documentation",      "Help");
    Request.addToNavbar(register(new Tutorials()),           "Tutorials Home",         "Help");
    Request.addToNavbar(register(new TutorialRFIris()),      "Random Forest Tutorial", "Help");
    Request.addToNavbar(register(new TutorialGLMProstate()), "GLM Tutorial",           "Help");
    Request.addToNavbar(register(new TutorialKMeans()),      "KMeans Tutorial",        "Help");

    // Beta things should be reachable by the API and web redirects, but not put in the menu.
    if(H2O.OPT_ARGS.beta == null) {
      register(new ImportFiles2());
      register(new Parse2());
      register(new Inspect2());
      register(new KMeans2());
      register(new hex.gbm.DRF());
      register(new hex.LR2());
      register(new FrameSplit());
    }
    else {
      Request.addToNavbar(register(new ImportFiles2()),   "Import Files2",        "Beta (FluidVecs!)");
      Request.addToNavbar(register(new Parse2()),         "Parse2",               "Beta (FluidVecs!)");
      Request.addToNavbar(register(new Inspect2()),       "Inspect2",             "Beta (FluidVecs!)");
      Request.addToNavbar(register(new KMeans2()),        "KMeans2",              "Beta (FluidVecs!)");
      Request.addToNavbar(register(new hex.gbm.DRF()),    "DRF2",                 "Beta (FluidVecs!)");
      Request.addToNavbar(register(new hex.LR2()),        "Linear Regression2",   "Beta (FluidVecs!)");
      Request.addToNavbar(register(new SummaryPage2()),   "Summary2",             "Beta (FluidVecs!)");
      Request.addToNavbar(register(new FrameSplit()),     "Frame Split",          "Beta (FluidVecs!)");
      Request.addToNavbar(register(new Console()),        "Console",              "Beta (FluidVecs!)");
    }

    // internal handlers
    //registerRequest(new StaticHTMLPage("/h2o/CoefficientChart.html","chart"));
    register(new Cancel());
    register(new DRFModelView());
    register(new DRFProgressPage());
    register(new DownloadDataset());
    register(new Exec2());
    register(new Exec());      // Will be replaced by Exec2
    register(new DataManip()); // Will be replaced by Exec2
    register(new ExportS3Progress());
    register(new GBMModelView());
    register(new GBMProgressPage());
    register(new GLMGridProgress());
    register(new GLMProgressPage());
    register(new GetVector()); // Will be replaced by Exec2
    register(new GridSearchProgress());
    register(new LogView.LogDownload());
    register(new NeuralNetProgress());
    register(new PostFile());
    register(new Progress());
    register(new Progress2());
    register(new PutValue());
    register(new PutVector()); // Will be replaced by Exec2
    register(new RFTreeView());
    register(new RFView());
    register(new RPackage());
    register(new RReaderProgress());
    register(new Remove());
    register(new RemoveAck());
    register(new RunScript());
    register(new SetColumnNames());
    register(new TypeaheadFileRequest());
    register(new TypeaheadGLMModelKeyRequest());
    register(new TypeaheadHdfsPathRequest());
    register(new TypeaheadHexKeyRequest());
    register(new TypeaheadKeysRequest("Existing H2O Key", "", null));
    register(new TypeaheadRFModelKeyRequest());
    register(new TypeaheadS3BucketRequest());
    // testing hooks
    register(new TestPoll());
    register(new TestRedirect());
    register(new GLMProgressPage2());
    register(new GLMModelView());
    register(new GLMValidationView());
    Request.initializeNavBar();
  }

  /**
   * Registers the request with the request server.
   */
  public static Request register(Request req) {
    String href = req.href();
    assert (! _requests.containsKey(href)) : "Request with href "+href+" already registered";
    _requests.put(href,req);
    req.registered();
    return req;
  }

  /**
   * Registers a request to train a model.
   */
  public static Request register(Model model) {
    return register(model.defaultTrainJob());
  }

  public static void unregister(Request req) {
    String href = req.href();
    _requests.remove(href);
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
