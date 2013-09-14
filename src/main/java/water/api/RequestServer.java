package water.api;

import hex.*;
import hex.NeuralNet.NeuralNetProgress;
import hex.NeuralNet.NeuralNetScore;

import java.io.*;
import java.net.ServerSocket;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import water.*;
import water.api.Script.RunScript;
import water.api.Upload.PostFile;
import water.util.Log;
import water.util.Log.Tag.Sys;

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
    _http404 = registerRequest(new HTTP404());
    _http500 = registerRequest(new HTTP500());


    Request.addToNavbar(registerRequest(new Inspect()),     "Inspect",      "Data");
    Request.addToNavbar(registerRequest(new StoreView()),   "View All",     "Data");
    Request.addToNavbar(registerRequest(new Parse()),       "Parse",        "Data");
    Request.addToNavbar(registerRequest(new RReader()),     "Parse R Data", "Data");
    Request.addToNavbar(registerRequest(new ImportFiles()), "Import Files", "Data");
    Request.addToNavbar(registerRequest(new ImportUrl()),   "Import URL",   "Data");
    Request.addToNavbar(registerRequest(new ImportS3()),    "Import S3",    "Data");
    Request.addToNavbar(registerRequest(new ExportS3()),    "Export S3",    "Data");
    Request.addToNavbar(registerRequest(new ImportHdfs()),  "Import HDFS",  "Data");
    Request.addToNavbar(registerRequest(new ExportHdfs()),  "Export HDFS",  "Data");
    Request.addToNavbar(registerRequest(new Upload()),      "Upload",       "Data");
    Request.addToNavbar(registerRequest(new Get()),         "Download",     "Data");
    Request.addToNavbar(registerRequest(new SummaryPage()), "Summary",      "Data");

    Request.addToNavbar(registerRequest(new RF()),          "Random Forest", "Model");
    Request.addToNavbar(registerRequest(new GLM()),         "GLM",           "Model");
    Request.addToNavbar(registerRequest(new GLMGrid()),     "GLMGrid",       "Model");
    Request.addToNavbar(registerRequest(new KMeans()),      "KMeans",        "Model");
    Request.addToNavbar(registerRequest(new KMeansGrid()),  "KMeansGrid",    "Model");
    Request.addToNavbar(registerRequest(new PCA()),         "PCA (Beta)",    "Model");
    Request.addToNavbar(registerRequest(new hex.gbm.GBM()), "GBM (Beta)",    "Model");
    Request.addToNavbar(registerRequest(new Console()),     "Console",       "Model");

    Request.addToNavbar(registerRequest(new RFScore()),     "Random Forest", "Score");
    Request.addToNavbar(registerRequest(new GLMScore()),    "GLM",           "Score");
    Request.addToNavbar(registerRequest(new KMeansScore()), "KMeans",        "Score");
    Request.addToNavbar(registerRequest(new KMeansApply()), "KMeans Apply",  "Score");
    Request.addToNavbar(registerRequest(new PCAScore()),    "PCA",           "Score");
    Request.addToNavbar(registerRequest(new GeneratePredictionsPage()),       "Predict",   "Score");
    Request.addToNavbar(registerRequest(new GeneratePredictions2()), "Predict2", "Score");
    Request.addToNavbar(registerRequest(new Score()),       "Apply Model",   "Score");


    //Request.addToNavbar(registerRequest(new Plot()),        "Basic",         "Plot");
    registerRequest(new Plot());

    Request.addToNavbar(registerRequest(new Jobs()),        "Jobs",          "Admin");
    Request.addToNavbar(registerRequest(new Cloud()),       "Cluster Status","Admin");
    Request.addToNavbar(registerRequest(new IOStatus()),    "Cluster I/O",   "Admin");
    Request.addToNavbar(registerRequest(new Timeline()),    "Timeline",      "Admin");
    Request.addToNavbar(registerRequest(new JStack()),      "Stack Dump",    "Admin");
    Request.addToNavbar(registerRequest(new Debug()),       "Debug Dump",    "Admin");
    Request.addToNavbar(registerRequest(new LogView()),     "Inspect Log",   "Admin");
    Request.addToNavbar(registerRequest(new Script()),      "Get Script",    "Admin");
    Request.addToNavbar(registerRequest(new DataDistrib()), "Data Distrib",  "Admin");
    Request.addToNavbar(registerRequest(new Shutdown()),    "Shutdown",      "Admin");

    Request.addToNavbar(registerRequest(new Documentation()),       "H2O Documentation",      "Help");
    Request.addToNavbar(registerRequest(new Tutorials()),           "Tutorials Home",         "Help");
    Request.addToNavbar(registerRequest(new TutorialRFIris()),      "Random Forest Tutorial", "Help");
    Request.addToNavbar(registerRequest(new TutorialGLMProstate()), "GLM Tutorial",           "Help");
    Request.addToNavbar(registerRequest(new TutorialKMeans()),      "KMeans Tutorial",        "Help");

    if(H2O.OPT_ARGS.beta != null) {
      Request.addToNavbar(registerRequest(new ImportFiles2()),"Import Files2",  "Beta (FluidVecs!)");
      Request.addToNavbar(registerRequest(new Parse2()),      "Parse2"       ,  "Beta (FluidVecs!)");
      Request.addToNavbar(registerRequest(new Inspect2()),    "Inspect",        "Beta (FluidVecs!)");
      Request.addToNavbar(registerRequest(new KMeans2()),     "KMeans2"      ,  "Beta (FluidVecs!)");
      Request.addToNavbar(registerRequest(new hex.gbm.DRF()), "DRF2"         ,  "Beta (FluidVecs!)");
      Request.addToNavbar(registerRequest(new hex.LR2()), "Linear Regression2", "Beta (FluidVecs!)");
      //Request.addToNavbar(registerRequest(new water.api.Quantiles()), "Quantiles",    "Beta (FluidVecs!)");
      Request.addToNavbar(registerRequest(new NeuralNet()),   "Neural Network", "Beta (FluidVecs!)");
      Request.addToNavbar(registerRequest(new NeuralNetScore()), "Neural Network Score", "Beta (FluidVecs!)");
    }

    // internal handlers
    //registerRequest(new StaticHTMLPage("/h2o/CoefficientChart.html","chart"));
    registerRequest(new Cancel());
    registerRequest(new DRFModelView());
    registerRequest(new DRFProgressPage());
    registerRequest(new DownloadDataset());
    registerRequest(new Exec());
    registerRequest(new ExportS3Progress());
    registerRequest(new GBMModelView());
    registerRequest(new GBMProgressPage());
    registerRequest(new GLMGridProgress());
    registerRequest(new GLMProgressPage());
    registerRequest(new GetVector());
    registerRequest(new LogView.LogDownload());
    registerRequest(new NeuralNetProgress());
    registerRequest(new PostFile());
    registerRequest(new Progress());
    registerRequest(new Progress2());
    registerRequest(new PutValue());
    registerRequest(new PutVector());
    registerRequest(new RFTreeView());
    registerRequest(new RFView());
    registerRequest(new RPackage());
    registerRequest(new RReaderProgress());
    registerRequest(new Remove());
    registerRequest(new RemoveAck());
    registerRequest(new RunScript());
    registerRequest(new SetColumnNames());
    registerRequest(new TypeaheadFileRequest());
    registerRequest(new TypeaheadGLMModelKeyRequest());
    registerRequest(new TypeaheadHdfsPathRequest());
    registerRequest(new TypeaheadHexKeyRequest());
    registerRequest(new TypeaheadKeysRequest("Existing H2O Key", "", null));
    registerRequest(new TypeaheadRFModelKeyRequest());
    registerRequest(new TypeaheadS3BucketRequest());
    // testing hooks
    registerRequest(new TestPoll());
    registerRequest(new TestRedirect());
    Request.initializeNavBar();
  }

  /** Registers the request with the request server.
   *
   * returns the request so that it can be further updated.
   */

  public static Request registerRequest(Request req) {
    String href = req.href();
    assert (! _requests.containsKey(href)) : "Request with href "+href+" already registered";
    _requests.put(href,req);
    req.registered();
    return req;
  }

  // Keep spinning until we get to launch the NanoHTTPD
  public static void start() {
    new Thread( new Runnable() {
        public void run()  {
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
