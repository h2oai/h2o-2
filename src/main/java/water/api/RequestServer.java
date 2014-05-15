package water.api;

import com.google.common.io.ByteStreams;
import com.google.common.io.Closeables;
import hex.GridSearch.GridSearchProgress;
import hex.KMeans2;
import hex.KMeans2.KMeans2ModelView;
import hex.KMeans2.KMeans2Progress;
import hex.ReBalance;
import hex.deeplearning.DeepLearning;
import hex.drf.DRF;
import hex.gapstat.GapStatistic;
import hex.gapstat.GapStatisticModelView;
import hex.gbm.GBM;
import hex.glm.GLM2;
import hex.glm.GLMGridView;
import hex.glm.GLMModelView;
import hex.glm.GLMProgress;
import hex.nb.NBModelView;
import hex.nb.NBProgressPage;
import hex.gapstat.GapStatisticProgressPage;
import hex.nb.NaiveBayes;
import hex.pca.PCA;
import hex.pca.PCAModelView;
import hex.pca.PCAProgressPage;
import hex.pca.PCAScore;
import hex.singlenoderf.SpeeDRF;
import hex.singlenoderf.SpeeDRFModelView;
import hex.singlenoderf.SpeeDRFProgressPage;
import water.Boot;
import water.H2O;
import water.NanoHTTPD;
import water.api.Upload.PostFile;
import water.api.rest.schemas.GBMSchemaBloody;
import water.api.rest.schemas.GBMSchemaV1;
import water.deploy.LaunchJar;
import water.util.Log;
import water.util.Log.Tag.Sys;
import water.util.Utils.ExpectedExceptionForDebug;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

  static final Request _http404;
  static final Request _http500;

  // initialization ------------------------------------------------------------
  static {
    boolean USE_NEW_TAB = true;

    _http404 = registerRequest(new HTTP404());
    _http500 = registerRequest(new HTTP500());


//    Request.addToNavbar(registerRequest(new Inspect4UX()),  "NEW Inspect",                "Data"); //disable for now
    // REST API helper
    Request.addToNavbar(registerRequest(new GBMSchemaV1()),    "GBM API call", "REST");
    Request.addToNavbar(registerRequest(new GBMSchemaBloody()), "GBM Bloody", "REST");

    // Data
    Request.addToNavbar(registerRequest(new ImportFiles2()),  "Import Files",           "Data");
    Request.addToNavbar(registerRequest(new Upload2()),       "Upload",                 "Data");
    Request.addToNavbar(registerRequest(new Parse2()),        "Parse",                  "Data");
    Request.addToNavbar(registerRequest(new Inspector()),     "Inspect",                "Data");
    Request.addToNavbar(registerRequest(new SummaryPage2()),  "Summary",                "Data");
    Request.addToNavbar(registerRequest(new QuantilesPage()), "Quantiles",              "Data");
    Request.addToNavbar(registerRequest(new StoreView()),     "View All",               "Data");
    Request.addToNavbar(registerRequest(new ExportFiles()),   "Export Files",           "Data");
    // Register Inspect2 just for viewing frames
    registerRequest(new Inspect2());

    // Not supported for now
//    Request.addToNavbar(registerRequest(new ExportS3()),    "Export S3",                  "Data");
//    Request.addToNavbar(registerRequest(new ExportHdfs()),  "Export HDFS",                "Data");

    // Remove VA based algos from GUI
//    Request.addToNavbar(registerRequest(new GLM()),         "GLM",                        "Model");
//    Request.addToNavbar(registerRequest(new GLMGrid()),     "GLM Grid",                   "Model");
//    Request.addToNavbar(registerRequest(new KMeans()),      "KMeans",                     "Model");
//    Request.addToNavbar(registerRequest(new RF()),          "Single Node RF",             "Model");

    // FVec models
    Request.addToNavbar(registerRequest(new PCA()),         "PCA",                      "Model");
    Request.addToNavbar(registerRequest(new GBM()),         "GBM",                      "Model");
    Request.addToNavbar(registerRequest(new DeepLearning()),"Deep Learning",            "Model");
    Request.addToNavbar(registerRequest(new DRF()),         "Distributed RF",           "Model");
    Request.addToNavbar(registerRequest(new GLM2()),        "GLM",                      "Model");
    Request.addToNavbar(registerRequest(new SpeeDRF()),     "SpeeDRF (Beta)",           "Model");
    Request.addToNavbar(registerRequest(new KMeans2()),     "KMeans (Beta)",            "Model");
    Request.addToNavbar(registerRequest(new NaiveBayes()),  "Naive Bayes (Beta)",       "Model");

    // FVec scoring
    Request.addToNavbar(registerRequest(new Predict()),     "Predict",                  "Score");
    Request.addToNavbar(registerRequest(new ConfusionMatrix()), "Confusion Matrix",     "Score");
    Request.addToNavbar(registerRequest(new AUC()),         "AUC",                      "Score");
    Request.addToNavbar(registerRequest(new HitRatio()),    "HitRatio",                 "Score");
    Request.addToNavbar(registerRequest(new PCAScore()),    "PCAScore",                 "Score");
    Request.addToNavbar(registerRequest(new Steam()),    "Multi-model Scoring (Beta)", "Score");

    // Admin
    Request.addToNavbar(registerRequest(new Jobs()),        "Jobs",                     "Admin");
    Request.addToNavbar(registerRequest(new Cloud()),       "Cluster Status",           "Admin");
    Request.addToNavbar(registerRequest(new IOStatus()),    "Cluster I/O",              "Admin");
    Request.addToNavbar(registerRequest(new Timeline()),    "Timeline",                 "Admin");
    Request.addToNavbar(registerRequest(new JProfile()),    "Profiler",                 "Admin");
    Request.addToNavbar(registerRequest(new JStack()),      "Stack Dump",               "Admin");
    Request.addToNavbar(registerRequest(new Debug()),       "Debug Dump",               "Admin");
    Request.addToNavbar(registerRequest(new LogView()),     "Inspect Log",              "Admin");
    Request.addToNavbar(registerRequest(new UnlockKeys()),  "Unlock Keys",              "Admin");
    Request.addToNavbar(registerRequest(new Shutdown()),    "Shutdown",                 "Admin");

    // Help and Tutorials
    Request.addToNavbar(registerRequest(new Documentation()),       "H2O Documentation",      "Help", USE_NEW_TAB);
    Request.addToNavbar(registerRequest(new Tutorials()),           "Tutorials Home",         "Help", USE_NEW_TAB);
    Request.addToNavbar(registerRequest(new TutorialGBM()),         "GBM Tutorial",           "Help", USE_NEW_TAB);
    Request.addToNavbar(registerRequest(new TutorialDeepLearning()),"Deep Learning Tutorial", "Help", USE_NEW_TAB);
    Request.addToNavbar(registerRequest(new TutorialRFIris()),      "Random Forest Tutorial", "Help", USE_NEW_TAB);
    Request.addToNavbar(registerRequest(new TutorialGLMProstate()), "GLM Tutorial",           "Help", USE_NEW_TAB);
    Request.addToNavbar(registerRequest(new TutorialKMeans()),      "KMeans Tutorial",        "Help", USE_NEW_TAB);
    Request.addToNavbar(registerRequest(new AboutH2O()),            "About H2O",              "Help");

    // Beta things should be reachable by the API and web redirects, but not put in the menu.
    if(H2O.OPT_ARGS.beta == null) {
      registerRequest(new hex.LR2());
      registerRequest(new ReBalance());
      registerRequest(new FrameSplitPage());
      registerRequest(new GapStatistic());
    } else {
      Request.addToNavbar(registerRequest(new hex.LR2()),        "Linear Regression2",   "Beta");
      Request.addToNavbar(registerRequest(new ReBalance()),      "ReBalance",            "Beta");
      Request.addToNavbar(registerRequest(new FrameSplitPage()), "Split frame",          "Beta");
      Request.addToNavbar(registerRequest(new Console()),        "Console",              "Beta");
      Request.addToNavbar(registerRequest(new GapStatistic()),   "Gap Statistic",        "Beta");
//      Request.addToNavbar(registerRequest(new ExportModel()),    "Export Model",         "Beta (FluidVecs!)");
//      Request.addToNavbar(registerRequest(new ImportModel()),    "Import Model",         "Beta (FluidVecs!)");
    }

    // VA stuff is only shown with -beta
    if(H2O.OPT_ARGS.beta == null) {
      registerRequest(new Inspect());
      registerRequest(new SummaryPage());
      registerRequest(new Parse());
      registerRequest(new ImportFiles());
      registerRequest(new Upload());
      registerRequest(new ImportUrl());
      registerRequest(new ImportS3());
      registerRequest(new ExportS3());
      registerRequest(new ImportHdfs());
      registerRequest(new GLM());
      registerRequest(new GLMGrid());
      registerRequest(new KMeans());
      registerRequest(new RF());
      registerRequest(new RFScore());
      registerRequest(new GLMScore());
      registerRequest(new KMeansScore());
      registerRequest(new KMeansApply());
      registerRequest(new GeneratePredictionsPage());
      registerRequest(new Score());
    } else {
      Request.addToNavbar(registerRequest(new Upload()), "Upload", "VA (deprecated)");
      Request.addToNavbar(registerRequest(new ImportFiles()), "Import", "VA (deprecated)");
      Request.addToNavbar(registerRequest(new ImportUrl()), "ImportURL", "VA (deprecated)");
      Request.addToNavbar(registerRequest(new ImportS3()), "ImportS3", "VA (deprecated)");
      Request.addToNavbar(registerRequest(new ImportHdfs()), "ImportHDFS", "VA (deprecated)");
      Request.addToNavbar(registerRequest(new Parse()), "Parse", "VA (deprecated)");
      Request.addToNavbar(registerRequest(new Inspect()), "Inspect", "VA (deprecated)");
      Request.addToNavbar(registerRequest(new SummaryPage()), "Summary", "VA (deprecated)");
//    Request.addToNavbar(registerRequest(new ExportS3()), "ExportS3", "VA (deprecated)");
      Request.addToNavbar(registerRequest(new GLM()), "GLM", "VA (deprecated)");
      Request.addToNavbar(registerRequest(new GLMGrid()), "GLMGrid", "VA (deprecated)");
      Request.addToNavbar(registerRequest(new GLMScore()), "GLMScore", "VA (deprecated)");
      Request.addToNavbar(registerRequest(new KMeans()), "KMeans", "VA (deprecated)");
      Request.addToNavbar(registerRequest(new KMeansScore()), "KMeansScore", "VA (deprecated)");
      Request.addToNavbar(registerRequest(new KMeansApply()), "KMeansApply", "VA (deprecated)");
      Request.addToNavbar(registerRequest(new RF()), "RF", "VA (deprecated)");
      Request.addToNavbar(registerRequest(new RFScore()), "RFScore", "VA (deprecated)");
      Request.addToNavbar(registerRequest(new GeneratePredictionsPage()), "GeneratePredictionsPage", "VA (deprecated)");
      Request.addToNavbar(registerRequest(new Score()), "Score", "VA (deprecated)");
    }

    registerRequest(new Get()); // Download
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
    registerRequest(new NeuralNetModelView());
    registerRequest(new NeuralNetProgressPage());
    registerRequest(new DeepLearningModelView());
    registerRequest(new DeepLearningProgressPage());
    registerRequest(new KMeans2Progress());
    registerRequest(new KMeans2ModelView());
    registerRequest(new NBProgressPage());
    registerRequest(new GapStatisticProgressPage());
    registerRequest(new NBModelView());
    registerRequest(new GapStatisticModelView());
    registerRequest(new PCAProgressPage());
    registerRequest(new PCAModelView());
    registerRequest(new PostFile());
    registerRequest(new water.api.Upload2.PostFile());
    registerRequest(new Progress());
    registerRequest(new Progress2());
    registerRequest(new PutValue());
    registerRequest(new RFTreeView());
    registerRequest(new RFView());
    registerRequest(new RReaderProgress());
    registerRequest(new Remove());
    registerRequest(new RemoveAll());
    registerRequest(new RemoveAck());
    registerRequest(new SetColumnNames());
    registerRequest(new SpeeDRFModelView());
    registerRequest(new SpeeDRFProgressPage());
    registerRequest(new water.api.SetColumnNames2());     // Set colnames for FluidVec objects
    registerRequest(new LogAndEcho());
    registerRequest(new ToEnum());
    registerRequest(new ToEnum2());
    registerRequest(new ToInt2());
    registerRequest(new GLMProgress());
    registerRequest(new hex.glm.GLMGridProgress());
    registerRequest(new water.api.Levels2());    // Temporary hack to get factor levels efficiently
    registerRequest(new water.api.Levels());    // Ditto the above for ValueArray objects
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
    registerRequest(new LaunchJar());
    Request.initializeNavBar();

    // Pure APIs, no HTML, to support The New World
    registerRequest(new Models());
    registerRequest(new Frames());
    registerRequest(new ModelMetrics());
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
            } catch( Exception ioe ) {
              Log.err(Sys.HTTPD,"Launching NanoHTTP server got ",ioe);
              try { Thread.sleep(1000); } catch( InterruptedException e ) { } // prevent denial-of-service
            }
          }
        }
      }, "Request Server launcher").start();
  }

  public static String maybeTransformRequest (String uri) {
    if (uri.isEmpty() || uri.equals("/")) {
      return "/Tutorials.html";
    }

    Pattern p = Pattern.compile("/R/bin/([^/]+)/contrib/([^/]+)(.*)");
    Matcher m = p.matcher(uri);
    boolean b = m.matches();
    if (b) {
      // On Jenkins, this command sticks his own R version's number
      // into the package that gets built.
      //
      //     R CMD INSTALL -l $(TMP_BUILD_DIR) --build h2o-package
      //
      String versionOfRThatJenkinsUsed = "3.0";

      String platform = m.group(1);
      String version = m.group(2);
      String therest = m.group(3);
      String s = "/R/bin/" + platform + "/contrib/" + versionOfRThatJenkinsUsed + therest;
      return s;
    }

    return uri;
  }

  // uri serve -----------------------------------------------------------------
  void maybeLogRequest (String uri, String method, Properties parms) {
    boolean filterOutRepetitiveStuff = true;

    if (filterOutRepetitiveStuff) {
      if (uri.endsWith(".css")) return;
      if (uri.endsWith(".js")) return;
      if (uri.endsWith(".png")) return;
      if (uri.endsWith(".ico")) return;
      if (uri.startsWith("/Typeahead")) return;
      if (uri.startsWith("/2/Typeahead")) return;
      if (uri.startsWith("/Cloud.json")) return;
      if (uri.endsWith("LogAndEcho.json")) return;
      if (uri.contains("Progress")) return;
      if (uri.startsWith("/Jobs.json")) return;
    }

    String log = String.format("%-4s %s", method, uri);
    for( Object arg : parms.keySet() ) {
      String value = parms.getProperty((String) arg);
      if( value != null && value.length() != 0 )
        log += " " + arg + "=" + value;
    }
    Log.info(Sys.HTTPD, log);
  }

  @Override public NanoHTTPD.Response serve( String uri, String method, Properties header, Properties parms ) {
    // Jack priority for user-visible requests
    Thread.currentThread().setPriority(Thread.MAX_PRIORITY-1);
    // update arguments and determine control variables
    uri = maybeTransformRequest(uri);
    // determine the request type
    Request.RequestType type = Request.RequestType.requestType(uri);
    String requestName = type.requestName(uri);

    maybeLogRequest(uri, method, parms);
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
    } catch( Exception e ) {
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
    // return new NanoHTTPD.Response(NanoHTTPD.HTTP_OK,mime,new ByteArrayInputStream(bytes));
    NanoHTTPD.Response res = new NanoHTTPD.Response(NanoHTTPD.HTTP_OK,mime,new ByteArrayInputStream(bytes));
    res.addHeader("Content-Length", Long.toString(bytes.length));
    // res.addHeader("Content-Disposition", "attachment; filename=" + uri);
    return res;
  }

}
