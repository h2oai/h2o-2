package water.api;

import com.google.common.io.ByteStreams;
import com.google.common.io.Closeables;

import hex.*;
import hex.GridSearch.GridSearchProgress;
import hex.KMeans2.KMeans2ModelView;
import hex.KMeans2.KMeans2Progress;
import hex.anomaly.Anomaly;
import hex.deepfeatures.DeepFeatures;
import hex.deeplearning.DeepLearning;
import hex.drf.DRF;
import hex.gapstat.GapStatistic;
import hex.gapstat.GapStatisticModelView;
import hex.gbm.GBM;
import hex.glm.*;
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
import water.*;
import water.api.Upload.PostFile;
import water.api.handlers.ModelBuildersMetadataHandlerV1;
import water.deploy.LaunchJar;
import water.ga.AppViewHit;
import water.schemas.HTTP404V1;
import water.schemas.HTTP500V1;
import water.schemas.Schema;
import water.util.Log;
import water.util.Log.Tag.Sys;
import water.util.Utils.ExpectedExceptionForDebug;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.net.ServerSocket;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** This is a simple web server. */
public class RequestServer extends NanoHTTPD {
  private static final int LATEST_VERSION = 2;

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

  // An array of regexs-over-URLs and handling Methods.
  // The list is searched in-order, first match gets dispatched.
  protected static final LinkedHashMap<String,Method> _handlers = new LinkedHashMap<String,Method>();

  static final Request _http404;
  static final Request _http500;

  public static final Response response404(NanoHTTPD server, Properties parms) { return _http404.serve(server, parms, Request.RequestType.www); }
  public static final Response response500(NanoHTTPD server, Properties parms) { return _http500.serve(server, parms, Request.RequestType.www); }

  // initialization ------------------------------------------------------------
  static {
    boolean USE_NEW_TAB = true;

    _http404 = registerRequest(new HTTP404());
    _http500 = registerRequest(new HTTP500());


    registerGET("/1/metadata/modelbuilders/.*", ModelBuildersMetadataHandlerV1.class, "show");
    registerGET("/1/metadata/modelbuilders", ModelBuildersMetadataHandlerV1.class, "list");

    // Data
    Request.addToNavbar(registerRequest(new ImportFiles2()),  "Import Files",           "Data");
    Request.addToNavbar(registerRequest(new Upload2()),       "Upload",                 "Data");
    Request.addToNavbar(registerRequest(new Parse2()),        "Parse",                  "Data");
    Request.addToNavbar(registerRequest(new Inspector()),     "Inspect",                "Data");
    Request.addToNavbar(registerRequest(new SummaryPage2()),  "Summary",                "Data");
    Request.addToNavbar(registerRequest(new QuantilesPage()), "Quantiles",              "Data");
    Request.addToNavbar(registerRequest(new Impute()),        "Impute",                 "Data");
    Request.addToNavbar(registerRequest(new Interaction()),   "Interaction",            "Data");
    Request.addToNavbar(registerRequest(new CreateFrame()),   "Create Frame",           "Data");
    Request.addToNavbar(registerRequest(new FrameSplitPage()),"Split Frame",            "Data");
    Request.addToNavbar(registerRequest(new StoreView()),     "View All",               "Data");
    Request.addToNavbar(registerRequest(new ExportFiles()),   "Export Files",           "Data");
    // Register Inspect2 just for viewing frames
    registerRequest(new Inspect2());
    registerRequest(new MMStats());
    registerRequest(new GLMMakeModel());
    // FVec models
    Request.addToNavbar(registerRequest(new DeepLearning()),"Deep Learning",                   "Model");
    Request.addToNavbar(registerRequest(new GLM2()),        "Generalized Linear Model",        "Model");
    Request.addToNavbar(registerRequest(new GBM()),         "Gradient Boosting Machine",       "Model");
    Request.addToNavbar(registerRequest(new KMeans2()),     "K-Means Clustering",              "Model");
    Request.addToNavbar(registerRequest(new PCA()),         "Principal Component Analysis",    "Model");
    Request.addToNavbar(registerRequest(new SpeeDRF()),     "Random Forest",                   "Model");
    Request.addToNavbar(registerRequest(new DRF()),         "Random Forest - Big Data",        "Model");
    Request.addToNavbar(registerRequest(new Anomaly()),     "Anomaly Detection (Beta)",        "Model");
    Request.addToNavbar(registerRequest(new CoxPH()),       "Cox Proportional Hazards (Beta)", "Model");
    Request.addToNavbar(registerRequest(new DeepFeatures()),"Deep Feature Extractor (Beta)",   "Model");
    Request.addToNavbar(registerRequest(new NaiveBayes()),  "Naive Bayes Classifier (Beta)",   "Model");

    // FVec scoring
    Request.addToNavbar(registerRequest(new Predict()),     "Predict",                  "Score");
    // only for glm to allow for overriding of lambda_submodel
    registerRequest(new GLMPredict());
    Request.addToNavbar(registerRequest(new ConfusionMatrix()), "Confusion Matrix",     "Score");
    Request.addToNavbar(registerRequest(new AUC()),             "AUC",                  "Score");
    Request.addToNavbar(registerRequest(new HitRatio()),        "HitRatio",             "Score");
    Request.addToNavbar(registerRequest(new PCAScore()),        "PCAScore",             "Score");
    Request.addToNavbar(registerRequest(new GainsLiftTable()),  "Gains/Lift Table",     "Score");
    Request.addToNavbar(registerRequest(new Steam()),      "Multi-model Scoring (Beta)","Score");

    // Admin
    Request.addToNavbar(registerRequest(new Jobs()),        "Jobs",                     "Admin");
    Request.addToNavbar(registerRequest(new Cloud()),       "Cluster Status",           "Admin");
    Request.addToNavbar(registerRequest(new WaterMeterPerfbar()),  "Water Meter (Perfbar)",    "Admin");
    Request.addToNavbar(registerRequest(new LogView()),     "Inspect Log",              "Admin");
    Request.addToNavbar(registerRequest(new JProfile()),    "Profiler",                 "Admin");
    Request.addToNavbar(registerRequest(new JStack()),      "Stack Dump",               "Admin");
    Request.addToNavbar(registerRequest(new NetworkTest()), "Network Test",             "Admin");
    Request.addToNavbar(registerRequest(new IOStatus()),    "Cluster I/O",              "Admin");
    Request.addToNavbar(registerRequest(new Timeline()),    "Timeline",                 "Admin");
    Request.addToNavbar(registerRequest(new UDPDropTest()), "UDP Drop Test",            "Admin");
    Request.addToNavbar(registerRequest(new TaskStatus()),  "Task Status",              "Admin");
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
      registerRequest(new NFoldFrameExtractPage());
      registerRequest(new Console());
      registerRequest(new GapStatistic());
      registerRequest(new InsertMissingValues());
      registerRequest(new KillMinus3());
      registerRequest(new SaveModel());
      registerRequest(new LoadModel());
      registerRequest(new CollectLinuxInfo());
      registerRequest(new SetLogLevel());
      registerRequest(new Debug());
      registerRequest(new UnlockKeys());
      registerRequest(new Order());
      registerRequest(new RemoveVec());
      registerRequest(new GarbageCollect());
    } else {
      Request.addToNavbar(registerRequest(new MatrixMultiply()),       "Matrix Multiply",      "Beta");
      Request.addToNavbar(registerRequest(new hex.LR2()),              "Linear Regression2",   "Beta");
      Request.addToNavbar(registerRequest(new ReBalance()),            "ReBalance",            "Beta");
      Request.addToNavbar(registerRequest(new NFoldFrameExtractPage()),"N-Fold Frame Extract", "Beta");
      Request.addToNavbar(registerRequest(new Console()),              "Console",              "Beta");
      Request.addToNavbar(registerRequest(new GapStatistic()),         "Gap Statistic",        "Beta");
      Request.addToNavbar(registerRequest(new InsertMissingValues()),  "Insert Missing Values","Beta");
      Request.addToNavbar(registerRequest(new KillMinus3()),           "Kill Minus 3",         "Beta");
      Request.addToNavbar(registerRequest(new SaveModel()),            "Save Model",           "Beta");
      Request.addToNavbar(registerRequest(new LoadModel()),            "Load Model",           "Beta");
      Request.addToNavbar(registerRequest(new CollectLinuxInfo()),     "Collect Linux Info",   "Beta");
      Request.addToNavbar(registerRequest(new SetLogLevel()),          "Set Log Level",        "Beta");
      Request.addToNavbar(registerRequest(new Debug()),                "Debug Dump (floods log file)","Beta");
      Request.addToNavbar(registerRequest(new UnlockKeys()),           "Unlock Keys (use with caution)","Beta");
      Request.addToNavbar(registerRequest(new Order()),                "Order",                "Beta");
      Request.addToNavbar(registerRequest(new RemoveVec()),            "RemoveVec",            "Beta");
      Request.addToNavbar(registerRequest(new GarbageCollect()),       "GarbageCollect",       "Beta");
    }

    registerRequest(new Up());
    registerRequest(new Get()); // Download
    //Column Expand
    registerRequest(new OneHot());
    // internal handlers
    //registerRequest(new StaticHTMLPage("/h2o/CoefficientChart.html","chart"));
    registerRequest(new Cancel());
    registerRequest(new CoxPHModelView());
    registerRequest(new CoxPHProgressPage());
    registerRequest(new DomainMapping());
    registerRequest(new DRFModelView());
    registerRequest(new DRFProgressPage());
    registerRequest(new DownloadDataset());
    registerRequest(new Exec2());
    registerRequest(new GBMModelView());
    registerRequest(new GBMProgressPage());
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
    registerRequest(new Progress2());
    registerRequest(new PutValue());
    registerRequest(new Remove());
    registerRequest(new RemoveAll());
    registerRequest(new DeleteHDFSDir());
    registerRequest(new RemoveAck());
    registerRequest(new SpeeDRFModelView());
    registerRequest(new SpeeDRFProgressPage());
    registerRequest(new water.api.SetColumnNames2());     // Set colnames for FluidVec objects
    registerRequest(new LogAndEcho());
    registerRequest(new ToEnum2());
    registerRequest(new ToInt2());
    registerRequest(new GLMProgress());
    registerRequest(new hex.glm.GLMGridProgress());
    registerRequest(new water.api.Levels2());    // Temporary hack to get factor levels efficiently
    registerRequest(new SetTimezone());
    registerRequest(new GetTimezone());
    registerRequest(new ListTimezones());
    // Typeahead
    registerRequest(new TypeaheadModelKeyRequest());
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
    registerRequest(new GLMModelUpdate());
    registerRequest(new GLMGridView());
//    registerRequest(new GLMValidationView());
    registerRequest(new LaunchJar());
    Request.initializeNavBar();

    // Pure APIs, no HTML, to support The New World
    registerRequest(new Models());
    registerRequest(new Frames());
    registerRequest(new ModelMetrics());

    // WaterMeter support APIs
    registerRequest(new WaterMeterPerfbar.WaterMeterCpuTicks());
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

  /** Registers the request with the request server.  */
  public static String registerGET   (String url, Class hclass, String hmeth) { return register("GET"   ,url,hclass,hmeth); }
  public static String registerPUT   (String url, Class hclass, String hmeth) { return register("PUT"   ,url,hclass,hmeth); }
  public static String registerDELETE(String url, Class hclass, String hmeth) { return register("DELETE",url,hclass,hmeth); }
  public static String registerPOST  (String url, Class hclass, String hmeth) { return register("POST"  ,url,hclass,hmeth); }
  private static String register(String method, String url, Class hclass, String hmeth) {
    try {
      assert lookup(method,url)==null; // Not shadowed
      Method meth = hclass.getDeclaredMethod(hmeth);
      _handlers.put(method+url,meth);
      return url;
    } catch( NoSuchMethodException nsme ) {
      throw new Error("NoSuchMethodException: "+hclass.getName()+"."+hmeth);
    }
  }

  // Lookup the method/url in the register list, and return a matching Method
  private static Method lookup( String method, String url ) {
    String s = method+url;
    for( String x : _handlers.keySet() )
      if( x.equals(s) )         // TODO: regex
        return _handlers.get(x);
    return null;
  }

  // Handling ------------------------------------------------------------------
  private Schema handle( Request.RequestType type, Method meth, int version, Properties parms ) throws Exception {
    Schema S;
    switch( type ) {
    // case html: // These request-types only dictate the response-type;
    case java: // the normal action is always done.
    case json:
    case xml: {
      Class x = meth.getDeclaringClass();
      Class<Handler> clz = (Class<Handler>)x;
      Handler h = clz.newInstance();
      return h.handle(version,meth,parms); // Can throw any Exception the handler throws
    }
    case query:
    case help:
    default:
      throw H2O.unimpl();
    }
  }

  private Response wrap( String http_code, Schema S, RequestStatics.RequestType type ) {
    // Convert Schema to desired output flavor
    switch( type ) {
    case json:   return new Response(http_code, MIME_JSON, new String(S.writeJSON(new AutoBuffer()).buf()));
/*
    case xml:  //return new Response(http_code, MIME_XML , new String(S.writeXML (new AutoBuffer()).buf()));
    case java:
      throw H2O.unimpl();
    case html: {
      RString html = new RString(_htmlTemplate);
      html.replace("CONTENTS", S.writeHTML(new water.util.DocGen.HTML()).toString());
      return new Response(http_code, MIME_HTML, html.toString());
    }
*/
    default:
      throw H2O.fail();
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
  void maybeLogRequest (String uri, String method, Properties parms, Properties header) {
    boolean filterOutRepetitiveStuff = true;

    String log = String.format("%-4s %s", method, uri);
    for( Object arg : parms.keySet() ) {
      String value = parms.getProperty((String) arg);
      if( value != null && value.length() != 0 )
        log += " " + arg + "=" + value;
    }

    Log.info_no_stdout(Sys.HTLOG, log);

    if (filterOutRepetitiveStuff) {
      if (uri.endsWith(".css")) return;
      if (uri.endsWith(".js")) return;
      if (uri.endsWith(".png")) return;
      if (uri.endsWith(".ico")) return;
      if (uri.startsWith("/Typeahead")) return;
      if (uri.startsWith("/2/Typeahead")) return;
      if (uri.endsWith("LogAndEcho.json")) return;
      if (uri.startsWith("/Cloud.json")) return;
      if (uri.contains("Progress")) return;
      if (uri.startsWith("/Jobs.json")) return;
      if (uri.startsWith("/Up.json")) return;
      if (uri.startsWith("/2/WaterMeter")) return;
    }

    Log.info(Sys.HTTPD, log);

    if(header.getProperty("user-agent") != null)
      H2O.GA.postAsync(new AppViewHit(uri).customDimension(H2O.CLIENT_TYPE_GA_CUST_DIM, header.getProperty("user-agent")));
    else
      H2O.GA.postAsync(new AppViewHit(uri));
  }

  ///////// Stuff for URL parsing brought over from H2O2:
  /** Returns the name of the request, that is the request url without the
   *  request suffix.  E.g. converts "/GBM.html/crunk" into "/GBM/crunk" */
  String requestName(String url) {
    String s = "."+toString();
    int i = url.indexOf(s);
    if( i== -1 ) return url;    // No, or default, type
    return url.substring(0,i)+url.substring(i+s.length());
  }

  // Parse version number.  Java has no ref types, bleah, so return the version
  // number and the "parse pointer" by shift-by-16 compaction.
  // /1/xxx     --> version 1
  // /2/xxx     --> version 2
  // /v1/xxx    --> version 1
  // /v2/xxx    --> version 2
  // /latest/xxx--> LATEST_VERSION
  // /xxx       --> LATEST_VERSION
  private int parseVersion( String uri ) {
    if( uri.length() <= 1 || uri.charAt(0) != '/' ) // If not a leading slash, then I am confused
      return (0<<16)|LATEST_VERSION;
    if( uri.startsWith("/latest") )
      return (("/latest".length())<<16)|LATEST_VERSION;
    int idx=1;                  // Skip the leading slash
    int version=0;
    char c = uri.charAt(idx);   // Allow both /### and /v###
    if( c=='v' ) c = uri.charAt(++idx);
    while( idx < uri.length() && '0' <= c && c <= '9' ) {
      version = version*10+(c-'0');
      c = uri.charAt(++idx);
    }
    if( idx > 10 || version > LATEST_VERSION || version < 1 || uri.charAt(idx) != '/' )
      return (0<<16)|LATEST_VERSION; // Failed number parse or baloney version
    // Happy happy version
    return (idx<<16)|version;
  }

  @Override public NanoHTTPD.Response serve( String uri, String method, Properties header, Properties parms ) {
    // Jack priority for user-visible requests
    Thread.currentThread().setPriority(Thread.MAX_PRIORITY-1);
    // update arguments and determine control variables
    uri = maybeTransformRequest(uri);
    // determine the request type
    Request.RequestType type = Request.RequestType.requestType(uri);
    String requestName = type.requestName(uri);

    maybeLogRequest(uri, method, parms, header);

    // determine version
    int version = parseVersion(uri);
    int idx = version>>16;
    version &= 0xFFFF;
    String uripath = uri.substring(idx);

    String path = requestName(uripath); // Strip suffix type from middle of URI
    Method meth = null;
    try {
      // Find handler for url
      meth = lookup(method,path);
      if (meth != null) {
        return wrap(HTTP_OK,handle(type,meth,version,parms),type);
      }
    } catch( IllegalArgumentException e ) {
      return wrap(HTTP_BADREQUEST,new HTTP404V1(e.getMessage(),uri),type);
    } catch( Exception e ) {
      // make sure that no Exception is ever thrown out from the request
      return wrap(e.getMessage()!="unimplemented"? HTTP_INTERNALERROR : HTTP_NOTIMPLEMENTED, new HTTP500V1(e),type);
    }

    // Wasn't a new type of handler:
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
