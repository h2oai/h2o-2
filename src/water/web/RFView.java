package water.web;
import hex.rf.Confusion;
import hex.rf.RFModel;

import java.util.Arrays;
import java.util.Properties;

import water.*;

import com.google.gson.JsonObject;

public class RFView extends H2OPage {
  public static final String DATA_KEY  = "dataKey";
  public static final String MODEL_KEY = "modelKey";
  public static final String CLASS_COL = "class";
  public static final String REQ_TREE  = "atree";
  public static final String NUM_TREE  = "ntree";
  public static final String IGNORE_COL= "ignore";
  public static final String OOBEE     = "OOBEE";
  public static final int MAX_CLASSES = 4096;

  @Override public String[] requiredArguments() {
    return new String[] { DATA_KEY, MODEL_KEY };
  }

  @Override public JsonObject serverJson(Server s, Properties p, String sessionID) throws PageError {
    // The dataset is required
    ValueArray ary = ServletUtil.check_array(p, DATA_KEY);

    // The model is required
    final Key modelKey = ServletUtil.check_key(p, MODEL_KEY);
    RFModel model = UKV.get(modelKey, new RFModel());
    if( model == null ) throw new PageError("RFModel key is missing");

    // Class is optional
    int classcol = getAsNumber(p, CLASS_COL, ary._cols.length-1);
    if( classcol < 0 || classcol >= ary._cols.length )
      throw new PageError("Class out of range");

    // Atree & Ntree are optional.
    // Atree - number of trees to display, if not all are available.
    // Ntree - final number of trees that will eventually be built.
    //   0 <= atree <= model.size() <= ntree
    int atree = getAsNumber(p, REQ_TREE,0);
    int ntree = getAsNumber(p, NUM_TREE, model.size());

    // Compute Out of Bag Error Estimate
    boolean oobee = getBoolean(p,OOBEE);

    double[] classWt = RandomForestPage.determineClassWeights(p.getProperty("classWt",""), ary, classcol, MAX_CLASSES);

    // Pick columns to ignore
    String igz = p.getProperty(IGNORE_COL);
    if( igz!=null ) System.out.println("[CM] ignoring: " + igz);

    // Validation is moderately expensive, so do not run validation unless
    // asked-for or all trees are finally available.  "atrees" is the number of
    // trees for which validation has been asked-for.  Only validate up to this
    // limit, unless all trees have finally arrived.
    if( model.size() == ntree ) atree = ntree;

    JsonObject res = new JsonObject();
    res.addProperty(DATA_KEY,    ary._key.toString());
    res.addProperty(MODEL_KEY,   modelKey.toString());
    res.addProperty(CLASS_COL,   classcol);
    res.addProperty(NUM_TREE,    ntree); // asked-for trees
    res.addProperty(REQ_TREE,    atree); // displayed trees
    res.addProperty("modelSize", model.size()); // how many we got

    // only create conf matrix if asked for
    if (p.getProperty("noCM","0").equals("0")) {
      // Make or find a C.M. against the model.  If the model has had a prior
      // C.M. run, we'll find it via hashing.  If not, we'll block while we build
      // the C.M.
      Confusion confusion = Confusion.make( model, ary._key, classcol, classWt, oobee );
      res.addProperty("confusionKey", confusion.keyFor().toString());
    }
    return res;
  }

  @Override public String serveImpl(Server s, Properties p, String sessionID) throws PageError {
    // Update the Model.
    // Compute the Confusion.
    JsonObject json = serverJson(s, p, sessionID);
    if( json.has("error") ) return H2OPage.error(json.get("error").toString());

    // The dataset is required
    ValueArray ary = ServletUtil.check_array(p, DATA_KEY);
    final int classcol = json.get(CLASS_COL).getAsInt();

    // The model is required
    final Key modelKey = ServletUtil.check_key(p, MODEL_KEY);
    RFModel model = UKV.get(modelKey, new RFModel());
    int atree = json.get(REQ_TREE).getAsInt();
    int ntree = json.get(NUM_TREE).getAsInt();
    if( model.size() == ntree ) atree = ntree;

    // Compute Out of Bag Error Estimate
    boolean oobee = getBoolean(p,OOBEE);

    double[] classWt = RandomForestPage.determineClassWeights(p.getProperty("classWt",""), ary, classcol, MAX_CLASSES);

    if (p.getProperty("clearCM","0").equals("1"))
      Confusion.remove(model,ary._key,classcol,oobee);

    RString response = new RString(html());
    // Since the model has already been run on this dataset (in the serverJson
    // above), and Confusion.make caches - calling it again a quick way to
    // de-serialize the Confusion from the H2O Store.
    Confusion confusion = Confusion.make( model, ary._key, classcol, classWt, oobee );
    if (confusion.isValid()) {
      confusion.report();
      // Display the confusion-matrix table here
      // First the title line
      final int N = model.classes();
      int cmin = (int)ary._cols[classcol]._min;
      StringBuilder sb = new StringBuilder();
      sb.append("<th>Actual \\ Predicted");
      for( int i=0; i<N; i++ )
        sb.append("<th>").append("class "+(i+cmin));
      sb.append("<th>Error");
      response.replace("chead",sb.toString());
      response.replace("flavor", confusion._computeOOB ? "OOB error estimate" : "full scoring");

      // Now the confusion-matrix body lines
      long ctots[] = new long[N]; // column totals
      long terrs = 0;
      for( int i=0; i<N; i++ ) {
        RString row = response.restartGroup("CtableRow");
        sb = new StringBuilder();
        sb.append("<td>").append("class "+(i+cmin));
        long tot=0;
        long err=0;
        for( int j=0; j<N; j++ ) {
          long v = confusion._matrix==null ? 0 : confusion._matrix[i][j];
          tot += v;               // Line totals
          ctots[j] += v;          // Column totals
          if( i==j ) sb.append("<td style='background-color:LightGreen'>");
          else { sb.append("<td>"); err += v; }
          sb.append(v);
        }
        terrs += err;             // Total errors
        sb.append("<td>");
        if( tot != 0 )
          sb.append(String.format("%5.3f = %d / %d",(double)err/tot,err,tot));
        row.replace("crow",sb.toString());
        row.append();
      }
      // Last the summary line
      RString row = response.restartGroup("CtableRow");
      sb = new StringBuilder();
      sb.append("<td>").append("Totals");
      long ttots= 0;
      for( int i=0; i<N; i++ ) {
        ttots += ctots[i];
        sb.append("<td>").append(ctots[i]);
      }
      sb.append("<td>");
      if( ttots != 0 )
        sb.append(String.format("%5.3f = %d / %d",(double)terrs/ttots,terrs,ttots));
      row.replace("crow",sb.toString());
      row.append();
    } else {
      response.replace("ctableStyle","display:none");
      response.replace("ctableMessage","<div class='alert alert-info'><b>Please wait!</b> The confusion matrix is already being calculated. The page will automatically refresh..</div>");
    }

    // Report on the basic model info
    if( atree < model.size() ) {
      RString button = new RString(htmlButton);
      button.replace(json);
      response.replace("validateMore", button.toString());
    } else {
      response.replace("validateMore", "");
    }
    response.replace(json);
    _refresh = model.size() < ntree ? 5 : 0; // Refresh in 5sec if not all trees yet

    // Compute a few stats over trees
    response.replace( "depth",model.depth());
    response.replace("leaves",model.leaves());

    response.replace("weights", classWt == null ? "default" : Arrays.toString(classWt));

    int limkeys = Math.min(model.size(),1000);
    for( int i=0; i<limkeys; i++ ) {
      RString trow = response.restartGroup("trees");
      trow.replace(MODEL_KEY,modelKey);
      trow.replace("n",i);
      trow.replace(DATA_KEY,ary._key);
      trow.replace(CLASS_COL,classcol);
      trow.append();
    }

    //confusion.report();  // Print on std out...

    RString url = new RString("RFViewQuery?modelKey=%$key&class=%class&dataKey=%$data");
    url.replace("key", modelKey);
    url.replace(CLASS_COL, classcol);
    url.replace("data",ary._key);
    response.replace("validateOther", url.toString());
    return response.toString();
  }

  // use a function instead of a constant so that a debugger can live swap it
  private String html() {
    return "\nRandom Forest of <a href='Inspect?Key=%$dataKey'>%dataKey</a>\n"
      +"<table><tbody>"
      + "<tr><td>Showing %atree of %ntree trees, with %modelSize trees built</td></tr>"
      + "<tr><td>%validateMore</td></tr>"
      + "</tbody></table>\n"
      + "<p><a href=\"%validateOther\">Validate model with another dataset</a></p>"
      + "<p>Model key:<b>%modelKey</b></p>"
      + "<p>Weighted voting:<b>%weights</b></p>"
      + "<h2>Confusion Matrix - %flavor</h2>"
      + "%ctableMessage"
      + "<table style='%ctableStyle' class='table table-striped table-bordered table-condensed'>"
      + "<thead>%chead</thead>\n"
      + "<tbody>\n"
      + "%CtableRow{<tr>%crow</tr>}\n"
      + "</tbody>\n"
      + "</table>\n"
      + "<p><p>\n"
      + "<h2>Random Decision Trees</h2>"
      + "min/avg/max depth=%depth, leaves=%leaves<p>\n"
      + "Click to view individual trees:<p>"
      + "%trees{\n"
      + "  <a href='/RFTreeView?modelKey=%$modelKey&n=%n&dataKey=%$dataKey&class=%class'>%n</a> "
      + "}\n"
      ;
  }
  private String htmlButton =
    "<a href='RFView?dataKey=%$dataKey&modelKey=%$modelKey&ntree=%ntree&atree=%modelSize&class=%class'><button class='btn btn-primary btn-mini'>Validate with %modelSize trees</button></a>";
}
