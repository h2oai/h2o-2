package water.api;

import hex.DGLM.CaseMode;
import hex.DGLM.Family;
import hex.DGLM.GLMModel;
import hex.DGLM.GLMParams;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Arrays;
import java.util.Properties;

import water.Key;
import water.ValueArray;
import water.api.RequestArguments.Bool;
import water.util.RString;

import com.google.gson.JsonObject;

/**
 * @author cliffc
 */
public class GLMGrid extends Request {
  public static final String JSON_GLM_Y = "y";
  public static final String JSON_GLM_X = "x";
  public static final String JSON_GLM_MAX_ITER = "max_iter";
  public static final String JSON_GLM_BETA_EPS = "beta_eps";
  public static final String JSON_GLM_WEIGHT = "weight";

  public static final String JSON_GLM_XVAL = "xval";
  public static final String JSON_GLM_CASE = "case";
  public static final String JSON_GLM_CASE_MODE = "case_mode";
  public static final String JSON_GLM_LINK = "link";
  public static final String JSON_GLM_FAMILY = "family";

  public static final String JSON_ROWS = "rows";
  public static final String JSON_TIME = "time";
  public static final String JSON_COEFFICIENTS = "coefficients";

  // Need a HEX key for GLM
  protected final H2OHexKey _key = new H2OHexKey(KEY);
  // Column to classify on
  protected final H2OHexKeyCol _y = new H2OHexKeyCol(JSON_GLM_Y, _key);
  // Columns used to run the GLM
  protected final HexColumnSelect _x = new HexNonConstantColumnSelect(JSON_GLM_X, _key, _y);


  // Args NOT Grid Searched
  protected final Int _maxIter = new Int(JSON_GLM_MAX_ITER, 50, 1, 1000000);
  protected final EnumArgument<Family> _family = new EnumArgument(JSON_GLM_FAMILY,Family.binomial,true);
  protected final LinkArg _link = new LinkArg(_family,JSON_GLM_LINK);

  protected final CaseModeSelect _caseMode = new CaseModeSelect(_key,_y, _family, JSON_GLM_CASE_MODE,CaseMode.none);
  protected final CaseSelect _case = new CaseSelect(_key,_y,_caseMode,JSON_GLM_CASE);
  protected final Real _weight = new Real(JSON_GLM_WEIGHT,1.0);

  protected final Int _xval = new Int(XVAL, 10, 0, 1000000);
  protected final Real _betaEps = new Real(JSON_GLM_BETA_EPS,1e-4);

  // Args that ARE Grid Searched
  protected final RSeq _lambda = new RSeq(Constants.LAMBDA, false, new NumberSequence("1e-8:1e3:100",true,10),true);
  protected final RSeq _alpha = new RSeq(Constants.ALPHA, false, new NumberSequence("0,0.25,0.5,0.75,1.0",false,1),false);
  protected final RSeq _thresholds = new RSeq(Constants.DTHRESHOLDS, false, new NumberSequence("0:1:0.01",false,0.1),false);

  protected final Bool _parallel = new Bool(PARALLEL, false, "Build models in parallel");

  public GLMGrid(){
    _requestHelp = "Perform grid search over GLM parameters. Calls glm with all parameter combination from user-defined parameter range. Results are ordered according to AUC. For more details see <a href='GLM.help'>GLM help</a>.";
    _key._requestHelp = "Dataset to be trained on.";
    _y._requestHelp = "Response variable column name.";
    _x._requestHelp = "Predictor columns to be trained on. Constant columns will be ignored.";
    _family._requestHelp =
      "Pick the general mathematical family for the trained model.<br><ul>"+
      "<li><b>gaussian</b> models describe a simple hyper-plane (for a single column this will be a simple line) for the response variable.  This is a suitable model for when you expect the response variable to vary as a linear combination of predictor variables.  An example might be predicting the gas mileage of cars, based on their weight, age, and engine size.</li>"+
      "<li><b>binomial</b> models form an S-curve response, showing probabilities that vary from 0 to 1.  This is a suitable model for when you expect a simple boolean result (e.g. alive/dead, or fraud/no-fraud).  The model gives a probability of the true event.  An example might be to predict the presence of prostate cancer given the patient age, race, and various blood chemical levels such as PSA.</li>"+
      "</ul>";
    _link._requestHelp = "Link function to be used.";
    _lambda._requestHelp = "Range of penalty arguments. Higher lambda means higher penalty is applied on the size of the beta vector.";
    _alpha._requestHelp = "Range of penalty distribution arguments. Controls distribution of penalty between L1 and L2. 1 means lasso, 0 means ridge regression";
    _betaEps._requestHelp = "Precision of the vector of coefficients. Computation stops when the maximal difference between two beta vectors is below than Beta epsilon.";
    _maxIter._requestHelp = "Number of maximum iterations.";
    _weight._requestHelp = "All rows for which the predicate is true will be weighted by weight. Weight=1 is neutral. Weight = 0.5 treats negative examples as twice more important than positive ones. Weight = 2.0 does the opposite.";
    _caseMode._requestHelp = "Predicate selection.";
    _case._requestHelp = "Value to be used to compare against using predicate given by case mode selector to turn the y column into boolean.";
    _thresholds._requestHelp = "Sequence of decision thresholds to be evaluated during validation (used for ROC curce computation and for picking optimal decision threshold of the resulting classifier).";
    _xval._requestHelp = "Number of fold used in cross-validation. 0 or 1 means no cross validation.";

  }


  @Override protected void queryArgumentValueSet(Argument arg, Properties inputArgs) {
    if(arg == _caseMode){
      if(_caseMode.value() == CaseMode.none){
        _case.disable("n/a");
        _weight.disable("case not set");
      }
    }
    if(arg == _family){
      if(_family.value() != Family.binomial){
        _caseMode.disable("Only for binomial family");
        _case.disable("Only for binomial family");
        _weight.disable("Only for binomial family");
        _thresholds.disable("Only for binomial family");
      }
    }
  }

  private int[] getCols(int [] xs, int y){
    int [] res = Arrays.copyOf(xs, xs.length+1);
    res[xs.length] = y;
    return res;
  }
  // ---
  // Make a new Grid Search object.
  @Override protected Response serve() {
    GLMParams glmp = new GLMParams(_family.value());
    glmp._betaEps = _betaEps.value();
    glmp._maxIter = _maxIter.value();
    glmp._caseMode = _caseMode.valid()?_caseMode.value():CaseMode.none;
    glmp._caseVal = _case.valid()?_case.value():Double.NaN;
    Key dest = Key.make();
    double [] ts = glmp._family == Family.binomial?_thresholds.value()._arr:null;
    hex.GLMGrid job = new hex.GLMGrid(dest,
                        _key.value(), // Hex data
                        glmp,
                        getCols(_x.value(), _y.value()),
                        _lambda.value()._arr, // Grid ranges
                        _alpha.value()._arr,  // Grid ranges
                        ts,
                        _xval.value(),
                        _parallel.value());
    job.start();

    // Redirect to the grid-search status page
    JsonObject j = new JsonObject();
    j.addProperty(Constants.DEST_KEY, dest.toString());
    Response r = GLMGridProgress.redirect(j, job.self(), dest);
    r.setBuilder(Constants.DEST_KEY, new KeyElementBuilder());
    return r;
  }


  // Make a link that lands on this page
  public static String link(Key k, String content) {
    RString rs = new RString("<a href='GLMGrid.query?%key_param=%$key'>%content</a>");
    rs.replace("key_param", KEY);
    rs.replace("key", k.toString());
    rs.replace("content", content);
    return rs.toString();
  }

  public static String link(GLMModel m, String content) {
    int [] colIds = m.selectedColumns();
    if(colIds == null)return ""; // the dataset is not in H2O any more
    RString rs = new RString("<a href='GLMGrid.query?%key_param=%$key&y=%ycol&x=%xcols&caseMode=%caseMode&case=%case'>%content</a>");
    rs.replace("key_param", KEY);
    rs.replace("key", m._dataKey.toString());
    rs.replace("content", content);
    rs.replace("ycol",m.responseName());
    rs.replace("case",m._glmParams._caseVal);
    try {
      StringBuilder sb = new StringBuilder(""+colIds[0]);
      for(int i = 1; i < colIds.length-1; ++i)
        sb.append(","+colIds[i]);
      rs.replace("xcols",sb.toString());
      rs.replace("caseMode",URLEncoder.encode(m._glmParams._caseMode.toString(),"utf8"));
    } catch( UnsupportedEncodingException e ) {
      throw new RuntimeException(e);
    }
    return rs.toString();
  }
}
