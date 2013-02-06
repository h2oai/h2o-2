package water.api;

import hex.*;
import hex.GLMSolver.*;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.*;

import water.*;
import water.api.RequestArguments.*;
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
  protected final Int _maxIter = new Int(JSON_GLM_MAX_ITER, GLMSolver.DEFAULT_MAX_ITER, 1, 1000000);
  protected final EnumArgument<Family> _family = new EnumArgument(JSON_GLM_FAMILY,Family.binomial,true);
  protected final LinkArg _link = new LinkArg(_family,JSON_GLM_LINK);

  protected final CaseModeSelect _caseMode = new CaseModeSelect(_key,_y, _family, JSON_GLM_CASE_MODE,CaseMode.none);
  protected final CaseSelect _case = new CaseSelect(_key,_y,_caseMode,JSON_GLM_CASE);
  protected final Real _weight = new Real(JSON_GLM_WEIGHT,1.0);

  protected final Int _xval = new Int(JSON_GLM_XVAL, 10, 0, 1000000);
  protected final Real _betaEps = new Real(JSON_GLM_BETA_EPS,GLMSolver.DEFAULT_BETA_EPS);

  // Args that ARE Grid Searched
  protected final RSeq _lambda = new RSeq(Constants.LAMBDA, false, new NumberSequence("1e-8:1e3:100",true,10),true);
  protected final RSeq _alpha = new RSeq(Constants.ALPHA, false, new NumberSequence("0,0.25,0.5,0.75,1.0",false,1),false);
  protected final RSeq _thresholds = new RSeq(Constants.DTHRESHOLDS, false,new NumberSequence("0:1:0.01",false,0.1),false);




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
    // The "task key" for this Grid search.  Used to track job progress, to
    // shutdown early, to collect best-so-far & grid results, etc.  Pinned to
    // self, because it's almost always updated locally.
    Key taskey = Key.make("Task"+UUID.randomUUID().toString(),(byte)0,Key.TASK,H2O.SELF);

    GLMParams glmp = new GLMParams();
    glmp._betaEps = _betaEps.value();
    glmp._maxIter = _maxIter.value();
    glmp._caseMode = _caseMode.valid()?_caseMode.value():CaseMode.none;
    glmp._caseVal = _case.valid()?_case.value():Double.NaN;
    glmp._f = _family.value();
    glmp._l = glmp._f.defaultLink;

    GLMGridStatus task =
      new GLMGridStatus(taskey,       // Self/status/task key
                        _key.value(), // Hex data
                        glmp,
                        getCols(_x.value(), _y.value()),
                        _lambda.value()._arr, // Grid ranges
                        _alpha.value()._arr,  // Grid ranges
                        _thresholds.value()._arr,
                        _xval.value());

    // Put the task Out There for all to find
    UKV.put(taskey,task);
    // Start the grid search
    assert task._working == true;
    H2O.FJP_NORM.submit(task);

    // Redirect to the grid-search status page
    JsonObject j = new JsonObject();
    j.addProperty(Constants.DEST_KEY, taskey.toString());
    Response r = GLMGridProgress.redirect(j, taskey);
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
    RString rs = new RString("<a href='GLMGrid.query?%key_param=%$key&y=%ycol&x=%xcols&caseMode=%caseMode&case=%case'>%content</a>");
    rs.replace("key_param", KEY);
    rs.replace("key", m._ary._key.toString());
    rs.replace("content", content);
    rs.replace("ycol",m.responseName());
    rs.replace("case",m._glmParams._caseVal);
    try {
      rs.replace("xcols",URLEncoder.encode(m.xcolNames(),"utf8"));
      rs.replace("caseMode",URLEncoder.encode(m._glmParams._caseMode.toString(),"utf8"));
    } catch( UnsupportedEncodingException e ) {
      throw new RuntimeException(e);
    }
    return rs.toString();
  }
}
