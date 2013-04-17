package water.api;

import hex.*;
import hex.DGLM.*;
import hex.DLSM.ADMMSolver;
import hex.DLSM.GeneralizedGradientSolver;
import hex.DLSM.LSMSolver;
import hex.DLSM.LSMSolverType;
import hex.NewRowVecTask.DataFrame;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.text.DecimalFormat;
import java.util.*;
import java.util.Map.Entry;

import water.*;
import water.api.RequestBuilders.KeyElementBuilder;
import water.api.RequestBuilders.Response;
import water.util.RString;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class GLM extends Request {
  protected final H2OHexKey _key = new H2OHexKey(KEY);
  protected final H2OHexKeyCol _y = new H2OHexKeyCol(Y, _key);
  protected final HexColumnSelect _x = new HexNonConstantColumnSelect(X, _key, _y);
  protected final H2OGLMModelKey _modelKey = new H2OGLMModelKey(MODEL_KEY,false);
  protected final EnumArgument<Family> _family = new EnumArgument(FAMILY,Family.gaussian,true);
  protected final LinkArg _link = new LinkArg(_family,LINK);
  protected final Real _lambda = new Real(LAMBDA, 1e-5); // TODO I do not know the bounds
  protected final Real _alpha = new Real(ALPHA, 0.5, 0, 1, "");
  protected final Real _caseWeight = new Real(WEIGHT,1.0);
  protected final CaseModeSelect _caseMode = new CaseModeSelect(_key,_y,_family, CASE_MODE,CaseMode.none);
  protected final CaseSelect _case = new CaseSelect(_key,_y,_caseMode,CASE);
  protected final Int _xval = new Int(XVAL, 10, 0, 1000000);
  protected final Bool _expertSettings = new Bool("expert_settings", false,"Show expert settings.");
  // ------------------------------------- ADVANCED SETTINGS ------------------------------------------------------------------------------------
  protected final Bool _standardize = new Bool("standardize", true, "Set to standardize (0 mean, unit variance) the data before training.");
  protected final RSeq _thresholds = new RSeq(DTHRESHOLDS, false, new NumberSequence("0:1:0.01", false, 0.01),false);
  protected final EnumArgument<LSMSolverType> _lsmSolver = new EnumArgument<LSMSolverType>("lsm_solver", LSMSolverType.AUTO);
  protected final Real _betaEps = new Real(BETA_EPS,1e-4);
  protected final Int _maxIter = new Int(MAX_ITER, 50, 1, 1000000);
  //protected final Bool _reweightGram = new Bool("reweigthed_gram_xval", false, "Set to force reweighted gram matrix for cross-validation (non-reweighted xval is much faster, less precise).");

  public GLM() {
      _requestHelp = "Compute generalized linear model with penalized maximum likelihood. Penalties include the lasso (L1 penalty), ridge regression (L2 penalty) or elastic net penalty (combination of L1 and L2) penalties. The penalty function is defined as :<br/>" +
      "<pre>\n" +
      "       P(&beta;) = 0.5*(1 - &alpha;)*||&beta;||<sub>2</sub><sup>2</sup> + &alpha;*||&beta;||<sub>1</sub><br/>"+
      "</pre>" +
      "By setting &alpha; to 0, we get ridge regression, setting it to 1 gets us lasso. <p>See our <a href='https://github.com/0xdata/h2o/wiki/GLM#wiki-Details' target=\"_blank\">wiki</a> for details.<p>";    _key._requestHelp = "Dataset to be trained on.";
    _y._requestHelp = "Response variable column name.";

    _x._requestHelp = "Predictor columns to be trained on. Constant columns will be ignored.";
    _modelKey._hideInQuery = true;
    _modelKey._requestHelp = "The H2O's Key name for the model";
    _family._requestHelp =
      "Pick the general mathematical family for the trained model.<br><ul>"+
      "<li><b>gaussian</b> models describe a simple hyper-plane (for a single column this will be a simple line) for the response variable.  This is a suitable model for when you expect the response variable to vary as a linear combination of predictor variables.  An example might be predicting the gas mileage of cars, based on their weight, age, and engine size.</li>"+
      "<li><b>binomial</b> models form an S-curve response, showing probabilities that vary from 0 to 1.  This is a suitable model for when you expect a simple boolean result (e.g. alive/dead, or fraud/no-fraud).  The model gives a probability of the true event.  An example might be to predict the presence of prostate cancer given the patient age, race, and various blood chemical levels such as PSA.</li>"+
      "</ul>";
    _link._requestHelp = "Link function to be used.";
    _lambda._requestHelp = "Penalty argument. Higher lambda means higher penalty is applied on the size of the beta vector.";
    _alpha._requestHelp = "Penalty distribution argument. Controls distribution of penalty between L1 and L2 norm according to the formula above.";
    _betaEps._requestHelp = "Precision of the vector of coefficients. Computation stops when the maximal difference between two beta vectors is below than Beta epsilon.";
    _maxIter._requestHelp = "Number of maximum iterations.";
    _caseWeight._requestHelp = "All rows for which the predicate is true will be weighted by weight. Weight=1 is neutral. Weight = 0.5 treats negative examples as twice more important than positive ones. Weight = 2.0 does the opposite.";
    _caseMode._requestHelp = "Predicate selection.";
    _case._requestHelp = "Value to be used to compare against using predicate given by case mode selector to turn the y column into boolean.";
    _thresholds._requestHelp = "Sequence of decision thresholds to be evaluated during validation (used for ROC curce computation and for picking optimal decision threshold of the resulting classifier).";
    _xval._requestHelp = "Number of fold used in cross-validation. 0 or 1 means no cross validation.";
    _expertSettings.setRefreshOnChange();
  }


  public static String link(Key k, String content) {
    RString rs = new RString("<a href='GLM.query?%key_param=%$key'>%content</a>");
    rs.replace("key_param", KEY);
    rs.replace("key", k.toString());
    rs.replace("content", content);
    return rs.toString();
  }


  static void getColumnIdxs(StringBuilder sb, ValueArray ary, String [] cols ){
    Arrays.sort(cols);
    boolean firstCol = false;
    for(int i = 0; i < ary._cols.length; ++i)
      if(Arrays.binarySearch(cols, ary._cols[i]._name) >= 0)
        if(firstCol){
          sb.append(""+i);
          firstCol = false;
        } else
          sb.append(","+i);
  }
  public static String link(Key k, GLMModel m, String content) {
    int [] colIds = m.selectedColumns();
    if(colIds == null)return ""; /// the dataset is no longer on H2O, no link shoudl be produced!
    try {
      StringBuilder sb = new StringBuilder("<a href='GLM.query?");
      sb.append(KEY + "=" + k.toString());
      sb.append("&y=" + m.responseName());
      // find the column idxs...(the model keeps only names)
      sb.append("&x=" + colIds[0]);
      for(int i = 1; i < colIds.length-1; ++i)
        sb.append(","+colIds[i]);
      sb.append("&family=" + m._glmParams._family.toString());
      sb.append("&link=" + m._glmParams._link.toString());
      sb.append("&lambda=" + m._solver._lambda);
      sb.append("&alpha=" + m._solver._alpha);
      sb.append("&beta_eps=" + m._glmParams._betaEps);
      sb.append("&weight=" + m._glmParams._caseWeight);
      sb.append("&max_iter=" + m._glmParams._maxIter);
      sb.append("&caseMode=" + URLEncoder.encode(m._glmParams._caseMode.toString(),"utf8"));
      sb.append("&case=" + m._glmParams._caseVal);
      sb.append("'>" + content + "</a>");
      return sb.toString();
    } catch( UnsupportedEncodingException e ) {
      throw new RuntimeException(e);
    }
  }

  @Override protected void queryArgumentValueSet(Argument arg, Properties inputArgs) {
    if(arg == _caseMode){
      if(_caseMode.value() == CaseMode.none)
        _case.disable("n/a");
    } else if (arg == _family) {
      if (_family.value() != Family.binomial) {
        _case.disable("Only for family binomial");
        _caseMode.disable("Only for family binomial");
        _caseWeight.disable("Only for family binomial");
        _thresholds.disable("Only for family binomial");
      }
    } else if (arg == _expertSettings){
      if(_expertSettings.value()){
        _lsmSolver._hideInQuery = false;
        _thresholds._hideInQuery = false;
        _maxIter._hideInQuery = false;
        _betaEps._hideInQuery = false;
        //_reweightGram._hideInQuery = false;
        _standardize._hideInQuery = false;
      } else {
        _lsmSolver._hideInQuery = true;
        _thresholds._hideInQuery = true;
        _maxIter._hideInQuery = true;
        _betaEps._hideInQuery = true;
        //_reweightGram._hideInQuery = true;
        _standardize._hideInQuery = true;
      }
    }
  }

  /** Returns an array of columns to use for GLM, the last of them being the
   * result column y.
   */
  private  int[] createColumns() {
    BitSet cols = new BitSet();
    for( int i :    _x.value() ) cols.set  (i);
    //for( int i : _negX.value() ) cols.clear(i);
    int[] res = new int[cols.cardinality()+1];
    int x=0;
    for( int i = cols.nextSetBit(0); i >= 0; i = cols.nextSetBit(i+1))
      res[x++] = i;
    res[x] = _y.value();
    return res;
  }

  static JsonObject getCoefficients(int [] columnIds, ValueArray ary, double [] beta){
    JsonObject coefficients = new JsonObject();
    for( int i = 0; i < beta.length; ++i ) {
      String colName = (i == (beta.length - 1)) ? "Intercept" : ary._cols[columnIds[i]]._name;
      coefficients.addProperty(colName, beta[i]);
    }
    return coefficients;
  }

  GLMParams getGLMParams(){
    GLMParams res = new GLMParams(_family.value(),_link.value());
    if( res._link == Link.familyDefault )
      res._link = res._family.defaultLink;
    res._maxIter = _maxIter.value();
    res._betaEps = _betaEps.value();
    if(_caseWeight.valid())
      res._caseWeight = _caseWeight.value();
    if(_case.valid())
      res._caseVal = _case.value();
    res._caseMode = _caseMode.valid()?_caseMode.value():CaseMode.none;
    return res;
  }


  @Override protected Response serve() {
    try {
      JsonObject res = new JsonObject();
      ValueArray ary = _key.value();
      int[] columns = createColumns();
      res.addProperty("key", ary._key.toString());
      res.addProperty("h2o", H2O.SELF.toString());
      GLMParams glmParams = getGLMParams();
      DataFrame data = DGLM.getData(ary, columns, null, _standardize.value());
      LSMSolver lsm = null;
      switch(_lsmSolver.value()){
      case AUTO:
        lsm = //data.expandedSz() < 1000?
            new ADMMSolver(_lambda.value(),_alpha.value());//:
            //new GeneralizedGradientSolver(_lambda.value(),_alpha.value());
         break;
      case ADMM:
        lsm = new ADMMSolver(_lambda.value(),_alpha.value());
        break;
      case GenGradient:
        lsm = new GeneralizedGradientSolver(_lambda.value(),_alpha.value());
      }
      GLMJob job = DGLM.startGLMJob(data, lsm, glmParams, null, _xval.value(), true);
      JsonObject j = new JsonObject();
      j.addProperty(Constants.DEST_KEY, job.dest().toString());
      Response r = GLMProgressPage.redirect(j, job.self(), job.dest(),job.progressKey());
      r.setBuilder(Constants.DEST_KEY, new KeyElementBuilder());
      return r;
    }catch(GLMException e){
      return Response.error(e.getMessage());
    } catch (Throwable t) {
      t.printStackTrace();
      return Response.error(t.getMessage());
    }
  }




}
