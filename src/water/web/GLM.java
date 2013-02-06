package water.web;

import hex.*;
import hex.GLMSolver.Family;
import hex.GLMSolver.GLMModel;
import hex.GLMSolver.GLMParams;
import hex.GLMSolver.Link;

import java.text.DecimalFormat;
import java.util.*;
import java.util.Map.Entry;

import water.H2O;
import water.ValueArray;

import com.google.gson.*;

public class GLM extends H2OPage {

  static class GLMInputException  extends  RuntimeException {
    public GLMInputException(String msg) {
      super(msg);
    }
  }
//  static String getColName(int colId, String[] colNames) {
//    return colId == colNames.length ? "Intercept" : colName(colId,colNames);
//  }

  @Override
  public String[] requiredArguments() {
    return new String[] { "Key", "Y" };
  }



  double [] getFamilyArgs(Family f, Properties p){
    double [] res = null;
    if(f == Family.binomial){
      res = new double []{1.0,1.0};
      try{res[GLMSolver.FAMILY_ARGS_CASE] = Double.valueOf(p.getProperty("case", "1.0"));}catch(NumberFormatException e){throw new GLMInputException("illegal case value" + p.getProperty("case", "1.0"));}
      if(p.containsKey("weight")){
        try{res[GLMSolver.FAMILY_ARGS_WEIGHT] = Double.valueOf(p.getProperty("weight", "1.0"));}catch(NumberFormatException e){throw new GLMInputException("illegal weight value " + p.getProperty("weight"));}
      }
    }
    return res;
  }
  GLMParams getGLMParams(Properties p){
    GLMParams res = new GLMParams();
    try{res._f = GLMSolver.Family.valueOf(p.getProperty("family", "gaussian").toLowerCase());}catch(IllegalArgumentException e){throw new GLMInputException("unknown family " + p.getProperty("family", "gaussian"));}

    if(p.containsKey("link"))
     try{res._l = Link.valueOf(p.getProperty("link").toLowerCase());}catch(Exception e){throw new GLMInputException("invalid link argument " + p.getProperty("link"));}
    else
      res._l = res._f.defaultLink;
    res._maxIter = getIntArg(p, "ITER", GLMSolver.DEFAULT_MAX_ITER);
    res._betaEps = getDoubleArg(p, "betaEps", GLMSolver.DEFAULT_BETA_EPS);

    return res;
  }

  int getIntArg(Properties p, String name, int defaultValue){
    if(!p.containsKey(name))return defaultValue;
    try{return Integer.parseInt(p.getProperty(name));}catch (NumberFormatException e){throw new GLMInputException("invalid value of argument " + name);}
  }

  double getDoubleArg(Properties p, String name, double defaultValue){
    if(!p.containsKey(name))return defaultValue;
    try{return Double.parseDouble(p.getProperty(name));}catch (NumberFormatException e){throw new GLMInputException("invalid value of argument " + name);}
  }
  LSMSolver getLSMSolver(Properties p){
    if(!p.containsKey("norm"))
      return LSMSolver.makeSolver();
    String norm = p.getProperty("norm");
    if(norm.equalsIgnoreCase("L1")){
      double lambda = getDoubleArg(p, "lambda",LSMSolver.DEFAULT_LAMBDA);
      double alpha = getDoubleArg(p, "",LSMSolver.DEFAULT_ALPHA);
      return LSMSolver.makeSolver(lambda, alpha);
    } else if(norm.equalsIgnoreCase("L2")){
      double lambda = getDoubleArg(p, "lambda",LSMSolver.DEFAULT_LAMBDA);
      return LSMSolver.makeL2Solver(lambda);
    } else if(norm.equalsIgnoreCase("ENET")){
      double lambda = getDoubleArg(p, "lambda",LSMSolver.DEFAULT_LAMBDA);
      double alpha = getDoubleArg(p, "",LSMSolver.DEFAULT_ALPHA);
      return LSMSolver.makeSolver(lambda, alpha);
    } else
      throw new GLMInputException("unknown norm " + norm);
  }

  static final double [] thresholds = new double [] {0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9};
  @Override
  public JsonObject serverJson(Server s, Properties p, String sessionID) throws PageError {
    JsonObject res = new JsonObject();
    try {
      ValueArray ary = ServletUtil.check_array(p, "Key");
      int[] yarr = parseVariableExpression(ary, p.getProperty("Y"));
      if( yarr.length != 1 )
        throw new GLMInputException("Y has to refer to exactly one column!");
      int Y = yarr[0];
      if( 0 > Y || Y >= ary._cols.length)
        throw new GLMInputException("invalid Y value, column " + Y
            + " does not exist!");
      int[] X = null;
      // ignore empty X == make as if X not present
      if (p.containsKey("X") && ((p.getProperty("X") == null) || (p.getProperty("X").isEmpty())))
        p.remove("X");
      if( p.containsKey("X") ) X = parseVariableExpression(ary,
          p.getProperty("X"));
      else {
        X = new int[ary._cols.length - 1];
        int idx = 0;
        for( int i = 0; i < ary._cols.length; ++i ) {
          if( i != Y ) X[idx++] = i;
        }
      }
      int[] xComp = (p.containsKey("-X")) ? parseVariableExpression(ary,
          p.getProperty("-X")) : new int[0];
      Arrays.sort(xComp);
      int n = X.length;
      for( int i = 0; i < X.length; ++i ) {
        if( X[i] == Y ) {
          X[i] = -1;
          --n;
          continue;
        }
        for( int j = 0; j < xComp.length; ++j ) {
          if( xComp[j] == X[i] ) {
            --n;
            X[i] = -1;
          }
        }
      }
      if( n < 1 ) {
        res.addProperty("error", "Invalid input: no input columns specified");
        return res;
      }
      int[] columns = new int[n + 1];
      int idx = 0;
      for( int i = 0; i < X.length; ++i )
        if( X[i] != -1 ) columns[idx++] = X[i];
      columns[n] = Y;
      for( int x : columns )
        if( 0 > x || x >= ary._cols.length) {
          res.addProperty("error", "Invalid input: column " + x + " does not exist!");
          return res;
        }
      res.addProperty("key", ary._key.toString());
      res.addProperty("h2o", H2O.SELF.toString());

      GLMModel m = new GLMModel(ary, columns, getLSMSolver(p), getGLMParams(p), null);
      if( m.isSolved() ) m.validateOn(ary, null,thresholds);
      res.add("GLMModel", m.toJson());

      if( m.isSolved() && p.containsKey("xval") ) {
        int fold = getIntArg(p, "xval", 10);
        res.add("xval", m.xvalidate(ary,fold,thresholds).toJson());
      }
    } catch( GLMInputException e1 ) {
      res.addProperty("error", "Invalid input:" + e1.getMessage());
    }
    return res;
  }

  static DecimalFormat dformat = new DecimalFormat("###.####");


  static String buildCM(JsonArray arr){
    StringBuilder bldr = new StringBuilder();
    bldr.append("<table class='table table-striped table-bordered table-condensed'><thead>");
    boolean firstRow = true;
    for(JsonElement e:arr){
      bldr.append("<tr>\n");
      String [] tags = new String[]{"<td>","</td>"};
      String [] htags = new String[]{"<th>","</th>"};
      boolean firstCol = true;
      for(JsonElement f:e.getAsJsonArray()){
        if(firstCol || firstRow)
          bldr.append(htags[0] + f.getAsString() + htags[1]);
        else
          bldr.append(tags[0] + dformat.format(f.getAsDouble()) + tags[1]);
        firstCol = false;
      }
      bldr.append("</tr>\n");
      firstRow = false;
    }
    bldr.append("</tbody></table>\n");
    return bldr.toString();
  }

  public RString response() {
    return new RString("<div class='alert %succ'>GLM on data <a href='/Inspect?Key=%$key'>%key</a>. %iterations computed in %time[ms]. %warningMsgs</div> %Model %Validation");
  }

  public String getGLMParamsHTML(JsonObject glmParams, JsonObject lsmParams){
    StringBuilder bldr = new StringBuilder();
    bldr.append("<span><b>family: </b>" + glmParams.get("family").getAsString() + "</span>");
    bldr.append(" <span><b>link: </b>" + glmParams.get("link").getAsString() + "</span>");
    bldr.append(" <span><b>&epsilon;<sub>&beta;</sub>: </b>" + glmParams.get("betaEps").getAsString() + "</span>");
    if(glmParams.has("weight"))
      bldr.append(" <span><b>weight<sub>1</sub>:</b>" + dformat.format(glmParams.get("weight").getAsDouble()) + "</span>");
    if(glmParams.has("threshold"))
      bldr.append(" <span><b>threshold: </b>" + glmParams.get("threshold").getAsString() + "</span>");
    String [] params = new String[]{"norm","lambda","lambda2","rho","alpha","weights"};
    String [] paramHTML = new String[]{"norm","&lambda;<sub>1</sub>","&lambda;<sub>2</sub>","&rho;","&alpha;","weights"};
    for(int i = 0; i < params.length; ++i){
      if(!lsmParams.has(params[i]))continue;
      String s = lsmParams.get(params[i]).getAsString();
      if(s.equals("0.0"))continue;
      bldr.append(" <span><b>" + paramHTML[i] + ":</b>" + s + "</span> ");
    }
    return bldr.toString();
  }
  public String getLSMParamsHTML(JsonObject json){

    return "";
  }

  public String getCoefficientsHTML(JsonObject coefs){
    StringBuilder bldr = new StringBuilder();
    bldr.append("<div>");

    for(Entry<String,JsonElement> e:coefs.entrySet()){
      bldr.append(" <span><b>" + e.getKey() + "</b>=" + dformat.format(e.getValue().getAsDouble()) + "</span> ");
    }
    bldr.append("</div>");
    return bldr.toString();
  }

  public String getModelSRCHTML(Link l, JsonObject obj){
    RString m = null;

    switch(l){
    case identity:
      m = new RString("y = %equation");
      break;
    case logit:
      m = new RString("y = 1/(1 + Math.exp(-(%equation)))");
      break;
    default:
      assert false;
      return "";
    }
    boolean first = true;
    StringBuilder bldr = new StringBuilder();
    for(Entry<String,JsonElement> e:obj.entrySet()){

      double v = e.getValue().getAsDouble();
      if(v == 0)continue;
      if(!first)
        bldr.append(((v < 0)?" - ":" + ") + dformat.format(Math.abs(v)));
      else
        bldr.append(dformat.format(v));
      first = false;
      bldr.append("*x[" + e.getKey() + "]");
    }
    m.replace("equation",bldr.toString());
    return m.toString();
  }

  public String getModelHTML(JsonObject json){
    RString responseTemplate = new RString(
        "<div class='alert %succ'>GLM on data <a href='/Inspect?Key=%key'>%key</a>. %iterations iterations computed in %time[ms]. %warningMsgs</div>"
            + "<h3>GLM Parameters</h3>"
            + " %LSMParams %GLMParams"
            + "<h3>Coefficients</h3>"
            + "<div>%coefficients</div>"
            + "<h5>Model SRC</h5>"
            + "<div><code>%modelSrc</code></div>");
    if(json.has("warnings")){
      responseTemplate.replace("succ","alert-warning");
      System.err.println(json.get("warnings"));
      String[] warn = new Gson().fromJson(json.get("warnings"),String[].class);
      StringBuilder wsb = new StringBuilder();
      for( String s : warn )
        wsb.append(s).append("<br>");
      responseTemplate.replace("warningMsgs",wsb);
    } else
      responseTemplate.replace("succ","alert-success");
    responseTemplate.replace("key",json.get("dataset").getAsString());
    responseTemplate.replace("time",json.get("time").getAsString());
    responseTemplate.replace("iterations",json.get("iterations").getAsString());
    responseTemplate.replace("GLMParams",getGLMParamsHTML(json.get("GLMParams").getAsJsonObject(),json.get("LSMParams").getAsJsonObject()));
    responseTemplate.replace("coefficients",getCoefficientsHTML(json.get("coefficients").getAsJsonObject()));
    responseTemplate.replace("modelSrc",getModelSRCHTML(Link.valueOf(json.get("GLMParams").getAsJsonObject().get("link").getAsString()),json.get("coefficients").getAsJsonObject()));
    return responseTemplate.toString() + (json.has("validations")?getValidationHTML(json.get("validations").getAsJsonArray()):"");
  }

  public String getXModelHTML(JsonObject json){
    RString responseTemplate = new RString(
        "<div class='alert %succ'>GLM on data <a href='/Inspect?Key=%key'>%key</a>. %iterations iterations computed in %time[ms]. %warningMsgs</div>"
            + "<div>%coefficients</div>");

    if(json.has("warnings")){
      responseTemplate.replace("succ","alert-warning");
      JsonArray ja = json.get("warnings").getAsJsonArray();
      StringBuilder wsb = new StringBuilder();
      for( JsonElement el : ja )
        wsb.append(el.getAsJsonPrimitive().getAsString()).append("<br>");
      responseTemplate.replace("warningMsgs",wsb.toString());
    } else
      responseTemplate.replace("succ","alert-success");
    responseTemplate.replace("key",json.get("dataset").getAsString());
    responseTemplate.replace("time",json.get("time").getAsString());
    responseTemplate.replace("iterations",json.get("iterations").getAsString());
    responseTemplate.replace("coefficients",getCoefficientsHTML(json.get("coefficients").getAsJsonObject()));
    return responseTemplate.toString() + getXValidationHTML(json.get("validations").getAsJsonArray());
  }

  public String getValidationHTML(JsonArray arr){
    StringBuilder res = new StringBuilder("<h2>Validations</h2>");

    for(JsonElement e:arr){
      RString template = new RString("<table class='table table-striped table-bordered table-condensed'>"
            + "<tr><th>Dataset:</th><td>%dataset</td></tr>"
            + "<tr><th>Degrees of freedom:</th><td>%DegreesOfFreedom total (i.e. Null);  %ResidualDegreesOfFreedom Residual</td></tr>"
            + "<tr><th>Null Deviance</th><td>%nullDev</td></tr>"
            + "<tr><th>Residual Deviance</th><td>%resDev</td></tr>"
            + "<tr><th>AIC</th><td>%AIC</td></tr>"
            + "<tr><th>Training Error Rate Avg</th><td>%err</td></tr>"
            + "</table> %cm");

      JsonObject val = e.getAsJsonObject();
      if(val.has("cm"))
        val.addProperty("cm", buildCM(val.get("cm").getAsJsonArray()));
      if(val.has("classErr"))
        val.remove("classErr");
      template.replace(val);
      template.replace("DegreesOfFreedom",val.get("nrows").getAsLong()-1);
      template.replace("ResidualDegreesOfFreedom",val.get("dof").getAsLong());
      res.append(template.toString());
    }
    return res.toString();
  }

  public String getXValidationHTML(JsonArray arr){
    return buildCM(arr.get(0).getAsJsonObject().get("cm").getAsJsonArray());
  }

  @Override
  protected String serveImpl(Server server, Properties args, String sessionID) throws PageError {
    JsonObject json = serverJson(server, args, sessionID);
    if( json.has("error") )
      return H2OPage.error(json.get("error").getAsString());
    String res = getModelHTML(json.get("GLMModel").getAsJsonObject());
    if(args.containsKey("xval")){
      StringBuilder xvalStr = new StringBuilder("<h3>Cross Validation</h3>");
      JsonArray arr = json.get("xval").getAsJsonArray();
      for(JsonElement e:arr){
        xvalStr.append("<br/>");
        xvalStr.append(getXModelHTML(e.getAsJsonObject()));
      }
      res = res + xvalStr.toString();
    }
    return res;
  }
}
