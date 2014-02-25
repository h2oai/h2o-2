package water.api;

import hex.*;
import hex.DGLM.CaseMode;
import hex.DGLM.Family;
import hex.DGLM.GLMModel;
import hex.DGLM.GLMParams;
import hex.DGLM.GLMValidation;
import hex.DLSM.LSMSolver;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.text.DecimalFormat;
import java.util.*;
import java.util.Map.Entry;

import water.*;
import water.Job.ChunkProgress;
import water.util.Log;
import water.util.RString;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class GLMProgressPage extends Request {
  static String getTimeStr(long t){
    int hrs = (int)(t/(60*60*1000));
    t -= hrs*60*60*1000;
    int mins = (int)(t/60000);
    t -= mins * 60000;
    int secs = (int)(t/1000);
    t -= secs*1000;
    int millis = (int)t;
    return hrs + "hrs " + mins + "m " + secs + "s " + millis + "ms";
  }
  protected final H2OKey _job  = new H2OKey(JOB,false);
  protected final H2OKey _dest = new H2OKey(DEST_KEY,true);
  protected final H2OKey _progress = new H2OKey(PROGRESS_KEY,false);

   public static Response redirect(JsonObject resp, Key job, Key dest, Key progress) {
    JsonObject redir = new JsonObject();
    if( job != null ) redir.addProperty(JOB, job.toString());
    redir.addProperty(DEST_KEY, dest.toString());
    redir.addProperty(PROGRESS_KEY, progress.toString());
    return Response.redirect(resp, GLMProgressPage.class, redir);
  }


  @Override
  protected Response serve() {
    JsonObject response = new JsonObject();
    Key dest = _dest.value();
    response.addProperty(Constants.DEST_KEY, dest.toString());
    ChunkProgress p = null;
    Value v = _progress.value()==null ? null : DKV.get(_progress.value());
    if(v != null){
      p = v.get();
      if(p.error() != null){
        UKV.remove(_progress.value());
       return Response.error(p.error());
      }
    }
    GLMModel m = (GLMModel)UKV.get(dest);
    if(m!=null){
      response.addProperty("computation_time", getTimeStr(m._time));
      response.add("GLMModel",m.toJson());
    }
    Response r = null;
    // Display HTML setup
    if(_job.value()== null || !Job.isRunning(_job.value()))
      r =  Response.done(response);
    else if(p != null)
      r = Response.poll(response,p.progress());
    else
     r = Response.poll(response,0);
    r.setBuilder(""/*top-level do-it-all builder*/,new GLMBuilder(m,_job.value()));
    return r;
  }

  static class GLMBuilder extends ObjectBuilder {
    final GLMModel _m;
    GLMBuilder( GLMModel m, Key job) { _m=m; _job = job;}
    final Key _job;
    public String build(Response response, JsonObject json, String contextName) {
      StringBuilder sb = new StringBuilder();;
      JsonElement mje = json.get(GLMModel.NAME);
      if( mje == null ) return "<div class='error'>Cancelled!</div>";
      modelHTML(_m,mje.getAsJsonObject(),sb);
      return sb.toString();
    }

    private void modelHTML( GLMModel m, JsonObject json, StringBuilder sb ) {
      switch(m.status()){
      case Done:
        sb.append("<div class='alert'>Actions: " + (m.isSolved() ? (GLMScore.link(m._key,"0:1:0.01"/*m._vals[0].bestThreshold()*/, "Validate on another dataset") + ", "):"") + GLM.link(m._dataKey,m, "Compute new model") + "</div>");
        break;
      case ComputingModel:
      case ComputingValidation:
        if(_job != null)
          sb.append("<div class='alert'>Actions:" + Cancel.link(_job, "Cancel Job") + "</div>");
        break;
      case Cancelled:
        sb.append("<div class='error'>Cancelled!</div>");
        break;
      default:
        assert false:"unexpected status " + m.status();
      }

      RString R = new RString(
          "<div class='alert %succ'>GLM on data <a href='/Inspect.html?"+KEY+"=%key'>%key</a>.<br>" +
          "%status %iterations iterations computed in %time. %xval %warnings %action</div>" +
          "<h4>GLM Parameters</h4>" +
          " %GLMParams %LSMParams" +
          "<h4>Equation: </h4>" +
          "<div><code>%modelSrc</code></div>"+
          "<h4>Coefficients</h4>" +
          "<div>%coefficients</div>" +
          "<h4>Normalized Coefficients</h4>" +
          "<div>%normalized_coefficients</div>"
                              );
      // Warnings
      if( m._warnings != null && m._warnings.length > 0) {
        StringBuilder wsb = new StringBuilder();
        for( String s : m._warnings )
          wsb.append("<br>").append("<b>Warning:</b>" + s);
        R.replace("warnings",wsb);
        R.replace("succ","alert-warning");
        if(!m.converged())
          R.replace("action","Computation did not converge. Suggested action: Go to " + (m.isSolved() ? (GLMGrid.link(m, "Grid search") + ", "):"") + " to search for better parameters");
      } else {
        R.replace("succ","alert-success");
      }
      // Basic model stuff
      R.replace("key",m._dataKey);
      R.replace("time",PrettyPrint.msecs(m._time,true));
      long xtime = 0;
      if(m._vals != null) for( GLMValidation v : m._vals )
        xtime += v.computationTime();
      if( xtime > 0 ) {
        R.replace("xval", "<br>validations computed in " +
            PrettyPrint.msecs(xtime, true) +".");
      } else {
        R.replace("xval", "");
      }

      R.replace("status",m.status() + ".");
      R.replace("iterations",m._iterations);
      R.replace("GLMParams",glmParamsHTML(m));
      R.replace("LSMParams",lsmParamsHTML(m));

      // Pretty equations
      if( m.isSolved() ) {
        JsonObject coefs = json.get("coefficients").getAsJsonObject();
        R.replace("modelSrc",equationHTML(m,coefs));
        R.replace("coefficients",coefsHTML(m,coefs));
        if(json.has("normalized_coefficients"))
          R.replace("normalized_coefficients",coefsHTML(m,json.get("normalized_coefficients").getAsJsonObject()));
      }
      sb.append(R);
      // Validation / scoring
      if(m._vals != null)
        validationHTML(m,m._vals,sb);
    }

    private static final String ALPHA   = "&alpha;";
    private static final String LAMBDA  = "&lambda;";
    private static final String EPSILON = "&epsilon;<sub>&beta;</sub>";

    private static final DecimalFormat DFORMAT = new DecimalFormat("###.####");
    private static final String dformat( double d ) {
      return Double.isNaN(d) ? "NaN" : DFORMAT.format(d);
    }

    private static void parm( StringBuilder sb, String x, Object... y ) {
      sb.append("<span><b>").append(x).append(": </b>").append(y[0]).append("</span> ");
    }

    private static String glmParamsHTML( GLMModel m ) {
      StringBuilder sb = new StringBuilder();
      GLMParams glmp = m._glmParams;
      parm(sb,"family",glmp._family._family);
      parm(sb,"link",glmp._link._link);
      parm(sb,"&alpha;",m._solver._alpha);
      parm(sb,"&lambda;",m._solver._lambda);
      parm(sb,EPSILON,glmp._betaEps);

      if( glmp._caseMode != CaseMode.none) {
         parm(sb,"case",glmp._caseMode.exp(glmp._caseVal));
         parm(sb,"weight",glmp._caseWeight);
      }
      return sb.toString();
    }

    private static String lsmParamsHTML( GLMModel m ) {
      StringBuilder sb = new StringBuilder();
      LSMSolver lsm = m._solver;
      //parm(sb,LAMBDA,lsm._lambda);
      //parm(sb,ALPHA  ,lsm._alpha);
      return sb.toString();
    }

    // Pretty equations
    private static String equationHTML( GLMModel m, JsonObject coefs ) {
      RString eq = null;
      switch( m._glmParams._link._link ) {
      case identity: eq = new RString("y = %equation");   break;
      case logit:    eq = new RString("y = 1/(1 + Math.exp(-(%equation)))");  break;
      case log:      eq = new RString("y = Math.exp((%equation)))");  break;
      case inverse:  eq = new RString("y = 1/(%equation)");  break;
      case tweedie:  eq = new RString("y = (%equation)^(1 - " + m._glmParams._link._tweedieLinkPower + ")"); break;
      default:       eq = new RString("equation display not implemented"); break;
      }
      StringBuilder sb = new StringBuilder();
      for( Entry<String,JsonElement> e : coefs.entrySet() ) {
        if( e.getKey().equals("Intercept") ) continue;
        double v = e.getValue().getAsDouble();
        if( v == 0 ) continue;
        sb.append(dformat(v)).append("*x[").append(e.getKey()).append("] + ");
      }
      sb.append(coefs.get("Intercept").getAsDouble());
      eq.replace("equation",sb.toString());
      return eq.toString();
    }

    private static class Coef {
      final String _name; final double _d;
      Coef( Entry<String,JsonElement> e ) { _name = e.getKey(); _d = e.getValue().getAsDouble(); }
      @Override public String toString() { return _name+"="+_d; }
    }
    private static String coefsHTML( GLMModel m, JsonObject coefs ) {
      Set<Entry<String,JsonElement>> ee = coefs.entrySet();
      Coef cs[] = new Coef[ee.size()];
      int i=0;
      for( Entry<String,JsonElement> e : ee )
        cs[i++] = new Coef(e);
      Arrays.sort(cs,new Comparator<Coef>() {
            @Override public int compare(Coef c0, Coef c1) { return (c0._d<c1._d) ? 1 : (c0._d==c1._d?0:-1);}
          });
      StringBuilder sb = new StringBuilder();
      sb.append("<table class='table table-bordered table-condensed'>");
      sb.append("<tr>");
      for( Coef c : cs ) {
        if(c._name.equals("Intercept"))continue;
        sb.append("<th>").append(c._name).append("</th>");
      }
      sb.append("<th>").append("Intercept").append("</th>");
      sb.append("</tr>");
      sb.append("<tr>");
      for( Coef c : cs ) {
        if(c._name.equals("Intercept"))continue;
        sb.append("<td>").append(c._d).append("</td>");
      }
      sb.append("<td>").append(coefs.get("Intercept").getAsDouble()).append("</td>");
      sb.append("</tr>");
      sb.append("</table>");
      return sb.toString();
    }


    static void validationHTML(GLMModel m, GLMValidation val, StringBuilder sb){
      RString valHeader = new RString("<div class='alert'>Validation on dataset <a href='/Inspect.html?"+KEY+"=%dataKey'>%dataKey</a></div>");
      RString xvalHeader = new RString("<div class='alert'>%valName</div>");

      RString R = new RString("<table class='table table-striped table-bordered table-condensed'>"
          + "<tr><th>Degrees of freedom:</th><td>%DegreesOfFreedom total (i.e. Null);  %ResidualDegreesOfFreedom Residual</td></tr>"
          + "<tr><th>Null Deviance</th><td>%nullDev</td></tr>"
          + "<tr><th>Residual Deviance</th><td>%resDev</td></tr>"
          + "<tr><th>AIC</th><td>%AIC</td></tr>"
          + "<tr><th>Training Error Rate Avg</th><td>%err</td></tr>"
          +"%CM"
          + "</table>");
      RString R2 = new RString(
          "<tr><th>AUC</th><td>%AUC</td></tr>"
          + "<tr><th>Best Threshold</th><td>%threshold</td></tr>");
      if(val.fold() > 1){
        xvalHeader.replace("valName", val.fold() + " fold cross validation");
        sb.append(xvalHeader.toString());
      } else {
        valHeader.replace("dataKey",val.dataKey());
        sb.append(valHeader.toString());
      }

      R.replace("DegreesOfFreedom",m._nLines-1);
      R.replace("ResidualDegreesOfFreedom",m._dof);
      R.replace("nullDev",val._nullDeviance);
      R.replace("resDev",val._deviance);
      R.replace("AIC", dformat(val.AIC()));
      R.replace("err",val.err());


      if(val._cm != null){
        R2.replace("AUC", dformat(val.AUC()));
        R2.replace("threshold", dformat(val.bestThreshold()));
        R.replace("CM",R2);
      }
      sb.append(R);
      if (val._cm != null && val._fprs != null && val._tprs != null)
        ROCplot(val, sb);
      confusionHTML(val.bestCM(),sb);
      if(val.fold() > 1){
        int nclasses = 2;
        sb.append("<table class='table table-bordered table-condensed'>");
        if(val._cm != null){
          sb.append("<tr><th>Model</th><th>Best Threshold</th><th>AUC</th>");
          for(int c = 0; c < nclasses; ++c)
            sb.append("<th>Err(" + c + ")</th>");
          sb.append("</tr>");
          // Display all completed models
          int i=0;
          for(GLMModel xm:val.models()){
            String mname = "Model " + i++;
            sb.append("<tr>");
            try {
              sb.append("<td>" + "<a href='Inspect.html?"+KEY+"="+URLEncoder.encode(xm._key.toString(),"UTF-8")+"'>" + mname + "</a></td>");
            } catch( UnsupportedEncodingException e ) {
              throw  Log.errRTExcept(e);
            }
            sb.append("<td>" + dformat(xm._vals[0].bestThreshold()) + "</td>");
            sb.append("<td>" + dformat(xm._vals[0].AUC()) + "</td>");
            for(double e:xm._vals[0].classError())
              sb.append("<td>" + dformat(e) + "</td>");
            sb.append("</tr>");
          }
        } else {
          sb.append("<tr><th>Model</th><th>Error</th>");
          sb.append("</tr>");
          // Display all completed models
          int i=0;
          for(GLMModel xm:val.models()){
            String mname = "Model " + i++;
            sb.append("<tr>");
            try {
              sb.append("<td>" + "<a href='Inspect.html?"+KEY+"="+URLEncoder.encode(xm._key.toString(),"UTF-8")+"'>" + mname + "</a></td>");
            } catch( UnsupportedEncodingException e ) {
              throw  Log.errRTExcept(e);
            }
            sb.append("<td>" + ((xm._vals != null)?xm._vals[0]._err:Double.NaN) + "</td>");
            sb.append("</tr>");
          }
        }
        sb.append("</table>");
      }
    }

    private static void validationHTML( GLMModel m, GLMValidation[] vals, StringBuilder sb) {
      if( vals == null || vals.length == 0 ) return;
      sb.append("<h4>Validations</h4>");
      for( GLMValidation val : vals )
        if(val != null)validationHTML(m,val, sb);
    }

    private static void cmRow( StringBuilder sb, String hd, double c0, double c1, double cerr ) {
      sb.append("<tr><th>").append(hd).append("</th><td>");
      if( !Double.isNaN(c0  )) sb.append( dformat(c0  ));
      sb.append("</td><td>");
      if( !Double.isNaN(c1  )) sb.append( dformat(c1  ));
      sb.append("</td><td>");
      if( !Double.isNaN(cerr)) sb.append( dformat(cerr));
      sb.append("</td></tr>");
    }

    private static void confusionHTML( hex.ConfusionMatrix cm, StringBuilder sb) {
      if( cm == null ) return;
      sb.append("<table class='table table-bordered table-condensed'>");
      sb.append("<tr><th>Actual / Predicted</th><th>false</th><th>true</th><th>Err</th></tr>");
      double err0 = cm._arr[0][1]/(double)(cm._arr[0][0]+cm._arr[0][1]);
      cmRow(sb,"false",cm._arr[0][0],cm._arr[0][1],err0);
      double err1 = cm._arr[1][0]/(double)(cm._arr[1][0]+cm._arr[1][1]);
      cmRow(sb,"true ",cm._arr[1][0],cm._arr[1][1],err1);
      double err2 = cm._arr[1][0]/(double)(cm._arr[0][0]+cm._arr[1][0]);
      double err3 = cm._arr[0][1]/(double)(cm._arr[0][1]+cm._arr[1][1]);
      cmRow(sb,"Err ",err2,err3,cm.err());
      sb.append("</table>");
    }


      public static void ROCplot(GLMValidation xval, StringBuilder sb ) {
          sb.append("<script type=\"text/javascript\" src='/h2o/js/d3.v3.min.js'></script>");
          sb.append("<div id=\"ROC\">");
          sb.append("<style type=\"text/css\">");
          sb.append(".axis path," +
                  ".axis line {\n" +
                  "fill: none;\n" +
                  "stroke: black;\n" +
                  "shape-rendering: crispEdges;\n" +
                  "}\n" +

                  ".axis text {\n" +
                  "font-family: sans-serif;\n" +
                  "font-size: 11px;\n" +
                  "}\n");

          sb.append("</style>");
          sb.append("<div id=\"rocCurve\" style=\"display:inline;\">");
          sb.append("<script type=\"text/javascript\">");

          sb.append("//Width and height\n");
          sb.append("var w = 500;\n"+
                  "var h = 300;\n"+
                  "var padding = 40;\n"
          );
          sb.append("var dataset = [");

          for(int c = 0; c < xval._cm.length; c++) {
            if (c == 0) {
              sb.append("["+String.valueOf(xval._fprs[c])+",").append(String.valueOf(xval._tprs[c])).append("]");
            }
            sb.append(", ["+String.valueOf(xval._fprs[c])+",").append(String.valueOf(xval._tprs[c])).append("]");
          }
          for(int c = 0; c < 2*xval._cm.length; c++) {
            sb.append(", ["+String.valueOf(c/(2.0*xval._cm.length))+",").append(String.valueOf(c/(2.0*xval._cm.length))).append("]");
          }
          sb.append("];\n");

          sb.append(
                  "//Create scale functions\n"+
                          "var xScale = d3.scale.linear()\n"+
                          ".domain([0, d3.max(dataset, function(d) { return d[0]; })])\n"+
                          ".range([padding, w - padding * 2]);\n"+

                          "var yScale = d3.scale.linear()"+
                          ".domain([0, d3.max(dataset, function(d) { return d[1]; })])\n"+
                          ".range([h - padding, padding]);\n"+

                          "var rScale = d3.scale.linear()"+
                          ".domain([0, d3.max(dataset, function(d) { return d[1]; })])\n"+
                          ".range([2, 5]);\n"+

                          "//Define X axis\n"+
                          "var xAxis = d3.svg.axis()\n"+
                          ".scale(xScale)\n"+
                          ".orient(\"bottom\")\n"+
                          ".ticks(5);\n"+

                          "//Define Y axis\n"+
                          "var yAxis = d3.svg.axis()\n"+
                          ".scale(yScale)\n"+
                          ".orient(\"left\")\n"+
                          ".ticks(5);\n"+

                          "//Create SVG element\n"+
                          "var svg = d3.select(\"#rocCurve\")\n"+
                          ".append(\"svg\")\n"+
                          ".attr(\"width\", w)\n"+
                          ".attr(\"height\", h);\n"+

                          "//Create circles\n"+
                          "svg.selectAll(\"circle\")\n"+
                          ".data(dataset)\n"+
                          ".enter()\n"+
                          ".append(\"circle\")\n"+
                          ".attr(\"cx\", function(d) {\n"+
                          "return xScale(d[0]);\n"+
                          "})\n"+
                          ".attr(\"cy\", function(d) {\n"+
                          "return yScale(d[1]);\n"+
                          "})\n"+
                          ".attr(\"fill\", function(d) {\n"+
                          "  if (d[0] == d[1]) {\n"+
                          "    return \"red\"\n"+
                          "  } else {\n"+
                          "  return \"blue\"\n"+
                          "  }\n"+
                          "})\n"+
                          ".attr(\"r\", function(d) {\n"+
                          "  if (d[0] == d[1]) {\n"+
                          "    return 1\n"+
                          "  } else {\n"+
                          "  return 2\n"+
                          "  }\n"+
                          "})\n" +
                          ".on(\"mouseover\", function(d,i){\n" +
                          "   if(i <= 100) {" +
                          "     document.getElementById(\"select\").selectedIndex = 100 - i\n" +
                          "     show_cm(i)\n" +
                          "   }\n" +
                          "});\n"+

                          "/*"+
                          "//Create labels\n"+
                          "svg.selectAll(\"text\")"+
                          ".data(dataset)"+
                          ".enter()"+
                          ".append(\"text\")"+
                          ".text(function(d) {"+
                          "return d[0] + \",\" + d[1];"+
                          "})"+
                          ".attr(\"x\", function(d) {"+
                          "return xScale(d[0]);"+
                          "})"+
                          ".attr(\"y\", function(d) {"+
                          "return yScale(d[1]);"+
                          "})"+
                          ".attr(\"font-family\", \"sans-serif\")"+
                          ".attr(\"font-size\", \"11px\")"+
                          ".attr(\"fill\", \"red\");"+
                          "*/\n"+

                          "//Create X axis\n"+
                          "svg.append(\"g\")"+
                          ".attr(\"class\", \"axis\")"+
                          ".attr(\"transform\", \"translate(0,\" + (h - padding) + \")\")"+
                          ".call(xAxis);\n"+

                          "//X axis label\n"+
                          "d3.select('#rocCurve svg')"+
                          ".append(\"text\")"+
                          ".attr(\"x\",w/2)"+
                          ".attr(\"y\",h - 5)"+
                          ".attr(\"text-anchor\", \"middle\")"+
                          ".text(\"False Positive Rate\");\n"+

                          "//Create Y axis\n"+
                          "svg.append(\"g\")"+
                          ".attr(\"class\", \"axis\")"+
                          ".attr(\"transform\", \"translate(\" + padding + \",0)\")"+
                          ".call(yAxis);\n"+

                          "//Y axis label\n"+
                          "d3.select('#rocCurve svg')"+
                          ".append(\"text\")"+
                          ".attr(\"x\",150)"+
                          ".attr(\"y\",-5)"+
                          ".attr(\"transform\", \"rotate(90)\")"+
                          //".attr(\"transform\", \"translate(0,\" + (h - padding) + \")\")"+
                          ".attr(\"text-anchor\", \"middle\")"+
                          ".text(\"True Positive Rate\");\n"+

                          "//Title\n"+
                          "d3.select('#rocCurve svg')"+
                          ".append(\"text\")"+
                          ".attr(\"x\",w/2)"+
                          ".attr(\"y\",padding - 20)"+
                          ".attr(\"text-anchor\", \"middle\")"+
                          ".text(\"ROC\");\n");

          sb.append("</script>");
          sb.append("</div>");
      }
  }

}
