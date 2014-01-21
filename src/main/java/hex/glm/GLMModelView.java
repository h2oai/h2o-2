package hex.glm;

import hex.ConfusionMatrix;
import hex.glm.GLMModel.Submodel;
import hex.glm.GLMParams.Family;
import hex.glm.GLMValidation.GLMXValidation;
import water.*;
import water.api.*;
import water.api.Request.API;
import water.api.Request.Default;
import water.api.RequestArguments.H2OKey;
import water.api.RequestBuilders.Response;
import water.util.RString;

import java.text.DecimalFormat;

public class GLMModelView extends Request2 {
  public GLMModelView(){}
  public GLMModelView(GLMModel m){glm_model = m;}

  static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  @API(help="GLM Model Key", required=true, filter=GLMModelKeyFilter.class)
  Key _modelKey;

  @API(help="Lambda value which should be displayed as main model", required=false, filter=Default.class)
  double lambda = Double.NaN;

  class GLMModelKeyFilter extends H2OKey { public GLMModelKeyFilter() { super("",true); } }

  @API(help="GLM Model")
  GLMModel glm_model;

  @API(help="job key",required=false, filter=Default.class)
  Key job_key;

  public static String link(String txt, Key model) {return link(txt,model,Double.NaN);}
  public static String link(String txt, Key model, double lambda) {
    return "<a href='GLMModelView.html?_modelKey=" + model + "&lambda=" + lambda + "'>" + txt + "</a>";
  }
  public static Response redirect(Request req, Key modelKey) {
    return Response.redirect(req, "/2/GLMModelView", "_modelKey", modelKey);
  }
  public static Response redirect(Request req, Key modelKey, Key job_key) {
    return Response.redirect(req, "/2/GLMModelView", "_modelKey", modelKey,"job_key",job_key);
  }
  @Override public boolean toHTML(StringBuilder sb){
//      if(title != null && !title.isEmpty())DocGen.HTML.title(sb,title);
    if(glm_model == null){
      sb.append("No model yet...");
      return true;
    }
    DocGen.HTML.paragraph(sb,"Model Key: "+glm_model._selfKey);
    if(glm_model.submodels != null)
      DocGen.HTML.paragraph(sb,water.api.Predict.link(glm_model._selfKey,"Predict!"));
    String succ = (glm_model.warnings == null || glm_model.warnings.length == 0)?"alert-success":"alert-warning";
    sb.append("<div class='alert " + succ + "'>");
    pprintTime(sb.append(glm_model.iteration() + " iterations computed in "),glm_model.run_time);
    if(glm_model.warnings != null && glm_model.warnings.length > 0){
      sb.append("<b>Warnings:</b><ul>");
      for(String w:glm_model.warnings)sb.append("<li>" + w + "</li>");
      sb.append("</ul>");
    }
    sb.append("</div>");
    sb.append("<h4>Parameters</h4>");
    parm(sb,"family",glm_model.glm.family);
    parm(sb,"link",glm_model.glm.link);
    parm(sb,"&epsilon;<sub>&beta;</sub>",glm_model.beta_eps);
    parm(sb,"&alpha;",glm_model.alpha);
    parm(sb,"&lambda;",DFORMAT2.format(lambda));

    if(glm_model.submodels.length > 1){
      sb.append("\n<table class='table table-bordered table-condensed'>\n");
      StringBuilder firstRow = new StringBuilder("\t<tr><th>&lambda;</th>\n");
      StringBuilder secondRow = new StringBuilder("\t<tr><th>nonzeros</th>\n");
      StringBuilder thirdRow = new StringBuilder("\t<tr><th>Deviance Explained</th>\n");
      StringBuilder fourthRow = new StringBuilder("\t<tr><th>" + (glm_model.glm.family == Family.binomial?"AUC":"AIC") + "</th>\n");
      for(int i = 0; i < glm_model.submodels.length; ++i){
        final Submodel sm = glm_model.submodels[i];
        if(sm.validation == null)break;
        if(glm_model.lambdas[i] == lambda)firstRow.append("\t\t<td><b>" + DFORMAT2.format(glm_model.lambdas[i]) + "</b></td>\n");
        else firstRow.append("\t\t<td>" + link(DFORMAT2.format(glm_model.lambdas[i]),glm_model._selfKey,glm_model.lambdas[i]) + "</td>\n");
        secondRow.append("\t\t<td>" + (sm.rank-1) + "</td>\n");
        thirdRow.append("\t\t<td>" + DFORMAT.format(1-sm.validation.residual_deviance/sm.validation.null_deviance) + "</td>\n");
        fourthRow.append("\t\t<td>" + DFORMAT.format(glm_model.glm.family==Family.binomial?sm.validation.auc:sm.validation.aic) + "</td>\n");
      }
      sb.append(firstRow.append("\t</tr>\n"));
      sb.append(secondRow.append("\t</tr>\n"));
      sb.append(thirdRow.append("\t</tr>\n"));
      sb.append(fourthRow.append("\t</tr>\n"));
      sb.append("</table>\n");
    }
    Submodel sm = glm_model.submodels[glm_model.best_lambda_idx];
    if(!Double.isNaN(lambda) && glm_model.lambdas[glm_model.best_lambda_idx] != lambda){
      int ii = 0;
      sm = glm_model.submodels[0];
      while(glm_model.lambdas[ii] != lambda && ++ii < glm_model.submodels.length)
        sm = glm_model.submodels[ii];
      if(ii == glm_model.submodels.length)throw new IllegalArgumentException("Unexpected value of lambda '" + lambda + "'");
    }
    if(glm_model.beta() != null)
      coefs2html(sm,sb);
    GLMValidation val = sm.validation;
    if(val != null)val2HTML(sm,val, sb);
    return true;
  }


  public void val2HTML(Submodel sm,GLMValidation val, StringBuilder sb) {
    String title = (val instanceof GLMXValidation)?"Cross Validation":"Validation";
    sb.append("<h4>" + title + "</h4>");
    sb.append("<table class='table table-striped table-bordered table-condensed'>");
    final long null_dof = val.nobs-1, res_dof = Math.max(0,val.nobs-sm.rank);
    sb.append("<tr><th>Degrees of freedom:</th><td>" + null_dof + " total (i.e. Null); " + res_dof + " Residual</td></tr>");
    sb.append("<tr><th>Null Deviance</th><td>" + val.null_deviance + "</td></tr>");
    sb.append("<tr><th>Residual Deviance</th><td>" + val.residual_deviance + "</td></tr>");
    sb.append("<tr><th>AIC</th><td>" + val.aic() + "</td></tr>");
    sb.append("<tr><th>Training Error Rate Avg</th><td>" + val.avg_err + "</td></tr>");
    if(glm_model.glm.family == Family.binomial)sb.append("<tr><th>AUC</th><td>" + DFORMAT.format(val.auc()) + "</td></tr>");
    sb.append("</table>");

    if(glm_model.glm.family == Family.binomial){
      sb.append("<span><b>ROC curve</b></span>");
      ROCc(sb,val);
      int best = (int)(100*glm_model.threshold);
      confusionHTML(val._cms[best], sb);
      sb.append("\n<script type=\"text/javascript\">");//</script>");
      sb.append("var cms = [\n");
      for(ConfusionMatrix cm:val._cms){
        sb.append("\t[\n");
        for(long [] line:cm._arr) {
          sb.append("\t\t[");
          for(long l:line) sb.append(l + ",");
          sb.append("],\n");
        }
        sb.append("\t],\n");
      }
      sb.append("];\n");
      sb.append("function show_cm(i){\n");
      //sb.append("\t" + "console.log(i);\n");
      sb.append("\t" + "document.getElementById('TN').innerHTML = cms[i][0][0];\n");
      sb.append("\t" + "document.getElementById('TP').innerHTML = cms[i][1][1];\n");
      sb.append("\t" + "document.getElementById('FN').innerHTML = cms[i][0][1];\n");
      sb.append("\t" + "document.getElementById('FP').innerHTML = cms[i][1][0];\n");
      sb.append("}\n");
      sb.append("</script>\n");
      sb.append("\n<div><b>Confusion Matrix at decision threshold:</b></div><select id=\"select\" onchange='show_cm(this.value)'>\n");
      for(int i = 0; i < GLMValidation.DEFAULT_THRESHOLDS.length; ++i)
        sb.append("\t<option value='" + i + "'" + (GLMValidation.DEFAULT_THRESHOLDS[i] == glm_model.threshold?"selected='selected'":"") +">" + GLMValidation.DEFAULT_THRESHOLDS[i] + "</option>\n");
      sb.append("</select>\n");
    }
    if(val instanceof GLMXValidation){
      GLMXValidation xval = (GLMXValidation)val;
      // add links to the xval models
      sb.append("<h4>Cross Validation Models</h4>");
      sb.append("<table class='table table-bordered table-condensed'>");
      sb.append("<tr><th>Model</th><th>nonzeros</th>");
      sb.append("<th>" + ((glm_model.glm.family == Family.binomial)?"AUC":"AIC") + "</th>");
      sb.append("<th>Deviance Explained</th>");
      sb.append("</tr>");
      int i = 0;
      for(Key k:xval.xval_models){
        Value v = DKV.get(k);
        if(v == null)continue;
        GLMModel m = v.get();
        sb.append("<tr>");
        sb.append("<td>" + GLMModelView.link("Model " + ++i, k) + "</td>");
        sb.append("<td>" + (m.rank()-1) + "</td>");
        sb.append("<td>" + ((glm_model.glm.family == Family.binomial)?format(m.auc()):format(m.aic())) + "</td>");
        sb.append("<td>" + format(m.devExplained()) + "</td>");
        sb.append("</tr>");
      }
      sb.append("</table>");
    }
  }

  private static final DecimalFormat DFORMAT3 = new DecimalFormat("##.##");

  private static String format(double d){
    return DFORMAT3.format(0.01*(int)(100*d));
  }

  private static void confusionHTML( hex.ConfusionMatrix cm, StringBuilder sb) {
    if( cm == null ) return;

    sb.append("<table class='table table-bordered table-condensed'>");
//    sb.append("<tr><th>Actual / Predicted</th><th>false</th><th>true</th><th>Err</th></tr>");
    sb.append("<tr><th>Actual / Predicted</th><th>false</th><th>true</th></tr>");
//    double err0 = cm._arr[0][1]/(double)(cm._arr[0][0]+cm._arr[0][1]);
    sb.append("<tr><th>false</th><td id='TN'>" + cm._arr[0][0] + "</td><td id='FN'>" + cm._arr[0][1] + "</td></tr>");
    sb.append("<tr><th>true</th><td id='FP'>" + cm._arr[1][0] + "</td><td id='TP'>" + cm._arr[1][1] + "</td></tr>");
//    cmRow(sb,"false",cm._arr[0][0],cm._arr[0][1],err0);
//    double err1 = cm._arr[1][0]/(double)(cm._arr[1][0]+cm._arr[1][1]);
//    cmRow(sb,"true ",cm._arr[1][0],cm._arr[1][1],err1);
//    double err2 = cm._arr[1][0]/(double)(cm._arr[0][0]+cm._arr[1][0]);
//    double err3 = cm._arr[0][1]/(double)(cm._arr[0][1]+cm._arr[1][1]);
//    cmRow(sb,"Err ",err2,err3,cm.err());
    sb.append("</table>");
  }

  private static void cmRow( StringBuilder sb, String hd, double c0, double c1, double cerr ) {
    sb.append("<tr><th>").append(hd).append("</th><td>");
    if( !Double.isNaN(c0)) sb.append(DFORMAT.format(c0));
    sb.append("</td><td>");
    if( !Double.isNaN(c1)) sb.append(DFORMAT.format(c1));
    sb.append("</td><td>");
    if( !Double.isNaN(cerr)) sb.append(DFORMAT.format(cerr));
    sb.append("</td></tr>");
  }



  public void ROCc(StringBuilder sb, GLMValidation xval) {
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
    for(int c = 0; c < xval._cms.length; c++) {
      if (c == 0) {
        sb.append("["+String.valueOf(xval.fprs[c])+",").append(String.valueOf(xval.tprs[c])).append("]");
      }
      sb.append(", ["+String.valueOf(xval.fprs[c])+",").append(String.valueOf(xval.tprs[c])).append("]");
    }
    for(int c = 0; c < 2*xval._cms.length; c++) {
        sb.append(", ["+String.valueOf(c/(2.0*xval._cms.length))+",").append(String.valueOf(c/(2.0*xval._cms.length))).append("]");
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

  private static void parm( StringBuilder sb, String x, Object... y ) {
    sb.append("<span><b>").append(x).append(": </b>").append(y[0]).append("</span> ");
  }
  private static final DecimalFormat DFORMAT = new DecimalFormat("###.###");
  private static final DecimalFormat DFORMAT2 = new DecimalFormat("0.##E0");

  private void coefs2html(final Submodel sm,StringBuilder sb){
    StringBuilder names = new StringBuilder();
    StringBuilder equation = new StringBuilder();
    StringBuilder vals = new StringBuilder();
    StringBuilder normVals = sm.norm_beta == null?null:new StringBuilder();
    String [] cNames = glm_model.coefficients_names;
    boolean first = true;
    for(int i:sm.idxs){
      names.append("<th>" + cNames[i] + "</th>");
      vals.append("<td>" + sm.beta[i] + "</td>");
      if(first){
        equation.append(DFORMAT.format(sm.beta[i]));
      } else {
        equation.append(sm.beta[i] > 0?" + ":" - ");
        equation.append(DFORMAT.format(Math.abs(sm.beta[i])));
      }
      if(i < (cNames.length-1))
         equation.append("*x[" + cNames[i] + "]");
      if(sm.norm_beta != null) normVals.append("<td>" + sm.norm_beta[i] + "</td>");
    }
    sb.append("<h4>Equation</h4>");
    RString eq = null;
    switch( glm_model.glm.link ) {
    case identity: eq = new RString("y = %equation");   break;
    case logit:    eq = new RString("y = 1/(1 + Math.exp(-(%equation)))");  break;
    case log:      eq = new RString("y = Math.exp((%equation)))");  break;
    case inverse:  eq = new RString("y = 1/(%equation)");  break;
    case tweedie:  eq = new RString("y = (%equation)^(1 -  )"); break;
    default:       eq = new RString("equation display not implemented"); break;
    }
    eq.replace("equation",equation.toString());
    sb.append("<div style='width:100%;overflow:scroll;'>");
    sb.append("<div><code>" + eq + "</code></div>");
    sb.append("<h4>Coefficients</h4><table class='table table-bordered table-condensed'>");
    sb.append("<tr>" + names.toString() + "</tr>");
    sb.append("<tr>" + vals.toString() + "</tr>");
    sb.append("</table>");
    if(sm.norm_beta != null){
      sb.append("<h4>Normalized Coefficients</h4>" +
          "<table class='table table-bordered table-condensed'>");
      sb.append("<tr>" + names.toString()    + "</tr>");
      sb.append("<tr>" + normVals.toString() + "</tr>");
      sb.append("</table>");
    }
    sb.append("</div>");
  }
  private void pprintTime(StringBuilder sb, long t){
    long hrs = t / (1000*60*60);
    long minutes = (t -= 1000*60*60*hrs)/(1000*60);
    long seconds = (t -= 1000*60*minutes)/1000;
    t -= 1000*seconds;
    if(hrs > 0)sb.append(hrs + "hrs ");
    if(hrs > 0 || minutes > 0)sb.append(minutes + "min ");
    if(hrs > 0 || minutes > 0 | seconds > 0)sb.append(seconds + "sec ");
    sb.append(t + "msec");
  }

//  Job jjob = null;
//  if( job_key != null )
//    jjob = Job.findJob(job_key);
//  if( jjob != null && jjob.exception != null )
//    return Response.error(jjob.exception == null ? "cancelled" : jjob.exception);
//  if( jjob == null || jjob.end_time > 0 || jjob.cancelled() )
//    return jobDone(jjob, destination_key);
//  return jobInProgress(jjob, destination_key);

  @Override protected Response serve() {
    Job jjob = ( job_key != null )?Job.findJob(job_key):null;
    if( jjob != null && jjob.exception != null )
      return Response.error(jjob.exception == null ? "cancelled" : jjob.exception);
    Value v = DKV.get(_modelKey);
    if(v != null){
      glm_model = v.get();
      if(Double.isNaN(lambda))lambda = glm_model.lambdas[glm_model.best_lambda_idx];
    }
    if( jjob == null || jjob.end_time > 0 || jjob.cancelled() )
      return Response.done(this);
    return Response.poll(this,(int)(100*jjob.progress()),100,"_modelKey",_modelKey.toString());
  }

//  @Override protected Response serve() {
//    Value v = DKV.get(_modelKey);
//    if(v == null)
//      return Response.poll(this, 0, 100, "_modelKey", _modelKey.toString());
//    glm_model = v.get();
//    if(Double.isNaN(lambda))lambda = glm_model.lambdas[glm_model.best_lambda_idx];
//    Job j;
//    if((j = Job.findJob(glm_model.job_key)) != null && j.exception != null)
//      return Response.error(j.exception);
//    if(DKV.get(glm_model.job_key) != null && j != null)
//      return Response.poll(this, (int) (100 * j.progress()), 100, "_modelKey", _modelKey.toString());
//    else
//      return Response.done(this);
//  }
}

