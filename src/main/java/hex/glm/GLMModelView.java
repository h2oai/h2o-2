package hex.glm;

import hex.glm.GLMModel.Submodel;
import hex.glm.GLMParams.Family;
import hex.glm.GLMValidation.GLMXValidation;
import water.*;
import water.api.*;
import water.util.RString;
import water.util.UIUtils;

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
    glm_model.get_params().makeJsonBox(sb);
    DocGen.HTML.paragraph(sb,"Model Key: "+glm_model._key);
    if(glm_model.submodels != null) {
      DocGen.HTML.paragraph(sb,water.api.GLMPredict.link(glm_model._key,lambda,"Predict!"));
      DocGen.HTML.paragraph(sb,UIUtils.qlink(SaveModel.class, "model", glm_model._key, "Save model"));
    }
    String succ = (glm_model.warnings == null || glm_model.warnings.length == 0)?"alert-success":"alert-warning";
    sb.append("<div class='alert " + succ + "'>");
    pprintTime(sb.append(glm_model.iteration() + " iterations computed in "),glm_model.run_time);
    if(glm_model.warnings != null && glm_model.warnings.length > 0){
      sb.append("<ul>");
      for(String w:glm_model.warnings)sb.append("<li><b>Warning:</b>" + w + "</li>");
      sb.append("</ul>");
    }
    sb.append("</div>");
    if(!Double.isNaN(lambda) && lambda != glm_model.submodels[glm_model.best_lambda_idx].lambda_value){ // show button to permanently set lambda_value to this value
      sb.append("<div class='alert alert-warning'>\n");
      sb.append(GLMModelUpdate.link("Set lambda_value to current value!",_modelKey,lambda) + "\n");
      sb.append("</div>");
    }
    sb.append("<h4>Parameters</h4>");
    parm(sb,"family",glm_model.glm.family);
    parm(sb,"link",glm_model.glm.link);
    parm(sb,"&epsilon;<sub>&beta;</sub>",glm_model.beta_eps);
    parm(sb,"&alpha;",glm_model.alpha);
    if(!Double.isNaN(glm_model.lambda_max))
      parm(sb,"&lambda;<sub>max</sub>",DFORMAT2.format(glm_model.lambda_max));
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
        if (glm_model.submodels[i].lambda_value == lambda)
          firstRow.append("\t\t<td><b>" + DFORMAT2.format(glm_model.submodels[i].lambda_value) + "</b></td>\n");
        else
          firstRow.append("\t\t<td>" + link(DFORMAT2.format(glm_model.submodels[i].lambda_value), glm_model._key, glm_model.submodels[i].lambda_value) + "</td>\n");
        secondRow.append("\t\t<td>" + Math.max(0,(sm.rank - 1)) + "</td>\n"); // rank counts intercept, that's why -1 is there, however, intercept can be 0 as well, so just prevent -1
        if(sm.xvalidation != null){
          thirdRow.append("\t\t<td>"  + DFORMAT.format(1 - sm.xvalidation.residual_deviance / glm_model.null_validation.residualDeviance()) + "<sub>x</sub>(" + DFORMAT.format(1 - sm.validation.residual_deviance /glm_model.null_validation.residualDeviance()) + ")" + "</td>\n");
          fourthRow.append("\t\t<td>" + DFORMAT.format(glm_model.glm.family == Family.binomial ? sm.xvalidation.auc : sm.xvalidation.aic) + "<sub>x</sub>("+ DFORMAT.format(glm_model.glm.family == Family.binomial ? sm.validation.auc : sm.validation.aic) + ")</td>\n");
        } else {
          thirdRow.append("\t\t<td>" + DFORMAT.format(1 - sm.validation.residual_deviance / glm_model.null_validation.residualDeviance()) + "</td>\n");
          fourthRow.append("\t\t<td>" + DFORMAT.format(glm_model.glm.family == Family.binomial ? sm.validation.auc : sm.validation.aic) + "</td>\n");
        }
      }
      sb.append(firstRow.append("\t</tr>\n"));
      sb.append(secondRow.append("\t</tr>\n"));
      sb.append(thirdRow.append("\t</tr>\n"));
      sb.append(fourthRow.append("\t</tr>\n"));
      sb.append("</table>\n");
    }
    if(glm_model.submodels.length == 0)return true;
    Submodel sm = glm_model.submodels[glm_model.best_lambda_idx];
    if(!Double.isNaN(lambda) && glm_model.submodels[glm_model.best_lambda_idx].lambda_value != lambda){
      int ii = 0;
      sm = glm_model.submodels[0];
      while(glm_model.submodels[ii].lambda_value != lambda && ++ii < glm_model.submodels.length)
        sm = glm_model.submodels[ii];
      if(ii == glm_model.submodels.length)throw new IllegalArgumentException("Unexpected value of lambda '" + lambda + "'");
    }
    if(glm_model.submodels != null)
      coefs2html(sm,sb);
    if(sm.xvalidation != null)
      val2HTML(sm,sm.xvalidation,sb);
    else if(sm.validation != null)
      val2HTML(sm,sm.validation, sb);
    // Variable importance
    if (glm_model.varimp() != null) {
      glm_model.varimp().toHTML(glm_model, sb);
    }
    return true;
  }


  public void val2HTML(Submodel sm,GLMValidation val, StringBuilder sb) {
    String title = (val instanceof GLMXValidation)?"Cross Validation":"Validation";
    sb.append("<h4>" + title + "</h4>");
    sb.append("<table class='table table-striped table-bordered table-condensed'>");
    final long null_dof = val.nobs-1, res_dof = Math.max(0,val.nobs-sm.rank);
    sb.append("<tr><th>Degrees of freedom:</th><td>" + null_dof + " total (i.e. Null); " + res_dof + " Residual</td></tr>");
    sb.append("<tr><th>Null Deviance</th><td>" + glm_model.null_validation.residualDeviance() + "</td></tr>");
    sb.append("<tr><th>Residual Deviance</th><td>" + val.residual_deviance + "</td></tr>");
    sb.append("<tr><th>AIC</th><td>" + val.aic() + "</td></tr>");
    if(glm_model.glm.family == Family.binomial)sb.append("<tr><th>AUC</th><td>" + DFORMAT.format(val.auc()) + "</td></tr>");
    sb.append("</table>");
    if(glm_model.glm.family == Family.binomial)new AUC(val._cms,val.thresholds,glm_model._domains[glm_model._domains.length-1]).toHTML(sb);
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
    int [] sortedIds = new int[sm.beta.length];
    for(int i = 0; i < sortedIds.length; ++i)
      sortedIds[i] = i;
    final double [] b = sm.norm_beta == null?sm.beta:sm.norm_beta;
    // now sort the indeces according to their abs value from biggest to smallest (but keep intercept last)
    int r = sortedIds.length-1;
    for(int i = 1; i < r; ++i){
      for(int j = 1; j < r-i;++j){
        if(Math.abs(b[sortedIds[j-1]]) < Math.abs(b[sortedIds[j]])){
          int jj = sortedIds[j];
          sortedIds[j] = sortedIds[j-1];
          sortedIds[j-1] = jj;
        }
      }
    }

    String [] cNames = glm_model.coefficients_names;
    boolean first = true;
    int j = 0;
    for(int i:sortedIds){
      names.append("<th>" + cNames[sm.idxs[i]] + "</th>");
      vals.append("<td>" + sm.beta[i] + "</td>");
      if(first){
        equation.append(DFORMAT.format(sm.beta[i]));
        first = false;
      } else {
        equation.append(sm.beta[i] > 0?" + ":" - ");
        equation.append(DFORMAT.format(Math.abs(sm.beta[i])));
      }
      if(i < (cNames.length-1))
         equation.append("*x[" + cNames[i] + "]");
      if(sm.norm_beta != null) normVals.append("<td>" + sm.norm_beta[i] + "</td>");
      ++j;
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
      if(Double.isNaN(lambda) && glm_model.submodels.length != 0)
        lambda = glm_model.submodels[glm_model.best_lambda_idx].lambda_value;
    }
    if( jjob == null || jjob.end_time > 0 || jjob.isCancelledOrCrashed() )
      return Response.done(this);
    return Response.poll(this,(int)(100*jjob.progress()),100,"_modelKey",_modelKey.toString());
  }

//  @Override protected Response serve() {
//    Value v = DKV.get(_modelKey);
//    if(v == null)
//      return Response.poll(this, 0, 100, "_modelKey", _modelKey.toString());
//    glm_model = v.get();
//    if(Double.isNaN(lambda_value))lambda_value = glm_model.lambdas[glm_model.best_lambda_idx];
//    Job j;
//    if((j = Job.findJob(glm_model.job_key)) != null && j.exception != null)
//      return Response.error(j.exception);
//    if(DKV.get(glm_model.job_key) != null && j != null)
//      return Response.poll(this, (int) (100 * j.progress()), 100, "_modelKey", _modelKey.toString());
//    else
//      return Response.done(this);
//  }
}

