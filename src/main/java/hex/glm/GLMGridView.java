package hex.glm;

import hex.glm.GLM2.GLMGrid;
import hex.glm.GLMParams.Family;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import water.*;
import water.api.DocGen;
import water.api.Request;

public class GLMGridView extends Request2 {
  public GLMGridView(){}
  public GLMGridView(GLMGrid g){grid = g;}

  static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  @API(help="GLM Grid Key", required=true, filter=GLMGridKeyFilter.class)
  Key grid_key;

  class GLMGridKeyFilter extends H2OKey { public GLMGridKeyFilter() { super("",true); } }

  @API(help="GLM Grid object")
  GLMGrid grid;

  public static String link(String txt, Key grid) {
    return "<a href='GLMGridView.html?grid=" + grid + "'>" + txt + "</a>";
  }
  public static Response redirect(Request req, Key gridKey) {
    return Response.redirect(req, "/2/GLMGridView", "grid_key", gridKey);
  }

  public static Response redirect2(Request req, Key modelKey) {
    return Response.redirect(req, "/2/GLMGridView", "grid_key", modelKey);
  }

  public static final DecimalFormat AUC_DFORMAT = new DecimalFormat("#.###");

  public static final String aucStr(double auc){
    return AUC_DFORMAT.format(Math.round(1000*auc)*0.001);
  }
  public static final DecimalFormat AIC_DFORMAT = new DecimalFormat("###.###");

  public static final String aicStr(double aic){
    return AUC_DFORMAT.format(Math.round(1000*aic)*0.001);
  }
  public static final DecimalFormat DEV_EXPLAINED_DFORMAT = new DecimalFormat("#.###");
  public static final String devExplainedStr(double dev){
    return AUC_DFORMAT.format(Math.round(1000*dev)*0.001);
  }

  @Override public boolean toHTML(StringBuilder sb){
//        if(title != null && !title.isEmpty())DocGen.HTML.title(sb,title);
    ArrayList<GLMModel> models = new ArrayList<GLMModel>(grid.destination_keys.length);
    for(int i = 0; i < grid.destination_keys.length; ++i){
      Value v = DKV.get(grid.destination_keys[i]);
      if(v != null)models.add(v.<GLMModel>get());
    }
    if(models.isEmpty()){
      sb.append("no models computed yet..");
    } else {
      DocGen.HTML.arrayHead(sb);
      sb.append("<tr>");
      sb.append("<th>&alpha;</th>");
      sb.append("<th>&lambda;<sub>max</sub></th>");
      sb.append("<th>&lambda;<sub>min</sub></th>");
      sb.append("<th>&lambda;<sub>best</sub></th>");
      sb.append("<th>nonzeros</th>");
      sb.append("<th>iterations</td>");
      if(models.get(0).glm.family == Family.binomial)
        sb.append("<th>AUC</td>");
      if(models.get(0).glm.family != Family.gamma)
        sb.append("<th>AIC</td>");
      sb.append("<th>Deviance Explained</td>");
      sb.append("<th>Model</th>");
//      sb.append("<th>Progress</th>");
      sb.append("</tr>");
      Collections.sort(models);//, _cmp);
      for(int i = 0; i < models.size();++i){
        GLMModel m = models.get(i);
        sb.append("<tr>");
        sb.append("<td>" + m.alpha + "</td>");
        sb.append("<td>" + m.submodels[0].lambda_value + "</td>");
        sb.append("<td>" + m.submodels[m.submodels.length-1].lambda_value + "</td>");
        sb.append("<td>" + m.lambda() + "</td>");
        sb.append("<td>" + (m.rank()-1) + "</td>");
        sb.append("<td>" + m.iteration() + "</td>");
        if(m.glm.family == Family.binomial)
          sb.append("<td>" + aucStr(m.auc()) +  "</td>");
        if(m.glm.family != Family.gamma)
          sb.append("<td>" + aicStr(m.aic()) +  "</td>");
        sb.append("<td>" + devExplainedStr(m.devExplained()) +  "</td>");
        sb.append("<td>" + GLMModelView.link("View Model", m._key) + "</td>");
//        if(job != null && !job.isDone())DocGen.HTML.progress(job.progress(), sb.append("<td>")).append("</td>");
//        else sb.append("<td class='alert alert-success'>" + "DONE" + "</td>");
        sb.append("</tr>");
      }
      DocGen.HTML.arrayTail(sb);
    }
    return true;
  }

  @Override protected Response serve() {
    grid = DKV.get(grid_key).get();
    Job j = null;
    if((j = UKV.get(grid._jobKey)) != null){
      switch(j.state){
        case DONE:     return Response.done(this);
        case FAILED:  return Response.error(j.exception);
        case CANCELLED:return Response.error("Job was cancelled by user!");
        case RUNNING:  return Response.poll(this, (int) (100 * j.progress()), 100, "grid_key", grid_key.toString());
        default: break;
      }
    }
    return Response.poll(this, 0, 100, "grid_key", grid_key.toString());
  }
}




