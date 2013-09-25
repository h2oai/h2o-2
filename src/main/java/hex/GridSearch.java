package hex;

import hex.gbm.GBM.GBMModel;

import java.util.ArrayList;

import water.*;
import water.api.*;

public class GridSearch extends Progress2 {

  @API(help = "Jobs launched by the grid search")
  public Job[] jobs;

  @Override public boolean toHTML(StringBuilder sb) {
    DocGen.HTML.arrayHead(sb);
    sb.append("<tr class='warning'>");
    ArrayList<Argument> args = jobs[0].arguments();
    for( int i = 0; i < args.size(); i++ )
      sb.append("<td><b>").append(args.get(i)._name).append("</b></td>");
    sb.append("<td><b>").append("run time (s)").append("</b></td>");
    sb.append("<td><b>").append("model key").append("</b></td>");
    sb.append("<td><b>").append("prediction error %").append("</b></td>");
    sb.append("<td><b>").append("precision & recall").append("</b></td>");
    sb.append("</tr>");

    for( int j = 0; j < jobs.length; j++ ) {
      sb.append("<tr>");
      for( Argument a : args ) {
        try {
          sb.append("<td><b>").append(a._field.get(jobs[j])).append("</b></td>");
        } catch( Exception e ) {
          throw new RuntimeException(e);
        }
      }
      sb.append("<td><b>").append((System.currentTimeMillis() - jobs[j].start_time) / 1000).append("</b></td>");

      Object value = UKV.get(jobs[j].destination_key);
      Model model = value instanceof Model ? (Model) value : null;
      String link = jobs[j].destination_key.toString();
      if( model instanceof GBMModel )
        link = GBMModelView.link(link, jobs[j].destination_key);
      sb.append("<td><b>").append(link).append("</b></td>");

      String pct = "", f1 = "";
      if( model != null ) {
        pct = String.format("%.2f", 100 * model.predictionError()) + "%";
        ConfusionMatrix cm = model.cm();
        if( cm != null && cm._arr.length == 2 )
          f1 = String.format("%.2f", cm.precisionAndRecall());
      }
      sb.append("<td><b>").append(pct).append("</b></td>");
      sb.append("<td><b>").append(f1).append("</b></td>");
      sb.append("</tr>");
    }
    DocGen.HTML.arrayTail(sb);
    return true;
  }

  @Override protected Response jobDone(final Job job, final String dst) {
    return new Response(Response.Status.done, this, 0, 0, null);
  }
}
