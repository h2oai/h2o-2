package water.api;

import hex.gbm.GBM.GBMModel;
import water.*;
import water.api.RequestBuilders.Response;

public class GBMModelView extends Request2 {

  public static Response redirect(Request req, Key modelKey) {
    return new Response(Response.Status.redirect, req, -1, -1, "GBMModelView", "_modelKey", modelKey);
  }
  @API(help="GBM Model Key", required=true, filter=GBMModelKeyFilter.class)
  Key _modelKey;
  static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  class GBMModelKeyFilter extends H2OKey { public GBMModelKeyFilter() { super("model_key",true); } }

  public static void generateHTML(GBMModel m, StringBuilder sb){
    DocGen.HTML.title(sb,"GBM Model");
    DocGen.HTML.section(sb,"Confusion Matrix");

    // Top row of CM
    DocGen.HTML.arrayHead(sb);
    sb.append("<tr class='warning'>");
    sb.append("<th>Actual / Predicted</th>"); // Row header
    for( int i=0; i<m.cm.length; i++ )
      sb.append("<th>").append(m.domain[i+m.ymin]).append("</th>");
    sb.append("<th>Error</th>");
    sb.append("</tr>");

    // Main CM Body
    long tsum=0, terr=0;                   // Total observations & errors
    for( int i=0; i<m.cm.length; i++ ) { // Actual loop
      sb.append("<tr>");
      sb.append("<th>").append(m.domain[i+m.ymin]).append("</th>");// Row header
      long sum=0, err=0;                     // Per-class observations & errors
      for( int j=0; j<m.cm[i].length; j++ ) { // Predicted loop
        sb.append(i==j ? "<td style='background-color:LightGreen'>":"<td>");
        sb.append(m.cm[i][j]).append("</td>");
        sum += m.cm[i][j];              // Per-class observations
        if( i != j ) err += m.cm[i][j]; // and errors
      }
      sb.append(String.format("<th>%5.3f = %d / %d</th>", (double)err/sum, err, sum));
      tsum += sum;  terr += err; // Bump totals
    }
    sb.append("</tr>");

    // Last row of CM
    sb.append("<tr>");
    sb.append("<th>Totals</th>");// Row header
    for( int j=0; j<m.cm.length; j++ ) { // Predicted loop
      long sum=0;
      for( int i=0; i<m.cm.length; i++ ) sum += m.cm[i][j];
      sb.append("<td>").append(sum).append("</td>");
    }
    sb.append(String.format("<th>%5.3f = %d / %d</th>", (double)terr/tsum, terr, tsum));
    sb.append("</tr>");
    DocGen.HTML.arrayTail(sb);
    DocGen.HTML.section(sb,"Error Rate by Tree");
    DocGen.HTML.arrayHead(sb);
    sb.append("<tr><th>Trees</th>");
    for( int i=0; i<m.errs.length; i++ )
      sb.append("<td>").append(i+1).append("</td>");
    sb.append("</tr>");
    sb.append("<tr><th class='warning'>Error Rate</th>");
    for( int i=0; i<m.errs.length; i++ )
      sb.append(String.format("<td>%5.3f</td>",m.errs[i]));
    sb.append("</tr>");
    DocGen.HTML.arrayTail(sb);
  }
  public boolean toHTML(StringBuilder sb){
    GBMModel m = DKV.get(_modelKey).get();
    generateHTML(m, sb);
    return true;
  }

  @Override protected Response serve() {
    return new Response(Response.Status.done,this,-1,-1,null);
  }
}
