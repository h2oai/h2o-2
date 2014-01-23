package water.api;

import java.text.DateFormat;
import java.util.Date;
import water.H2O;
import water.Iced;
import water.util.Log;
import water.util.JStackCollectorTask;

public class JStack extends Request {
  static final int API_WEAVER=1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  // This Request supports the HTML 'GET' command, and this is the help text
  // for GET.
  static final String DOC_GET = "Displays stack dumps from all nodes.";

  @API(help="This node's name")
  public String node_name;

  @API(help="The cloud's name")
  public String cloud_name;

  @API(help="Current time")
  public String time;

  public static class StackSummary extends Iced {
    static final int API_WEAVER=1; // This file has auto-gen'd doc & json fields
    static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.
    public StackSummary( String name, String traces ) { this.name  = name; this.traces= traces; }
    @API(help="Node name")    final String name;
    @API(help="Stack traces") final String traces;
  }
  @API(help="Array of Stack Traces, one per Node in the Cluster")
  public StackSummary nodes[];

  @Override public Response serve() {
    String traces[] = new JStackCollectorTask().invokeOnAllNodes()._result;
    nodes = new StackSummary[H2O.CLOUD.size()];
    for( int i=0; i<nodes.length; i++ )
      nodes[i] = new StackSummary(H2O.CLOUD._memary[i].toString(),traces[i]);
    node_name = H2O.SELF.toString();
    cloud_name = H2O.NAME;
    time = DateFormat.getInstance().format(new Date());
    for( int i=0; i<nodes.length; i++ )
      Log.debug(Log.Tag.Sys.WATER,nodes[i].name,nodes[i].traces);
    return Response.done(this);
  }

  @Override public boolean toHTML( StringBuilder sb ) {
    // build tab list
    sb.append("<div class='tabbable tabs-left'>\n");
    sb.append(" <ul class='nav nav-tabs' id='nodesTab'>\n");
    for( int i = 0; i < nodes.length; ++i ) {
      sb.append("<li class='").append(i == 0 ? "active" : "").append("'>\n");
      sb.append("<a href='#tab").append(i).append("' data-toggle='tab'>");
      sb.append(nodes[i].name).append("</a>\n");
      sb.append("</li>");
    }
    sb.append("</ul>\n");

    // build the tab contents
    sb.append(" <div class='tab-content' id='nodesTabContent'>\n");
    for( int i = 0; i < nodes.length; ++i ) {
      sb.append("<div class='tab-pane").append(i == 0 ? " active": "").append("' ");
      sb.append("id='tab").append(i).append("'>\n");
      sb.append("<pre>").append(nodes[i].traces).append("</pre>");
      sb.append("</div>");
    }
    sb.append("  </div>");
    sb.append("</div>");

    sb.append("<script type='text/javascript'>" +
              "$(document).ready(function() {" +
              "  $('#nodesTab a').click(function(e) {" +
              "    e.preventDefault(); $(this).tab('show');" +
              "  });" +
              "});" +
              "</script>");
    return true;
  }
}
