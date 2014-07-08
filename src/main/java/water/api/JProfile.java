package water.api;

import water.Func;
import water.H2O;
import water.Iced;
import water.Request2;
import water.fvec.Frame;
import water.fvec.Vec;
import water.util.Log;
import water.util.ProfileCollectorTask;

import java.text.DateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.Map;
import java.util.TreeMap;

public class JProfile extends Func {
  static final int API_WEAVER=1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  // This Request supports the HTML 'GET' command, and this is the help text
  // for GET.
  static final String DOC_GET = "Displays profile dumps from all nodes.";

  @API(help="Stack trace depth", required=true, filter=Default.class, json=true)
  public int depth = 5;

  @API(help="This node's name")
  public String node_name;

  @API(help="The cloud's name")
  public String cloud_name;

  @API(help="Current time")
  public String time;


  public static class ProfileSummary extends Iced {
    static final int API_WEAVER=1; // This file has auto-gen'd doc & json fields
    static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.
    public ProfileSummary( String name, ProfileCollectorTask.NodeProfile profile) { this.name  = name; this.profile= profile; }
    @API(help="Node name")    final String name;
    @API(help="Profile") final ProfileCollectorTask.NodeProfile profile;
  }
  @API(help="Array of Profiles, one per Node in the Cluster")
  public ProfileSummary nodes[];

  @Override public void execImpl() {
    ProfileCollectorTask.NodeProfile profiles[] = new ProfileCollectorTask(depth).invokeOnAllNodes()._result;
    nodes = new ProfileSummary[H2O.CLOUD.size()];
    for( int i=0; i<nodes.length; i++ )
      nodes[i] = new ProfileSummary(H2O.CLOUD._memary[i].toString(),profiles[i]);
    node_name = H2O.SELF.toString();
    cloud_name = H2O.NAME;
    time = DateFormat.getInstance().format(new Date());
    for( int i=0; i<nodes.length; i++ )
      Log.debug(Log.Tag.Sys.WATER,nodes[i].name,nodes[i].profile);
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
      for (int j=0; j<nodes[i].profile.counts.length; ++j) {
        sb.append("<pre>").append(nodes[i].profile.counts[j]).append("\n").append(nodes[i].profile.stacktraces[j]).append("</pre>");
      }
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
