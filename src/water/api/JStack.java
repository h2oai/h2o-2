
package water.api;

import java.text.DateFormat;
import java.util.Date;

import water.H2O;
import water.util.JStackCollectorTask;

import com.google.gson.*;

public class JStack extends Request {

  public JStack() {
    _requestHelp = "Displays stack dumps from all nodes.";
  }

  @Override public Response serve() {
    JStackCollectorTask collector = new JStackCollectorTask();
    collector.invokeOnAllNodes();

    JsonObject json = new JsonObject();

    json.addProperty(NODE_NAME, H2O.SELF.toString());
    json.addProperty(CLOUD_NAME, H2O.NAME);
    json.addProperty(TIME, DateFormat.getInstance().format(new Date()));

    JsonArray nodes = new JsonArray();
    for (int i=0; i<collector.result.length; ++i) {
      JsonObject el = new JsonObject();
      el.addProperty(NODE, H2O.CLOUD._memary[i].toString());
      el.addProperty(STACK_TRACES, collector.result[i]);
      nodes.add(el);
    }
    json.add(NODES, nodes);


    Response r = Response.done(json);
    r.setBuilder(NODES, new NodeTableBuilder());
    return r;
  }

  private static class NodeTableBuilder extends ArrayBuilder {
    @Override public String build(Response r, JsonArray arr, String name) {
      StringBuilder sb = new StringBuilder();

      // build tab list
      sb.append("<div class='tabbable tabs-left'>\n");
      sb.append(" <ul class='nav nav-tabs' id='nodesTab'>\n");
      for( int i = 0; i < arr.size(); ++i ) {
        JsonObject o = (JsonObject)arr.get(i);
        sb.append("<li class='").append(i == 0 ? "active" : "").append("'>\n");
        sb.append("<a href='#tab").append(i).append("' data-toggle='tab'>");
        sb.append(o.get(NODE).getAsString());
        sb.append("</a>\n");
        sb.append("</li>");
      }
      sb.append("</ul>\n");

      // build the tab contents
      sb.append(" <div class='tab-content' id='nodesTabContent'>\n");
      for( int i = 0; i < arr.size(); ++i ) {
        JsonObject o = (JsonObject)arr.get(i);
        sb.append("<div class='tab-pane").append(i == 0 ? " active": "").append("' ");
        sb.append("id='tab").append(i).append("'>\n");

        sb.append("<pre>").append(o.get(STACK_TRACES).getAsString()).append("</pre>");
        sb.append("</div>");
      }
      sb.append("  </div>");
      sb.append("</div>");

      sb.append(""
          + "<script type='text/javascript'>"
          + "$(document).ready(function() {"
          + "  $('#nodesTab a').click(function(e) {"
          + "    e.preventDefault(); $(this).tab('show');"
          + "  });"
          + "});"
          + "</script>");
      return sb.toString();
    }
  }
}
