/**
 *
 */
package water.web;

import java.text.DateFormat;
import java.util.*;

import water.H2O;
import water.util.JStackCollectorTask;

import com.google.gson.*;

/**
 * @author michal
 *
 */
public class DebugJStackView extends H2OPage {

  @Override
  public JsonObject serverJson(Server server, Properties parms, String sessionID) throws PageError {
    JStackCollectorTask collector = new JStackCollectorTask();
    collector.invokeOnAllNodes();

    JsonObject result = new JsonObject();
    JsonArray nodes = new JsonArray();
    result.addProperty("node_name", H2O.SELF.toString());
    result.addProperty("cloud_name", H2O.NAME);
    result.addProperty("time", DateFormat.getInstance().format(new Date()));
    result.add("nodes", nodes);
    for (int i=0; i<collector.result.length; ++i) {
      JsonObject el = new JsonObject();
      el.addProperty("id", i);
      el.addProperty("node", H2O.CLOUD._memary[i].toString());
      el.addProperty("traces", collector.result[i]);
      nodes.add(el);
    }

    return result;
  }

  @Override
  protected String serveImpl(Server server, Properties args, String sessionID) throws PageError {
    JsonObject result = serverJson(server, args, sessionID);
    RString r = new RString(html());

    r.replace("cloud_name", result.get("cloud_name"));
    r.replace("node_name", result.get("node_name"));
    r.replace("time", result.get("time"));

    JsonArray jary = result.getAsJsonArray("nodes");
    int i = 0;
    for (JsonElement el : jary) {
      formatNodesTabs((JsonObject) el, r, i);
      i++;
    }
    i = 0;
    for (JsonElement el : jary) {
      formatNodesTraces((JsonObject) el, r, i);
      i++;
    }

    return r.toString();
  }

  private void formatNodesTabs(JsonObject el, RString response, int idx) {
    RString row = response.restartGroup("nodeTab");
    if (idx==0) row.replace("active", "active"); else row.replace("active", "");
    row.replace(el);
    row.append();
  }
  private void formatNodesTraces(JsonObject el, RString response, int idx) {
    RString row = response.restartGroup("nodeContent");
    if (idx==0) row.replace("active", " active"); else row.replace("active", "");
    row.replace(el);
    row.append();
  }

  @Override
  protected String[] additionalScripts() {
    return new String[] { "bootstrap/js/bootstrap.min.js" };
  }

  private String html() {
    return
        "<div class='alert alert-success'>"
         + "You are connected to cloud <strong>%cloud_name</strong> and node <strong>%node_name</strong>."
         + "</div>"
    	 + "<ul class='nav nav-tabs'>"
    	 + " <li class=''><a href='DebugView'>Keys</a></li>"
         + " <li class='active'><a href='DbgJStack'>JStack</a></li>\n"
         + "</ul>\n"
         + "<div class='alert alert-success'>"
         + "Nodes stack traces generated for cloud %cloud_name at %time from node %node_name."
         + "</div>"
         + "<div class='tabbable tabs-left'>\n"
         + " <ul class='nav nav-tabs' id='nodesTab'>\n"
         + "%nodeTab{"
         + "   <li class='%active'>\n"
         + "    <a href='#tab%id' data-toggle='tab'>%node</a>\n"
         + "   </li>\n"
         + "}"
         + " </ul>\n"
         + " <div class='tab-content' id='nodesTabContent'>\n"
         + "%nodeContent{"
         + "  <div class='tab-pane%active' id='tab%id'>\n"
         + "   <pre>%traces</pre>\n"
         + "  </div>\n"
         + "}"
         + "</div>\n"
         + "</div>\n"
         + "<script type='text/javascript'>"
         + "$(document).ready(function() {"
         + "  $('#nodesTab a').click(function(e) {"
         + "    e.preventDefault(); $(this).tab('show');"
         + "  });"
         + "});"
         + "</script>";
  }

}
