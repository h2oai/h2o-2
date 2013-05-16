package water.api;
import hex.ColSummaryTask;
import java.util.Iterator;
import water.ValueArray;
import water.util.Utils;

import com.google.gson.*;

public class SummaryPage extends Request {
  protected final H2OHexKey _key = new H2OHexKey(KEY);
  protected final HexColumnSelect _columns = new HexColumnSelect(X, _key);

  @Override protected Response serve() {
    int [] cols = _columns.value();
    ValueArray ary = _key.value();
    ColSummaryTask sum = new ColSummaryTask(ary,cols);
    sum.invoke(ary._key);
    JsonObject res = new JsonObject();
    res.add("summary", sum.result().toJson());
    Response r = Response.done(res);
    r.setBuilder("summary.columns", new Builder() {
      @Override public String build(Response response, JsonElement element, String contextName) {
        StringBuilder sb = new StringBuilder();
        JsonArray cols = element.getAsJsonArray();
        Iterator<JsonElement> it = cols.iterator();
        while(it.hasNext()){
          JsonObject o = it.next().getAsJsonObject();
          String cname = o.get("name").getAsString();
          sb.append("<div class='table' id='col_" + cname + "' style='border-top-style:solid;'><h4>Column: " + cname + "</h4>\n");
          if(o.has("min") && o.has("max")){
            sb.append("<h5>Base Stats</h5><table>");
            StringBuilder minRow = new StringBuilder("<tr><th>min[5]</th>");
            StringBuilder maxRow = new StringBuilder("<tr><th>max[5]</th>");
            Iterator<JsonElement> iter = o.get("min").getAsJsonArray().iterator();
            while(iter.hasNext())minRow.append("<td>" + Utils.p2d(iter.next().getAsDouble()) + "</td>");
            minRow.append("</tr>");
            iter = o.get("max").getAsJsonArray().iterator();
            while(iter.hasNext())maxRow.append("<td>" + Utils.p2d(iter.next().getAsDouble()) + "</td>");
            maxRow.append("</tr>");
            sb.append("<table>");
            sb.append(minRow.toString());
            sb.append(maxRow.toString());
            sb.append("<tr><th>&mu;</th><td>" + Utils.p2d(o.get("mean").getAsDouble()) + "</td><td></td><td></td><td></td><td></td>" + "</tr><tr><th>&sigma;</th><td>" + Utils.p2d(o.get("sigma").getAsDouble()) + "</td><td></td><td></td><td></td><td></td></tr>");
          }
          sb.append("</table>");
          if(o.has("percentiles")){
            JsonObject percentiles = o.get("percentiles").getAsJsonObject();
            JsonArray thresholds = percentiles.get("thresholds").getAsJsonArray();
            JsonArray values = percentiles.get("values").getAsJsonArray();
            Iterator<JsonElement> tIter = thresholds.iterator();
            Iterator<JsonElement> vIter = values.iterator();
            StringBuilder t = new StringBuilder("<tr><th>Threshold</th>");
            StringBuilder v = new StringBuilder("<tr><th>Value</th>");
            while(tIter.hasNext() && vIter.hasNext()){
              t.append("<td>" + tIter.next().getAsString() + "</td>");
              v.append("<td>" + Utils.p2d(vIter.next().getAsDouble()) + "</td>");
            }
            t.append("</tr>");
            v.append("</tr>");
            sb.append("<h5>Percentiles</h5><table>" + t.toString() + v.toString() + "</table>");
          }
          sb.append("<h5>Histogram</h5>");
          JsonObject histo = o.get("histogram").getAsJsonObject();
          JsonArray bins = histo.get("bins").getAsJsonArray();
          JsonArray names = histo.get("bin_names").getAsJsonArray();
          Iterator<JsonElement> bIter = bins.iterator();
          Iterator<JsonElement> nIter = names.iterator();
          StringBuilder n = new StringBuilder("<tr>");
          StringBuilder b = new StringBuilder("<tr>");
          while(bIter.hasNext() && nIter.hasNext()){
            n.append("<td>" + nIter.next().getAsString() + "</td>");
            b.append("<td>" + bIter.next().getAsLong() + "</td>");
          }
          n.append("</tr>");
          b.append("</tr>");
          sb.append("<table>" + n.toString() + b.toString() + "</table>");
          sb.append("\n</div>\n");
        }
        return sb.toString();
      }
    });
    return r;
  }

}
