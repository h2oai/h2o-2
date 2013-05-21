package water.api;

import hex.ColSummaryTask;

import java.util.Iterator;

import water.Key;
import water.ValueArray;
import water.util.Utils;

import com.google.gson.*;

public class SummaryPage extends Request {
  protected final H2OHexKey _key = new H2OHexKey(KEY);
  protected final HexColumnSelect _columns = new HexColumnSelect(X, _key);

  public static String link(Key k, String s){
    return "<a href='SummaryPage.html?key="+k+"'>" + s + "</a>";
  }

  public final static int MAX_HISTO_BINS_DISPLAYED = 1000;
  @Override protected Response serve() {
    int [] cols = _columns.value();
    ValueArray ary = _key.value();
    if(cols.length == 0){
      cols = new int[ary._cols.length];
      for(int i = 0; i < ary._cols.length; ++i) cols[i] = i;
    }
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
        sb.append("<div class='table' style='height:90%;overflow-y:scroll'>");
        while(it.hasNext()){
          JsonObject o = it.next().getAsJsonObject();
          String cname = o.get("name").getAsString();
          long N = o.get("N").getAsLong();
          sb.append("<div class='table'  id='col_" + cname + "' style='width:100%;heigth:90%;overflow-y:scroll;border-top-style:solid;'><h4>Column: " + cname + "</h4>\n");
          if(o.has("min") && o.has("max")){
            StringBuilder minRow = new StringBuilder("<tr><th>&mu;</th><td>" + Utils.p2d(o.get("mean").getAsDouble())+"</td><th style='border-left-style:solid; borde-left:1px;border-left-color:#ddd;'>min[5]</th>");
            StringBuilder maxRow = new StringBuilder("<tr><th>&sigma;</th><td>" + Utils.p2d(o.get("sigma").getAsDouble()) + "</td><th style='border-left-style:solid; borde-left:1px;border-left-color:#ddd;'>max[5]</th>");
            Iterator<JsonElement> iter = o.get("min").getAsJsonArray().iterator();
            int nCols = 3;
            while(iter.hasNext()){
              ++nCols;
              minRow.append("<td>" + Utils.p2d(iter.next().getAsDouble()) + "</td>");
            }
            iter = o.get("max").getAsJsonArray().iterator();
            while(iter.hasNext())maxRow.append("<td>" + Utils.p2d(iter.next().getAsDouble()) + "</td>");
            StringBuilder firstRow = new StringBuilder("<tr><th colspan='" + nCols + "' style='text-align:center;'>Base Stats</th>");
            if(o.has("percentiles")){
              firstRow.append("<th colspan='12' style='text-align:center;border-left-style:solid; borde-left:1px;border-left-color:#ddd;'>Percentiles</th>");
              JsonObject percentiles = o.get("percentiles").getAsJsonObject();
              JsonArray thresholds = percentiles.get("thresholds").getAsJsonArray();
              JsonArray values = percentiles.get("values").getAsJsonArray();
              Iterator<JsonElement> tIter = thresholds.iterator();
              Iterator<JsonElement> vIter = values.iterator();
              minRow.append("<th style='border-left-style:solid; borde-left:1px;border-left-color:#ddd;'>Threshold</th>");
              maxRow.append("<th style='border-left-style:solid; borde-left:1px;border-left-color:#ddd;'>Value</th>");
              while(tIter.hasNext() && vIter.hasNext()){
                minRow.append("<td>" + tIter.next().getAsString() + "</td>");
                maxRow.append("<td>" + Utils.p2d(vIter.next().getAsDouble()) + "</td>");
              }
            }
            firstRow.append("</tr>");
            minRow.append("</tr>");
            maxRow.append("</tr>");
            sb.append("<div style='width:100%;overflow:scroll;'><table>");
            sb.append(firstRow.toString());
            sb.append(minRow.toString());
            sb.append(maxRow.toString());
            sb.append("</table></div>");
          }
          sb.append("<h5>Histogram</h5>");
          JsonObject histo = o.get("histogram").getAsJsonObject();
          JsonArray bins = histo.get("bins").getAsJsonArray();
          JsonArray names = histo.get("bin_names").getAsJsonArray();
          Iterator<JsonElement> bIter = bins.iterator();
          Iterator<JsonElement> nIter = names.iterator();
          StringBuilder n = new StringBuilder("<tr>");
          StringBuilder b = new StringBuilder("<tr>");
          StringBuilder p = new StringBuilder("<tr>");
          int i = 0;
          while(bIter.hasNext() && nIter.hasNext() && i++ < MAX_HISTO_BINS_DISPLAYED){
            n.append("<td>" + nIter.next().getAsString() + "</td>");
            long cnt = bIter.next().getAsLong();
            b.append("<td>" + cnt + "</td>");
            p.append(String.format("<td>%.1f%%</td>",(100.0*cnt/N)));
          }
          if(i >= MAX_HISTO_BINS_DISPLAYED && bIter.hasNext())
            sb.append("<div class='alert'>Histogram for this column was too big and was truncated to 1000 values!</div>");
          n.append("</tr>\n");
          b.append("</tr>\n");
          p.append("</tr>\n");
          sb.append("<div style='width:100%;overflow:scroll;'><table>" + n.toString() + b.toString() + p.toString() + "</table></div>");
          sb.append("\n</div>\n");
        }
        sb.append("</div>");
        return sb.toString();
      }
    });
    return r;
  }

}
