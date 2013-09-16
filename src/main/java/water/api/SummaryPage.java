package water.api;

import hex.ColSummaryTask;

import java.util.Iterator;

import water.Key;
import water.ValueArray;
import water.util.Utils;

import com.google.gson.*;

public class SummaryPage extends Request {
  protected final H2OHexKey _key = new H2OHexKey(KEY);
  protected final HexColumnSelect _columns = new HexColumnSelect(X, _key, 2500);
  protected final Int             _max_column   = new Int(COLUMNS_DISPLAY, MAX_COLUMNS_TO_DISPLAY);

  static final int MAX_COLUMNS_TO_DISPLAY = 1000;

  public static String link(Key k, String s){
    return "<a href='SummaryPage.html?key="+k+"'>" + s + "</a>";
  }

  public final static int MAX_HISTO_BINS_DISPLAYED = 1000;

  @Override protected Response serve() {
    int [] cols = _columns.value();
    ValueArray ary = _key.value();

    final boolean did_trim_columns;
    final int max_columns_to_display;
    if (_max_column.value() >= 0)
      max_columns_to_display = Math.min(_max_column.value(), cols.length == 0 ? ary._cols.length : cols.length);
    else
      max_columns_to_display = Math.min(MAX_COLUMNS_TO_DISPLAY, cols.length == 0 ? ary._cols.length : cols.length);

    if(cols.length == 0){
      did_trim_columns = ary._cols.length > max_columns_to_display;
      cols = new int[ Math.min(ary._cols.length, max_columns_to_display) ];
      for(int i = 0; i < cols.length; ++i) cols[i] = i;
    } else if (cols.length > max_columns_to_display){
      int [] cols2 = new int[ max_columns_to_display ];
      for (int j=0; j < max_columns_to_display; j++) cols2[ j ] = cols[ j ];
      cols = cols2;
      did_trim_columns = true;
    } else {
      did_trim_columns = false;
    }



    ColSummaryTask sum = new ColSummaryTask(ary,cols);
    sum.invoke(ary._key);
    JsonObject res = new JsonObject();
    res.add("summary", sum.result().toJson());
    Response r = Response.done(res);
    r.setBuilder(ROOT_OBJECT, new Builder() {
      @Override public String build(Response response, JsonElement element, String contextName) {
        StringBuilder pageBldr = new StringBuilder("<div class=container-fluid'><div class='row-fluid'><div class='span2' style='overflow-y:scroll;height:100%;left:0;position:fixed;text-align:right;overflow-x:scroll;'><h5>Columns</h5>");
        StringBuilder sb = new StringBuilder("<div class='span10' style='float:right;height:90%;overflow-y:scroll'>");
        JsonArray cols = element.getAsJsonObject().get("summary").getAsJsonObject().get("columns").getAsJsonArray();
        Iterator<JsonElement> it = cols.iterator();

        if (did_trim_columns )
          sb.append("<p style='text-align:center;'><center><h4 style='font-weight:800; color:red;'>Columns trimmed to " + max_columns_to_display + "</h4></center></p>");


        while(it.hasNext()){
          JsonObject o = it.next().getAsJsonObject();
          String cname = o.get("name").getAsString();
          pageBldr.append("<div><a href='#col_" + cname + "'>" + cname + "</a></div>");
          long N = o.get("N").getAsLong();
          sb.append("<div class='table' id='col_" + cname + "' style='width:90%;heigth:90%;overflow-y:scroll;border-top-style:solid;'><div class='alert-success'><h4>Column: " + cname + "</h4></div>\n");
          // !enum
          if(o.has("min") && o.has("max")){
            StringBuilder baseStats = new StringBuilder("<div style='width:100%;overflow:scroll;'><table class='table-bordered'>");
            baseStats.append("<tr><th colspan='" + 100 + "' style='text-align:center;'>Base Stats</th></tr>");

            baseStats.append("<th>avg</th><td>" + Utils.p2d(o.get("mean").getAsDouble())+"</td>");
            baseStats.append("<th>sd</th><td>" + Utils.p2d(o.get("sigma").getAsDouble()) + "</td>");

            baseStats.append("<th>NAs</th>  <td>" + o.get("na").getAsLong() + "</td>");
            baseStats.append("<th>zeros</th>");
            baseStats.append("<td>" + o.get("zeros").getAsLong() + "</td>");

            StringBuilder minmax = new StringBuilder();
            int min_count = 0;
            Iterator<JsonElement> iter = o.get("min").getAsJsonArray().iterator();
            while(iter.hasNext()){
              min_count++;
              minmax.append("<td>" + Utils.p2d(iter.next().getAsDouble()) + "</td>");
            }
            baseStats.append("<th>min[" + min_count + "]</th>");
            baseStats.append(minmax.toString());

            baseStats.append("<th>max[" + min_count + "]</th>");
            iter = o.get("max").getAsJsonArray().iterator();
            while(iter.hasNext()) baseStats.append("<td>" + Utils.p2d(iter.next().getAsDouble()) + "</td>");
            baseStats.append("</tr> </table>");
            baseStats.append("</div>");

            sb.append( baseStats.toString());

            StringBuilder threshold = new StringBuilder();
            StringBuilder value = new StringBuilder();
            if(o.has("percentiles")){
              JsonObject percentiles = o.get("percentiles").getAsJsonObject();
              JsonArray thresholds = percentiles.get("thresholds").getAsJsonArray();
              JsonArray values = percentiles.get("values").getAsJsonArray();
              Iterator<JsonElement> tIter = thresholds.iterator();
              Iterator<JsonElement> vIter = values.iterator();

              threshold.append("<tr><th>Threshold</th>");
              value.append("<tr><th>Value</th>");
              while(tIter.hasNext() && vIter.hasNext()){
                threshold.append("<td>" + tIter.next().getAsString() + "</td>");
                value.append("<td>" + Utils.p2d(vIter.next().getAsDouble()) + "</td>");
              }
              threshold.append("</tr>");
              value.append("</tr>");

              sb.append("<div style='width:100%;overflow:scroll;'><table class='table-bordered'>");
              sb.append("<th colspan='12' style='text-align:center;'>Percentiles</th>");
              sb.append(threshold.toString());
              sb.append(value.toString());
              sb.append("</table>");
              sb.append("</div>");
            }

          } else {
            // this should be the _enum case, in which I want to report NA count
            sb.append("<div style='width:100%;overflow:scroll;'><table class='table-bordered'>");
            sb.append("<tr><th colspan='" + 4 + "' style='text-align:center;'>Base Stats</th></tr>");
            // na row
            sb.append("<tr><th>NAs</th>  <td>" + o.get("na").getAsLong() + "</td>");
            sb.append("<th>cardinality</th>  <td>" + o.get("enumCardinality").getAsLong() + "</td></tr>");
            sb.append("</table></div>");
          }
          // sb.append("<h5>Histogram</h5>");
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
            n.append("<th>" + nIter.next().getAsString() + "</th>");
            long cnt = bIter.next().getAsLong();
            b.append("<td>" + cnt + "</td>");
            p.append(String.format("<td>%.1f%%</td>",(100.0*cnt/N)));
          }
          if(i >= MAX_HISTO_BINS_DISPLAYED && bIter.hasNext())
            sb.append("<div class='alert'>Histogram for this column was too big and was truncated to 1000 values!</div>");
          n.append("</tr>\n");
          b.append("</tr>\n");
          p.append("</tr>\n");
          sb.append("<div style='width:100%;overflow:scroll;'><table class='table-bordered'>");
          sb.append("<thead> <th colspan=" + (MAX_HISTO_BINS_DISPLAYED + 1) + " style='text-align:center;'>Histogram </th> </thead>");
          sb.append(n.toString() + b.toString() + p.toString() + "</table></div>");
          sb.append("\n</div>\n");
        }
        sb.append("</div>");
        pageBldr.append("</div>");
        pageBldr.append(sb);
        pageBldr.append("</div>");

        return pageBldr.toString();
      }
    });
    return r;
  }

}
