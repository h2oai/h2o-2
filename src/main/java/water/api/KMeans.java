package water.api;

import hex.KMeans.KMeansModel;
import water.*;
import water.H2O.H2OCountedCompleter;
import water.util.RString;

import com.google.gson.*;

public class KMeans extends Request {
  protected final H2OHexKey          _source  = new H2OHexKey(SOURCE_KEY);
  protected final Int                _k       = new Int(K);
  protected final Real               _epsilon = new Real(EPSILON, 1e-6);
  protected final HexAllColumnSelect _columns = new HexAllColumnSelect(COLS, _source);
  protected final H2OKey             _dest    = new H2OKey(DEST_KEY, (Key) null);

  @Override
  protected Response serve() {
    final ValueArray va = _source.value();
    final Key source = va._key;
    final int k = _k.value();
    final double epsilon = _epsilon.value();
    final int[] cols = _columns.value();
    Key dest = _dest.value();

    if( dest == null ) {
      String n = source.toString();
      int dot = n.lastIndexOf('.');
      if( dot > 0 )
        n = n.substring(0, dot);
      dest = Key.make(n + Extensions.KMEANS);
    }

    final Job job = hex.KMeans.startJob(dest, va, k, epsilon, cols);
    try {
      H2O.submitTask(new H2OCountedCompleter() {
        @Override
        public void compute2() {
          hex.KMeans.run(job, va, k, epsilon, cols);
          tryComplete();
        }
      });

      JsonObject response = new JsonObject();
      response.addProperty(JOB, job.self().toString());
      response.addProperty(DEST_KEY, dest.toString());

      Response r = Progress.redirect(response, job.self(), dest);
      r.setBuilder(DEST_KEY, new KeyElementBuilder());
      return r;
    } catch( IllegalArgumentException e ) {
      return Response.error(e.getMessage());
    } catch( Error e ) {
      return Response.error(e.getMessage());
    }
  }

  // Make a link that lands on this page
  public static String link(Key k, String content) {
    RString rs = new RString("<a href='KMeans.query?%key_param=%$key'>%content</a>");
    rs.replace("key_param", SOURCE_KEY);
    rs.replace("key", k.toString());
    rs.replace("content", content);
    return rs.toString();
  }

  static class Builder extends ObjectBuilder {
    final KMeansModel _m;

    Builder(KMeansModel m) {
      _m = m;
    }

    public String build(Response response, JsonObject json, String contextName) {
      StringBuilder sb = new StringBuilder();
      modelHTML(_m, json, sb);
      return sb.toString();
    }

    private void modelHTML(KMeansModel m, JsonObject json, StringBuilder sb) {
      // sb.append("<div class='alert'>Actions: " + Plot.link(m._selfKey, "Plot");

      JsonArray rows = json.getAsJsonArray(CLUSTERS);
      JsonArray row0 = rows.get(0).getAsJsonArray();

      sb.append("<span style='display: inline-block;'>");
      sb.append("<table class='table table-striped table-bordered'>");
      sb.append("<tr>");
      sb.append("<th>Clusters</th>");
      for( int i = 0; i < row0.size(); i++ )
        sb.append("<th>").append(i).append("</th>");
      sb.append("</tr>");

      for( int r = 0; r < rows.size(); r++ ) {
        sb.append("<tr>");
        sb.append("<td>").append(r).append("</td>");
        for( int c = 0; c < row0.size(); c++ ) {
          JsonElement e = rows.get(r).getAsJsonArray().get(c);
          sb.append("<td>").append(ElementBuilder.format(e.getAsDouble())).append("</td>");
        }
        sb.append("</tr>");
      }
      sb.append("</table></span>");
    }
  }
}
