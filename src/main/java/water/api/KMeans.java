package water.api;

import hex.KMeansModel;

import java.util.Random;

import water.Key;
import water.ValueArray;
import water.util.RString;

import com.google.gson.*;

public class KMeans extends Request {
  protected final H2OHexKey _source = new H2OHexKey(SOURCE_KEY);
  protected final Int _k = new Int(K);
  protected final Real _epsilon = new Real(EPSILON, 1e-4);
  protected final LongInt _seed = new LongInt(SEED, new Random().nextLong(), "");
  protected final Bool _normalize = new Bool(NORMALIZE, false, "");
  protected final HexAllColumnSelect _columns = new HexAllColumnSelect(COLS, _source);
  protected final H2OKey _dest = new H2OKey(DEST_KEY, hex.KMeans.makeKey());

  @Override protected Response serve() {
    ValueArray va = _source.value();
    Key source = va._key;
    int k = _k.value();
    double epsilon = _epsilon.value();
    long seed = _seed.record()._valid ? _seed.value() : _seed._defaultValue;
    boolean normalize = _normalize.record()._valid ? _normalize.value() : _normalize._defaultValue;
    int[] cols = _columns.value();
    Key dest = _dest.value();

    if( dest == null ) {
      String n = source.toString();
      int dot = n.lastIndexOf('.');
      if( dot > 0 ) n = n.substring(0, dot);
      dest = Key.make(n + Extensions.KMEANS);
    }

    try {
      hex.KMeans job = hex.KMeans.start(dest, va, k, epsilon, seed, normalize, cols);
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
      JsonArray rows = json.getAsJsonArray(CLUSTERS);

      sb.append("<div class='alert'>Actions: " + KMeansScore.link(m._selfKey, "Score on dataset") + ", "
          + KMeansApply.link(m._selfKey, "Apply to dataset") + ", " + KMeans.link(m._dataKey, "Compute new model")
          + "</div>");
      sb.append("<span style='display: inline-block;'>");
      sb.append("<table class='table table-striped table-bordered'>");
      sb.append("<tr>");
      sb.append("<th>Clusters</th>");
      for( int i = 0; i < m._va._cols.length - 1; i++ )
        sb.append("<th>").append(m._va._cols[i]._name).append("</th>");
      sb.append("</tr>");

      for( int r = 0; r < rows.size(); r++ ) {
        sb.append("<tr>");
        sb.append("<td>").append(r).append("</td>");
        for( int c = 0; c < m._va._cols.length - 1; c++ ) {
          JsonElement e = rows.get(r).getAsJsonArray().get(c);
          sb.append("<td>").append(ElementBuilder.format(e.getAsDouble())).append("</td>");
        }
        sb.append("</tr>");
      }
      sb.append("</table></span>");
    }
  }
}
