package water.api;

import hex.KMeansModel;
import water.Key;
import water.util.RString;

import com.google.gson.*;

public class KMeans extends KMeansShared {
  static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  // This Request supports the HTML 'GET' command, and this is the help text
  // for GET.
  static final String DOC_GET = "K-means algorithm";

  @API(help = "Number of clusters")
  final Int k = new Int("k");

  @API(help = "Maximum number of iterations before stopping")
  final Int max_iter = new Int("max_iter", 0);

  public KMeans() {
    // Reorder arguments
    _arguments.remove(k);
    _arguments.add(1, k);
    _arguments.remove(max_iter);
    _arguments.add(2, max_iter);
  }

  @Override protected Response serve() {
    try {
      hex.KMeans job = start(destination_key.value(), k.value(), max_iter.value());
      JsonObject response = new JsonObject();
      response.addProperty(JOB, job.self().toString());
      response.addProperty(DEST_KEY, job.dest().toString());

      Response r = Progress.redirect(response, job.self(), job.dest());
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
