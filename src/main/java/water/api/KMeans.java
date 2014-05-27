package water.api;

import hex.KMeans.Initialization;
import hex.KMeansModel;

import java.util.Random;

import water.Key;
import water.ValueArray;
import water.util.RString;

import com.google.gson.*;

public class KMeans extends Request {
  static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.
  static final String DOC_GET = "K-means algorithm";

  @API(help = "Key for input dataset")
  final H2OHexKey source_key = new H2OHexKey("source_key");

  @API(help = "Number of clusters")
  final Int k = new Int("k");

  @API(help = "Clusters initialization")
  final EnumArgument<Initialization> initialization = new EnumArgument<Initialization>("initialization",
      Initialization.None);

  @API(help = "Maximum number of iterations before stopping")
  final Int max_iter = new Int("max_iter", 100);

  @API(help = "Seed for the random number generator")
  final LongInt seed = new LongInt("seed", new Random().nextLong(), "");

  @API(help = "Whether data should be normalized")
  final Bool normalize = new Bool("normalize", false, "");

  @API(help = "Columns to use as input")
  final HexAllColumnSelect cols = new HexAllColumnSelect("cols", source_key);

  @API(help = "Destination key")
  final H2OKey destination_key = new H2OKey("destination_key", hex.KMeans.makeKey());

  @Override protected Response serve() {
    try {
      if( destination_key.record()._originalValue != null && destination_key.record()._originalValue.equals(source_key.record()._originalValue) )
        throw new IllegalArgumentException("destination_key cannot be source_key");
      hex.KMeans job = start(destination_key.value(), k.value(), max_iter.value());
      JsonObject response = new JsonObject();
      response.addProperty(JOB, job.self().toString());
      response.addProperty(DEST_KEY, job.dest().toString());

      Response r = Progress.redirect(response, job.self(), job.dest());
      r.setBuilder(DEST_KEY, new KeyElementBuilder());
      return r;
    } catch( Throwable e ) {
      return Response.error(e);
    }
  }

  final hex.KMeans start(Key dest, int k, int maxIter) {
    ValueArray va = source_key.value();
    Key source = va._key;
    Initialization init = initialization.value();
    long seed_ = seed.record()._valid ? seed.value() : seed._defaultValue;
    boolean norm = normalize.record()._valid ? normalize.value() : normalize._defaultValue;
    int[] columns = cols.value();

    if( dest == null ) {
      String n = source.toString();
      int dot = n.lastIndexOf('.');
      if( dot > 0 )
        n = n.substring(0, dot);
      dest = Key.make(n + Extensions.KMEANS);
    }

    return hex.KMeans.start(dest, va, k, init, maxIter, seed_, norm, columns);
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

    @Override public String build(Response response, JsonObject json, String contextName) {
      StringBuilder sb = new StringBuilder();
      modelHTML(_m, json, sb);
      return sb.toString();
    }

    private void modelHTML(KMeansModel m, JsonObject json, StringBuilder sb) {
      JsonArray rows = json.getAsJsonArray(CLUSTERS);

      sb.append("<div class='alert'>Actions: " + KMeansScore.link(m._key, "Score on dataset") + ", "
          + KMeansApply.link(m._key, "Apply to dataset") + ", " + KMeans.link(m._dataKey, "Compute new model")
          + "</div>");
      DocGen.HTML.section(sb, "Error: " + _m._error);
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
