package water.api;

import hex.KMeans.*;
import water.Key;
import water.Model;
import water.ValueArray;
import water.util.RString;

import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

/**
 * Simple web page to trigger kmeans validation on another dataset.
 * The dataset must contain the same columns (NOTE:identified by names) or error is returned.
 *
 * @author cliffc
 *
 */
public class KMeansScore extends Request {
  protected final H2OKMeansModelKey _modelKey = new H2OKMeansModelKey(MODEL_KEY,true);
  protected final H2OHexKey _dataKey = new H2OHexKey(KEY);

  @Override
  protected void queryArgumentValueSet(water.api.RequestArguments.Argument arg, java.util.Properties inputArgs) {
    if( arg == _dataKey ) {     // Check for dataset compatibility
      ValueArray va = _dataKey.value();
      KMeansModel model = _modelKey.value();
      int colIds[] = model.columnMapping(va.colNames());
      if( !Model.isCompatible(colIds) ) {
        for( int i=0; i<colIds.length; i++ )
          if( colIds[i] == -1 )
            throw new IllegalArgumentException("Incompatible dataset: "+va._key+" does not have column '"+model._va._cols[i]._name+"'");
      }
    }
  };

  public static String link(Key k, String content) {
    RString rs = new RString("<a href='KMeansScore.query?%key_param=%$key'>%content</a>");
    rs.replace("key_param", MODEL_KEY);
    rs.replace("key", k.toString());
    rs.replace("content", content);
    return rs.toString();
  }

  static class KMeansScoreBuilder extends ObjectBuilder {
    KMeansScoreBuilder( double mean_err[], long rows[] ) { }
    public String build(Response response, JsonObject json, String contextName) {
      StringBuilder sb = new StringBuilder();
      sb.append("hey some kmeans scoring display here");
      //GLMBuilder.validationHTML(_val,sb);
      return sb.toString();
    }
  }

  @Override protected Response serve() {
    try {
      JsonObject res = new JsonObject();
      ValueArray ary = _dataKey.value();
      KMeansModel m = _modelKey.value();
      hex.KMeans.KMeansScore kms = hex.KMeans.KMeansScore.score(m,ary);
      res.add("score", new JsonPrimitive("some kmeans score should be here"));
      for( int i=0; i<kms._rows.length; i++ ) {
        System.err.println("Cluster "+i+" rows: "+kms._rows[i]+" norm mean dist:"+Math.sqrt(kms._dist[i]/kms._rows[i]));
      }
      // Display HTML setup
      Response r = Response.done(res);
      r.setBuilder("", new KMeansScoreBuilder(null,null));
      return r;
    } catch( Error e ) {
      return Response.error(e.getMessage());
    }
  }
}
