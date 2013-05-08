package water.api;

import hex.KMeans.KMeansModel;
import water.*;
import water.util.RString;

import com.google.gson.JsonObject;

/**
 * Simple web page to trigger k-means validation on another dataset. The dataset must contain the
 * same columns (NOTE:identified by names) or error is returned.
 *
 * @author cliffc
 *
 */
public class KMeansScore extends Request {
  protected final H2OKMeansModelKey _modelKey = new H2OKMeansModelKey(MODEL_KEY, true);
  protected final H2OHexKey _dataKey = new H2OHexKey(KEY);

  @Override protected void queryArgumentValueSet(water.api.RequestArguments.Argument arg, java.util.Properties inputArgs) {
    if( arg == _dataKey ) {     // Check for dataset compatibility
      ValueArray va = _dataKey.value();
      KMeansModel model = _modelKey.value();
      int colIds[] = model.columnMapping(va.colNames());
      if( !Model.isCompatible(colIds) ) {
        for( int i = 0; i < colIds.length; i++ )
          if( colIds[i] == -1 ) throw new IllegalArgumentException("Incompatible dataset: " + va._key
              + " does not have column '" + model._va._cols[i]._name + "'");
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

  @Override protected Response serve() {
    try {
      JsonObject res = new JsonObject();
      ValueArray ary = _dataKey.value();
      KMeansModel m = _modelKey.value();
      hex.KMeans.KMeansScore kms = hex.KMeans.KMeansScore.score(m, ary);
      res.add("score", kms.toJson());
      return Response.done(res);
    } catch( Error e ) {
      return Response.error(e.getMessage());
    }
  }
}
