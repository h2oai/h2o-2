package water.api;

import hex.KMeans.KMeansModel;
import water.*;
import water.util.RString;

import com.google.gson.JsonObject;

/**
 * Creates a set of classes by applies k-means on a dataset.
 */
public class KMeansApply extends Request {
  protected final H2OKMeansModelKey _modelKey = new H2OKMeansModelKey(MODEL_KEY, true);
  protected final H2OHexKey _dataKey = new H2OHexKey(DATA_KEY);
  protected final H2OKey _dest = new H2OKey(DEST_KEY, true);;

  public KMeansApply() {
  }

  @Override protected void queryArgumentValueSet(water.api.RequestArguments.Argument arg, java.util.Properties inputArgs) {
    if( arg == _dataKey ) {     // Check for dataset compatibility
      ValueArray va = _dataKey.value();
      KMeansModel model = _modelKey.value();
      int colIds[] = model.columnMapping(va.colNames());
      if( !Model.isCompatible(colIds) ) {
        for( int i = 0; i < colIds.length; i++ )
          if( colIds[i] == -1 )
            throw new IllegalArgumentException("Incompatible dataset: " + va._key + " does not have column '"
                + model._va._cols[i]._name + "'");
      }
    }
  };

  public static String link(Key k, String content) {
    RString rs = new RString("<a href='KMeansApply.query?%key_param=%$key'>%content</a>");
    rs.replace("key_param", MODEL_KEY);
    rs.replace("key", k.toString());
    rs.replace("content", content);
    return rs.toString();
  }

  @Override protected Response serve() {
    try {
      KMeansModel model = _modelKey.value();
      ValueArray data = _dataKey.value();
      Key dest = _dest.value();
      Job job = hex.KMeans.KMeansApply.run(dest, model, data);
      JsonObject response = new JsonObject();
      response.addProperty(RequestStatics.DEST_KEY, _dest.value().toString());
      return Progress.redirect(response, job._self, _dest.value());
    } catch( Error e ) {
      return Response.error(e.getMessage());
    }
  }
}
