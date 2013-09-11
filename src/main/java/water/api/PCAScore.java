package water.api;

import hex.DPCA.PCAModel;
import hex.PCAScoreTask;

import com.google.gson.JsonObject;

import water.*;
import water.util.RString;

public class PCAScore extends Request {
  protected final H2OPCAModelKey _modelKey = new H2OPCAModelKey(MODEL_KEY, true);
  protected final H2OHexKey _dataKey = new H2OHexKey(KEY);
  protected final H2OKey _destKey = new H2OKey(DEST_KEY, true);

  @Override protected void queryArgumentValueSet(water.api.RequestArguments.Argument arg, java.util.Properties inputArgs) {
    if( arg == _dataKey ) {     // Check for dataset compatibility
      ValueArray va = _dataKey.value();
      PCAModel model = _modelKey.value();
      int colIds[] = model.columnMapping(va.colNames());
      if( !Model.isCompatible(colIds) ) {
        for( int i = 0; i < colIds.length; i++ )
          if( colIds[i] == -1 )
            throw new IllegalArgumentException("Incompatible dataset: " + va._key + " does not have column '"
                + model._va._cols[i]._name + "'");
      }
    }
  }

  public static String link(Key k, String content) {
    RString rs = new RString("<a href='PCAScore.query?%key_param=%$key'>%content</a>");
    rs.replace("key_param", MODEL_KEY);
    rs.replace("key", k.toString());
    rs.replace("content", content);
    return rs.toString();
  }

  @Override protected Response serve() {
    try {
      JsonObject res = new JsonObject();
      ValueArray ary = _dataKey.value();
      PCAModel m = _modelKey.value();
      if(m._pcaParams._standardized)
        // TODO: Need to standardize ary data set if PCs calculated on standardized data!
      PCAScoreTask.score(ary.asFrame(), m._eigVec, _destKey.value());
      return Response.done(res);
    } catch( Error e ) {
      return Response.error(e.getMessage());
    }
  }
}
