package water.api;

import hex.DPCA.PCAModel;
import hex.NewRowVecTask.DataFrame;
import hex.DPCA;
import hex.PCAScoreTask;

import com.google.gson.JsonObject;

import water.*;
import water.api.RequestArguments.HexColumnSelect;
import water.api.RequestArguments.HexPCAColumnSelect;
import water.fvec.Frame;
import water.util.RString;

public class PCAScore extends Request {
  protected final H2OPCAModelKey _modelKey = new H2OPCAModelKey(MODEL_KEY, true);
  protected final H2OHexKey _dataKey = new H2OHexKey(KEY);
  protected final H2OKey _destKey = new H2OKey(DEST_KEY, true);
  protected final Int _numPC = new Int("num_pc", 2, 1, 1000000);   // TODO: Set default to # features

  @Override protected void queryArgumentValueSet(water.api.RequestArguments.Argument arg, java.util.Properties inputArgs) {
    if( arg == _dataKey ) {     // Check for dataset compatibility
      ValueArray va = _dataKey.value();
      PCAModel model = _modelKey.value();
      int colIds[] = model == null ? null : model.columnMapping(va.colNames());
      if( !Model.isCompatible(colIds) ) {
        for( int i = 0; i < colIds.length; i++ )
          if( colIds[i] == -1 )
            throw new IllegalArgumentException("Incompatible dataset: " + va._key + " does not have column '"
                + model._va._cols[i]._name + "'");
      }
    }
    if( arg == _numPC ) {
      PCAModel model = _modelKey.value();
      if(model != null && model._eigVec[0].length < _numPC.value())
        throw new IllegalArgumentException("Maximum number of principal components is " + model._eigVec[0].length);
    }
  }

  public static String link(Key modelKey, String content) {
    return link(MODEL_KEY, modelKey, content);
  }

  public static String link(String key_param, Key k, String content) {
    RString rs = new RString("<a href='PCAScore.query?%key_param=%$key'>%content</a>");
    rs.replace("key_param", key_param);
    rs.replace("key", k.toString());
    rs.replace("content", content);
    return rs.toString();
  }

  @Override protected Response serve() {
    try {
      JsonObject res = new JsonObject();
      res.addProperty(RequestStatics.DEST_KEY, _destKey.value().toString());
      ValueArray ary = _dataKey.value();
      PCAModel m = _modelKey.value();

      int[] cols = new int[ary._cols.length];
      for(int i = 0; i < cols.length; i++) cols[i] = i;
      DataFrame temp = DataFrame.makePCAData(ary, cols, m._pcaParams._standardized);
      Frame data = DPCA.StandardizeTask.standardize(temp);
      PCAScoreTask.score(data, m._eigVec, _numPC.value(), _destKey.value());

      JsonObject redir = new JsonObject();
      // if( job != null ) redir.addProperty(JOB, job.toString());
      redir.addProperty(KEY, _destKey.value().toString());
      // return Progress.redirect(response, job.job_key, _destKey.value());
      return Response.redirect(res, Inspect.class, redir);
    } catch( Error e ) {
      return Response.error(e.getMessage());
    }
  }
}
