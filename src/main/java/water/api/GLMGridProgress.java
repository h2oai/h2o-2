package water.api;

import hex.DGLM.Family;
import hex.DGLM.GLMModel;
import hex.DLSM.LSMSolver;
import hex.GLMGrid.GLMModels;

import java.util.Map;

import water.*;

import com.google.common.base.Objects;
import com.google.common.collect.Maps;
import com.google.gson.*;

public class GLMGridProgress extends Request {
  protected final H2OKey _job  = new H2OKey(JOB);
  protected final H2OKey _dest = new H2OKey(DEST_KEY);

  public static Response redirect(JsonObject resp, Key job, Key dest) {
    JsonObject redir = new JsonObject();
    redir.addProperty(JOB, job.toString());
    redir.addProperty(DEST_KEY, dest.toString());
    return Response.redirect(resp, GLMGridProgress.class, redir);
  }

  @Override
  protected Response serve() {
    Key dest = _dest.value();
    GLMModels models = UKV.get(dest);

    JsonObject response = new JsonObject();
    response.addProperty(Constants.DEST_KEY, dest.toString());

    JsonArray array = new JsonArray();
    for( GLMModel m : models.sorted() ) {
      JsonObject o = new JsonObject();
      LSMSolver lsm = m._solver;
      o.addProperty(KEY, m._selfKey.toString());
      o.addProperty(LAMBDA, lsm._lambda);
      o.addProperty(ALPHA, lsm._alpha);
      if(m._glmParams._family == Family.binomial) {
        o.addProperty(BEST_THRESHOLD, m._vals[0].bestThreshold());
        o.addProperty(AUC, m._vals[0].AUC());
        double[] classErr = m._vals[0].classError();
        for( int j = 0; j < classErr.length; ++j ) {
          o.addProperty(ERROR + "_" + j, classErr[j]);
        }
      } else {
        o.addProperty(ERROR, m._vals[0]._err);
      }
      JsonArray arr = new JsonArray();
      if( m._warnings != null ) {
        for( String w : m._warnings ) {
          arr.add(new JsonPrimitive(w));
        }
      }
      o.add(WARNINGS, arr);
      array.add(o);
    }
    response.add(MODELS, array);

    Response r;
    if( DKV.get(_job.value()) != null )
      r = Response.poll(response, models.progress());
    else
      r = Response.done(response);

    r.setBuilder(Constants.DEST_KEY, new HideBuilder());
    r.setBuilder(MODELS, new GridBuilder2());
    r.setBuilder(MODELS + "." + KEY, new KeyCellBuilder());
    r.setBuilder(MODELS + "." + WARNINGS, new WarningCellBuilder());
    return r;
  }

  private static class GridBuilder2 extends ArrayBuilder {
    private final Map<String, String> _m = Maps.newHashMap();
    {
      _m.put(KEY, "Model");
      _m.put(LAMBDA, "&lambda;");
      _m.put(ALPHA, "&alpha;");
    }

    @Override
    public String header(String key) {
      return Objects.firstNonNull(_m.get(key), super.header(key));
    }
  }
}
