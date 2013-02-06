package water.api;

import hex.GLMSolver.Family;
import hex.GLMSolver.GLMModel;
import hex.*;

import java.util.Map;

import water.Key;
import water.Value;

import com.google.common.base.Objects;
import com.google.common.collect.Maps;
import com.google.gson.*;

public class GLMGridProgress extends Request {
  protected final H2OExistingKey _taskey = new H2OExistingKey(DEST_KEY);

  public static Response redirect(JsonObject resp, Key taskey) {
    JsonObject redir = new JsonObject();
    redir.addProperty(DEST_KEY, taskey.toString());
    return Response.redirect(resp, GLMGridProgress.class, redir);
  }

  @Override protected Response serve() {
    Value v = _taskey.value();
    GLMGridStatus status = v.get(new GLMGridStatus());

    JsonObject response = new JsonObject();
    response.addProperty(Constants.DEST_KEY, v._key.toString());

    JsonArray models = new JsonArray();
    for( GLMModel m : status.computedModels() ) {
      JsonObject o = new JsonObject();
      LSMSolver lsm = m._solver;
      o.addProperty(KEY, m._selfKey.toString());
      o.addProperty(LAMBDA, lsm._lambda);
      o.addProperty(ALPHA, lsm._alpha);
      if(m._glmParams._f == Family.binomial){
        o.addProperty(BEST_THRESHOLD, m._vals[0].bestThreshold());
        o.addProperty(AUC, m._vals[0].AUC());
        double[] classErr = m._vals[0].classError();
        for( int j = 0; j < classErr.length; ++j ) {
          o.addProperty(ERROR +"_"+ j, classErr[j]);
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
      models.add(o);
    }
    response.add(MODELS, models);

    Response r = status._working
      ? Response.poll(response,status.progress())
      : Response.done(response);

    r.setBuilder(Constants.DEST_KEY, new HideBuilder());
    r.setBuilder(MODELS, new GridBuilder2());
    r.setBuilder(MODELS+"."+KEY, new KeyCellBuilder());
    r.setBuilder(MODELS+"."+WARNINGS, new WarningCellBuilder());
    return r;
  }

  private static class GridBuilder2 extends ArrayBuilder {
    private final Map<String, String> _m = Maps.newHashMap(); {
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
