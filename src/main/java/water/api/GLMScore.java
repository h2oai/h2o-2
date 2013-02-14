package water.api;

import com.google.gson.JsonObject;
import hex.GLMSolver.Family;
import hex.GLMSolver.GLMException;
import hex.GLMSolver.GLMModel;
import hex.GLMSolver.GLMValidation;
import water.Key;
import water.ValueArray;
import water.api.GLM.GLMBuilder;
import water.util.RString;

/**
 * Simple web page to trigger glm validation on another dataset.
 * The dataset must contain the same columns (NOTE:identified by names) or error is returned.
 *
 * @author tomasnykodym
 *
 */
public class GLMScore extends Request {
  protected final H2OGLMModelKey _modelKey = new H2OGLMModelKey(MODEL_KEY,true);
  protected final H2OHexKey _dataKey = new H2OHexKey(KEY);
  protected final RSeq _thresholds = new RSeq(DTHRESHOLDS, false,
                                              new NumberSequence("0:1:0.01", false, 0.01),false);

  @Override
  protected void queryArgumentValueSet(water.api.RequestArguments.Argument arg, java.util.Properties inputArgs) {
    if(arg == _modelKey && _modelKey.specified()){
      GLMModel m = _modelKey.value();
      if(m._glmParams._f == Family.binomial){
        _thresholds._hideInQuery = false;
      }else{
        _thresholds.disable("only for binomial");
        _thresholds._hideInQuery =true;
      }
    }
    if( arg == _dataKey ) {     // Check for dataset compatibility
      ValueArray va = _dataKey.value();
      GLMModel model = _modelKey.value();
      int colIds[] = model.columnMapping(va.colNames());
      if( !GLMModel.isCompatible(colIds) ) {
        for( int i=0; i<colIds.length; i++ )
          if( colIds[i] == -1 )
            throw new IllegalArgumentException("Incompatible dataset: "+va._key+" does not have column '"+model._va._cols[i]._name+"'");
      }
    }
  };

  public static String link(Key k, double threshold, String content) {
    RString rs = new RString("<a href='GLMScore.query?%key_param=%$key&thresholds=%threshold'>%content</a>");
    rs.replace("key_param", MODEL_KEY);
    rs.replace("key", k.toString());
    rs.replace("threshold", threshold);
    rs.replace("content", content);
    return rs.toString();
  }



  public GLMScore() {}

  static class GLMValidationBuilder extends ObjectBuilder {
    final GLMValidation _val;
    GLMValidationBuilder( GLMValidation v) { _val=v; }
    public String build(Response response, JsonObject json, String contextName) {
      StringBuilder sb = new StringBuilder();
      GLMBuilder.validationHTML(_val,sb);
      return sb.toString();
    }
  }

  @Override protected Response serve() {
    try {
      JsonObject res = new JsonObject();
      ValueArray ary = _dataKey.value();
      GLMModel m = _modelKey.value();
      GLMValidation v = _thresholds.disabled()?m.validateOn(ary, null, null):m.validateOn(ary, null, _thresholds.value()._arr);
      // Display HTML setup
      Response r = Response.done(res);
      r.setBuilder("", new GLMValidationBuilder(v));
      return r;
    }catch(GLMException e){
      return Response.error(e.getMessage());
    } catch (Throwable t) {
      t.printStackTrace();
      return Response.error(t.getMessage());
    }
  }
}
