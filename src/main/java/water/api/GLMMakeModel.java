package water.api;

import hex.glm.GLMModel;
import hex.glm.GLMModelView;
import water.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Properties;

/**
 * Created by tomasnykodym on 1/27/15.
 */
public class GLMMakeModel extends Request2 {
  static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  @API(help = "source model", required = true, filter = Default.class)
  public GLMModel model;

  @API(help = "coefficient names", required = true, filter = Default.class)
  public String names;

  @API(help = "Beta coefficients", required = true, filter = Default.class)
  public double[] beta;

  @API(help = "", filter = Default.class, json = true, importance = ParamImportance.SECONDARY)
  protected double threshold;

  @API(help="",json=true)
  protected Key destination_key;

  @Override
  protected NanoHTTPD.Response serveGrid(NanoHTTPD server, Properties parms, RequestType type) {
    return superServeGrid(server, parms, type);
  }

  @Override
  protected Response serve() {
    try {
      double[] b;
      String[] ns = names.split(",");
      if (beta.length == model.coefficients_names.length && Arrays.equals(ns, model.coefficients_names))
        b = beta;
      else {
        b = MemoryManager.malloc8d(model.coefficients_names.length);
        HashMap<String, Integer> map = new HashMap<String, Integer>();
        for (int i = 0; i < model.coefficients_names.length; ++i)
          map.put(model.coefficients_names[i], i);
        for (int i = 0; i < ns.length; ++i) {
          String s = ns[i];
          if (!map.containsKey(s))
            throw new IllegalArgumentException("Unknown coefficient " + s);
          b[map.get(s)] = beta[i];
        }
      }
      destination_key = Key.make();//Key.make((byte) 1, /*Key.HIDDEN_USER_KEY*/Key.USER_KEY, H2O.SELF);
      GLMModel m = new GLMModel(model.get_params(),destination_key, model._dataKey, model.getParams(),model.coefficients_names, b, model.dinfo(), threshold);
      m.delete_and_lock(null).unlock(null);
      return Response.done(this);
    } catch (Throwable t) {
      return Response.error(t);
    }
  }
}
