package water.api;

import java.util.HashMap;
import java.util.Map;

import hex.GridSearch;
import hex.glm.GLM2;
import hex.la.DMatrix;
import water.*;
import water.api.RequestBuilders.Response;
import water.fvec.Frame;
import water.util.RString;
import water.util.UIUtils;

/**
 * This is just a simple Spring-like name-driven request redirector.
 *
 * <p>The page never returns actual content, but provides a
 * redirect to proper page.</p>
 *
 * <p>
 * Note: The best redirector would be based on a simple pattern:
 * incoming class name is suffixed by "View" which composes a redirect link.</p>
 *
 */
public class Inspector extends Request2 {
  static final int API_WEAVER=1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  private static Map<Class, String[]> REDIRECTS;
  static {
    REDIRECTS = new HashMap<Class, String[]>();
    // All attempts to view frame redirect to Inspect frame
    REDIRECTS.put(Frame.class,    sa("/2/Inspect2",      "src_key"));
    // All attempts to view a model redirect to <model_name>View
    REDIRECTS.put(Model.class,    sa("/2/%typename{}View", "_modelKey"));
    REDIRECTS.put(GLM2.GLMGrid.class, sa("/2/GLMGridView", "grid_key"));
    REDIRECTS.put(GridSearch.class, sa("/2/%typename{}Progress", "destination_key"));
    REDIRECTS.put(DMatrix.MatrixMulStats.class,sa("/2/MMStats","src_key"));
  }

  @API(help="H2O key to inspect.", filter=Default.class, json=true, required=true, gridable=false)
  Key src_key;

  @Override protected Response serve() {
    Value v = DKV.get(src_key);
    if (v==null) throw new IllegalArgumentException("Key " + src_key + " does not exist!");
    String typename = v.className();
    try {
      Class klazz = Class.forName(typename);
      if (REDIRECTS.containsKey(klazz)) {
        String[] r = REDIRECTS.get(klazz);
        return redirect(klazz.getSimpleName(), r[0], r[1]);
      } else {
        // Find first matching class
        for (Class k : REDIRECTS.keySet()) {
           if (k.isAssignableFrom(klazz)) {
             String[] r = REDIRECTS.get(k);
             return redirect(klazz.getSimpleName(), r[0], r[1]);
           }
        }
      }
    } catch (ClassNotFoundException e) {
      // This is critical error since it should not happen
      return Response.error(e);
    }
    throw new IllegalArgumentException("Unknown key type! Key = " + src_key + " and type = " + typename);
  }

  public static String link(String txt, String key) {
    return UIUtils.link(Inspector.class, "src_key", key, txt);
  }

  private Response redirect(String typename, String urlTemplate, String keyParamName) {
    RString r = new RString(urlTemplate);
    r.replace("typename", typename);
    return Response.redirect(this, r.toString(), keyParamName, src_key.toString());
  }

  private static String[] sa(String ...s) { return s; }

  //Called from some other page, to redirect that other page to this page.
  public static Response redirect(Request req, Key src_key) {
    return Response.redirect(req, "/2/Inspector", "src_key", src_key.toString());
  }
}
