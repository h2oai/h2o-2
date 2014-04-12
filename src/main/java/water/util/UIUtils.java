package water.util;

import hex.gbm.DTree.TreeModel;
import water.Key;

public class UIUtils {

  /** Return the query link to this page */
  public static <T> String link(Class<T> page, Key k, String content) {
    RString rs = new RString("<a href='/2/%page.query?source=%$key'>%content</a>");
    rs.replace("page", page.getSimpleName());
    rs.replace("key", k.toString());
    rs.replace("content", content);
    return rs.toString();
  }

  public static <T extends TreeModel> String builderLink(Class<T> model, Key source, String response, String content) {
    String name = model.getSimpleName();
    name = name.substring(0, name.indexOf("Model"));
    RString rs = new RString("<a href='/2/%page.query?source=%$source&response=%response'>%content</a>");
    rs.replace("page", name);
    rs.replace("source", source.toString());
    rs.replace("response", response);
    rs.replace("content", content);
    return rs.toString();
  }
}
