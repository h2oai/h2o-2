package water.util;

import water.Key;
import water.Model;
import water.api.RequestStatics.RequestType;

public class UIUtils {

  /** Return the query link to this page */
  public static <T> String qlink(Class<T> page, Key k, String content) {
    return qlink(page, "source", k, content );
  }
  public static <T> String qlink(Class<T> page, String keyPlaceholder, Key k, String content) {
    return link(page, RequestType.query, keyPlaceholder, k.toString(), content);
  }
  public static <T> String link(Class<T> page, String keyPlaceholder, String k, String content) {
    return link(page, RequestType.www, keyPlaceholder, k, content);
  }
  public static <T> String link(Class<T> page, RequestType rtype, String keyPlaceholder, String k, String content) {
    RString rs = new RString("<a href='/2/%page%rtype?%keyPlaceholder=%$key'>%content</a>");
    rs.replace("keyPlaceholder", keyPlaceholder);
    rs.replace("rtype", rtype._suffix);
    rs.replace("page", page.getSimpleName());
    rs.replace("key", k);
    rs.replace("content", content);
    return rs.toString();
  }
  public static <T extends Model> String builderModelLink(Class<T> model, Key source, String response, String content) {
    return builderModelLink(model, source, response, content, null);
  }
  public static <T extends Model> String builderModelLink(Class<T> model, Key source, String response, String content, String onClick) {
    String name = model.getSimpleName();
    name = name.substring(0, name.indexOf("Model"));
    RString rs = new RString("<a href='/2/%page.query?source=%$source&response=%response' %onclick >%content</a>");
    rs.replace("page", name);
    rs.replace("source", source!=null ? source.toString() : "");
    rs.replace("response", response);
    rs.replace("content", content);
    rs.replace("onclick", onClick!=null ? "onclick=\""+onClick+"\"" : "");
    return rs.toString();
  }

  public static <T extends Model> String builderLink(Class<T> model, Key source, String response, Key checkpoint, String content) {
    String name = model.getSimpleName();
    name = name.substring(0, name.indexOf("Model"));
    RString rs = new RString("<a href='/2/%page.query?source=%$source&response=%response&checkpoint=%$checkpoint'>%content</a>");
    rs.replace("page", name);
    rs.replace("source", source!=null ? source.toString() : "");
    rs.replace("response", response);
    rs.replace("content", content);
    rs.replace("checkpoint", checkpoint!=null ? checkpoint.toString() : "");
    return rs.toString();
  }
}
