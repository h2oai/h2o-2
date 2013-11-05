package water.api;

import water.Key;
import water.util.RString;


public class Inspect4UX extends Inspect {
  static private String _inspectTemplate;

  static {
    _inspectTemplate = loadTemplate("/Inspect.html");
  }

  @Override protected String htmlTemplate() {
    return _inspectTemplate;
  }

  public static String link(Key k, String content) {
    RString rs = new RString("<a href='/Inspect4UX.html?%key_param=%$key'>%content</a>");
    rs.replace("key_param", KEY);
    rs.replace("key", k.toString());
    rs.replace("content", content);
    return rs.toString();
  }
}