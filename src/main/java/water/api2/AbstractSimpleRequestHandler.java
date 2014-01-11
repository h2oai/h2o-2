package water.api2;

import water.NanoHTTPD;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 */
public abstract class AbstractSimpleRequestHandler {
  // HTTP Method Lists (HML).
  static String[] HML_GET = {"GET"};

  static int SINCE_V3 = 3;
  static int UNTIL_FOREVER = Integer.MAX_VALUE;

  // Class members.
  private String _httpMethods[];
  private String _uriRegex;
  private int _since;
  private int _until;

  public AbstractSimpleRequestHandler(String[] httpMethods, int since, int until, String uriRegex) {
    _httpMethods = httpMethods;
    _uriRegex = uriRegex;
    _since = since;
    _until = until;
  }

  public boolean matches(String method, String uri) {
    int apiVersion;
    String uri_after_removing_version;
    {
      Pattern p = Pattern.compile("/v(\\d+)(/.*)");
      Matcher m = p.matcher(uri);
      boolean b = m.matches();
      if (! b) {
        return false;
      }
      assert(m.groupCount() == 2);
      apiVersion = Integer.parseInt(m.group(1));
      uri_after_removing_version = m.group(2);
    }

    // Check if request is too old.
    if (apiVersion < _since) {
      return false;
    }

    // Check if request is too new.
    if (apiVersion > _until) {
      return false;
    }

    // If the regex doesn't match, then reject.
    if (! uri_after_removing_version.matches(_uriRegex)) {
      return false;
    }

    // If any method matches, accept.
    for (String m : _httpMethods) {
      if (method.equals(m)) {
        return true;
      }
    }

    // No match.
    return false;
  }

  public abstract NanoHTTPD.Response serve(NanoHTTPD.IHTTPSession session) throws Exception;
}
