package water.api2;

import water.NanoHTTPD;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Base class for simple request handlers.
 */
public abstract class AbstractSimpleRequestHandler {
  // HTTP Method Lists (HML).
  static String[] HML_GET = {"GET"};

  // Version codes.
  static int SINCE_V3 = 3;
  static int UNTIL_FOREVER = Integer.MAX_VALUE;

  // Class members.
  private String _httpMethods[];
  private String _uriRegex;
  private int _since;
  private int _until;

  /**
   * Constructor.
   *
   * @param httpMethods One of the HML lists specifying supported HTTP methods for this servlet.
   * @param since One of the SINCE values.
   * @param until One of the UNTIL values.
   * @param uriRegex A regular expression for uri matching.
   */
  public AbstractSimpleRequestHandler(String[] httpMethods, int since, int until, String uriRegex) {
    _httpMethods = httpMethods;
    _uriRegex = uriRegex;
    _since = since;
    _until = until;
  }

  /**
   * Whether this servlet matches the incoming URI.
   *
   * @param method Session HTTP method.
   * @param uri Session URI.
   * @return true if this servlet should handle the given request.  false otherwise.
   */
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

  /**
   * Serve the request with this servlet.
   *
   * @param session Contains request information.
   * @return A response for the request.
   * @throws ASRIllegalArgumentException When an argument is malformed or missing.
   * @throws Exception Results in an Internal Server Error.
   */
  @SuppressWarnings("DuplicateThrows")
  public abstract NanoHTTPD.Response serve(NanoHTTPD.IHTTPSession session) throws ASRIllegalArgumentException, Exception;
}
