package water.ga;

import water.util.Log;
import static water.ga.GaUtils.appendSystemProperty;
import static water.ga.GaUtils.isEmpty;

/**
 * Default request parameter discoverer. Discovers following parameters.
 * <ul>
 * 	<li>Creates User Agent as java/1.6.0_45-b06/Sun Microsystems Inc./Java HotSpot(TM) 64-Bit Server VM/Windows 7/6.1/amd64</li>
 *  <li>User Language, and Country</li>
 *  <li>File Encoding</li>
 * </ul>
 *
 * @author Santhosh Kumar
 *
 * This copy of google-analytics-java is a back port of version 1.1.1 of the library.
 * This backport removes the slf4j dependency, and modifies the code to work with the
 * 4.1 version of the Apache http client library.
 *
 * Original sources can be found at https://github.com/brsanthu/google-analytics-java.
 * All copyrights retained by original authors.
 */
public class DefaultRequestParameterDiscoverer implements RequestParameterDiscoverer {

  @Override
  public DefaultRequest discoverParameters(GoogleAnalyticsConfig config, DefaultRequest request) {
    try {
      if (isEmpty(config.getUserAgent())) {
        config.setUserAgent(getUserAgentString());
      }

      if (isEmpty(request.userLanguage())) {
        String region = System.getProperty("user.region");
        if (isEmpty(region)) {
          region = System.getProperty("user.country");
        }
        request.userLanguage(System.getProperty("user.language") + "-" + region);
      }

      if (isEmpty(request.documentEncoding())) {
        request.documentEncoding(System.getProperty("file.encoding"));
      }

    } catch (Exception e) {
      Log.warn("Exception while deriving the System properties for request " + request);
    }

    return request;
  }

  protected String getUserAgentString() {
    StringBuilder sb = new StringBuilder("java");
    appendSystemProperty(sb, "java.runtime.version");
    appendSystemProperty(sb, "java.specification.vendor");
    appendSystemProperty(sb, "java.vm.name");
    appendSystemProperty(sb, "os.name");
    appendSystemProperty(sb, "os.version");
    appendSystemProperty(sb, "os.arch");

    return sb.toString();
  }

}
