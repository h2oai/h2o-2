package water.ga;

/**
 * Mechanism to discover some default request parameters.
 *
 * A small library for interacting with Google Analytics Measurement Protocol.  This
 * copy is a back port of version 1.1.1 of the library.  This backport removes
 * the slf4j dependency, and modifies the code to work with the 4.1 version of the
 * Apache http client library.
 *
 * Original sources can be found at https://github.com/brsanthu/google-analytics-java.
 * All copyrights retained by original authors.
 *
 */
public interface RequestParameterDiscoverer {

  public DefaultRequest discoverParameters(GoogleAnalyticsConfig config, DefaultRequest request);

}
