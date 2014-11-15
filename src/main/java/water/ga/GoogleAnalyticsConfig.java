/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package water.ga;

/**
 * Properties that can be configured in this library. These would include any properties that are required to process the
 * tracking request or enhance the tracking request (but not specified in measurement protocol like User agent).
 * <p>
 * Most of the properties are initialization level and request level. If a property is a initialization level property,
 * it should be set at the time of GoogleAnalytics object initialization. If a property is a request level property,
 * it can be set any time and it will be effective.
 * <p>
 * All properties of this config object supports method chaining. So for example, you could do,
 * <code>new GoogleAnalyticsConfig().setMaxThreads(2).setThreadNameFormat("name");</code>
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
public class GoogleAnalyticsConfig {
  private String threadNameFormat = "googleanalytics-thread-{0}";
  private boolean enabled = true;
  private int maxThreads = 1;
  private boolean useHttps = true;
  private boolean validate = true;
  private String httpUrl = "http://www.google-analytics.com/collect";
  private String httpsUrl = "https://ssl.google-analytics.com/collect";
  private String userAgent = null;
  private String proxyHost = null;
  private int proxyPort = 80;
  private String proxyUserName = null;
  private String proxyPassword = null;
  private boolean discoverRequestParameters = true;
  private boolean gatherStats = false;
  private RequestParameterDiscoverer requestParameterDiscoverer = new DefaultRequestParameterDiscoverer();

  public RequestParameterDiscoverer getRequestParameterDiscoverer() {
    return requestParameterDiscoverer;
  }

  /**
   * Sets the appropriate request parameter discoverer. Default is {@link DefaultRequestParameterDiscoverer} but
   * can be changed to {@link AwtRequestParameterDiscoverer} if you want to use Toolkit to derive the screen resolution etc.
   *
   * Please make sure you also enable the discovery using {@link #setDiscoverRequestParameters(boolean)}
   *
   * @param requestParameterDiscoverer can be null and is so, parameters will not be discovered.
   */
  public void setRequestParameterDiscoverer(RequestParameterDiscoverer requestParameterDiscoverer) {
    this.requestParameterDiscoverer = requestParameterDiscoverer;
  }

  public boolean isGatherStats() {
    return gatherStats;
  }
  /**
   * If set to true, {@link GoogleAnalytics} will collect the basic stats about successful event postings
   * for various hit types and keeps a copy of {@link GoogleAnalyticsStats}, which can be retrieved
   * using {@link GoogleAnalytics#getStats()}
   *
   * @param gatherStats
   */
  public void setGatherStats(boolean gatherStats) {
    this.gatherStats = gatherStats;
  }

  /**
   * Sets the thread name format that should be while creating the threads.
   * <p>
   * Default is "googleanalytics-thread-{0}" where {0} is the thread counter. If you specify
   * a custom format, make sure {0} is there somewhere otherwise all threads will be nameed
   * same and can be an issue for troubleshooting.
   *
   * @param threadNameFormat non-null string for thread name.
   */
  public GoogleAnalyticsConfig setThreadNameFormat(String threadNameFormat) {
    this.threadNameFormat = threadNameFormat;
    return this;
  }

  public String getThreadNameFormat() {
    return threadNameFormat;
  }

  /**
   * Deprecated since 1.0.6
   *
   * @deprecated Use {@link #setDiscoverRequestParameters(boolean)} instead
   */
  @Deprecated
  public GoogleAnalyticsConfig setDeriveSystemParameters(boolean deriveSystemProperties) {
    return setDiscoverRequestParameters(deriveSystemProperties);
  }

  /**
   * If true, derives the system properties (User Language, Region, Country, Screen Size, Color Depth, and File encoding) and adds to
   * the default request.
   *
   * <p>This is <strong>initialization</strong> level configuration (must be set while creating GoogleAnalytics object).</p>
   */
  public GoogleAnalyticsConfig setDiscoverRequestParameters(boolean discoverSystemParameters) {
    this.discoverRequestParameters = discoverSystemParameters;
    return this;
  }

  /**
   * Deprecated since 1.0.6
   *
   * @deprecated Use {@link #isDiscoverRequestParameters()} instead
   */
  @Deprecated
  public boolean isDeriveSystemParameters() {
    return isDiscoverRequestParameters();
  }

  public boolean isDiscoverRequestParameters() {
    return discoverRequestParameters;
  }
  /**
   * Sets the user name which should be used to authenticate to the proxy server. This is applicable only if {@link #setProxyHost(String)} is not empty.
   *
   * <p>This is <strong>initialization</strong> level configuration (must be set while creating GoogleAnalytics object).</p>
   */
  public GoogleAnalyticsConfig setProxyUserName(String proxyUserName) {
    this.proxyUserName = proxyUserName;
    return this;
  }

  public String getProxyUserName() {
    return proxyUserName;
  }

  public String getProxyPassword() {
    return proxyPassword;
  }

  /**
   * Sets the password which should be used to authenticate to the proxy server. This is applicable only if {@link #setProxyHost(String)} and {@link #setProxyUserName(String)} is not empty.
   *
   * <p>This is <strong>initialization</strong> level configuration (must be set while creating GoogleAnalytics object).</p>
   */
  public GoogleAnalyticsConfig setProxyPassword(String proxyPassword) {
    this.proxyPassword = proxyPassword;
    return this;
  }
  public String getProxyHost() {
    return proxyHost;
  }

  /**
   * Sets the host name of the proxy server, to connect to Google analytics.
   *
   * <p>This is <strong>initialization</strong> level configuration (must be set while creating GoogleAnalytics object).</p>
   */
  public GoogleAnalyticsConfig setProxyHost(String proxyHost) {
    this.proxyHost = proxyHost;
    return this;
  }
  public int getProxyPort() {
    return proxyPort;
  }

  /**
   * Sets the host name of the proxy server, to connect to Google analytics.
   *
   * <p>This is <strong>initialization</strong> level configuration (must be set while creating GoogleAnalytics object).</p>
   */
  public GoogleAnalyticsConfig setProxyPort(int proxyPort) {
    this.proxyPort = proxyPort;
    return this;
  }
  public String getUserAgent() {
    return userAgent;
  }

  /**
   * Sets the user agent string that should be sent while making the http request. Default is Apache Http Client's user agent,
   * which looks something similar to this. <code>Apache-HttpClient/release (java 1.5)</code>
   *
   * <p>This is <strong>initialization</strong> level configuration (must be set while creating GoogleAnalytics object).</p>
   */
  public GoogleAnalyticsConfig setUserAgent(String userAgent) {
    this.userAgent = userAgent;
    return this;
  }
  public boolean isEnabled() {
    return enabled;
  }

  /**
   * Enables or disables the GoogleAnalytics posting. If disabled, library will continue to accept the send/post requests but silently skips
   * sending the event and returns successful response. Default is <code>false</code>.
   *
   * <p>This is <strong>request</strong> level configuration (can be changed any time).</p>
   */
  public GoogleAnalyticsConfig setEnabled(boolean enabled) {
    this.enabled = enabled;
    return this;
  }

  /**
   * Maximum threads to use to process the asynchronous event posting and Http client connection pooling. Default is 1.
   *
   * <p>This is <strong>initialization</strong> level configuration (must be set while creating GoogleAnalytics object).</p>
   */
  public int getMaxThreads() {
    return maxThreads;
  }
  public GoogleAnalyticsConfig setMaxThreads(int maxThreads) {
    this.maxThreads = maxThreads;
    return this;
  }
  public boolean isUseHttps() {
    return useHttps;
  }

  /**
   * Instructs to use https url to send the events. Default is true.
   *
   * <p>This is <strong>request</strong> level configuration (can be changed any time).</p>
   */
  public GoogleAnalyticsConfig setUseHttps(boolean useHttps) {
    this.useHttps = useHttps;
    return this;
  }
  public boolean isValidate() {
    return validate;
  }

  /**
   * If set, validates the request before sending to Google Analytics. If any errors found, GoogleAnalyticsException will be thrown with details.
   * Default is false. Note that, if you are sending the event in async mode, then request is always validated and logged to log file as warnings irrespective
   * of this flag.
   *
   * <p>This is <strong>request</strong> level configuration (can be changed any time).</p>
   */
  public GoogleAnalyticsConfig setValidate(boolean validate) {
    this.validate = validate;
    return this;
  }
  public String getHttpUrl() {
    return httpUrl;
  }

  /**
   * URL to use when posting the event in http mode. This url is Google Analytics service url and usually not updated by the clients.
   *
   * <p>Default value is <code>http://www.google-analytics.com/collect</code></p>
   *
   * <p>This is <strong>request</strong> level configuration (can be changed any time).</p>
   */
  public GoogleAnalyticsConfig setHttpUrl(String httpUrl) {
    this.httpUrl = httpUrl;
    return this;
  }
  public String getHttpsUrl() {
    return httpsUrl;
  }

  /**
   * URL to use when posting the event in https mode. This url is Google Analytics service url and usually not updated by the clients.
   * <p>Default value is <code>https://ssl.google-analytics.com/collect</code>
   *
   * <p>This is <strong>request</strong> level configuration (can be changed any time).</p>
   */
  public GoogleAnalyticsConfig setHttpsUrl(String httpsUrl) {
    this.httpsUrl = httpsUrl;
    return this;
  }

  String getUrl() {
    return useHttps?httpsUrl:httpUrl;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("GoogleAnalyticsConfig [");
    if (threadNameFormat != null) {
      builder.append("threadNameFormat=");
      builder.append(threadNameFormat);
      builder.append(", ");
    }
    builder.append("enabled=");
    builder.append(enabled);
    builder.append(", maxThreads=");
    builder.append(maxThreads);
    builder.append(", useHttps=");
    builder.append(useHttps);
    builder.append(", validate=");
    builder.append(validate);
    builder.append(", ");
    if (httpUrl != null) {
      builder.append("httpUrl=");
      builder.append(httpUrl);
      builder.append(", ");
    }
    if (httpsUrl != null) {
      builder.append("httpsUrl=");
      builder.append(httpsUrl);
      builder.append(", ");
    }
    if (userAgent != null) {
      builder.append("userAgent=");
      builder.append(userAgent);
      builder.append(", ");
    }
    if (proxyHost != null) {
      builder.append("proxyHost=");
      builder.append(proxyHost);
      builder.append(", ");
    }
    builder.append("proxyPort=");
    builder.append(proxyPort);
    builder.append(", ");
    if (proxyUserName != null) {
      builder.append("proxyUserName=");
      builder.append(proxyUserName);
      builder.append(", ");
    }
    if (proxyPassword != null) {
      builder.append("proxyPassword=");
      builder.append(mask(proxyPassword));
      builder.append(", ");
    }
    builder.append("deriveSystemParameters=");
    builder.append(discoverRequestParameters);
    builder.append(", gatherStats=");
    builder.append(gatherStats);
    builder.append("]");
    return builder.toString();
  }

  public static String mask(String value) {
    return value == null?null:"********";
  }
}
