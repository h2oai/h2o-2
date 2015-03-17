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

import static water.ga.GaUtils.isEmpty;
import static water.ga.GaUtils.isNotEmpty;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.http.NameValuePair;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.HttpHost;
import org.apache.http.params.CoreProtocolPNames;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.message.BasicHttpResponse;
import org.apache.http.conn.params.ConnRoutePNames;
import org.apache.http.impl.conn.tsccm.ThreadSafeClientConnManager;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;

import water.util.Log;
/**
 * This is the main class of this library that accepts the requests from clients and
 * sends the events to Google Analytics (GA).
 *
 * Clients needs to instantiate this object with {@link GoogleAnalyticsConfig} and {@link DefaultRequest}.
 * Configuration contains sensible defaults so one could just initialize using one of the convenience constructors.
 *
 * This object is ThreadSafe and it is intended that clients create one instance of this for each GA Tracker Id
 * and reuse each time an event needs to be posted.
 *
 * This object contains resources which needs to be shutdown/disposed. So {@link #close()} method is called
 * to release all resources. Once close method is called, this instance cannot be reused so create new instance
 * if required.
 *
 * This copy of google-analytics-java is a back port of version 1.1.1 of the library.
 * This backport removes the slf4j dependency, and modifies the code to work with the
 * 4.1 version of the Apache http client library.
 *
 * Original sources can be found at https://github.com/brsanthu/google-analytics-java.
 * All copyrights retained by original authors.
 */
public class GoogleAnalytics {

  private static final Charset UTF8 = Charset.forName("UTF-8");

  private GoogleAnalyticsConfig config = null;
  private DefaultRequest defaultRequest = null;
  private HttpClient httpClient = null;
  private ThreadPoolExecutor executor = null;
  private GoogleAnalyticsStats stats = new GoogleAnalyticsStats();

  public GoogleAnalytics(String trackingId) {
    this(new GoogleAnalyticsConfig(), new DefaultRequest().trackingId(trackingId));
  }

  public GoogleAnalytics(GoogleAnalyticsConfig config, String trackingId) {
    this(config, new DefaultRequest().trackingId(trackingId));
  }

  public GoogleAnalytics(String trackingId, String appName, String appVersion) {
    this(new GoogleAnalyticsConfig(), trackingId, appName, appVersion);
  }

  public GoogleAnalytics(GoogleAnalyticsConfig config, String trackingId, String appName, String appVersion) {
    this(config, new DefaultRequest().trackingId(trackingId).applicationName(appName).applicationVersion(appVersion));
  }

  public GoogleAnalytics(GoogleAnalyticsConfig config, DefaultRequest defaultRequest) {
    if (config.isDiscoverRequestParameters() && config.getRequestParameterDiscoverer() != null) {
      config.getRequestParameterDiscoverer().discoverParameters(config, defaultRequest);
    }

    //Log.debug("Initializing Google Analytics with config=" + config + " and defaultRequest=" + defaultRequest);

    this.config = config;
    this.defaultRequest = defaultRequest;
    this.defaultRequest.userAgent(config.getUserAgent());
    this.httpClient = createHttpClient(config);
  }

  public GoogleAnalyticsConfig getConfig() {
    return config;
  }

  public HttpClient getHttpClient() {
    return httpClient;
  }

  public DefaultRequest getDefaultRequest() {
    return defaultRequest;
  }

  public void setDefaultRequest(DefaultRequest request) {
    this.defaultRequest = request;
  }

  public void setHttpClient(HttpClient httpClient) {
    this.httpClient = httpClient;
  }

  @SuppressWarnings({ "rawtypes" })
  public GoogleAnalyticsResponse post(GoogleAnalyticsRequest request) {
    GoogleAnalyticsResponse response = new GoogleAnalyticsResponse();
    if (!config.isEnabled()) {
      return response;
    }

    BasicHttpResponse httpResponse = null;
    try {
      List<NameValuePair> postParms = new ArrayList<NameValuePair>();

      //Log.debug("GA Processing " + request);

      //Process the parameters
      processParameters(request, postParms);

      //Process custom dimensions
      processCustomDimensionParameters(request, postParms);

      //Process custom metrics
      processCustomMetricParameters(request, postParms);

      //Log.debug("GA Processed all parameters and sending the request " + postParms);

      HttpPost httpPost = new HttpPost(config.getUrl());
      try {
        httpPost.setEntity(new UrlEncodedFormEntity(postParms, "UTF-8"));
      } catch (UnsupportedEncodingException e) { Log.warn("This systems doesn't support UTF-8!"); }

      try {
        httpResponse = (BasicHttpResponse) httpClient.execute(httpPost);
      } catch (ClientProtocolException e) {
        //Log.trace("GA connectivity had a problem or the connectivity was aborted.  "+e.toString());
      } catch (IOException e) {
        //Log.trace("GA connectivity suffered a protocol error.  "+e.toString());
      }

      //Log.debug("GA response: " +httpResponse.toString());
      response.setStatusCode(httpResponse.getStatusLine().getStatusCode());
      response.setPostedParms(postParms);

      try {
        EntityUtils.consume(httpResponse.getEntity());
      } catch (IOException e) {/*consume quietly*/}

      if (config.isGatherStats()) {
        gatherStats(request);
      }

    } catch (Exception e) {
      if (e instanceof UnknownHostException) {
        //Log.trace("Coudln't connect to GA. Internet may not be available. " + e.toString());
      } else {
        //Log.trace("Exception while sending the GA tracker request: " + request +".  "+ e.toString());
      }
    }

    return response;
  }

  //@SuppressWarnings({ "rawtypes", "unchecked" })
  private void processParameters(GoogleAnalyticsRequest request, List<NameValuePair> postParms) {
    Map<GoogleAnalyticsParameter, String> requestParms = request.getParameters();
    Map<GoogleAnalyticsParameter, String> defaultParms = defaultRequest.getParameters();
    for (GoogleAnalyticsParameter parm : defaultParms.keySet()) {
      String value = requestParms.get(parm);
      String defaultValue = defaultParms.get(parm);
      if (isEmpty(value) && !isEmpty(defaultValue)) {
        requestParms.put(parm, defaultValue);
      }
    }
    for (GoogleAnalyticsParameter key : requestParms.keySet()) {
      postParms.add(new BasicNameValuePair(key.getParameterName(), requestParms.get(key)));
    }
  }

  /**
   * Processes the custom dimensions and adds the values to list of parameters, which would be posted to GA.
   *
   * @param request
   * @param postParms
   */
  private void processCustomDimensionParameters(@SuppressWarnings("rawtypes") GoogleAnalyticsRequest request, List<NameValuePair> postParms) {
    Map<String, String> customDimParms = new HashMap<String, String>();
    for (String defaultCustomDimKey : defaultRequest.customDimentions().keySet()) {
      customDimParms.put(defaultCustomDimKey, defaultRequest.customDimentions().get(defaultCustomDimKey));
    }

    @SuppressWarnings("unchecked")
    Map<String, String> requestCustomDims = request.customDimentions();
    for (String requestCustomDimKey : requestCustomDims.keySet()) {
      customDimParms.put(requestCustomDimKey, requestCustomDims.get(requestCustomDimKey));
    }

    for (String key : customDimParms.keySet()) {
      postParms.add(new BasicNameValuePair(key, customDimParms.get(key)));
    }
  }

  /**
   * Processes the custom metrics and adds the values to list of parameters, which would be posted to GA.
   *
   * @param request
   * @param postParms
   */
  private void processCustomMetricParameters(@SuppressWarnings("rawtypes") GoogleAnalyticsRequest request, List<NameValuePair> postParms) {
    Map<String, String> customMetricParms = new HashMap<String, String>();
    for (String defaultCustomMetricKey : defaultRequest.custommMetrics().keySet()) {
      customMetricParms.put(defaultCustomMetricKey, defaultRequest.custommMetrics().get(defaultCustomMetricKey));
    }

    @SuppressWarnings("unchecked")
    Map<String, String> requestCustomMetrics = request.custommMetrics();
    for (String requestCustomDimKey : requestCustomMetrics.keySet()) {
      customMetricParms.put(requestCustomDimKey, requestCustomMetrics.get(requestCustomDimKey));
    }

    for (String key : customMetricParms.keySet()) {
      postParms.add(new BasicNameValuePair(key, customMetricParms.get(key)));
    }
  }


  private void gatherStats(@SuppressWarnings("rawtypes") GoogleAnalyticsRequest request) {
    String hitType = request.hitType();

    if ("pageview".equalsIgnoreCase(hitType)) {
      stats.pageViewHit();

    } else if ("appview".equalsIgnoreCase(hitType)) {
      stats.appViewHit();

    } else if ("event".equalsIgnoreCase(hitType)) {
      stats.eventHit();

    } else if ("item".equalsIgnoreCase(hitType)) {
      stats.itemHit();

    } else if ("transaction".equalsIgnoreCase(hitType)) {
      stats.transactionHit();

    } else if ("social".equalsIgnoreCase(hitType)) {
      stats.socialHit();

    } else if ("timing".equalsIgnoreCase(hitType)) {
      stats.timingHit();
    }
  }

  public Future<GoogleAnalyticsResponse> postAsync(final RequestProvider requestProvider) {
    if (!config.isEnabled()) {
      return null;
    }

    Future<GoogleAnalyticsResponse> future = getExecutor().submit(new Callable<GoogleAnalyticsResponse>() {
      public GoogleAnalyticsResponse call() throws Exception {
        try {
          @SuppressWarnings("rawtypes")
          GoogleAnalyticsRequest request = requestProvider.getRequest();
          if (request != null) {
            return post(request);
          }
        } catch (Exception e) {
          //Log.trace("Request Provider (" + requestProvider + ") thrown exception " + e.toString() + " and hence nothing is posted to GA.");
        }

        return null;
      }
    });
    return future;
  }

  @SuppressWarnings("rawtypes")
  public Future<GoogleAnalyticsResponse> postAsync(final GoogleAnalyticsRequest request) {
    if (!config.isEnabled()) {
      return null;
    }

    Future<GoogleAnalyticsResponse> future = getExecutor().submit(new Callable<GoogleAnalyticsResponse>() {
      public GoogleAnalyticsResponse call() throws Exception {
        return post(request);
      }
    });
    return future;
  }

  public void close() {
    try {
      executor.shutdown();
    } catch (Exception e) {
      //ignore
    }
  }

  protected HttpClient createHttpClient(GoogleAnalyticsConfig config) {
    ThreadSafeClientConnManager connManager = new ThreadSafeClientConnManager();
    connManager.setDefaultMaxPerRoute(getDefaultMaxPerRoute(config));

    BasicHttpParams params = new BasicHttpParams();

    if (isNotEmpty(config.getUserAgent())) {
      params.setParameter(CoreProtocolPNames.USER_AGENT, config.getUserAgent());
    }

    if (isNotEmpty(config.getProxyHost())) {
      params.setParameter(ConnRoutePNames.DEFAULT_PROXY, new HttpHost(config.getProxyHost(), config.getProxyPort()));
    }

    DefaultHttpClient client = new DefaultHttpClient(connManager, params);

    if (isNotEmpty(config.getProxyUserName())) {
      BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
      credentialsProvider.setCredentials(new AuthScope(config.getProxyHost(), config.getProxyPort()),
              new UsernamePasswordCredentials(config.getProxyUserName(), config.getProxyPassword()));
      client.setCredentialsProvider(credentialsProvider);
    }

    return client;
  }

  protected int getDefaultMaxPerRoute(GoogleAnalyticsConfig config) {
    return Math.max(config.getMaxThreads(), 1);
  }

  protected ThreadPoolExecutor getExecutor() {
    if (executor == null) {
      executor = createExecutor(config);
    }
    return executor;
  }

  protected synchronized ThreadPoolExecutor createExecutor(GoogleAnalyticsConfig config) {
    return new ThreadPoolExecutor(0, config.getMaxThreads(), 5, TimeUnit.MINUTES, new LinkedBlockingDeque<Runnable>(), createThreadFactory());
  }

  protected ThreadFactory createThreadFactory() {
    return new GoogleAnalyticsThreadFactory(config.getThreadNameFormat());
  }

  public GoogleAnalyticsStats getStats() {
    return stats;
  }

  public void resetStats() {
    stats = new GoogleAnalyticsStats();
  }

  public void setEnabled(boolean b) { config.setEnabled(b);}
  public boolean getEnabled() { return config.isEnabled();}

}

class GoogleAnalyticsThreadFactory implements ThreadFactory {
  private final AtomicInteger threadNumber = new AtomicInteger(1);
  private String threadNameFormat = null;

  public GoogleAnalyticsThreadFactory(String threadNameFormat) {
    this.threadNameFormat = threadNameFormat;
  }

  public Thread newThread(Runnable r) {
    Thread thread = new Thread(Thread.currentThread().getThreadGroup(), r, MessageFormat.format(threadNameFormat, threadNumber.getAndIncrement()), 0);
    thread.setDaemon(true);
    thread.setPriority(Thread.MIN_PRIORITY);
    return thread;
  }

}
