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


import java.util.HashMap;
import java.util.Map;

import static water.ga.GoogleAnalyticsParameter.*;

/**
 * Base GA Tracking Request containing the standard and custom parameter values.
 *
 * <p>It also provides type safe getter/setters for all parameters that are applicable
 * for all hit types. Hit specific setters/getters are available in corresponding
 * Hit specific request objects (like {@link EventHit} or {@link PageViewHit} etc)
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
@SuppressWarnings("unchecked")
public class GoogleAnalyticsRequest<T> {

  protected Map<GoogleAnalyticsParameter, String> parms = new HashMap<GoogleAnalyticsParameter, String>();
  protected Map<String, String> customDimentions = new HashMap<String, String>();
  protected Map<String, String> customMetrics = new HashMap<String, String>();

  public GoogleAnalyticsRequest() {
    this(null, null, null, null);
  }

  public GoogleAnalyticsRequest(String hitType) {
    this(hitType, null, null, null);
  }

  public GoogleAnalyticsRequest(String hitType, String trackingId, String appName, String appVersion) {
    hitType(isEmpty(hitType)?"pageview":hitType);
    trackingId(trackingId);
    applicationName(appName);
    applicationVersion(appVersion);

    protocolVersion("1");
  }

  /**
   * Sets the String value for specified parameter. If value is null, the parameter
   * is removed from the parameters list.
   *
   * @param parameter
   * @param value
   * @return
   */
  protected T setString(GoogleAnalyticsParameter parameter, String value) {
    if (value == null) {
      parms.remove(parameter);
    } else {
      String stringValue = value;
      parms.put(parameter, stringValue);
    }
    return (T) this;
  }

  protected String getString(GoogleAnalyticsParameter parameter) {
    return parms.get(parameter);
  }

  protected T setInteger(GoogleAnalyticsParameter parameter, Integer value) {
    if (value == null) {
      parms.remove(parameter);
    } else {
      String stringValue = fromInteger(value);
      parms.put(parameter, stringValue);
    }
    return (T) this;
  }

  protected Double getDouble(GoogleAnalyticsParameter parameter) {
    return toDouble(parms.get(parameter));
  }

  protected T setDouble(GoogleAnalyticsParameter parameter, Double value) {
    if (value == null) {
      parms.remove(parameter);
    } else {
      String stringValue = fromDouble(value);
      parms.put(parameter, stringValue);
    }
    return (T) this;
  }

  protected Boolean getBoolean(GoogleAnalyticsParameter parameter) {
    return toBoolean(parms.get(parameter));
  }

  protected T setBoolean(GoogleAnalyticsParameter parameter, Boolean value) {
    if (value == null) {
      parms.remove(parameter);
    } else {
      String stringValue = fromBoolean(value);
      parms.put(parameter, stringValue);
    }
    return (T) this;
  }

  protected Integer getInteger(GoogleAnalyticsParameter parameter) {
    return toInteger(parms.get(parameter));
  }

  protected String fromBoolean(Boolean booleanString) {
    if (booleanString == null) {
      return null;
    }

    return "" + booleanString;
  }

  protected Boolean toBoolean(String booleanString) {
    if (isEmpty(booleanString)) {
      return null;
    }

    return new Boolean(booleanString).booleanValue();
  }

  protected String fromInteger(Integer intValue) {
    if (intValue == null) {
      return null;
    }

    return "" + intValue;
  }

  protected Integer toInteger(String intString) {
    if (isEmpty(intString)) {
      return null;
    }

    return Integer.parseInt(intString);
  }

  protected String fromDouble(Double doubleValue) {
    if (doubleValue == null) {
      return null;
    }

    return "" + doubleValue;
  }

  protected Double toDouble(String doubleString) {
    if (isEmpty(doubleString)) {
      return null;
    }

    return Double.parseDouble(doubleString);
  }

  protected T parameter(GoogleAnalyticsParameter parameter, String value) {
    if (value == null) {
      parms.remove(parameter);
    } else {
      parms.put(parameter, value);
    }
    return (T) this;
  }

  protected String parameter(GoogleAnalyticsParameter parameter) {
    return parms.get(parameter);
  }

  public Map<GoogleAnalyticsParameter, String> getParameters() {
    return parms;
  }

  public String customDimension(int index) {
    return customDimentions.get("cd" + index);
  }

  /**
   * <div class="ind">
   * 	<p>
   * 		Optional.
   * 	</p>
   * 	<p>Each custom dimension has an associated index. There is a maximum of 20 custom dimensions (200 for Premium accounts). The name suffix must be a positive integer between 1 and 200, inclusive.</p>
   * 	<table border="1">
   * 		<tbody>
   * 			<tr>
   * 				<th>Parameter</th>
   * 				<th>Value Type</th>
   * 				<th>Default Value</th>
   * 				<th>Max Length</th>
   * 				<th>Supported Hit Types</th>
   * 			</tr>
   * 			<tr>
   * 				<td><code>cd[1-9][0-9]*</code></td>
   * 				<td>text</td>
   * 				<td><span class="none">None</span>
   * 				</td>
   * 				<td>150 Bytes
   * 				</td>
   * 				<td>all</td>
   * 			</tr>
   * 		</tbody>
   * 	</table>
   * 	<div>
   * 		Example value: <code>Sports</code><br>
   * 		Example usage: <code>cd[1-9][0-9]*=Sports</code>
   * 	</div>
   * </div>
   */
  public T customDimension(int index, String value) {
    customDimentions.put("cd" + index, value);
    return (T) this;
  }

  /**
   * <div class="ind">
   * 	<p>
   * 		Optional.
   * 	</p>
   * 	<p>Each custom metric has an associated index. There is a maximum of 20 custom metrics (200 for Premium accounts). The name suffix must be a positive integer between 1 and 200, inclusive.</p>
   * 	<table border="1">
   * 		<tbody>
   * 			<tr>
   * 				<th>Parameter</th>
   * 				<th>Value Type</th>
   * 				<th>Default Value</th>
   * 				<th>Max Length</th>
   * 				<th>Supported Hit Types</th>
   * 			</tr>
   * 			<tr>
   * 				<td><code>cm[1-9][0-9]*</code></td>
   * 				<td>integer</td>
   * 				<td><span class="none">None</span>
   * 				</td>
   * 				<td><span class="none">None</span>
   * 				</td>
   * 				<td>all</td>
   * 			</tr>
   * 		</tbody>
   * 	</table>
   * 	<div>
   * 		Example value: <code>47</code><br>
   * 		Example usage: <code>cm[1-9][0-9]*=47</code>
   * 	</div>
   * </div>
   */
  public T customMetric(int index, String value) {
    customMetrics.put("cm" + index, value);
    return (T) this;
  }

  public String customMetric(int index) {
    return customMetrics.get("cm" + index);
  }

  public Map<String, String> customDimentions() {
    return customDimentions;
  }

  public Map<String, String> custommMetrics() {
    return customMetrics;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("Request [");
    if (parms != null) {
      builder.append("parms=");
      builder.append(parms);
      builder.append(", ");
    }
    if (customDimentions != null) {
      builder.append("customDimentions=");
      builder.append(customDimentions);
      builder.append(", ");
    }
    if (customMetrics != null) {
      builder.append("customMetrics=");
      builder.append(customMetrics);
    }
    builder.append("]");
    return builder.toString();
  }

  /**
   * <div class="ind">
   * 	<p>
   * 		<strong>Required for all hit types.</strong>
   * 	</p>
   * 	<p>The Protocol version. The current value is '1'. This will only change when there are changes made that are not backwards compatible.</p>
   * 	<table border="1">
   * 		<tbody>
   * 			<tr>
   * 				<th>Parameter</th>
   * 				<th>Value Type</th>
   * 				<th>Default Value</th>
   * 				<th>Max Length</th>
   * 				<th>Supported Hit Types</th>
   * 			</tr>
   * 			<tr>
   * 				<td><code>v</code></td>
   * 				<td>text</td>
   * 				<td><span class="none">None</span>
   * 				</td>
   * 				<td><span class="none">None</span>
   * 				</td>
   * 				<td>all</td>
   * 			</tr>
   * 		</tbody>
   * 	</table>
   * 	<div>
   * 		Example value: <code>1</code><br>
   * 		Example usage: <code>v=1</code>
   * 	</div>
   * </div>
   */
  public T protocolVersion(String value) {
    setString(PROTOCOL_VERSION, value);
    return (T) this;
  }
  public String protocolVersion() {
    return getString(PROTOCOL_VERSION);
  }

  /**
   * <div class="ind">
   * 	<p>
   * 		<strong>Required for all hit types.</strong>
   * 	</p>
   * 	<p>The tracking ID / web property ID. The format is UA-XXXX-Y. All collected data is associated by this ID.</p>
   * 	<table border="1">
   * 		<tbody>
   * 			<tr>
   * 				<th>Parameter</th>
   * 				<th>Value Type</th>
   * 				<th>Default Value</th>
   * 				<th>Max Length</th>
   * 				<th>Supported Hit Types</th>
   * 			</tr>
   * 			<tr>
   * 				<td><code>tid</code></td>
   * 				<td>text</td>
   * 				<td><span class="none">None</span>
   * 				</td>
   * 				<td><span class="none">None</span>
   * 				</td>
   * 				<td>all</td>
   * 			</tr>
   * 		</tbody>
   * 	</table>
   * 	<div>
   * 		Example value: <code>UA-XXXX-Y</code><br>
   * 		Example usage: <code>tid=UA-XXXX-Y</code>
   * 	</div>
   * </div>
   */
  public T trackingId(String value) {
    setString(TRACKING_ID, value);
    return (T) this;
  }
  public String trackingId() {
    return getString(TRACKING_ID);
  }

  /**
   * <div class="ind">
   * 	<p>
   * 		Optional.
   * 	</p>
   * 	<p>When present, the IP address of the sender will be anonymized. For example, the IP will be anonymized if any of the following parameters are present in the payload: &amp;aip=, &amp;aip=0, or &amp;aip=1</p>
   * 	<table border="1">
   * 		<tbody>
   * 			<tr>
   * 				<th>Parameter</th>
   * 				<th>Value Type</th>
   * 				<th>Default Value</th>
   * 				<th>Max Length</th>
   * 				<th>Supported Hit Types</th>
   * 			</tr>
   * 			<tr>
   * 				<td><code>aip</code></td>
   * 				<td>boolean</td>
   * 				<td><span class="none">None</span>
   * 				</td>
   * 				<td><span class="none">None</span>
   * 				</td>
   * 				<td>all</td>
   * 			</tr>
   * 		</tbody>
   * 	</table>
   * 	<div>
   * 		Example value: <code>1</code><br>
   * 		Example usage: <code>aip=1</code>
   * 	</div>
   * </div>
   */
  public T anonymizeIp(Boolean value) {
    setBoolean(ANONYMIZE_IP, value);
    return (T) this;
  }
  public Boolean anonymizeIp() {
    return getBoolean(ANONYMIZE_IP);
  }

  /**
   * <div class="ind">
   * 	<p>
   * 		Optional.
   * 	</p>
   * 	<p>Used to collect offline / latent hits. The value represents the time delta (in milliseconds) between when the hit being reported occurred and the time the hit was sent. The value must be greater than or equal to 0. Values greater than four hours may lead to hits not being processed.</p>
   * 	<table border="1">
   * 		<tbody>
   * 			<tr>
   * 				<th>Parameter</th>
   * 				<th>Value Type</th>
   * 				<th>Default Value</th>
   * 				<th>Max Length</th>
   * 				<th>Supported Hit Types</th>
   * 			</tr>
   * 			<tr>
   * 				<td><code>qt</code></td>
   * 				<td>integer</td>
   * 				<td><span class="none">None</span>
   * 				</td>
   * 				<td><span class="none">None</span>
   * 				</td>
   * 				<td>all</td>
   * 			</tr>
   * 		</tbody>
   * 	</table>
   * 	<div>
   * 		Example value: <code>560</code><br>
   * 		Example usage: <code>qt=560</code>
   * 	</div>
   * </div>
   */
  public T queueTime(Integer value) {
    setInteger(QUEUE_TIME, value);
    return (T) this;
  }
  public Integer queueTime() {
    return getInteger(QUEUE_TIME);
  }

  /**
   * <div class="ind">
   * 	<p>
   * 		Optional.
   * 	</p>
   * 	<p>Used to send a random number in GET requests to ensure browsers and proxies don't cache hits. It should be sent as the final parameter of the request since we've seen some 3rd party internet filtering software add additional parameters to HTTP requests incorrectly. This value is not used in reporting.</p>
   * 	<table border="1">
   * 		<tbody>
   * 			<tr>
   * 				<th>Parameter</th>
   * 				<th>Value Type</th>
   * 				<th>Default Value</th>
   * 				<th>Max Length</th>
   * 				<th>Supported Hit Types</th>
   * 			</tr>
   * 			<tr>
   * 				<td><code>z</code></td>
   * 				<td>text</td>
   * 				<td><span class="none">None</span>
   * 				</td>
   * 				<td><span class="none">None</span>
   * 				</td>
   * 				<td>all</td>
   * 			</tr>
   * 		</tbody>
   * 	</table>
   * 	<div>
   * 		Example value: <code>289372387623</code><br>
   * 		Example usage: <code>z=289372387623</code>
   * 	</div>
   * </div>
   */
  public T cacheBuster(String value) {
    setString(CACHE_BUSTER, value);
    return (T) this;
  }
  public String cacheBuster() {
    return getString(CACHE_BUSTER);
  }

  /**
   * <div class="ind">
   * 	<p>
   * 		<strong>Required for all hit types.</strong>
   * 	</p>
   * 	<p>This anonymously identifies a particular user, device, or browser instance. For the web, this is generally stored as a first-party cookie with a two-year expiration. For mobile apps, this is randomly generated for each particular instance of an application install. The value of this field should be a random UUID (version 4) as described in http://www.ietf.org/rfc/rfc4122.txt</p>
   * 	<table border="1">
   * 		<tbody>
   * 			<tr>
   * 				<th>Parameter</th>
   * 				<th>Value Type</th>
   * 				<th>Default Value</th>
   * 				<th>Max Length</th>
   * 				<th>Supported Hit Types</th>
   * 			</tr>
   * 			<tr>
   * 				<td><code>cid</code></td>
   * 				<td>text</td>
   * 				<td><span class="none">None</span>
   * 				</td>
   * 				<td><span class="none">None</span>
   * 				</td>
   * 				<td>all</td>
   * 			</tr>
   * 		</tbody>
   * 	</table>
   * 	<div>
   * 		Example value: <code>35009a79-1a05-49d7-b876-2b884d0f825b</code><br>
   * 		Example usage: <code>cid=35009a79-1a05-49d7-b876-2b884d0f825b</code>
   * 	</div>
   * </div>
   */
  public T clientId(String value) {
    setString(CLIENT_ID, value);
    return (T) this;
  }
  public String clientId() {
    return getString(CLIENT_ID);
  }

  /**
   *<div class="ind">
   *  <strong>
   *     Optional.
   *  </strong>
   *  <p>This is intended to be a known identifier for a user provided by the site owner/tracking library user. It may not itself be PII. The value should never be persisted in GA cookies or other Analytics provided storage.</p>
   *  <table>
   *    <tbody><tr>
   *      <th>Parameter</th>
   *      <th>Value Type</th>
   *      <th>Default Value</th>
   *      <th>Max Length</th>
   *      <th>Supported Hit Types</th>
   *    </tr>
   *    <tr>
   *      <td><code>uid</code></td>
   *      <td>text</td>
   *      <td><span class="none">None</span>
   *          </td>
   *      <td><span class="none">None</span>
   *          </td>
   *      <td>all</td>
   *    </tr>
   *  </tbody></table>
   *
   *
   *  <div>
   *    Example value: <code>as8eknlll</code><br>
   *    Example usage: <code>uid=as8eknlll</code>
   *  </div>
   *
   *
   *</div>
   *
   * @param value
   * @return
   */
  public T userId(String value) {
    setString(USER_ID, value);
    return (T) this;
  }
  public String userId() {
    return getString(USER_ID);
  }

  /**
   * <div class="ind">
   * 	<p>
   * 		Optional.
   * 	</p>
   * 	<p>Used to control the session duration. A value of 'start' forces a new session to start with this hit and 'end' forces the current session to end with this hit. All other values are ignored.</p>
   * 	<table border="1">
   * 		<tbody>
   * 			<tr>
   * 				<th>Parameter</th>
   * 				<th>Value Type</th>
   * 				<th>Default Value</th>
   * 				<th>Max Length</th>
   * 				<th>Supported Hit Types</th>
   * 			</tr>
   * 			<tr>
   * 				<td><code>sc</code></td>
   * 				<td>text</td>
   * 				<td><span class="none">None</span>
   * 				</td>
   * 				<td><span class="none">None</span>
   * 				</td>
   * 				<td>all</td>
   * 			</tr>
   * 		</tbody>
   * 	</table>
   * 	<div>
   * 		Example value: <code>start</code><br>
   * 		Example usage: <code>sc=start</code>
   * 	</div>
   * 	<br>
   * 	<div>
   * 		Example value: <code>end</code><br>
   * 		Example usage: <code>sc=end</code>
   * 	</div>
   * </div>
   */
  public T sessionControl(String value) {
    setString(SESSION_CONTROL, value);
    return (T) this;
  }
  public String sessionControl() {
    return getString(SESSION_CONTROL);
  }

  /**
   * <div class="ind">
   * 	<p>
   * 		Optional.
   * 	</p>
   * 	<p>Specifies which referral source brought traffic to a website. This value is also used to compute the traffic source. The format of this value is a URL.</p>
   * 	<table border="1">
   * 		<tbody>
   * 			<tr>
   * 				<th>Parameter</th>
   * 				<th>Value Type</th>
   * 				<th>Default Value</th>
   * 				<th>Max Length</th>
   * 				<th>Supported Hit Types</th>
   * 			</tr>
   * 			<tr>
   * 				<td><code>dr</code></td>
   * 				<td>text</td>
   * 				<td><span class="none">None</span>
   * 				</td>
   * 				<td>2048 Bytes
   * 				</td>
   * 				<td>all</td>
   * 			</tr>
   * 		</tbody>
   * 	</table>
   * 	<div>
   * 		Example value: <code>http://example.com</code><br>
   * 		Example usage: <code>dr=http%3A%2F%2Fexample.com</code>
   * 	</div>
   * </div>
   */
  public T documentReferrer(String value) {
    setString(DOCUMENT_REFERRER, value);
    return (T) this;
  }
  public String documentReferrer() {
    return getString(DOCUMENT_REFERRER);
  }

  /**
   * <div class="ind">
   * 	<p>
   * 		Optional.
   * 	</p>
   * 	<p>Specifies the campaign name.</p>
   * 	<table border="1">
   * 		<tbody>
   * 			<tr>
   * 				<th>Parameter</th>
   * 				<th>Value Type</th>
   * 				<th>Default Value</th>
   * 				<th>Max Length</th>
   * 				<th>Supported Hit Types</th>
   * 			</tr>
   * 			<tr>
   * 				<td><code>cn</code></td>
   * 				<td>text</td>
   * 				<td><span class="none">None</span>
   * 				</td>
   * 				<td>100 Bytes
   * 				</td>
   * 				<td>all</td>
   * 			</tr>
   * 		</tbody>
   * 	</table>
   * 	<div>
   * 		Example value: <code>(direct)</code><br>
   * 		Example usage: <code>cn=%28direct%29</code>
   * 	</div>
   * </div>
   */
  public T campaignName(String value) {
    setString(CAMPAIGN_NAME, value);
    return (T) this;
  }
  public String campaignName() {
    return getString(CAMPAIGN_NAME);
  }

  /**
   * <div class="ind">
   * 	<p>
   * 		Optional.
   * 	</p>
   * 	<p>Specifies the campaign source.</p>
   * 	<table border="1">
   * 		<tbody>
   * 			<tr>
   * 				<th>Parameter</th>
   * 				<th>Value Type</th>
   * 				<th>Default Value</th>
   * 				<th>Max Length</th>
   * 				<th>Supported Hit Types</th>
   * 			</tr>
   * 			<tr>
   * 				<td><code>cs</code></td>
   * 				<td>text</td>
   * 				<td><span class="none">None</span>
   * 				</td>
   * 				<td>100 Bytes
   * 				</td>
   * 				<td>all</td>
   * 			</tr>
   * 		</tbody>
   * 	</table>
   * 	<div>
   * 		Example value: <code>(direct)</code><br>
   * 		Example usage: <code>cs=%28direct%29</code>
   * 	</div>
   * </div>
   */
  public T campaignSource(String value) {
    setString(CAMPAIGN_SOURCE, value);
    return (T) this;
  }
  public String campaignSource() {
    return getString(CAMPAIGN_SOURCE);
  }

  /**
   * <div class="ind">
   * 	<p>
   * 		Optional.
   * 	</p>
   * 	<p>Specifies the campaign medium.</p>
   * 	<table border="1">
   * 		<tbody>
   * 			<tr>
   * 				<th>Parameter</th>
   * 				<th>Value Type</th>
   * 				<th>Default Value</th>
   * 				<th>Max Length</th>
   * 				<th>Supported Hit Types</th>
   * 			</tr>
   * 			<tr>
   * 				<td><code>cm</code></td>
   * 				<td>text</td>
   * 				<td><span class="none">None</span>
   * 				</td>
   * 				<td>50 Bytes
   * 				</td>
   * 				<td>all</td>
   * 			</tr>
   * 		</tbody>
   * 	</table>
   * 	<div>
   * 		Example value: <code>organic</code><br>
   * 		Example usage: <code>cm=organic</code>
   * 	</div>
   * </div>
   */
  public T campaignMedium(String value) {
    setString(CAMPAIGN_MEDIUM, value);
    return (T) this;
  }
  public String campaignMedium() {
    return getString(CAMPAIGN_MEDIUM);
  }

  /**
   * <div class="ind">
   * 	<p>
   * 		Optional.
   * 	</p>
   * 	<p>Specifies the campaign keyword.</p>
   * 	<table border="1">
   * 		<tbody>
   * 			<tr>
   * 				<th>Parameter</th>
   * 				<th>Value Type</th>
   * 				<th>Default Value</th>
   * 				<th>Max Length</th>
   * 				<th>Supported Hit Types</th>
   * 			</tr>
   * 			<tr>
   * 				<td><code>ck</code></td>
   * 				<td>text</td>
   * 				<td><span class="none">None</span>
   * 				</td>
   * 				<td>500 Bytes
   * 				</td>
   * 				<td>all</td>
   * 			</tr>
   * 		</tbody>
   * 	</table>
   * 	<div>
   * 		Example value: <code>Blue Shoes</code><br>
   * 		Example usage: <code>ck=Blue%20Shoes</code>
   * 	</div>
   * </div>
   */
  public T campaignKeyword(String value) {
    setString(CAMPAIGN_KEYWORD, value);
    return (T) this;
  }
  public String campaignKeyword() {
    return getString(CAMPAIGN_KEYWORD);
  }

  /**
   * <div class="ind">
   * 	<p>
   * 		Optional.
   * 	</p>
   * 	<p>Specifies the campaign content.</p>
   * 	<table border="1">
   * 		<tbody>
   * 			<tr>
   * 				<th>Parameter</th>
   * 				<th>Value Type</th>
   * 				<th>Default Value</th>
   * 				<th>Max Length</th>
   * 				<th>Supported Hit Types</th>
   * 			</tr>
   * 			<tr>
   * 				<td><code>cc</code></td>
   * 				<td>text</td>
   * 				<td><span class="none">None</span>
   * 				</td>
   * 				<td>500 Bytes
   * 				</td>
   * 				<td>all</td>
   * 			</tr>
   * 		</tbody>
   * 	</table>
   * 	<div>
   * 		Example value: <code>content</code><br>
   * 		Example usage: <code>cc=content</code>
   * 	</div>
   * </div>
   */
  public T campaignContent(String value) {
    setString(CAMPAIGN_CONTENT, value);
    return (T) this;
  }
  public String campaignContent() {
    return getString(CAMPAIGN_CONTENT);
  }

  /**
   * <div class="ind">
   * 	<p>
   * 		Optional.
   * 	</p>
   * 	<p>Specifies the campaign ID.</p>
   * 	<table border="1">
   * 		<tbody>
   * 			<tr>
   * 				<th>Parameter</th>
   * 				<th>Value Type</th>
   * 				<th>Default Value</th>
   * 				<th>Max Length</th>
   * 				<th>Supported Hit Types</th>
   * 			</tr>
   * 			<tr>
   * 				<td><code>ci</code></td>
   * 				<td>text</td>
   * 				<td><span class="none">None</span>
   * 				</td>
   * 				<td>100 Bytes
   * 				</td>
   * 				<td>all</td>
   * 			</tr>
   * 		</tbody>
   * 	</table>
   * 	<div>
   * 		Example value: <code>ID</code><br>
   * 		Example usage: <code>ci=ID</code>
   * 	</div>
   * </div>
   */
  public T campaignId(String value) {
    setString(CAMPAIGN_ID, value);
    return (T) this;
  }
  public String campaignId() {
    return getString(CAMPAIGN_ID);
  }

  /**
   * <div class="ind">
   * 	<p>
   * 		Optional.
   * 	</p>
   * 	<p>Specifies the Google AdWords Id.</p>
   * 	<table border="1">
   * 		<tbody>
   * 			<tr>
   * 				<th>Parameter</th>
   * 				<th>Value Type</th>
   * 				<th>Default Value</th>
   * 				<th>Max Length</th>
   * 				<th>Supported Hit Types</th>
   * 			</tr>
   * 			<tr>
   * 				<td><code>gclid</code></td>
   * 				<td>text</td>
   * 				<td><span class="none">None</span>
   * 				</td>
   * 				<td><span class="none">None</span>
   * 				</td>
   * 				<td>all</td>
   * 			</tr>
   * 		</tbody>
   * 	</table>
   * 	<div>
   * 		Example value: <code>CL6Q-OXyqKUCFcgK2goddQuoHg</code><br>
   * 		Example usage: <code>gclid=CL6Q-OXyqKUCFcgK2goddQuoHg</code>
   * 	</div>
   * </div>
   */
  public T adwordsId(String value) {
    setString(ADWORDS_ID, value);
    return (T) this;
  }
  public String adwordsId() {
    return getString(ADWORDS_ID);
  }

  /**
   * <div class="ind">
   * 	<p>
   * 		Optional.
   * 	</p>
   * 	<p>Specifies the Google Display Ads Id.</p>
   * 	<table border="1">
   * 		<tbody>
   * 			<tr>
   * 				<th>Parameter</th>
   * 				<th>Value Type</th>
   * 				<th>Default Value</th>
   * 				<th>Max Length</th>
   * 				<th>Supported Hit Types</th>
   * 			</tr>
   * 			<tr>
   * 				<td><code>dclid</code></td>
   * 				<td>text</td>
   * 				<td><span class="none">None</span>
   * 				</td>
   * 				<td><span class="none">None</span>
   * 				</td>
   * 				<td>all</td>
   * 			</tr>
   * 		</tbody>
   * 	</table>
   * 	<div>
   * 		Example value: <code>d_click_id</code><br>
   * 		Example usage: <code>dclid=d_click_id</code>
   * 	</div>
   * </div>
   */
  public T displayadId(String value) {
    setString(DISPLAY_ADS_ID, value);
    return (T) this;
  }
  public String displayadId() {
    return getString(DISPLAY_ADS_ID);
  }

  /**
   * <div class="ind">
   * 	<p>
   * 		Optional.
   * 	</p>
   * 	<p>Specifies the screen resolution.</p>
   * 	<table border="1">
   * 		<tbody>
   * 			<tr>
   * 				<th>Parameter</th>
   * 				<th>Value Type</th>
   * 				<th>Default Value</th>
   * 				<th>Max Length</th>
   * 				<th>Supported Hit Types</th>
   * 			</tr>
   * 			<tr>
   * 				<td><code>sr</code></td>
   * 				<td>text</td>
   * 				<td><span class="none">None</span>
   * 				</td>
   * 				<td>20 Bytes
   * 				</td>
   * 				<td>all</td>
   * 			</tr>
   * 		</tbody>
   * 	</table>
   * 	<div>
   * 		Example value: <code>800x600</code><br>
   * 		Example usage: <code>sr=800x600</code>
   * 	</div>
   * </div>
   */
  public T screenResolution(String value) {
    setString(SCREEN_RESOLUTION, value);
    return (T) this;
  }
  public String screenResolution() {
    return getString(SCREEN_RESOLUTION);
  }

  /**
   * <div class="ind">
   * 	<p>
   * 		Optional.
   * 	</p>
   * 	<p>Specifies the viewable area of the browser / device.</p>
   * 	<table border="1">
   * 		<tbody>
   * 			<tr>
   * 				<th>Parameter</th>
   * 				<th>Value Type</th>
   * 				<th>Default Value</th>
   * 				<th>Max Length</th>
   * 				<th>Supported Hit Types</th>
   * 			</tr>
   * 			<tr>
   * 				<td><code>vp</code></td>
   * 				<td>text</td>
   * 				<td><span class="none">None</span>
   * 				</td>
   * 				<td>20 Bytes
   * 				</td>
   * 				<td>all</td>
   * 			</tr>
   * 		</tbody>
   * 	</table>
   * 	<div>
   * 		Example value: <code>123x456</code><br>
   * 		Example usage: <code>vp=123x456</code>
   * 	</div>
   * </div>
   */
  public T viewportSize(String value) {
    setString(VIEWPORT_SIZE, value);
    return (T) this;
  }
  public String viewportSize() {
    return getString(VIEWPORT_SIZE);
  }

  /**
   * <div class="ind">
   * 	<p>
   * 		Optional.
   * 	</p>
   * 	<p>Specifies the character set used to encode the page / document.</p>
   * 	<table border="1">
   * 		<tbody>
   * 			<tr>
   * 				<th>Parameter</th>
   * 				<th>Value Type</th>
   * 				<th>Default Value</th>
   * 				<th>Max Length</th>
   * 				<th>Supported Hit Types</th>
   * 			</tr>
   * 			<tr>
   * 				<td><code>de</code></td>
   * 				<td>text</td>
   * 				<td><code>UTF-8</code>
   * 				</td>
   * 				<td>20 Bytes
   * 				</td>
   * 				<td>all</td>
   * 			</tr>
   * 		</tbody>
   * 	</table>
   * 	<div>
   * 		Example value: <code>UTF-8</code><br>
   * 		Example usage: <code>de=UTF-8</code>
   * 	</div>
   * </div>
   */
  public T documentEncoding(String value) {
    setString(DOCUMENT_ENCODING, value);
    return (T) this;
  }
  public String documentEncoding() {
    return getString(DOCUMENT_ENCODING);
  }

  /**
   * <div class="ind">
   * 	<p>
   * 		Optional.
   * 	</p>
   * 	<p>Specifies the screen color depth.</p>
   * 	<table border="1">
   * 		<tbody>
   * 			<tr>
   * 				<th>Parameter</th>
   * 				<th>Value Type</th>
   * 				<th>Default Value</th>
   * 				<th>Max Length</th>
   * 				<th>Supported Hit Types</th>
   * 			</tr>
   * 			<tr>
   * 				<td><code>sd</code></td>
   * 				<td>text</td>
   * 				<td><span class="none">None</span>
   * 				</td>
   * 				<td>20 Bytes
   * 				</td>
   * 				<td>all</td>
   * 			</tr>
   * 		</tbody>
   * 	</table>
   * 	<div>
   * 		Example value: <code>24-bits</code><br>
   * 		Example usage: <code>sd=24-bits</code>
   * 	</div>
   * </div>
   */
  public T screenColors(String value) {
    setString(SCREEN_COLORS, value);
    return (T) this;
  }
  public String screenColors() {
    return getString(SCREEN_COLORS);
  }

  /**
   * <div class="ind">
   * 	<p>
   * 		Optional.
   * 	</p>
   * 	<p>Specifies the language.</p>
   * 	<table border="1">
   * 		<tbody>
   * 			<tr>
   * 				<th>Parameter</th>
   * 				<th>Value Type</th>
   * 				<th>Default Value</th>
   * 				<th>Max Length</th>
   * 				<th>Supported Hit Types</th>
   * 			</tr>
   * 			<tr>
   * 				<td><code>ul</code></td>
   * 				<td>text</td>
   * 				<td><span class="none">None</span>
   * 				</td>
   * 				<td>20 Bytes
   * 				</td>
   * 				<td>all</td>
   * 			</tr>
   * 		</tbody>
   * 	</table>
   * 	<div>
   * 		Example value: <code>en-us</code><br>
   * 		Example usage: <code>ul=en-us</code>
   * 	</div>
   * </div>
   */
  public T userLanguage(String value) {
    setString(USER_LANGUAGE, value);
    return (T) this;
  }
  public String userLanguage() {
    return getString(USER_LANGUAGE);
  }

  /**
   * <div class="ind">
   * 	<p>
   * 		Optional.
   * 	</p>
   * 	<p>Specifies whether Java was enabled.</p>
   * 	<table border="1">
   * 		<tbody>
   * 			<tr>
   * 				<th>Parameter</th>
   * 				<th>Value Type</th>
   * 				<th>Default Value</th>
   * 				<th>Max Length</th>
   * 				<th>Supported Hit Types</th>
   * 			</tr>
   * 			<tr>
   * 				<td><code>je</code></td>
   * 				<td>boolean</td>
   * 				<td><span class="none">None</span>
   * 				</td>
   * 				<td><span class="none">None</span>
   * 				</td>
   * 				<td>all</td>
   * 			</tr>
   * 		</tbody>
   * 	</table>
   * 	<div>
   * 		Example value: <code>1</code><br>
   * 		Example usage: <code>je=1</code>
   * 	</div>
   * </div>
   */
  public T javaEnabled(Boolean value) {
    setBoolean(JAVA_ENABLED, value);
    return (T) this;
  }
  public Boolean javaEnabled() {
    return getBoolean(JAVA_ENABLED);
  }

  /**
   * <div class="ind">
   * 	<p>
   * 		Optional.
   * 	</p>
   * 	<p>Specifies the flash version.</p>
   * 	<table border="1">
   * 		<tbody>
   * 			<tr>
   * 				<th>Parameter</th>
   * 				<th>Value Type</th>
   * 				<th>Default Value</th>
   * 				<th>Max Length</th>
   * 				<th>Supported Hit Types</th>
   * 			</tr>
   * 			<tr>
   * 				<td><code>fl</code></td>
   * 				<td>text</td>
   * 				<td><span class="none">None</span>
   * 				</td>
   * 				<td>20 Bytes
   * 				</td>
   * 				<td>all</td>
   * 			</tr>
   * 		</tbody>
   * 	</table>
   * 	<div>
   * 		Example value: <code>10 1 r103</code><br>
   * 		Example usage: <code>fl=10%201%20r103</code>
   * 	</div>
   * </div>
   */
  public T flashVersion(String value) {
    setString(FLASH_VERSION, value);
    return (T) this;
  }
  public String flashVersion() {
    return getString(FLASH_VERSION);
  }

  /**
   * <div class="ind">
   * 	<p>
   * 		<strong>Required for all hit types.</strong>
   * 	</p>
   * 	<p>The type of hit. Must be one of 'pageview', 'appview', 'event', 'transaction', 'item', 'social', 'exception', 'timing'.</p>
   * 	<table border="1">
   * 		<tbody>
   * 			<tr>
   * 				<th>Parameter</th>
   * 				<th>Value Type</th>
   * 				<th>Default Value</th>
   * 				<th>Max Length</th>
   * 				<th>Supported Hit Types</th>
   * 			</tr>
   * 			<tr>
   * 				<td><code>t</code></td>
   * 				<td>text</td>
   * 				<td><span class="none">None</span>
   * 				</td>
   * 				<td><span class="none">None</span>
   * 				</td>
   * 				<td>all</td>
   * 			</tr>
   * 		</tbody>
   * 	</table>
   * 	<div>
   * 		Example value: <code>pageview</code><br>
   * 		Example usage: <code>t=pageview</code>
   * 	</div>
   * </div>
   */
  public T hitType(String value) {
    setString(HIT_TYPE, value);
    return (T) this;
  }
  public String hitType() {
    return getString(HIT_TYPE);
  }

  /**
   * <div class="ind">
   * 	<p>
   * 		Optional.
   * 	</p>
   * 	<p>Specifies that a hit be considered non-interactive.</p>
   * 	<table border="1">
   * 		<tbody>
   * 			<tr>
   * 				<th>Parameter</th>
   * 				<th>Value Type</th>
   * 				<th>Default Value</th>
   * 				<th>Max Length</th>
   * 				<th>Supported Hit Types</th>
   * 			</tr>
   * 			<tr>
   * 				<td><code>ni</code></td>
   * 				<td>boolean</td>
   * 				<td><span class="none">None</span>
   * 				</td>
   * 				<td><span class="none">None</span>
   * 				</td>
   * 				<td>all</td>
   * 			</tr>
   * 		</tbody>
   * 	</table>
   * 	<div>
   * 		Example value: <code>1</code><br>
   * 		Example usage: <code>ni=1</code>
   * 	</div>
   * </div>
   */
  public T nonInteractionHit(String value) {
    setString(NON_INTERACTION_HIT, value);
    return (T) this;
  }
  public String nonInteractionHit() {
    return getString(NON_INTERACTION_HIT);
  }

  /**
   * <div class="ind">
   * 	<p>
   * 		Optional.
   * 	</p>
   * 	<p>Use this parameter to send the full URL (document location) of the page on which content resides. You can use the &amp;dh and &amp;dp parameters to override the hostname and path + query portions of the document location, accordingly. The JavaScript clients determine this parameter using the concatenation of the document.location.origin + document.location.pathname + document.location.search browser parameters. Be sure to remove any user authentication or other private information from the URL if present.</p>
   * 	<table border="1">
   * 		<tbody>
   * 			<tr>
   * 				<th>Parameter</th>
   * 				<th>Value Type</th>
   * 				<th>Default Value</th>
   * 				<th>Max Length</th>
   * 				<th>Supported Hit Types</th>
   * 			</tr>
   * 			<tr>
   * 				<td><code>dl</code></td>
   * 				<td>text</td>
   * 				<td><span class="none">None</span>
   * 				</td>
   * 				<td>2048 Bytes
   * 				</td>
   * 				<td>all</td>
   * 			</tr>
   * 		</tbody>
   * 	</table>
   * 	<div>
   * 		Example value: <code>http://foo.com/home?a=b</code><br>
   * 		Example usage: <code>dl=http%3A%2F%2Ffoo.com%2Fhome%3Fa%3Db</code>
   * 	</div>
   * </div>
   */
  public T documentUrl(String value) {
    setString(DOCUMENT_URL, value);
    return (T) this;
  }
  public String documentUrl() {
    return getString(DOCUMENT_URL);
  }

  /**
   * <div class="ind">
   * 	<p>
   * 		Optional.
   * 	</p>
   * 	<p>Specifies the hostname from which content was hosted.</p>
   * 	<table border="1">
   * 		<tbody>
   * 			<tr>
   * 				<th>Parameter</th>
   * 				<th>Value Type</th>
   * 				<th>Default Value</th>
   * 				<th>Max Length</th>
   * 				<th>Supported Hit Types</th>
   * 			</tr>
   * 			<tr>
   * 				<td><code>dh</code></td>
   * 				<td>text</td>
   * 				<td><span class="none">None</span>
   * 				</td>
   * 				<td>100 Bytes
   * 				</td>
   * 				<td>all</td>
   * 			</tr>
   * 		</tbody>
   * 	</table>
   * 	<div>
   * 		Example value: <code>foo.com</code><br>
   * 		Example usage: <code>dh=foo.com</code>
   * 	</div>
   * </div>
   */
  public T documentHostName(String value) {
    setString(DOCUMENT_HOST_NAME, value);
    return (T) this;
  }
  public String documentHostName() {
    return getString(DOCUMENT_HOST_NAME);
  }

  /**
   * <div class="ind">
   * 	<p>
   * 		Optional.
   * 	</p>
   * 	<p>The path portion of the page URL. Should begin with '/'.</p>
   * 	<table border="1">
   * 		<tbody>
   * 			<tr>
   * 				<th>Parameter</th>
   * 				<th>Value Type</th>
   * 				<th>Default Value</th>
   * 				<th>Max Length</th>
   * 				<th>Supported Hit Types</th>
   * 			</tr>
   * 			<tr>
   * 				<td><code>dp</code></td>
   * 				<td>text</td>
   * 				<td><span class="none">None</span>
   * 				</td>
   * 				<td>2048 Bytes
   * 				</td>
   * 				<td>all</td>
   * 			</tr>
   * 		</tbody>
   * 	</table>
   * 	<div>
   * 		Example value: <code>/foo</code><br>
   * 		Example usage: <code>dp=%2Ffoo</code>
   * 	</div>
   * </div>
   */
  public T documentPath(String value) {
    setString(DOCUMENT_PATH, value);
    return (T) this;
  }
  public String documentPath() {
    return getString(DOCUMENT_PATH);
  }

  /**
   * <div class="ind">
   * 	<p>
   * 		Optional.
   * 	</p>
   * 	<p>The title of the page / document.</p>
   * 	<table border="1">
   * 		<tbody>
   * 			<tr>
   * 				<th>Parameter</th>
   * 				<th>Value Type</th>
   * 				<th>Default Value</th>
   * 				<th>Max Length</th>
   * 				<th>Supported Hit Types</th>
   * 			</tr>
   * 			<tr>
   * 				<td><code>dt</code></td>
   * 				<td>text</td>
   * 				<td><span class="none">None</span>
   * 				</td>
   * 				<td>1500 Bytes
   * 				</td>
   * 				<td>all</td>
   * 			</tr>
   * 		</tbody>
   * 	</table>
   * 	<div>
   * 		Example value: <code>Settings</code><br>
   * 		Example usage: <code>dt=Settings</code>
   * 	</div>
   * </div>
   */
  public T documentTitle(String value) {
    setString(DOCUMENT_TITLE, value);
    return (T) this;
  }
  public String documentTitle() {
    return getString(DOCUMENT_TITLE);
  }

  /**
   * <div class="ind">
   * 	<p>
   * 		Optional.
   * 	</p>
   * 	<p>If not specified, this will default to the unique URL of the page by either using the &amp;dl parameter as-is or assembling it from &amp;dh and &amp;dp. App tracking makes use of this for the 'Screen Name' of the appview hit.</p>
   * 	<table border="1">
   * 		<tbody>
   * 			<tr>
   * 				<th>Parameter</th>
   * 				<th>Value Type</th>
   * 				<th>Default Value</th>
   * 				<th>Max Length</th>
   * 				<th>Supported Hit Types</th>
   * 			</tr>
   * 			<tr>
   * 				<td><code>cd</code></td>
   * 				<td>text</td>
   * 				<td><span class="none">None</span>
   * 				</td>
   * 				<td>2048 Bytes
   * 				</td>
   * 				<td>all</td>
   * 			</tr>
   * 		</tbody>
   * 	</table>
   * 	<div>
   * 		Example value: <code>High Scores</code><br>
   * 		Example usage: <code>cd=High%20Scores</code>
   * 	</div>
   * </div>
   */
  public T contentDescription(String value) {
    setString(CONTENT_DESCRIPTION, value);
    return (T) this;
  }
  public String contentDescription() {
    return getString(CONTENT_DESCRIPTION);
  }

  /**
   * <div class="ind">
   * 	<p>
   * 		Optional.
   * 	</p>
   * 	<p>Specifies the application name. Only visible in app views (profiles).</p>
   * 	<table border="1">
   * 		<tbody>
   * 			<tr>
   * 				<th>Parameter</th>
   * 				<th>Value Type</th>
   * 				<th>Default Value</th>
   * 				<th>Max Length</th>
   * 				<th>Supported Hit Types</th>
   * 			</tr>
   * 			<tr>
   * 				<td><code>an</code></td>
   * 				<td>text</td>
   * 				<td><span class="none">None</span>
   * 				</td>
   * 				<td>100 Bytes
   * 				</td>
   * 				<td>all</td>
   * 			</tr>
   * 		</tbody>
   * 	</table>
   * 	<div>
   * 		Example value: <code>My App</code><br>
   * 		Example usage: <code>an=My%20App</code>
   * 	</div>
   * </div>
   */
  public T applicationName(String value) {
    setString(APPLICATION_NAME, value);
    return (T) this;
  }
  public String applicationName() {
    return getString(APPLICATION_NAME);
  }

  /**
   * <div class="ind">
   * 	<p>
   * 		Optional.
   * 	</p>
   * 	<p>Specifies the application version. Only visible in app views (profiles).</p>
   * 	<table border="1">
   * 		<tbody>
   * 			<tr>
   * 				<th>Parameter</th>
   * 				<th>Value Type</th>
   * 				<th>Default Value</th>
   * 				<th>Max Length</th>
   * 				<th>Supported Hit Types</th>
   * 			</tr>
   * 			<tr>
   * 				<td><code>av</code></td>
   * 				<td>text</td>
   * 				<td><span class="none">None</span>
   * 				</td>
   * 				<td>100 Bytes
   * 				</td>
   * 				<td>all</td>
   * 			</tr>
   * 		</tbody>
   * 	</table>
   * 	<div>
   * 		Example value: <code>1.2</code><br>
   * 		Example usage: <code>av=1.2</code>
   * 	</div>
   * </div>
   */
  public T applicationVersion(String value) {
    setString(APPLICATION_VERSION, value);
    return (T) this;
  }
  public String applicationVersion() {
    return getString(APPLICATION_VERSION);
  }
  /**
   * <div class="ind">
   * 	<p>
   * 		Optional.
   * 	</p>
   * 	<p>Specifies the application ID. Only visible in app views (profiles).</p>
   * 	<table border="1">
   * 		<tbody>
   * 			<tr>
   * 				<th>Parameter</th>
   * 				<th>Value Type</th>
   * 				<th>Default Value</th>
   * 				<th>Max Length</th>
   * 				<th>Supported Hit Types</th>
   * 			</tr>
   * 			<tr>
   * 				<td><code>aid</code></td>
   * 				<td>text</td>
   * 				<td><span class="none">None</span>
   * 				</td>
   * 				<td>150 Bytes
   * 				</td>
   * 				<td>all</td>
   * 			</tr>
   * 		</tbody>
   * 	</table>
   * 	<div>
   * 		Example value: <code>1.2</code><br>
   * 		Example usage: <code>aid=1.2</code>
   * 	</div>
   * </div>
   */
  public T applicationID(String value) {
    setString(APPLICATION_INSTALLER_ID, value);
    return (T) this;
  }
  public String applicationID() {
    return getString(APPLICATION_ID);
  }
  /**
   * <div class="ind">
   * 	<p>
   * 		Optional.
   * 	</p>
   * 	<p>Specifies the application installer ID. Only visible in app views (profiles).</p>
   * 	<table border="1">
   * 		<tbody>
   * 			<tr>
   * 				<th>Parameter</th>
   * 				<th>Value Type</th>
   * 				<th>Default Value</th>
   * 				<th>Max Length</th>
   * 				<th>Supported Hit Types</th>
   * 			</tr>
   * 			<tr>
   * 				<td><code>aiid</code></td>
   * 				<td>text</td>
   * 				<td><span class="none">None</span>
   * 				</td>
   * 				<td>150 Bytes
   * 				</td>
   * 				<td>all</td>
   * 			</tr>
   * 		</tbody>
   * 	</table>
   * 	<div>
   * 		Example value: <code>1.2</code><br>
   * 		Example usage: <code>aiid=1.2</code>
   * 	</div>
   * </div>
   */
  public T applicationInstallerID(String value) {
    setString(APPLICATION_INSTALLER_ID, value);
    return (T) this;
  }
  public String applicationInstallerID() {
    return getString(APPLICATION_INSTALLER_ID);
  }
  /**
   * <div class="ind">
   *   <p>
   *      Optional.
   *
   *   </p>
   *   <p>This parameter specifies that this visitor has been exposed to an experiment with the given ID. It should be sent in conjunction with the Experiment Variant parameter.</p>
   *   <table>
   *     <tbody><tr>
   *       <th>Parameter</th>
   *       <th>Value Type</th>
   *       <th>Default Value</th>
   *       <th>Max Length</th>
   *       <th>Supported Hit Types</th>
   *     </tr>
   *     <tr>
   *       <td><code>xid</code></td>
   *       <td>text</td>
   *       <td><span class="none">None</span>
   *           </td>
   *       <td>40 Bytes
   *           </td>
   *       <td>all</td>
   *     </tr>
   *   </tbody></table>
   *
   *
   *   <div>
   *     Example value: <code>Qp0gahJ3RAO3DJ18b0XoUQ</code><br>
   *     Example usage: <code>xid=Qp0gahJ3RAO3DJ18b0XoUQ</code>
   *   </div>
   * </div>
   */
  public T expirementId(String value) {
    setString(EXPERIMENT_ID, value);
    return (T) this;
  }
  public String expirementId() {
    return getString(EXPERIMENT_ID);
  }

  /**
   *<div class="ind">
   *  <p>
   *     Optional.
   *
   *  </p>
   *  <p>This parameter specifies that this visitor has been exposed to a particular variation of an experiment. It should be sent in conjunction with the Experiment ID parameter.</p>
   *  <table>
   *    <tbody><tr>
   *      <th>Parameter</th>
   *      <th>Value Type</th>
   *      <th>Default Value</th>
   *      <th>Max Length</th>
   *      <th>Supported Hit Types</th>
   *    </tr>
   *    <tr>
   *      <td><code>xvar</code></td>
   *      <td>text</td>
   *      <td><span class="none">None</span>
   *          </td>
   *      <td><span class="none">None</span>
   *          </td>
   *      <td>all</td>
   *    </tr>
   *  </tbody></table>
   *
   *  <div>
   *    Example value: <code>1</code><br>
   *    Example usage: <code>xvar=1</code>
   *  </div>
   *</div>
   */
  public T expirementVariant(String value) {
    setString(EXPERIMENT_VARIANT, value);
    return (T) this;
  }
  public String expirementVariant() {
    return getString(EXPERIMENT_VARIANT);
  }

  /**
   * IP Override
   * parameter: uip
   * Should be a valid IP address. This will always be anonymized just as though &aip (anonymize IP) had been used.
   * example: &uip=1.2.3.4
   */
  public T userIp(String value) {
    setString(GoogleAnalyticsParameter.USER_IP, value);
    return (T) this;
  }
  public String userIp() {
    return getString(GoogleAnalyticsParameter.USER_IP);
  }

  /**
   * User Agent Override
   * parameter: &ua
   * Should be a User Agent reported by the browser. Note: We have libraries to identify real user agents. Hand crafting your own agent could break at any time.
   * example: &ua=Opera%2F9.80%20(Windows%20NT%206.0)%20Presto%2F2.12.388%20Version%2F12.14
   */
  public T userAgent(String value) {
    setString(GoogleAnalyticsParameter.USER_AGENT, value);
    return (T) this;
  }
  public String userAgent() {
    return getString(GoogleAnalyticsParameter.USER_AGENT);
  }

  protected boolean isEmpty(String string) {
    return string == null || string.trim().length() == 0;
  }

}
