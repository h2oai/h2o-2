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

import static water.ga.GoogleAnalyticsParameter.DNS_TIME;
import static water.ga.GoogleAnalyticsParameter.PAGE_DOWNLOAD_TIME;
import static water.ga.GoogleAnalyticsParameter.PAGE_LOAD_TIME;
import static water.ga.GoogleAnalyticsParameter.REDIRECT_RESPONSE_TIME;
import static water.ga.GoogleAnalyticsParameter.SERVER_RESPONSE_TIME;
import static water.ga.GoogleAnalyticsParameter.TCP_CONNECT_TIME;
import static water.ga.GoogleAnalyticsParameter.USER_TIMING_CATEGORY;
import static water.ga.GoogleAnalyticsParameter.USER_TIMING_LABEL;
import static water.ga.GoogleAnalyticsParameter.USER_TIMING_TIME;
import static water.ga.GoogleAnalyticsParameter.USER_TIMING_VARIABLE_NAME;

/**
 * GA request to track performance timings like page load time, server response time etc.
 *
 * <p>For more information, see <a href="https://developers.google.com/analytics/devguides/collection/protocol/v1/parameters#timing">GA Parameters Reference</a></p>
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
public class TimingHit extends GoogleAnalyticsRequest<TimingHit> {
  public TimingHit() {
    super("timing");
  }

  /**
   * <div class="ind">
   * 	<p>
   * 		Optional.
   * 	</p>
   * 	<p>Specifies the user timing category.</p>
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
   * 				<td><code>utc</code></td>
   * 				<td>text</td>
   * 				<td><span class="none">None</span>
   * 				</td>
   * 				<td>150 Bytes
   * 				</td>
   * 				<td>timing</td>
   * 			</tr>
   * 		</tbody>
   * 	</table>
   * 	<div>
   * 		Example value: <code>category</code><br>
   * 		Example usage: <code>utc=category</code>
   * 	</div>
   * </div>
   */
  public TimingHit userTimingCategory(String value) {
    setString(USER_TIMING_CATEGORY, value);
    return this;
  }
  public String userTimingCategory() {
    return getString(USER_TIMING_CATEGORY);
  }

  /**
   * <div class="ind">
   * 	<p>
   * 		Optional.
   * 	</p>
   * 	<p>Specifies the user timing variable.</p>
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
   * 				<td><code>utv</code></td>
   * 				<td>text</td>
   * 				<td><span class="none">None</span>
   * 				</td>
   * 				<td>500 Bytes
   * 				</td>
   * 				<td>timing</td>
   * 			</tr>
   * 		</tbody>
   * 	</table>
   * 	<div>
   * 		Example value: <code>lookup</code><br>
   * 		Example usage: <code>utv=lookup</code>
   * 	</div>
   * </div>
   */
  public TimingHit userTimingVariableName(String value) {
    setString(USER_TIMING_VARIABLE_NAME, value);
    return this;
  }
  public String userTimingVariableName() {
    return getString(USER_TIMING_VARIABLE_NAME);
  }

  /**
   * <div class="ind">
   * 	<p>
   * 		Optional.
   * 	</p>
   * 	<p>Specifies the user timing value. The value is in milliseconds.</p>
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
   * 				<td><code>utt</code></td>
   * 				<td>integer</td>
   * 				<td><span class="none">None</span>
   * 				</td>
   * 				<td><span class="none">None</span>
   * 				</td>
   * 				<td>timing</td>
   * 			</tr>
   * 		</tbody>
   * 	</table>
   * 	<div>
   * 		Example value: <code>123</code><br>
   * 		Example usage: <code>utt=123</code>
   * 	</div>
   * </div>
   */
  public TimingHit userTimingTime(Integer value) {
    setInteger(USER_TIMING_TIME, value);
    return this;
  }
  public Integer userTimingTime() {
    return getInteger(USER_TIMING_TIME);
  }

  /**
   * <div class="ind">
   * 	<p>
   * 		Optional.
   * 	</p>
   * 	<p>Specifies the user timing label.</p>
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
   * 				<td><code>utl</code></td>
   * 				<td>text</td>
   * 				<td><span class="none">None</span>
   * 				</td>
   * 				<td>500 Bytes
   * 				</td>
   * 				<td>timing</td>
   * 			</tr>
   * 		</tbody>
   * 	</table>
   * 	<div>
   * 		Example value: <code>label</code><br>
   * 		Example usage: <code>utl=label</code>
   * 	</div>
   * </div>
   */
  public TimingHit userTimingLabel(String value) {
    setString(USER_TIMING_LABEL, value);
    return this;
  }
  public String userTimingLabel() {
    return getString(USER_TIMING_LABEL);
  }

  /**
   * <div class="ind">
   * 	<p>
   * 		Optional.
   * 	</p>
   * 	<p>Specifies the time it took for a page to load. The value is in milliseconds.</p>
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
   * 				<td><code>plt</code></td>
   * 				<td>integer</td>
   * 				<td><span class="none">None</span>
   * 				</td>
   * 				<td><span class="none">None</span>
   * 				</td>
   * 				<td>timing</td>
   * 			</tr>
   * 		</tbody>
   * 	</table>
   * 	<div>
   * 		Example value: <code>3554</code><br>
   * 		Example usage: <code>plt=3554</code>
   * 	</div>
   * </div>
   */
  public TimingHit pageLoadTime(Integer value) {
    setInteger(PAGE_LOAD_TIME, value);
    return this;
  }
  public Integer pageLoadTime() {
    return getInteger(PAGE_LOAD_TIME);
  }

  /**
   * <div class="ind">
   * 	<p>
   * 		Optional.
   * 	</p>
   * 	<p>Specifies the time it took to do a DNS lookup.The value is in milliseconds.</p>
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
   * 				<td><code>dns</code></td>
   * 				<td>integer</td>
   * 				<td><span class="none">None</span>
   * 				</td>
   * 				<td><span class="none">None</span>
   * 				</td>
   * 				<td>timing</td>
   * 			</tr>
   * 		</tbody>
   * 	</table>
   * 	<div>
   * 		Example value: <code>43</code><br>
   * 		Example usage: <code>dns=43</code>
   * 	</div>
   * </div>
   */
  public TimingHit dnsTime(Integer value) {
    setInteger(DNS_TIME, value);
    return this;
  }
  public Integer dnsTime() {
    return getInteger(DNS_TIME);
  }

  /**
   * <div class="ind">
   * 	<p>
   * 		Optional.
   * 	</p>
   * 	<p>Specifies the time it took for the page to be downloaded. The value is in milliseconds.</p>
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
   * 				<td><code>pdt</code></td>
   * 				<td>integer</td>
   * 				<td><span class="none">None</span>
   * 				</td>
   * 				<td><span class="none">None</span>
   * 				</td>
   * 				<td>timing</td>
   * 			</tr>
   * 		</tbody>
   * 	</table>
   * 	<div>
   * 		Example value: <code>500</code><br>
   * 		Example usage: <code>pdt=500</code>
   * 	</div>
   * </div>
   */
  public TimingHit pageDownloadTime(Integer value) {
    setInteger(PAGE_DOWNLOAD_TIME, value);
    return this;
  }
  public Integer pageDownloadTime() {
    return getInteger(PAGE_DOWNLOAD_TIME);
  }

  /**
   * <div class="ind">
   * 	<p>
   * 		Optional.
   * 	</p>
   * 	<p>Specifies the time it took for any redirects to happen. The value is in milliseconds.</p>
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
   * 				<td><code>rrt</code></td>
   * 				<td>integer</td>
   * 				<td><span class="none">None</span>
   * 				</td>
   * 				<td><span class="none">None</span>
   * 				</td>
   * 				<td>timing</td>
   * 			</tr>
   * 		</tbody>
   * 	</table>
   * 	<div>
   * 		Example value: <code>500</code><br>
   * 		Example usage: <code>rrt=500</code>
   * 	</div>
   * </div>
   */
  public TimingHit redirectResponseTime(Integer value) {
    setInteger(REDIRECT_RESPONSE_TIME, value);
    return this;
  }
  public Integer redirectResponseTime() {
    return getInteger(REDIRECT_RESPONSE_TIME);
  }

  /**
   * <div class="ind">
   * 	<p>
   * 		Optional.
   * 	</p>
   * 	<p>Specifies the time it took for a TCP connection to be made. The value is in milliseconds.</p>
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
   * 				<td><code>tcp</code></td>
   * 				<td>integer</td>
   * 				<td><span class="none">None</span>
   * 				</td>
   * 				<td><span class="none">None</span>
   * 				</td>
   * 				<td>timing</td>
   * 			</tr>
   * 		</tbody>
   * 	</table>
   * 	<div>
   * 		Example value: <code>500</code><br>
   * 		Example usage: <code>tcp=500</code>
   * 	</div>
   * </div>
   */
  public TimingHit tcpConnectTime(Integer value) {
    setInteger(TCP_CONNECT_TIME, value);
    return this;
  }
  public Integer tcpConnectTime() {
    return getInteger(TCP_CONNECT_TIME);
  }

  /**
   * <div class="ind">
   * 	<p>
   * 		Optional.
   * 	</p>
   * 	<p>Specifies the time it took for the server to respond after the connect time. The value is in milliseconds.</p>
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
   * 				<td><code>srt</code></td>
   * 				<td>integer</td>
   * 				<td><span class="none">None</span>
   * 				</td>
   * 				<td><span class="none">None</span>
   * 				</td>
   * 				<td>timing</td>
   * 			</tr>
   * 		</tbody>
   * 	</table>
   * 	<div>
   * 		Example value: <code>500</code><br>
   * 		Example usage: <code>srt=500</code>
   * 	</div>
   * </div>
   */
  public TimingHit serverResponseTime(Integer value) {
    setInteger(SERVER_RESPONSE_TIME, value);
    return this;
  }
  public Integer serverResponseTime() {
    return getInteger(SERVER_RESPONSE_TIME);
  }
}
