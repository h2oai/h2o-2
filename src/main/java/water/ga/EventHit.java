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

import static water.ga.GoogleAnalyticsParameter.EVENT_ACTION;
import static water.ga.GoogleAnalyticsParameter.EVENT_CATEGORY;
import static water.ga.GoogleAnalyticsParameter.EVENT_LABEL;
import static water.ga.GoogleAnalyticsParameter.EVENT_VALUE;

/**
 * GA request to track events.
 *
 * <p>For more information, see <a href="https://developers.google.com/analytics/devguides/collection/protocol/v1/parameters#events">GA Parameters Reference</a></p>
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
public class EventHit extends GoogleAnalyticsRequest<EventHit> {

  public EventHit () {
    this(null, null, null, null);
  }

  public EventHit (String eventCategory, String eventAction) {
    this(eventCategory, eventAction, null, null);
  }

  public EventHit (String eventCategory, String eventAction, String eventLabel) {
    this(eventCategory, eventAction, eventLabel, null);
  }

  public EventHit (String eventCategory, String eventAction, String eventLabel, Integer eventValue) {
    super("event");
    eventCategory(eventCategory);
    eventAction(eventAction);
    eventLabel(eventLabel);
    eventValue(eventValue);
  }

  /**
   * <h2 id="events">Event Tracking</h2>
   * <div class="ind">
   * 	<p>
   * 		Optional.
   * 	</p>
   * 	<p>Specifies the event category. Must not be empty.</p>
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
   * 				<td><code>ec</code></td>
   * 				<td>text</td>
   * 				<td><span class="none">None</span>
   * 				</td>
   * 				<td>150 Bytes
   * 				</td>
   * 				<td>event</td>
   * 			</tr>
   * 		</tbody>
   * 	</table>
   * 	<div>
   * 		Example value: <code>Category</code><br>
   * 		Example usage: <code>ec=Category</code>
   * 	</div>
   * </div>
   */
  public EventHit eventCategory(String value) {
    setString(EVENT_CATEGORY, value);
    return this;
  }
  public String eventCategory() {
    return getString(EVENT_CATEGORY);
  }

  /**
   * <div class="ind">
   * 	<p>
   * 		Optional.
   * 	</p>
   * 	<p>Specifies the event action.  Must not be empty.</p>
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
   * 				<td><code>ea</code></td>
   * 				<td>text</td>
   * 				<td><span class="none">None</span>
   * 				</td>
   * 				<td>500 Bytes
   * 				</td>
   * 				<td>event</td>
   * 			</tr>
   * 		</tbody>
   * 	</table>
   * 	<div>
   * 		Example value: <code>Action</code><br>
   * 		Example usage: <code>ea=Action</code>
   * 	</div>
   * </div>
   */
  public EventHit eventAction(String value) {
    setString(EVENT_ACTION, value);
    return this;
  }
  public String eventAction() {
    return getString(EVENT_ACTION);
  }

  /**
   * <div class="ind">
   * 	<p>
   * 		Optional.
   * 	</p>
   * 	<p>Specifies the event label.</p>
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
   * 				<td><code>el</code></td>
   * 				<td>text</td>
   * 				<td><span class="none">None</span>
   * 				</td>
   * 				<td>500 Bytes
   * 				</td>
   * 				<td>event</td>
   * 			</tr>
   * 		</tbody>
   * 	</table>
   * 	<div>
   * 		Example value: <code>Label</code><br>
   * 		Example usage: <code>el=Label</code>
   * 	</div>
   * </div>
   */
  public EventHit eventLabel(String value) {
    setString(EVENT_LABEL, value);
    return this;
  }
  public String eventLabel() {
    return getString(EVENT_LABEL);
  }

  /**
   * <div class="ind">
   * 	<p>
   * 		Optional.
   * 	</p>
   * 	<p>Specifies the event value. Values must be non-negative.</p>
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
   * 				<td><code>ev</code></td>
   * 				<td>integer</td>
   * 				<td><span class="none">None</span>
   * 				</td>
   * 				<td><span class="none">None</span>
   * 				</td>
   * 				<td>event</td>
   * 			</tr>
   * 		</tbody>
   * 	</table>
   * 	<div>
   * 		Example value: <code>55</code><br>
   * 		Example usage: <code>ev=55</code>
   * 	</div>
   * </div>
   */
  public EventHit eventValue(Integer value) {
    setInteger(EVENT_VALUE, value);
    return this;
  }
  public Integer eventValue() {
    return getInteger(EVENT_VALUE);
  }
}
