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
import java.util.List;
import java.util.Map;

import org.apache.http.NameValuePair;

/**
 * Response for GA tracking request.
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
public class GoogleAnalyticsResponse {
  private int statusCode = 999;
  private List<NameValuePair> postedParms = null;

  public Map<String, String> getPostedParmsAsMap() {
    if (postedParms == null) {
      return null;
    }

    Map<String, String> paramsMap = new HashMap<String, String>();
    for (NameValuePair pair : postedParms) {
      paramsMap.put(pair.getName(), pair.getValue());
    }

    return paramsMap;
  }

  public List<NameValuePair> getPostedParms() {
    return postedParms;
  }

  public void setPostedParms(List<NameValuePair> postedParms) {
    this.postedParms = postedParms;
  }

  public void setStatusCode(int statusCode) {
    this.statusCode = statusCode;
  }

  public int getStatusCode() {
    return statusCode;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("Response [statusCode=");
    builder.append(statusCode);
    builder.append("]");
    return builder.toString();
  }
}
