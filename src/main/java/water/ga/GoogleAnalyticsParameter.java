/*
 * Licensed under the Apache License, Version 2.0 (the "License")),
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
 * Google Analytics Measurement Protocol Parameters.
 *
 * <p>For more information, see <a href="https://developers.google.com/analytics/devguides/collection/protocol/v1/parameters">GA Parameters Reference</a></p>
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
public enum GoogleAnalyticsParameter {
  //General
  PROTOCOL_VERSION("v", true),
  TRACKING_ID("tid", true),
  ANONYMIZE_IP("aip", "boolean"),
  QUEUE_TIME("qt", "integer"),
  CACHE_BUSTER("z"),
  USER_IP("uip"),
  USER_AGENT("ua"),

  //Visitor
  CLIENT_ID("cid", true),
  USER_ID("uid"),

  //Session
  SESSION_CONTROL("sc"),

  //Traffic Sources
  DOCUMENT_REFERRER("dr"),
  CAMPAIGN_NAME("cn"),
  CAMPAIGN_SOURCE("cs"),
  CAMPAIGN_MEDIUM("cm"),
  CAMPAIGN_KEYWORD("ck"),
  CAMPAIGN_CONTENT("cc"),
  CAMPAIGN_ID("ci"),
  ADWORDS_ID("gclid"),
  DISPLAY_ADS_ID("dclid"),

  //System Info
  SCREEN_RESOLUTION("sr"),
  VIEWPORT_SIZE("vp"),
  DOCUMENT_ENCODING("de"),
  SCREEN_COLORS("sd"),
  USER_LANGUAGE("ul"),
  JAVA_ENABLED("je", "boolean"),
  FLASH_VERSION("fl"),

  //Hit
  HIT_TYPE("t", true),
  NON_INTERACTION_HIT("ni"),

  //Content Information
  DOCUMENT_URL("dl"),
  DOCUMENT_HOST_NAME ("dh"),
  DOCUMENT_PATH ("dp"),
  DOCUMENT_TITLE ("dt"),
  CONTENT_DESCRIPTION ("cd"),

  //App Tracking
  APPLICATION_NAME("an"),
  APPLICATION_VERSION("av"),
  APPLICATION_ID("aid"),
  APPLICATION_INSTALLER_ID("aiid"),

  //Event Tracking
  EVENT_CATEGORY("ec", new String[] {"event"}),
  EVENT_ACTION("ea", new String[] {"event"}),
  EVENT_LABEL("el", new String[] {"event"}),
  EVENT_VALUE("ev", false, "integer", new String[] {"event"}),

  //E-Commerce
  TRANSACTION_ID("ti", new String[] {"transaction", "item"}),
  TRANSACTION_AFFILIATION("ta", new String[] {"transaction"}),
  TRANSACTION_REVENUE("tr", false, "currency", new String[] {"transaction"}),
  TRANSACTION_SHIPPING("ts", false, "currency", new String[] {"transaction"}),
  TRANSACTION_TAX("tt", false, "currency", new String[] {"transaction"}),
  ITEM_NAME("in", new String[] {"item"}),
  ITEM_PRICE("ip", false, "currency", new String[] {"item"}),
  ITEM_QUANTITY("iq", false, "integer", new String[] {"item"}),
  ITEM_CODE("ic", new String[] {"item"}),
  ITEM_CATEGORY("iv", new String[] {"item"}),
  CURRENCY_CODE("cu", new String[] {"transaction", "item"}),

  //Social Interactions
  SOCIAL_NETWORK("sn", new String[] {"social"}),
  SOCIAL_ACTION("sa", new String[] {"social"}),
  SOCIAL_ACTION_TARGET("st", new String[] {"social"}),

  //Timing
  USER_TIMING_CATEGORY("utc", new String[] {"timing"}),
  USER_TIMING_VARIABLE_NAME("utv", new String[] {"timing"}),
  USER_TIMING_TIME("utt", false, "integer", new String[] {"timing"}),
  USER_TIMING_LABEL("utl", new String[] {"timing"}),
  PAGE_LOAD_TIME("plt", false, "integer", new String[] {"timing"}),
  DNS_TIME("dns", false, "integer", new String[] {"timing"}),
  PAGE_DOWNLOAD_TIME("pdt", false, "integer", new String[] {"timing"}),
  REDIRECT_RESPONSE_TIME("rrt", false, "integer", new String[] {"timing"}),
  TCP_CONNECT_TIME("tcp", false, "integer", new String[] {"timing"}),
  SERVER_RESPONSE_TIME("srt", false, "integer", new String[] {"timing"}),

  //Exceptions
  EXCEPTION_DESCRIPTION("exd", new String[] {"exception"}),
  EXCEPTION_FATAL("exf", false, "boolean", new String[] {"exception"}),

  //Experiment Variations
  EXPERIMENT_ID("xid"),
  EXPERIMENT_VARIANT("xvar");

  private String parameterName = null;
  private boolean required = false;
  private String type = "text";
  private String[] supportedHitTypes = null;

  private GoogleAnalyticsParameter(String name) {
    this(name, false);
  }

  private GoogleAnalyticsParameter(String name, boolean required) {
    this(name, required, "text", null);
  }

  private GoogleAnalyticsParameter(String name, String type) {
    this(name, false, type, null);
  }

  private GoogleAnalyticsParameter(String name, String[] supportedHitTypes) {
    this(name, false, "text", supportedHitTypes);
  }

  private GoogleAnalyticsParameter(String name, boolean required, String type, String[] supportedHitTypes) {
    this.parameterName = name;
    this.required = required;
    this.type = type;
    this.supportedHitTypes = supportedHitTypes;
  }

  public String getParameterName() {
    return parameterName;
  }

  public String[] getSupportedHitTypes() {
    return supportedHitTypes;
  }

  public String getType() {
    return type;
  }

  public boolean isRequired() {
    return required;
  }
}