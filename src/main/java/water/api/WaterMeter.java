package water.api;

/**
 * Redirect to water meter page.
 */
public class WaterMeter extends HTMLOnlyRequest {
  protected String build(Response response) {
    return "<meta http-equiv=\"refresh\" content=\"0; url=watermeter/index.html\">";
  }
}

