package water.api;

/**
 * Redirect to online documentation page.
 */
public class Steam extends HTMLOnlyRequest {
  protected String build(Response response) {
    return "<meta http-equiv=\"refresh\" content=\"0; url=steam/index.html\">";
  }
}

