package water.api;

/**
 * Redirect to online documentation page.
 */
public class Documentation extends HTMLOnlyRequest {
  protected String build(Response response) {
    return "<meta http-equiv=\"refresh\" content=\"0; url=http://docs.0xdata.com/\">";
  }
}
