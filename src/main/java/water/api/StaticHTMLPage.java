package water.api;

import water.Boot;

public class StaticHTMLPage extends HTMLOnlyRequest {
  private final String _html;
  private final String _href;
  public StaticHTMLPage(String file, String href) {
    _href = href;
    _html = Boot._init.loadContent(file);
  }
  @Override protected String build(Response response) {
    return _html;
  }
  @Override public String href() {
    return _href;
  }
}
