package water.api;

import water.AbstractBuildVersion;
import water.H2O;

/**
 * Redirect to online documentation page.
 */
public class Documentation extends HTMLOnlyRequest {
  protected String build(Response response) {
    AbstractBuildVersion abv = H2O.getBuildVersion();
    String branchName = abv.branchName();
    String buildNumber = abv.buildNumber();
    String url = "http://s3.amazonaws.com/h2o-release/h2o/" + branchName + "/" + buildNumber + "/docs-website/index.html";
    return "<meta http-equiv=\"refresh\" content=\"0; url=" + url + "\">";
  }
}
