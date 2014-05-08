package water.api;

import water.AbstractBuildVersion;
import water.H2O;

/**
 * Redirect to online documentation page.
 */
public class Documentation extends HTMLOnlyRequest {
  protected String build(Response response) {
    String branchName = null;
    String buildNumber = null;
    try {
      AbstractBuildVersion abv = H2O.getBuildVersion();
      branchName = abv.branchName();
      String projectVersion = abv.projectVersion();
      buildNumber = projectVersion.split("\\.")[3];
    }
    catch (Exception xe) {}

    String url;
    if (branchName == null || buildNumber == null) {
      url = "http://docs.0xdata.com/";
    }
    else {
      url = "http://s3.amazonaws.com/h2o-release/h2o/" + branchName + "/" + buildNumber + "/docs-website/index.html";
    }

    return "<meta http-equiv=\"refresh\" content=\"0; url=" + url + "\">";
  }
}
