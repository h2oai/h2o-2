package water.api;

import water.AbstractBuildVersion;
import water.H2O;

/**
 * Print some information about H2O.
 */
public class AboutH2O extends HTMLOnlyRequest {
  @Override
  protected String build(Response response) {
    AbstractBuildVersion abv = H2O.getBuildVersion();
    String build_branch = abv.branchName();
    String build_hash = abv.lastCommitHash();
    String build_describe = abv.describe();
    String build_project_version = abv.projectVersion();
    String build_by = abv.compiledBy();
    String build_on = abv.compiledOn();

    StringBuffer sb = new StringBuffer();
    sb.append("<table border=\"5\" cellpadding=\"5\" align=\"center\">");
    sb.append("<tr><td>Build git branch:</td><td>" + build_branch + "</td></tr>");
    sb.append("<tr><td>Build git hash:</td><td>" + build_hash + "</td></tr>");
    sb.append("<tr><td>Build git describe:</td><td>" + build_describe + "</td></tr>");
    sb.append("<tr><td>Build project version:</td><td>" + build_project_version + "</td></tr>");
    sb.append("<tr><td>Built by:</td><td>" + "'" + build_by + "'" + "</td></tr>");
    sb.append("<tr><td>Built on:</td><td>" + "'" + build_on + "'" + "</td></tr>");
    sb.append("<tr></tr>");
    sb.append("</table>");
    sb.append("<br></br>");
    sb.append("<table border=\"5\" cellpadding=\"5\" align=\"center\" style=\"text-align:center;\">");
    sb.append("<tr><td>Join <a href=\"https://groups.google.com/forum/#!forum/h2ostream\" target=\"_blank\">h2ostream</a>, our google group community</td></tr>");
    sb.append("<tr><td>Follow us on Twitter, <a href=\"https://twitter.com/hexadata\" target=\"_blank\">@hexadata</a></td></tr>");
    sb.append("<tr><td>Email us at <a href=\"mailto:support@0xdata.com\" target=\"_top\">support@0xdata.com</a></td></tr>");
    sb.append("</table>");

    return sb.toString();
  }
}
