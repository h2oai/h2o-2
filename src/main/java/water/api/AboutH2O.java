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
    sb.append("<div class=\"container\">");

    //sb.append("<div class=\"hero-unit\">");
    sb.append("<h1 class=\"text-center\"><u>About H<sub>2</sub>O</u></h1><br />");

      sb.append("<div class=\"row\">");
        sb.append("<div class=\"well span6 offset3\">");
          row(sb, "Build git branch", build_branch);
          row(sb, "Build git hash",   build_hash);
          row(sb, "Build git describe",build_describe);
          row(sb, "Build project version", build_project_version);
          row(sb, "Built by", build_by);
          row(sb, "Built on", build_on);
        sb.append("</div>");
      sb.append("</div>");

      sb.append("<br />");
      sb.append("<div>");
        sb.append("<p class=\"lead text-center\">Join <a href=\"https://groups.google.com/forum/#!forum/h2ostream\" target=\"_blank\">h2ostream</a>, our google group community</p>");
        sb.append("<p class=\"lead text-center\">Follow us on Twitter, <a href=\"https://twitter.com/h2oai\" target=\"_blank\">@h2oai</a></p>");
        sb.append("<p class=\"lead text-center\">Email us at <a href=\"mailto:support@h2o.ai\" target=\"_top\">support@h2o.ai</a></p>");
        sb.append("</div>");
      sb.append("</div>");

    //sb.append("</div>");
    sb.append("</div>");

    return sb.toString();
  }

  private StringBuffer row(StringBuffer sb, String c1, String c2) {
    sb.append("<div class=\"row\">");
      sb.append("<div class=\"span2\"><p class=\"text-right\"><small>").append(c1).append("</small></p></div>");
      sb.append("<div class=\"span4\"><p>").append(c2).append("</p></div>");
    return sb.append("</div>");
  }
}
