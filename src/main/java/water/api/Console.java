package water.api;

import water.Boot;
import water.exec.ASTOp;
import water.util.RString;

public class Console extends HTMLOnlyRequest {
  @Override protected String build(Response response) {
    RString rs = new RString(Boot._init.loadContent("/h2o/console.html"));
    rs.replace("HELP", getHelp());
    return rs.toString();
  }

  private String getHelp() {
    StringBuilder sb = new StringBuilder();
    sb.append("jqconsole.Write(");
    sb.append("'Access keys directly by name (for example `iris.hex`).\\n' +");
    sb.append("'Available functions are:'+");
    for(String s : ASTOp.UNI_INFIX_OPS.keySet())
      sb.append("'\\n\\t").append(s).append("' +");
    sb.append("'\\n', 'jqconsole-output');");
    return sb.toString();
  }
}
