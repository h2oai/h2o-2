package water.api;


import java.io.*;

import water.Boot;
import water.exec.Function;
import water.util.RString;

import com.google.common.io.CharStreams;
import com.google.common.io.Closeables;


public class Console extends HTMLOnlyRequest {

  @Override
  protected String build(Response response) {
    RString rs = new RString(loadContent("/h2o/console.html"));
    rs.replace("HELP", getHelp());
    return rs.toString();
  }

  private String getHelp() {
    StringBuilder sb = new StringBuilder();
    sb.append("jqconsole.Write(");

    sb.append("'Access keys directly by name (for example `iris.hex`).\\n' +");
    sb.append("'Available functions are:'+");
    for(String s : Function.FUNCTIONS.keySet()) {
      sb.append("'\\n\\t").append(s).append("' +");
    }
    sb.append("'\\n', 'jqconsole-output');");
    return sb.toString();
  }

  private String loadContent(String fromFile) {
    BufferedReader reader = null;
    StringBuilder sb = new StringBuilder();
    try {
      InputStream is = Boot._init.getResource2(fromFile);
      reader = new BufferedReader(new InputStreamReader(is));
      CharStreams.copy(reader, sb);
    } catch( IOException e ){
      e.printStackTrace();
    } finally {
      Closeables.closeQuietly(reader);
    }
    return sb.toString();
  }
}
