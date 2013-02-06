package water.api;

import H2OInit.Boot;

import java.io.*;

import com.google.common.io.CharStreams;
import com.google.common.io.Closeables;


public class Console extends HTMLOnlyRequest {

  @Override
  protected String build(Response response) {
    return loadContent("/h2o/console.html");
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
