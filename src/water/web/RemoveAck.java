package water.web;

import java.util.Properties;

import water.Key;

public class RemoveAck extends H2OPage {

  @Override protected String serveImpl(Server server, Properties args, String sessionID) throws PageError {
    Key key = ServletUtil.check_key(args,"Key");
    RString response = new RString(html);
    response.replace("key",key);
    return response.toString();
  }

  @Override public String[] requiredArguments() {
    return new String[] { "Key" };
  }


  static final String html =
    "<div class='alert alert-error'>Are you sure you want to delete key <strong>%key</strong>?<br/>"
    + "There is no way back!"
    + "</div>"
    + "<div style='text-align:center'>"
    + "<a href='StoreView'><button class='btn btn-primary'>No, back to node</button></a>"
    + "&nbsp;&nbsp;&nbsp;"
    + "<a href='Remove?Key=%$key'><button class='btn btn-danger'>Yes!</button></a>"
    + "</div>"
    ;
}
