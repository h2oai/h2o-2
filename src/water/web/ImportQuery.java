package water.web;

import java.util.Properties;

public class ImportQuery extends H2OPage {

  private static final String html =
    "<p>Specify a folder whose files should be imported as "
    + "keys to H2O.  Please note that the folder must be local to all nodes "
    + "and the path needs to be absolute."
    + "<form class='well form-inline' action='ImportFolder'>"
    + "  <input type='text' class='input-small span8' placeholder='folder' name='Folder' id='Folder'>"
    + "  <button type='submit' class='btn btn-primary'>Import Folder</button><br /><br/>"
    + "  <input type='text' class='input-small span2' placeholder='replication (optional)' name='RF' id='RF' maxlength='512'>"
    + "  <input style='display:none' type='checkbox' class='input-small offset8' name='R' id='R'> import files recursively </input>"
    + "</form> "
    + "<p>Alternatively you can specify a URL to import from provided that "
    + "the node you are connected to can reach it:"
    + "<form class='well form-inline' action='ImportUrl'>"
    + " <input type='url' class='input-small span4' placeholder='url' name='Url' id='Url'>"
    + " <input type='text' class='input-small span3' placeholder='key (optional)' name='Key' id='Key' maxlength='512'>"
    + " <button type='submit' class='btn btn-primary'>Import URL</button>"
    + "</form> "
    ;

  @Override protected String serveImpl(Server server, Properties args, String sessionID) {
    return html;
  }
}
