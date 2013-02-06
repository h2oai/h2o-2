package water.web;

import java.util.Properties;
//import water.hdfs.PersistHdfs;

/** Stores a key using either GET or POST method */
public class PutQuery extends H2OPage {
  static final String html =
    "<p>You may either put a value:</p>"
    + "<form class='well form-inline' action='PutValue'>"
    + " <input type='text' class='input-small span4' placeholder='value' name='Value' id='Value'>"
    + " <input type='text' class='input-small span3' placeholder='key (optional)' name='Key' id='Key' maxlength='512'>"
    + " <input type='text' class='input-small span2' placeholder='replication (optional)' name='RF' id='RF' maxlength='512'>"
    + " <button type='submit' class='btn btn-primary'>Put</button>"
    + "</form> "
    + "<p>or you may select a local file to be uploaded:"
    + "<form class='well form-inline' id='Fileupload' action=''>"
    + " <span class='btn but-success fileinput-button'>"
    + " <i class='icon-plus icon-white'></i>"
    + " <span>Select file...</span>"
    + " <input type='file' name='File' id='File'>"
    + " </span>"
    + "</form> "
    + "<table class='table' id='UploadTable'>"
/* + " <tr><td class='span2' id='filename'></td>"
+ " <td class='span2' id='filesize'></td>"
+ " <td><input type='text' class='input-small span2' placeholder='key (optional)' name='Key' id='Key' maxlength='512'></td>"
+ " <td><input type='text' class='input-small span2' placeholder='replication (optional)' name='RF' id='RF' maxlength='512'></td>"
+ " <td><div id='progress' class='progress progress-striped span6'><div class='bar' style='width: 0%;'></div></div></td>"
+ " <td><button type='submit' class='btn btn-primary' id='UploadBtn'>Upload</button></td>"
+ " </tr>"
*/
    + "</table>"
    +"<div style='display:%hdfsenabled'>"
    + "<p>or you may write the HDFS path to the file you want to import:"
    + "<form class='well form-inline' action='PutHDFS'>"
    + " %hdfsroot<input type='text' class='input-small span4' placeholder='Path (excluding server)' name='path' id='path'>"
    + " <button type='submit' class='btn btn-primary'>Import</button>"
    + "</form>"
    + "</div>"
          
    ;

  @Override
  protected String[] additionalStyles() {
    return new String[] { "jquery.fileupload/css/jquery.fileupload-ui.css", };
  }
  @Override protected String[] additionalScripts() {
    return new String[] { "jquery.fileupload/js/vendor/jquery.ui.widget.js",
                          "jquery.fileupload/js/jquery.iframe-transport.js",
                          "jquery.fileupload/js/jquery.fileupload.js",
                          "jquery.fileupload/js/main.js",
                          };
  }
  @Override protected String serveImpl(Server server, Properties args, String sessionID) {
    RString r = new RString(html);
    String hdfsroot = null; //PersistHdfs.getHDFSRoot();
    if (hdfsroot != null) {
      r.replace("hdfsroot",hdfsroot+"/");
      r.replace("hdfsenabled","block");
    } else {
      r.replace("hdfsenabled","none");
    }
    return r.toString();
  }
}
