
package water.api;

public class PutFile extends HTMLOnlyRequest {
  protected String build(Response response) {
    return "<script type='text/javascript' src='jquery.fileupload/js/vendor/jquery.ui.widget.js'></script>"
    + "<script type='text/javascript' src='jquery.fileupload/js/jquery.iframe-transport.js'></script>"
    + "<script type='text/javascript' src='jquery.fileupload/js/jquery.fileupload.js'></script>"
    + "<script type='text/javascript' src='jquery.fileupload/js/main_api.js'></script>"
    + "<div class='container' style='margin: 0px auto'>"
    + "<h3>Request PutFile ( <a href='PutFile.help'>help</a> )</h3>"
    + "<p>Please specify the file to be uploaded.</p>"
    + "<form id='Fileupload' action='WWWFileUpload.html'>"
    + " <span class='btn but-success fileinput-button'>"
    + "   <i class='icon-plus icon-white'></i>"
    + "   <span>Select file...</span>"
    + "   <input type='file' name='File' id='File'>"
    + " </span>"
    + "</form>"
    + "<table class='table' style='border:0px' id='UploadTable'>"
    + "</table>"
    + "</div>";
  }
}
