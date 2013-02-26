
package water.api;

import com.google.gson.JsonObject;

public class Upload extends HTMLOnlyRequest {
  protected String build(Response response) {
    return "<script type='text/javascript' src='jquery.fileupload/js/vendor/jquery.ui.widget.js'></script>"
    + "<script type='text/javascript' src='jquery.fileupload/js/jquery.iframe-transport.js'></script>"
    + "<script type='text/javascript' src='jquery.fileupload/js/jquery.fileupload.js'></script>"
    + "<script type='text/javascript' src='jquery.fileupload/js/main.js'></script>"
    + "<div class='container' style='margin: 0px auto'>"
    + "<h3>Request Upload <a href='Upload.help'><i class='icon-question-sign'></i></a></h3>"
    + "<p>Please specify the file to be uploaded.</p>"
    + "<form id='Fileupload'>"
    + " <span class='btn but-success fileinput-button'>"
    + "   <i class='icon-plus icon-white'></i>"
    + "   <span>Select file...</span>"
    + "   <input type='file'>"
    + " </span>"
    + "</form>"
    + "<table class='table' style='border:0px' id='UploadTable'>"
    + "</table>"
    + "</div>";
  }

  public static class PostFile extends JSONOnlyRequest {
    @Override protected Response serve() {
      return Response.done(new JsonObject());
    }
  }
}
