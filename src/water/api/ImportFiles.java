package water.api;

import water.Futures;
import water.Key;
import water.util.FileIntegrityChecker;

import com.google.gson.*;

public class ImportFiles extends Request {
  protected final ExistingFile _path = new ExistingFile(PATH);

  public ImportFiles() {
    _requestHelp = "Imports the given file or directory.  All nodes in the " +
        "cloud must have an identical copy of the files in their local " +
        "file systems.";
    _path._requestHelp = "File or directory to import.";
  }

  @Override
  protected Response serve() {
    FileIntegrityChecker c = FileIntegrityChecker.check(_path.value());

    JsonObject json = new JsonObject();

    JsonArray succ = new JsonArray();
    JsonArray fail = new JsonArray();
    Futures fs = new Futures();
    for( int i = 0; i < c.size(); ++i ) {
      Key k = c.importFile(i, fs);
      if( k == null ) {
        fail.add(new JsonPrimitive(c.getFileName(i)));
      } else {
        JsonObject o = new JsonObject();
        o.addProperty(KEY, k.toString());
        o.addProperty(FILE, c.getFileName(i));
        succ.add(o);
      }
    }
    fs.blockForPending();

    json.add(SUCCEEDED, succ);
    json.add(FAILED, fail);

    Response r = Response.done(json);
    r.setBuilder(SUCCEEDED, new ArrayBuilder() {
      @Override
      public String header(JsonArray array) {
        return "<table class='table table-striped table-bordered'>" +
            "<tr><th>File</th></tr>";
      }

      @Override
      public Builder defaultBuilder(JsonElement element) {
        return new ObjectBuilder() {
          @Override
          public String build(Response response, JsonObject object,
              String contextName) {
            return "<tr><td>" +
                "<a href='Parse.html?source_key="+object.get(KEY).getAsString()+"'>" +
                object.get(FILE).getAsString() +
                "</a></td></tr>";
          }
        };
      }
    });
    return r;
  }
}
