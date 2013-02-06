package water.web;

import java.io.File;
import java.util.Properties;

import water.Futures;
import water.Key;
import water.util.FileIntegrityChecker;

import com.google.gson.*;

public class ImportFolder extends H2OPage {
  public String importFilesHTML(FileIntegrityChecker c) {
    StringBuilder sb = new StringBuilder();
    int correct = 0;
    Futures fs = new Futures();
    for (int i = 0; i < c.size(); ++i) {
      Key k = c.importFile(i, fs);
      if (k == null) {
        sb.append(ImportFolder.error("File <strong>"+c.getFileName(i)+"</strong> does not have the same size on all nodes."));
      } else {
        RString html = new RString("File <strong>%File</strong> imported as" +
         " key <a href='/Inspect?Key=%$Key'>%Key</a>");
        html.replace("File", c.getFileName(i));
        html.replace("Key", k);
        sb.append(ImportFolder.success(html.toString()));
        ++correct;
      }
    }
    fs.blockForPending();
    return "Out of "+c.size()+" a total of "+correct+" was successfully imported to the cloud."+sb.toString();
  }

  public JsonObject importFilesJson(FileIntegrityChecker c) {
    JsonObject result = new JsonObject();
    JsonArray ok = new JsonArray();
    JsonArray failed = new JsonArray();
    int correct = 0;
    Futures fs = new Futures();
    for (int i = 0; i < c.size(); ++i) {
      Key k = c.importFile(i, fs);
      if (k == null) {
        failed.add(new JsonPrimitive(c.getFileName(i)));
      } else {
        ok.add(new JsonPrimitive(c.getFileName(i)));
        ++correct;
      }
    }
    result.add("failed",failed);
    result.add("ok",ok);
    result.addProperty("imported",correct);
    fs.blockForPending();
    return result;
  }



  FileIntegrityChecker importFolder(Properties args) throws Exception {
    String folder = args.getProperty("Folder");
     File root = new File(folder);
    if (!root.exists())
      throw new Exception("Unable to import folder "+folder+". Folder not found.");
    folder = root.getCanonicalPath();
    return FileIntegrityChecker.check(new File(folder));
  }


  @Override public JsonObject serverJson(Server server, Properties args, String sessionID) {
    try {
      return importFilesJson(importFolder(args));
    } catch (Exception e) {
      JsonObject result = new JsonObject();
      result.addProperty("Error",e.getMessage());
      return result;
    }
  }


  @Override protected String serveImpl(Server server, Properties args, String sessionID) {
    try {
      return importFilesHTML(importFolder(args));
    } catch( Exception ex ) {
      return error(ex.getMessage());
    }
  }

  @Override public String[] requiredArguments() {
    return new String[] { "Folder" };
  }
}
